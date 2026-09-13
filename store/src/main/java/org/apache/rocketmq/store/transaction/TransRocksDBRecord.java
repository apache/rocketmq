/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.transaction;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Represents a single transaction record stored in the RocksDB trans column family.
 *
 * <p><b>Key format:</b> {@code offsetPy + "@" + topic + "@" + uniqKey}<br>
 * <b>Value format:</b> {@code [checkTimes (int) | sizePy (int)]} — 8 bytes total
 *
 * <p>Two record types share this structure:
 * <ul>
 *   <li><b>Half record</b> ({@code isOp=false}): persisted as a RocksDB put with
 *   the full value. Used to track a pending transaction for status checking.</li>
 *   <li><b>OP record</b> ({@code isOp=true}): persisted as a RocksDB delete
 *   (tombstone). Created when a commit/rollback OP message is dispatched,
 *   erasing the corresponding half record from the column family.</li>
 * </ul>
 */
public class TransRocksDBRecord {
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    public static final int VALUE_LENGTH = Integer.BYTES + Integer.BYTES;
    private static final String KEY_SPLIT = "@";

    // CommitLog physical offset prefix — primary sort key for RocksDB ordering
    protected long offsetPy;
    // Real business topic of the transaction
    private String topic;
    // Unique transaction ID (UniqID / transactionId)
    private String uniqKey;
    // How many times the transaction checker has probed this record
    private int checkTimes = 0;
    // Message body size in CommitLog, used to read the original message on check
    private int sizePy;
    // True if this record came from an OP message (commit/rollback tombstone)
    private boolean isOp;
    // True if the record should be deleted after exceeding max check times
    private boolean delete;
    // Transient holder for the deserialized MessageExt, not persisted
    private MessageExt messageExt;

    /**
     * Create a half record for initial indexing from CommitLog dispatch.
     *
     * @param offsetPy  CommitLog phy offset
     * @param topic     original business topic
     * @param uniqKey   transaction ID
     * @param sizePy    message body size in CommitLog
     * @param checkTimes initial check counter (usually 0)
     */
    public TransRocksDBRecord(long offsetPy, String topic, String uniqKey, int sizePy, int checkTimes) {
        this.offsetPy = offsetPy;
        this.topic = topic;
        this.uniqKey = uniqKey;
        this.sizePy = sizePy;
        this.checkTimes = checkTimes;
    }

    /**
     * Create an OP record referencing a half record by its offsetPy.
     *
     * <p>When {@code isOp=true}, this record signals a RocksDB delete
     * (tombstone) — no value is persisted.
     */
    public TransRocksDBRecord(long offsetPy, String topic, String uniqKey, boolean isOp) {
        this.offsetPy = offsetPy;
        this.topic = topic;
        this.uniqKey = uniqKey;
        this.isOp = isOp;
    }

    public TransRocksDBRecord() {}

    /**
     * get rocksDB record key:
     * offsetPy + KEY_SPLIT + topic + KEY_SPLIT + uniqKey(transactionId)
     *
     * @return get key bytes
     */
    public byte[] getKeyBytes() {
        if (offsetPy < 0L || StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey)) {
            return null;
        }
        byte[] keySuffixBytes = (KEY_SPLIT + topic + KEY_SPLIT + uniqKey).getBytes(StandardCharsets.UTF_8);
        int keyLength = Long.BYTES + keySuffixBytes.length;
        return ByteBuffer.allocate(keyLength).putLong(offsetPy).put(keySuffixBytes).array();
    }

    /**
     * Serialize value as {@code [checkTimes (4 bytes) | sizePy (4 bytes)]}.
     * Total encoded length is {@link #VALUE_LENGTH} (8 bytes).
     *
     * @return encoded value bytes, or null if checkTimes or sizePy is invalid
     */
    public byte[] getValueBytes() {
        if (checkTimes < 0 || sizePy <= 0) {
            logError.error("TransRocksDBRecord getValueBytes error, checkTimes: {}, sizePy: {}", checkTimes, sizePy);
            return null;
        }
        return ByteBuffer.allocate(VALUE_LENGTH).putInt(checkTimes).putInt(sizePy).array();
    }

    /**
     * Deserialize a record from its RocksDB key-value pair.
     *
     * <p>Key layout: {@code [offsetPy (8 bytes)][suffix ("@" + topic + "@" + uniqKey)]}<br>
     * Value layout: {@code [checkTimes (4 bytes) | sizePy (4 bytes)]}
     */
    public static TransRocksDBRecord decode(byte[] key, byte[] value) {
        if (null == key || key.length <= Long.BYTES || null == value || value.length != VALUE_LENGTH) {
            logError.error("TransRocksDBRecord decode param error, key: {}, value: {}", key, value);
            return null;
        }
        TransRocksDBRecord transRocksDBRecord = null;
        try {
            transRocksDBRecord = new TransRocksDBRecord();
            ByteBuffer keyByteBuffer = ByteBuffer.wrap(key);
            transRocksDBRecord.setOffsetPy(keyByteBuffer.getLong());
            byte[] keySuffix = new byte[key.length - Long.BYTES];
            keyByteBuffer.get(keySuffix);
            String[] keySuffixSplit = new String(keySuffix, StandardCharsets.UTF_8).split(KEY_SPLIT);
            if (keySuffixSplit.length != 3) {
                logError.error("TransRocksDBRecord decode keySuffixSplit parse error");
                return null;
            }
            transRocksDBRecord.setTopic(keySuffixSplit[1]);
            transRocksDBRecord.setUniqKey(keySuffixSplit[2]);
            ByteBuffer valueByteBuffer = ByteBuffer.wrap(value);
            transRocksDBRecord.setCheckTimes(valueByteBuffer.getInt());
            transRocksDBRecord.setSizePy(valueByteBuffer.getInt());
        } catch (Exception e) {
            logError.error("TransRocksDBRecord decode error, valueLength: {}, error: {}", value.length, e.getMessage());
            return null;
        }
        return transRocksDBRecord;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public void setUniqKey(String uniqKey) {
        this.uniqKey = uniqKey;
    }

    public int getCheckTimes() {
        return checkTimes;
    }

    public void setCheckTimes(int checkTimes) {
        this.checkTimes = checkTimes;
    }

    public int getSizePy() {
        return sizePy;
    }

    public void setSizePy(int sizePy) {
        this.sizePy = sizePy;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public void setOffsetPy(long offsetPy) {
        this.offsetPy = offsetPy;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    public void setMessageExt(MessageExt messageExt) {
        this.messageExt = messageExt;
    }

    public boolean isOp() {
        return isOp;
    }

    public void setOp(boolean op) {
        isOp = op;
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
