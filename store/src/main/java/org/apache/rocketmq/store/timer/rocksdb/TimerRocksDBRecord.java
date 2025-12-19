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
package org.apache.rocketmq.store.timer.rocksdb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class TimerRocksDBRecord {
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    public static final byte TIMER_ROCKSDB_PUT = (byte)0;
    public static final byte TIMER_ROCKSDB_DELETE = (byte)1;
    public static final byte TIMER_ROCKSDB_UPDATE = (byte)2;
    private static final int VALUE_LENGTH = Integer.BYTES + Long.BYTES;

    private long delayTime;
    private String uniqKey;
    private int sizePy;
    private long offsetPy;
    private long queueOffset;
    private long checkPoint;
    private byte actionFlag;
    private MessageExt messageExt;

    public TimerRocksDBRecord() {}

    public TimerRocksDBRecord(long delayTime, String uniqKey, long offsetPy, int sizePy, long queueOffset, MessageExt messageExt) {
        this.delayTime = delayTime;
        this.uniqKey = uniqKey;
        this.offsetPy = offsetPy;
        this.sizePy = sizePy;
        this.messageExt = messageExt;
        this.queueOffset = queueOffset;
    }

    public byte[] getKeyBytes() {
        if (StringUtils.isEmpty(uniqKey) || delayTime <= 0L) {
            return null;
        }
        try {
            byte[] uniqKeyBytes = uniqKey.getBytes(StandardCharsets.UTF_8);
            int keyLength = Long.BYTES + uniqKeyBytes.length;
            return ByteBuffer.allocate(keyLength).putLong(delayTime).put(uniqKeyBytes).array();
        } catch (Exception e) {
            logError.error("TimerRocksDBRecord getKeyBytes error: {}", e.getMessage());
            return null;
        }
    }

    public byte[] getValueBytes() {
        if (sizePy <= 0 || offsetPy < 0L) {
            return null;
        }
        try {
            return ByteBuffer.allocate(VALUE_LENGTH).putInt(sizePy).putLong(offsetPy).array();
        } catch (Exception e) {
            logError.error("TimerRocksDBRecord getValueBytes error: {}", e.getMessage());
            return null;
        }
    }

    public static TimerRocksDBRecord decode(byte[] key, byte[] value) {
        if (null == key || key.length < Long.BYTES || null == value || value.length != VALUE_LENGTH) {
            return null;
        }
        try {
            TimerRocksDBRecord rocksDBRecord = new TimerRocksDBRecord();
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            rocksDBRecord.setDelayTime(keyBuffer.getLong());
            byte[] uniqKey = new byte[key.length - Long.BYTES];
            keyBuffer.get(uniqKey);
            rocksDBRecord.setUniqKey(new String(uniqKey, StandardCharsets.UTF_8));
            ByteBuffer valueByteBuffer = ByteBuffer.wrap(value);
            rocksDBRecord.setSizePy(valueByteBuffer.getInt());
            rocksDBRecord.setOffsetPy(valueByteBuffer.getLong());
            return rocksDBRecord;
        } catch (Exception e) {
            logError.error("TimerRocksDBRecord decode error: {}", e.getMessage());
            return null;
        }
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public void setOffsetPy(long offsetPy) {
        this.offsetPy = offsetPy;
    }

    public void setSizePy(int sizePy) {
        this.sizePy = sizePy;
    }

    public int getSizePy() {
        return sizePy;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    public void setMessageExt(MessageExt messageExt) {
        this.messageExt = messageExt;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public void setUniqKey(String uniqKey) {
        this.uniqKey = uniqKey;
    }

    public void setCheckPoint(long checkPoint) {
        this.checkPoint = checkPoint;
    }

    public long getCheckPoint() {
        return checkPoint;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public byte getActionFlag() {
        return actionFlag;
    }

    public void setActionFlag(byte actionFlag) {
        this.actionFlag = actionFlag;
    }

    @Override
    public String toString() {
        return "TimerRocksDBRecord{" +
            "delayTime=" + delayTime +
            ", uniqKey=" + uniqKey +
            ", sizePy=" + sizePy +
            ", offsetPy=" + offsetPy +
            ", queueOffset=" + queueOffset +
            ", checkPoint=" + checkPoint +
            '}';
    }

}
