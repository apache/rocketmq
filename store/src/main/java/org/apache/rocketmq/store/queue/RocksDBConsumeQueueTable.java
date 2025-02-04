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
package org.apache.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueOffsetTable.PhyAndCQOffset;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import static org.apache.rocketmq.common.config.AbstractRocksDBStorage.CTRL_0;
import static org.apache.rocketmq.common.config.AbstractRocksDBStorage.CTRL_1;
import static org.apache.rocketmq.common.config.AbstractRocksDBStorage.CTRL_2;

/**
 * We use RocksDBConsumeQueueTable to store cqUnit.
 */
public class RocksDBConsumeQueueTable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * Rocksdb ConsumeQueue's store unit. Format:
     *
     * <pre>
     * ┌─────────────────────────┬───────────┬───────────────────────┬───────────┬───────────┬───────────┬───────────────────────┐
     * │ Topic Bytes Array Size  │  CTRL_1   │   Topic Bytes Array   │  CTRL_1   │  QueueId  │  CTRL_1   │  ConsumeQueue Offset  │
     * │        (4 Bytes)        │ (1 Bytes) │       (n Bytes)       │ (1 Bytes) │ (4 Bytes) │ (1 Bytes) │     (8 Bytes)         │
     * ├─────────────────────────┴───────────┴───────────────────────┴───────────┴───────────┴───────────┴───────────────────────┤
     * │                                                    Key Unit                                                             │
     * │                                                                                                                         │
     * </pre>
     *
     * <pre>
     * ┌─────────────────────────────┬───────────────────┬──────────────────┬──────────────────┐
     * │  CommitLog Physical Offset  │      Body Size    │   Tag HashCode   │  Msg Store Time  │
     * │        (8 Bytes)            │      (4 Bytes)    │    (8 Bytes)     │    (8 Bytes)     │
     * ├─────────────────────────────┴───────────────────┴──────────────────┴──────────────────┤
     * │                                                    Value Unit                         │
     * │                                                                                       │
     * </pre>
     * ConsumeQueue's store unit. Size:
     * CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) + Msg Store Time(8) = 28 Bytes
     */
    private static final int PHY_OFFSET_OFFSET = 0;
    private static final int PHY_MSG_LEN_OFFSET = 8;
    private static final int MSG_TAG_HASHCODE_OFFSET = 12;
    private static final int MSG_STORE_TIME_SIZE_OFFSET = 20;
    public static final int CQ_UNIT_SIZE = 8 + 4 + 8 + 8;

    /**
     * ┌─────────────────────────┬───────────┬───────────┬───────────┬───────────┬───────────────────────┐
     * │ Topic Bytes Array Size  │  CTRL_1   │  CTRL_1   │  QueueId  │  CTRL_1   │  ConsumeQueue Offset  │
     * │        (4 Bytes)        │ (1 Bytes) │ (1 Bytes) │ (4 Bytes) │ (1 Bytes) │     (8 Bytes)         │
     * ├─────────────────────────┴───────────┴───────────┴───────────┴───────────┴───────────────────────┤
     */
    private static final int CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 4 + 1 + 8;

    /**
     * ┌─────────────────────────┬───────────┬───────────┬───────────┬───────────────────┐
     * │ Topic Bytes Array Size  │  CTRL_1   │  CTRL_1   │  QueueId  │  CTRL_0(CTRL_2)   │
     * │        (4 Bytes)        │ (1 Bytes) │ (1 Bytes) │ (4 Bytes) │     (1 Bytes)     │
     * ├─────────────────────────┴───────────┴───────────┴───────────┴───────────────────┤
     */
    private static final int DELETE_CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 4 + 1;

    private final ConsumeQueueRocksDBStorage rocksDBStorage;
    private final DefaultMessageStore messageStore;

    private ColumnFamilyHandle defaultCFH;

    public RocksDBConsumeQueueTable(ConsumeQueueRocksDBStorage rocksDBStorage, DefaultMessageStore messageStore) {
        this.rocksDBStorage = rocksDBStorage;
        this.messageStore = messageStore;
    }

    public void load() {
        this.defaultCFH = this.rocksDBStorage.getDefaultCFHandle();
    }

    public void buildAndPutCQByteBuffer(final Pair<ByteBuffer, ByteBuffer> cqBBPair, final DispatchEntry request,
        final WriteBatch writeBatch) throws RocksDBException {
        final ByteBuffer cqKey = cqBBPair.getObject1();
        buildCQKeyByteBuffer(cqKey, request.topic, request.queueId, request.queueOffset);

        final ByteBuffer cqValue = cqBBPair.getObject2();
        buildCQValueByteBuffer(cqValue, request.commitLogOffset, request.messageSize, request.tagCode, request.storeTimestamp);

        writeBatch.put(this.defaultCFH, cqKey, cqValue);
    }

    public ByteBuffer getCQInKV(final String topic, final int queueId, final long cqOffset) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer keyBB = buildCQKeyByteBuffer(topicBytes, queueId, cqOffset);
        byte[] value = this.rocksDBStorage.getCQ(keyBB.array());
        return (value != null) ? ByteBuffer.wrap(value) : null;
    }

    public List<ByteBuffer> rangeQuery(final String topic, final int queueId, final long startIndex, final int num) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        final List<ColumnFamilyHandle> defaultCFHList = new ArrayList<>(num);
        final ByteBuffer[] resultList = new ByteBuffer[num];
        final List<Integer> kvIndexList = new ArrayList<>(num);
        final List<byte[]> kvKeyList = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            final ByteBuffer keyBB = buildCQKeyByteBuffer(topicBytes, queueId, startIndex + i);
            kvIndexList.add(i);
            kvKeyList.add(keyBB.array());
            defaultCFHList.add(this.defaultCFH);
        }
        int keyNum = kvIndexList.size();
        if (keyNum > 0) {
            List<byte[]> kvValueList = this.rocksDBStorage.multiGet(defaultCFHList, kvKeyList);
            final int valueNum = kvValueList.size();
            if (keyNum != valueNum) {
                throw new RocksDBException("rocksdb bug, multiGet");
            }
            for (int i = 0; i < valueNum; i++) {
                byte[] value = kvValueList.get(i);
                if (value == null) {
                    continue;
                }
                ByteBuffer byteBuffer = ByteBuffer.wrap(value);
                resultList[kvIndexList.get(i)] = byteBuffer;
            }
        }

        final int resultSize = resultList.length;
        List<ByteBuffer> bbValueList = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            ByteBuffer byteBuffer = resultList[i];
            if (byteBuffer == null) {
                break;
            }
            bbValueList.add(byteBuffer);
        }
        return bbValueList;
    }

    /**
     * When topic is deleted, we clean up its CqUnit in rocksdb.
     * @param topic
     * @param queueId
     * @throws RocksDBException
     */
    public void destroyCQ(final String topic, final int queueId, WriteBatch writeBatch) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer cqStartKey = buildDeleteCQKey(true, topicBytes, queueId);
        final ByteBuffer cqEndKey = buildDeleteCQKey(false, topicBytes, queueId);

        writeBatch.deleteRange(this.defaultCFH, cqStartKey.array(), cqEndKey.array());

        log.info("Rocksdb consumeQueue table delete topic. {}, {}", topic, queueId);
    }

    public long binarySearchInCQByTime(String topic, int queueId, long high, long low, long timestamp,
        long minPhysicOffset, BoundaryType boundaryType) throws RocksDBException {
        long result = -1L;
        long targetOffset = -1L, leftOffset = -1L, rightOffset = -1L;
        long ceiling = high, floor = low;
        // Handle the following corner cases first:
        // 1. store time of (high) < timestamp
        ByteBuffer buffer = getCQInKV(topic, queueId, ceiling);
        if (buffer != null) {
            long storeTime = buffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
            if (storeTime < timestamp) {
                switch (boundaryType) {
                    case LOWER:
                        return ceiling + 1;
                    case UPPER:
                        return ceiling;
                    default:
                        log.warn("Unknown boundary type");
                        break;
                }
            }
        }
        // 2. store time of (low) > timestamp
        buffer = getCQInKV(topic, queueId, floor);
        if (buffer != null) {
            long storeTime = buffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
            if (storeTime > timestamp) {
                switch (boundaryType) {
                    case LOWER:
                        return floor;
                    case UPPER:
                        return 0;
                    default:
                        log.warn("Unknown boundary type");
                        break;
                }
            }
        }
        while (high >= low) {
            long midOffset = low + ((high - low) >>> 1);
            ByteBuffer byteBuffer = getCQInKV(topic, queueId, midOffset);
            if (byteBuffer == null) {
                ERROR_LOG.warn("binarySearchInCQByTimeStamp Failed. topic: {}, queueId: {}, timestamp: {}, result: null",
                    topic, queueId, timestamp);
                low = midOffset + 1;
                continue;
            }

            long phyOffset = byteBuffer.getLong(PHY_OFFSET_OFFSET);
            if (phyOffset < minPhysicOffset) {
                low = midOffset + 1;
                leftOffset = midOffset;
                continue;
            }
            long storeTime = byteBuffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
            if (storeTime < 0) {
                return 0;
            } else if (storeTime == timestamp) {
                targetOffset = midOffset;
                break;
            } else if (storeTime > timestamp) {
                high = midOffset - 1;
                rightOffset = midOffset;
            } else {
                low = midOffset + 1;
                leftOffset = midOffset;
            }
        }
        if (targetOffset != -1) {
            // offset next to it might also share the same store-timestamp.
            switch (boundaryType) {
                case LOWER: {
                    while (true) {
                        long nextOffset = targetOffset - 1;
                        if (nextOffset < floor) {
                            break;
                        }
                        ByteBuffer byteBuffer = getCQInKV(topic, queueId, nextOffset);
                        long storeTime = byteBuffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
                        if (storeTime != timestamp) {
                            break;
                        }
                        targetOffset = nextOffset;
                    }
                    break;
                }
                case UPPER: {
                    while (true) {
                        long nextOffset = targetOffset + 1;
                        if (nextOffset > ceiling) {
                            break;
                        }
                        ByteBuffer byteBuffer = getCQInKV(topic, queueId, nextOffset);
                        long storeTime = byteBuffer.getLong(MSG_STORE_TIME_SIZE_OFFSET);
                        if (storeTime != timestamp) {
                            break;
                        }
                        targetOffset = nextOffset;
                    }
                    break;
                }
                default: {
                    log.warn("Unknown boundary type");
                    break;
                }
            }
            result = targetOffset;
        } else {
            switch (boundaryType) {
                case LOWER: {
                    result = rightOffset;
                    break;
                }
                case UPPER: {
                    result = leftOffset;
                    break;
                }
                default: {
                    log.warn("Unknown boundary type");
                    break;
                }
            }
        }
        return result;
    }

    public PhyAndCQOffset binarySearchInCQ(String topic, int queueId, long high, long low, long targetPhyOffset,
        boolean min) throws RocksDBException {
        long resultCQOffset = -1L;
        long resultPhyOffset = -1L;
        while (high >= low) {
            long midCQOffset = low + ((high - low) >>> 1);
            ByteBuffer byteBuffer = getCQInKV(topic, queueId, midCQOffset);
            if (this.messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("binarySearchInCQ. {}, {}, {}, {}, {}", topic, queueId, midCQOffset, low, high);
            }
            if (byteBuffer == null) {
                low = midCQOffset + 1;
                continue;
            }

            final long phyOffset = byteBuffer.getLong(PHY_OFFSET_OFFSET);
            if (phyOffset == targetPhyOffset) {
                if (min) {
                    resultCQOffset =  midCQOffset;
                    resultPhyOffset = phyOffset;
                }
                break;
            } else if (phyOffset > targetPhyOffset) {
                high = midCQOffset - 1;
                if (min) {
                    resultCQOffset = midCQOffset;
                    resultPhyOffset = phyOffset;
                }
            } else {
                low = midCQOffset + 1;
                if (!min) {
                    resultCQOffset = midCQOffset;
                    resultPhyOffset = phyOffset;
                }
            }
        }
        return new PhyAndCQOffset(resultPhyOffset, resultCQOffset);
    }

    public static Pair<ByteBuffer, ByteBuffer> getCQByteBufferPair() {
        ByteBuffer cqKey = ByteBuffer.allocateDirect(RocksDBConsumeQueueStore.MAX_KEY_LEN);
        ByteBuffer cqValue = ByteBuffer.allocateDirect(CQ_UNIT_SIZE);
        return new Pair<>(cqKey, cqValue);
    }

    private ByteBuffer buildCQKeyByteBuffer(final byte[] topicBytes, final int queueId, final long cqOffset) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
        buildCQKeyByteBuffer0(byteBuffer, topicBytes, queueId, cqOffset);
        return byteBuffer;
    }

    private void buildCQKeyByteBuffer(final ByteBuffer byteBuffer, final byte[] topicBytes, final int queueId, final long cqOffset) {
        byteBuffer.position(0).limit(CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
        buildCQKeyByteBuffer0(byteBuffer, topicBytes, queueId, cqOffset);
    }

    private void buildCQKeyByteBuffer0(final ByteBuffer byteBuffer, final byte[] topicBytes, final int queueId, final long cqOffset) {
        byteBuffer.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1).putInt(queueId).put(CTRL_1).putLong(cqOffset);
        byteBuffer.flip();
    }

    private void buildCQValueByteBuffer(final ByteBuffer byteBuffer, final long phyOffset, final int msgSize, final long tagsCode, final long storeTimestamp) {
        byteBuffer.position(0).limit(CQ_UNIT_SIZE);
        buildCQValueByteBuffer0(byteBuffer, phyOffset, msgSize, tagsCode, storeTimestamp);
    }

    private void buildCQValueByteBuffer0(final ByteBuffer byteBuffer, final long phyOffset, final int msgSize,
        final long tagsCode, final long storeTimestamp) {
        byteBuffer.putLong(phyOffset).putInt(msgSize).putLong(tagsCode).putLong(storeTimestamp);
        byteBuffer.flip();
    }

    private ByteBuffer buildDeleteCQKey(final boolean start, final byte[] topicBytes, final int queueId) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(DELETE_CQ_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);

        byteBuffer.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1).putInt(queueId).put(start ? CTRL_0 : CTRL_2);
        byteBuffer.flip();
        return byteBuffer;
    }
}
