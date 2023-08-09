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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.rocksdb.ConsumeQueueRocksDBStorage;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore.CHARSET_UTF8;
import static org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore.CTRL_1;

public class RocksDBConsumeQueueOffsetTable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    private static final Logger ROCKSDB_LOG = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

    private static final byte[] MAX_BYTES = "max".getBytes(CHARSET_UTF8);
    private static final byte[] MIN_BYTES = "min".getBytes(CHARSET_UTF8);

    /**
     * Rocksdb ConsumeQueue's Offset unit. Format:
     *
     * <pre>
     * ┌─────────────────────────┬───────────┬───────────────────────┬───────────┬───────────┬───────────┬─────────────┐
     * │ Topic Bytes Array Size  │  CTRL_1   │   Topic Bytes Array   │  CTRL_1   │  Max(Min) │  CTRL_1   │   QueueId   │
     * │        (4 Bytes)        │ (1 Bytes) │       (n Bytes)       │ (1 Bytes) │ (3 Bytes) │ (1 Bytes) │  (4 Bytes)  │
     * ├─────────────────────────┴───────────┴───────────────────────┴───────────┴───────────┴───────────┴─────────────┤
     * │                                                    Key Unit                                                   │
     * │                                                                                                               │
     * </pre>
     *
     * <pre>
     * ┌─────────────────────────────┬────────────────────────┐
     * │  CommitLog Physical Offset  │   ConsumeQueue Offset  │
     * │        (8 Bytes)            │    (8 Bytes)           │
     * ├─────────────────────────────┴────────────────────────┤
     * │                     Value Unit                       │
     * │                                                      │
     * </pre>
     * ConsumeQueue's Offset unit. Size: CommitLog Physical Offset(8) + ConsumeQueue Offset(8) =  16 Bytes
     */
    private static final int OFFSET_PHY_OFFSET = 0;
    private static final int OFFSET_CQ_OFFSET = 8;
    /**
     *
     * ┌─────────────────────────┬───────────┬───────────┬───────────┬───────────┬─────────────┐
     * │ Topic Bytes Array Size  │  CTRL_1   │  CTRL_1   │  Max(Min) │  CTRL_1   │   QueueId   │
     * │        (4 Bytes)        │ (1 Bytes) │ (1 Bytes) │ (3 Bytes) │ (1 Bytes) │  (4 Bytes)  │
     * ├─────────────────────────┴───────────┴───────────┴───────────┴───────────┴─────────────┤
     */
    private static final int OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES = 4 + 1 + 1 + 3 + 1 + 4;
    private static final int OFFSET_VALUE_LENGTH = 8 + 8;

    /**
     * We use a new system topic='CHECKPOINT_TOPIC' to record the maxPhyOffset built by CQ dispatch thread.
     * @see ConsumeQueueStore#getMaxPhyOffsetInConsumeQueue(), we use it to find the maxPhyOffset built by CQ dispatch thread.
     * If we do not record the maxPhyOffset, it may take us a long time to start traversing from the head of
     * RocksDBConsumeQueueOffsetTable to find it.
     */
    private static final String MAX_PHYSICAL_OFFSET_CHECKPOINT = TopicValidator.RMQ_SYS_ROCKSDB_OFFSET_TOPIC;
    private static final byte[] MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES = MAX_PHYSICAL_OFFSET_CHECKPOINT.getBytes(CHARSET_UTF8);
    private static final int INNER_CHECKPOINT_TOPIC_LEN = OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES.length;
    private static final ByteBuffer INNER_CHECKPOINT_TOPIC = ByteBuffer.allocateDirect(INNER_CHECKPOINT_TOPIC_LEN);
    private static final byte[] MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY = new byte[INNER_CHECKPOINT_TOPIC_LEN];
    private final ByteBuffer maxPhyOffsetBB;
    static {
        buildOffsetKeyBB0(INNER_CHECKPOINT_TOPIC, MAX_PHYSICAL_OFFSET_CHECKPOINT_BYTES, 0, true);
        INNER_CHECKPOINT_TOPIC.position(0).limit(INNER_CHECKPOINT_TOPIC_LEN);
        INNER_CHECKPOINT_TOPIC.get(MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY);
    }

    private final RocksDBConsumeQueueTable rocksDBConsumeQueueTable;
    private final ConsumeQueueRocksDBStorage rocksDBStorage;
    private final DefaultMessageStore messageStore;

    private ColumnFamilyHandle offsetCFH;

    /**
     * Although we have already put max(min) consumeQueueOffset and phyicalOffset in rocksdb, we still hope to get them
     * from heap to avoid accessing rocksdb.
     * @see ConsumeQueue#getMaxPhysicOffset(), maxPhysicOffset  --> topicQueueMaxCqOffset
     * @see ConsumeQueue#getMinLogicOffset(),   minLogicOffset  --> topicQueueMinOffset
     */
    private final Map<String/* topic-queueId */, PhyAndCQOffset> topicQueueMinOffset;
    private final Map<String/* topic-queueId */, Long> topicQueueMaxCqOffset;

    public RocksDBConsumeQueueOffsetTable(RocksDBConsumeQueueTable rocksDBConsumeQueueTable,
        ConsumeQueueRocksDBStorage rocksDBStorage, DefaultMessageStore messageStore) {
        this.rocksDBConsumeQueueTable = rocksDBConsumeQueueTable;
        this.rocksDBStorage = rocksDBStorage;
        this.messageStore = messageStore;
        this.topicQueueMinOffset = new ConcurrentHashMap(1024);
        this.topicQueueMaxCqOffset = new ConcurrentHashMap(1024);

        this.maxPhyOffsetBB = ByteBuffer.allocateDirect(8);
    }

    public void load() {
        this.offsetCFH = this.rocksDBStorage.getOffsetCFHandle();
    }

    public void updateTempTopicQueueMaxOffset(final Pair<ByteBuffer, ByteBuffer> offsetBBPair,
        final byte[] topicBytes, final DispatchRequest request,
        final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap) {
        buildOffsetKeyAndValueBB(offsetBBPair, topicBytes, request);
        ByteBuffer topicQueueId = offsetBBPair.getObject1();
        ByteBuffer maxOffsetBB = offsetBBPair.getObject2();
        Pair<ByteBuffer, DispatchRequest> old = tempTopicQueueMaxOffsetMap.get(topicQueueId);
        if (old == null) {
            tempTopicQueueMaxOffsetMap.put(topicQueueId, new Pair(maxOffsetBB, request));
        } else {
            long oldMaxOffset = old.getObject1().getLong(OFFSET_CQ_OFFSET);
            long maxOffset = maxOffsetBB.getLong(OFFSET_CQ_OFFSET);
            if (maxOffset >= oldMaxOffset) {
                ERROR_LOG.error("cqOffset invalid1. old: {}, now: {}", oldMaxOffset, maxOffset);
            }
        }
    }

    public void putMaxPhyAndCqOffset(final Map<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> tempTopicQueueMaxOffsetMap,
        final WriteBatch writeBatch, final long maxPhyOffset) throws RocksDBException {
        for (Map.Entry<ByteBuffer, Pair<ByteBuffer, DispatchRequest>> entry : tempTopicQueueMaxOffsetMap.entrySet()) {
            writeBatch.put(offsetCFH, entry.getKey(), entry.getValue().getObject1());

            DispatchRequest request = entry.getValue().getObject2();
            putHeapMaxCqOffset(request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
        }

        appendMaxPhyOffset(writeBatch, maxPhyOffset);
    }

    /**
     * When topic is deleted, we clean up its offset info in rocksdb.
     * @param topic
     * @param queueId
     * @throws RocksDBException
     */
    public void destroyOffset(String topic, int queueId, WriteBatch writeBatch) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
        final ByteBuffer minOffsetKey = buildOffsetKeyBB(topicBytes, queueId, false);
        byte[] minOffsetBytes = this.rocksDBStorage.getOffset(minOffsetKey.array());
        Long startCQOffset = (minOffsetBytes != null) ? ByteBuffer.wrap(minOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

        final ByteBuffer maxOffsetKey = buildOffsetKeyBB(topicBytes, queueId, true);
        byte[] maxOffsetBytes = this.rocksDBStorage.getOffset(maxOffsetKey.array());
        Long endCQOffset = (maxOffsetBytes != null) ? ByteBuffer.wrap(maxOffsetBytes).getLong(OFFSET_CQ_OFFSET) : null;

        writeBatch.delete(offsetCFH, minOffsetKey.array());
        writeBatch.delete(offsetCFH, maxOffsetKey.array());

        String topicQueueId = buildTopicQueueId(topic, queueId);
        removeHeapMinCqOffset(topicQueueId);
        removeHeapMaxCqOffset(topicQueueId);

        log.info("RocksDB offset table delete topic: {}, queueId: {}, minOffset: {}, maxOffset: {}", topic, queueId,
            startCQOffset, endCQOffset);
    }

    private void appendMaxPhyOffset(final WriteBatch writeBatch, final long maxPhyOffset) throws RocksDBException {
        final ByteBuffer maxPhyOffsetBB = this.maxPhyOffsetBB;
        maxPhyOffsetBB.position(0).limit(8);
        maxPhyOffsetBB.putLong(maxPhyOffset);
        maxPhyOffsetBB.flip();

        INNER_CHECKPOINT_TOPIC.position(0).limit(INNER_CHECKPOINT_TOPIC_LEN);
        writeBatch.put(offsetCFH, INNER_CHECKPOINT_TOPIC, maxPhyOffsetBB);
    }

    public long getMaxPhyOffset() throws RocksDBException {
        byte[] valueBytes = this.rocksDBStorage.getOffset(MAX_PHYSICAL_OFFSET_CHECKPOINT_KEY);
        if (valueBytes == null) {
            return 0;
        }
        ByteBuffer valueBB = ByteBuffer.wrap(valueBytes);
        return valueBB.getLong(0);
    }

    /**
     * Traverse the Offset table to find dirty topic
     * @param existTopicSet
     * @return
     */
    public Map<String, Set<Integer>> iterateOffsetTable2FindDirty(final Set<String> existTopicSet) {
        Map<String/* topic */, Set<Integer/* queueId */>> topicQueueIdToBeDeletedMap = new HashMap<>();

        RocksIterator iterator = null;
        try {
            iterator = rocksDBStorage.seekOffsetCF();
            if (iterator == null) {
                return topicQueueIdToBeDeletedMap;
            }
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                if (key == null || key.length <= OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES
                    || value == null || value.length != OFFSET_VALUE_LENGTH) {
                    continue;
                }
                ByteBuffer keyBB = ByteBuffer.wrap(key);
                int topicLen = keyBB.getInt(0);
                byte[] topicBytes = new byte[topicLen];
                /**
                 * "Topic Bytes Array Size" + "CTRL_1" = 4 + 1
                 */
                keyBB.position(4 + 1);
                keyBB.get(topicBytes);
                String topic = new String(topicBytes, CHARSET_UTF8);
                if (TopicValidator.isSystemTopic(topic)) {
                    continue;
                }

                /**
                 * "Topic Bytes Array Size" + "CTRL_1" + "Topic Bytes Array" + "CTRL_1"  + "Max(min)" +
                 *  = 4 + 1 + topicLen + 1 + 3 + 1
                 */
                int queueId = keyBB.getInt(4 + 1 + topicLen + 1 + 3 + 1);

                if (!existTopicSet.contains(topic)) {
                    ByteBuffer valueBB = ByteBuffer.wrap(value);
                    long cqOffset = valueBB.getLong(OFFSET_CQ_OFFSET);

                    Set<Integer> topicQueueIdSet = topicQueueIdToBeDeletedMap.get(topic);
                    if (topicQueueIdSet == null) {
                        Set<Integer> newSet = new HashSet<>();
                        newSet.add(queueId);
                        topicQueueIdToBeDeletedMap.put(topic, newSet);
                    } else {
                        topicQueueIdSet.add(queueId);
                    }
                    ERROR_LOG.info("RocksDBConsumeQueueStore has dirty cqOffset. topic: {}, queueId: {}, cqOffset: {}",
                        topic, queueId, cqOffset);
                }
            }
        } catch (Exception e) {
            ERROR_LOG.error("iterateOffsetTable2MarkDirtyCQ Failed.", e);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return topicQueueIdToBeDeletedMap;
    }

    public Long getMaxCqOffset(String topic, int queueId) throws RocksDBException {
        Long maxCqOffset = getHeapMaxCqOffset(topic, queueId);

        if (maxCqOffset == null) {
            final ByteBuffer bb = getMaxPhyAndCqOffsetInKV(topic, queueId);
            maxCqOffset = (bb != null) ? bb.getLong(OFFSET_CQ_OFFSET) : null;
            String topicQueueId = buildTopicQueueId(topic, queueId);
            this.topicQueueMaxCqOffset.putIfAbsent(topicQueueId, maxCqOffset != null ? maxCqOffset : -1L);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateMaxOffsetInQueue. {}, {}", topicQueueId, maxCqOffset);
            }
        }

        return maxCqOffset;
    }

    /**
     * truncate dirty offset in rocksdb
     * @param offsetToTruncate
     * @throws RocksDBException
     */
    public void truncateDirty(long offsetToTruncate) throws RocksDBException {
        correctMaxPyhOffset(offsetToTruncate);

        ConcurrentMap<String, TopicConfig> allTopicConfigMap = this.messageStore.getTopicConfigs();
        if (allTopicConfigMap == null) {
            return;
        }
        for (TopicConfig topicConfig : allTopicConfigMap.values()) {
            for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                truncateDirtyOffset(topicConfig.getTopicName(), i);
            }
        }
    }

    private Pair<Boolean, Long> isMinOffsetOk(final String topic, final int queueId, final long minPhyOffset) throws RocksDBException {
        PhyAndCQOffset phyAndCQOffset = getHeapMinOffset(topic, queueId);
        if (phyAndCQOffset != null) {
            final long phyOffset = phyAndCQOffset.getPhyOffset();
            final long cqOffset = phyAndCQOffset.getCqOffset();

            return (phyOffset >= minPhyOffset) ? new Pair(true, cqOffset) : new Pair(false, cqOffset);
        }
        ByteBuffer bb = getMinPhyAndCqOffsetInKV(topic, queueId);
        if (bb == null) {
            return new Pair(false, 0L);
        }
        final long phyOffset = bb.getLong(OFFSET_PHY_OFFSET);
        final long cqOffset = bb.getLong(OFFSET_CQ_OFFSET);
        if (phyOffset >= minPhyOffset) {
            String topicQueueId = buildTopicQueueId(topic, queueId);
            PhyAndCQOffset newPhyAndCQOffset = new PhyAndCQOffset(phyOffset, cqOffset);
            this.topicQueueMinOffset.putIfAbsent(topicQueueId, newPhyAndCQOffset);
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateMinOffsetInQueue. {}, {}", topicQueueId, newPhyAndCQOffset);
            }
            return new Pair(true, cqOffset);
        }
        return new Pair(false, cqOffset);
    }

    private void truncateDirtyOffset(String topic, int queueId) throws RocksDBException {
        final ByteBuffer byteBuffer = getMaxPhyAndCqOffsetInKV(topic, queueId);
        if (byteBuffer == null) {
            return;
        }

        long maxPhyOffset = byteBuffer.getLong(OFFSET_PHY_OFFSET);
        long maxCqOffset = byteBuffer.getLong(OFFSET_CQ_OFFSET);
        long maxPhyOffsetInCQ = getMaxPhyOffset();

        if (maxPhyOffset >= maxPhyOffsetInCQ) {
            correctMaxCqOffset(topic, queueId, maxCqOffset, maxPhyOffsetInCQ);
            Long newMaxCqOffset = getHeapMaxCqOffset(topic, queueId);
            ROCKSDB_LOG.warn("truncateDirtyLogicFile topic={}, queueId={} from {} to {}", topic, queueId, maxPhyOffset, newMaxCqOffset);
        }
    }

    private void correctMaxPyhOffset(long maxPhyOffset) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }
        try {
            WriteBatch writeBatch = new WriteBatch();
            long oldMaxPhyOffset = getMaxPhyOffset();
            if (oldMaxPhyOffset <= maxPhyOffset) {
                return;
            }
            log.info("correctMaxPyhOffset, oldMaxPhyOffset={}, newMaxPhyOffset={}", oldMaxPhyOffset, maxPhyOffset);
            appendMaxPhyOffset(writeBatch, maxPhyOffset);
            this.rocksDBStorage.batchPut(writeBatch);
        } catch (RocksDBException e) {
            ERROR_LOG.error("correctMaxPyhOffset Failed.", e);
            throw e;
        } finally {
            this.rocksDBStorage.release();
        }
    }

    public long getMinCqOffset(String topic, int queueId) throws RocksDBException {
        final long minPhyOffset = this.messageStore.getMinPhyOffset();
        Pair<Boolean, Long> pair = isMinOffsetOk(topic, queueId, minPhyOffset);
        final long cqOffset = pair.getObject2();
        if (!pair.getObject1() && correctMinCqOffset(topic, queueId, cqOffset, minPhyOffset)) {
            PhyAndCQOffset phyAndCQOffset = getHeapMinOffset(topic, queueId);
            if (phyAndCQOffset != null) {
                if (this.messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                    ROCKSDB_LOG.warn("getMinOffsetInQueue miss heap. topic: {}, queueId: {}, old: {}, new: {}",
                        topic, queueId, cqOffset, phyAndCQOffset);
                }
                return phyAndCQOffset.getCqOffset();
            }
        }
        return cqOffset;
    }

    public Long getMaxPhyOffset(String topic, int queueId) {
        try {
            ByteBuffer byteBuffer = getMaxPhyAndCqOffsetInKV(topic, queueId);
            if (byteBuffer != null) {
                return byteBuffer.getLong(OFFSET_PHY_OFFSET);
            }
        } catch (Exception e) {
            ERROR_LOG.info("getMaxPhyOffset error. topic: {}, queueId: {}", topic, queueId);
        }
        return null;
    }

    private ByteBuffer getMinPhyAndCqOffsetInKV(String topic, int queueId) throws RocksDBException {
        return getPhyAndCqOffsetInKV(topic, queueId, false);
    }

    private ByteBuffer getMaxPhyAndCqOffsetInKV(String topic, int queueId) throws RocksDBException {
        return getPhyAndCqOffsetInKV(topic, queueId, true);
    }

    private ByteBuffer getPhyAndCqOffsetInKV(String topic, int queueId, boolean max) throws RocksDBException {
        final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
        final ByteBuffer keyBB = buildOffsetKeyBB(topicBytes, queueId, max);

        byte[] value =  this.rocksDBStorage.getOffset(keyBB.array());
        return (value != null) ? ByteBuffer.wrap(value) : null;
    }

    private String buildTopicQueueId(final String topic, final int queueId) {
        return topic + "-" + queueId;
    }

    private void putHeapMinCqOffset(final String topic, final int queueId, final long minPhyOffset, final long minCQOffset) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        PhyAndCQOffset phyAndCQOffset = new PhyAndCQOffset(minPhyOffset, minCQOffset);
        this.topicQueueMinOffset.put(topicQueueId, phyAndCQOffset);
    }

    private void putHeapMaxCqOffset(final String topic, final int queueId, final long maxCQOffset) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        Long oldMaxCqOffset = this.topicQueueMaxCqOffset.put(topicQueueId, maxCQOffset);
        if (oldMaxCqOffset != null && oldMaxCqOffset > maxCQOffset) {
            ERROR_LOG.error("cqOffset invalid0. old: {}, now: {}", oldMaxCqOffset, maxCQOffset);
        }
    }

    private PhyAndCQOffset getHeapMinOffset(final String topic, final int queueId) {
        return this.topicQueueMinOffset.get(buildTopicQueueId(topic, queueId));
    }

    private Long getHeapMaxCqOffset(final String topic, final int queueId) {
        String topicQueueId = buildTopicQueueId(topic, queueId);
        return this.topicQueueMaxCqOffset.get(topicQueueId);
    }

    private PhyAndCQOffset removeHeapMinCqOffset(String topicQueueId) {
        return this.topicQueueMinOffset.remove(topicQueueId);
    }

    private Long removeHeapMaxCqOffset(String topicQueueId) {
        return this.topicQueueMaxCqOffset.remove(topicQueueId);
    }

    private void updateCqOffset(final String topic, final int queueId, final long phyOffset,
        final long cqOffset, boolean max) throws RocksDBException {
        if (!this.rocksDBStorage.hold()) {
            return;
        }
        WriteBatch writeBatch = new WriteBatch();
        try {
            final byte[] topicBytes = topic.getBytes(CHARSET_UTF8);
            final ByteBuffer offsetKey = buildOffsetKeyBB(topicBytes, queueId, max);

            final ByteBuffer offsetValue = buildOffsetValueBB(phyOffset, cqOffset);
            writeBatch.put(offsetCFH, offsetKey.array(), offsetValue.array());
            this.rocksDBStorage.batchPut(writeBatch);

            putHeapMinCqOffset(topic, queueId, phyOffset, cqOffset);
        } catch (RocksDBException e) {
            ERROR_LOG.error("updateOffset max:{} Failed.", max, e);
            throw e;
        } finally {
            writeBatch.close();
            this.rocksDBStorage.release();
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("updateTopicQueueOffset. topic:{}, queueId:{}, max:{}, phyOffset:{}, cqOffset:{}",
                    topic, queueId, max, phyOffset, cqOffset);
            }
        }
    }

    private boolean correctMaxCqOffset(final String topic, final int queueId, final long maxCQOffset,
        final long maxPhyOffsetInCQ) throws RocksDBException {
        // 'getMinOffsetInQueue' may correct minCqOffset and put it into heap
        long minCQOffset = getMinCqOffset(topic, queueId);
        PhyAndCQOffset minPhyAndCQOffset = getHeapMinOffset(topic, queueId);
        if (minPhyAndCQOffset == null || minPhyAndCQOffset.getCqOffset() != minCQOffset || minPhyAndCQOffset.getPhyOffset() > maxPhyOffsetInCQ) {
            ROCKSDB_LOG.info("[BUG] correctMaxCqOffset error! topic={}, queueId={}, maxPhyOffsetInCQ={}, minCqOffset={}, phyAndCQOffset={}",
                topic, queueId, maxPhyOffsetInCQ, minCQOffset, minPhyAndCQOffset);
            throw new RocksDBException("correctMaxCqOffset error");
        }

        long high = maxCQOffset;
        long low = minCQOffset;
        PhyAndCQOffset targetPhyAndCQOffset = this.rocksDBConsumeQueueTable.binarySearchInCQ(topic, queueId, high,
            low, maxPhyOffsetInCQ, false);

        long targetCQOffset = targetPhyAndCQOffset.getCqOffset();
        long targetPhyOffset = targetPhyAndCQOffset.getPhyOffset();

        if (targetCQOffset == -1) {
            if (maxCQOffset != minCQOffset) {
                updateCqOffset(topic, queueId, minPhyAndCQOffset.getPhyOffset(), minCQOffset, true);
            }
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyAndCQOffset.getPhyOffset());
            }
            return false;
        } else {
            updateCqOffset(topic, queueId, targetPhyOffset, targetCQOffset, true);
            return true;
        }
    }

    private boolean correctMinCqOffset(final String topic, final int queueId,
        final long minCQOffset, final long minPhyOffset) throws RocksDBException {
        final ByteBuffer maxBB = getMaxPhyAndCqOffsetInKV(topic, queueId);
        if (maxBB == null) {
            updateCqOffset(topic, queueId, minPhyOffset, 0L, false);
            return true;
        }
        final long maxPhyOffset = maxBB.getLong(OFFSET_PHY_OFFSET);
        final long maxCQOffset = maxBB.getLong(OFFSET_CQ_OFFSET);

        if (maxPhyOffset < minPhyOffset) {
            updateCqOffset(topic, queueId, minPhyOffset, maxCQOffset + 1, false);
            return true;
        }

        long high = maxCQOffset;
        long low = minCQOffset;
        PhyAndCQOffset phyAndCQOffset = this.rocksDBConsumeQueueTable.binarySearchInCQ(topic, queueId, high, low,
            minPhyOffset, true);
        long targetCQOffset = phyAndCQOffset.getCqOffset();
        long targetPhyOffset = phyAndCQOffset.getPhyOffset();

        if (targetCQOffset == -1) {
            if (maxCQOffset != minCQOffset) {
                updateCqOffset(topic, queueId, maxPhyOffset, maxCQOffset, false);
            }
            if (messageStore.getMessageStoreConfig().isEnableRocksDBLog()) {
                ROCKSDB_LOG.warn("correct error. {}, {}, {}, {}, {}", topic, queueId, minCQOffset, maxCQOffset, minPhyOffset);
            }
            return false;
        } else {
            updateCqOffset(topic, queueId, targetPhyOffset, targetCQOffset, false);
            return true;
        }
    }

    public static Pair<ByteBuffer, ByteBuffer> getOffsetByteBufferPair() {
        ByteBuffer offsetKey = ByteBuffer.allocateDirect(RocksDBConsumeQueueStore.MAX_KEY_LEN);
        ByteBuffer offsetValue = ByteBuffer.allocateDirect(OFFSET_VALUE_LENGTH);
        return new Pair<>(offsetKey, offsetValue);
    }

    private void buildOffsetKeyAndValueBB(final Pair<ByteBuffer, ByteBuffer> offsetBBPair,
        final byte[] topicBytes, final DispatchRequest request) {
        final ByteBuffer offsetKey = offsetBBPair.getObject1();
        buildOffsetKeyBB(offsetKey, topicBytes, request.getQueueId(), true);

        final ByteBuffer offsetValue = offsetBBPair.getObject2();
        buildOffsetValueBB(offsetValue, request.getCommitLogOffset(), request.getConsumeQueueOffset());
    }

    private ByteBuffer buildOffsetKeyBB(final byte[] topicBytes, final int queueId, final boolean max) {
        ByteBuffer bb = ByteBuffer.allocate(OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
        buildOffsetKeyBB0(bb, topicBytes, queueId, max);
        return bb;
    }

    private void buildOffsetKeyBB(final ByteBuffer bb, final byte[] topicBytes, final int queueId, final boolean max) {
        bb.position(0).limit(OFFSET_KEY_LENGTH_WITHOUT_TOPIC_BYTES + topicBytes.length);
        buildOffsetKeyBB0(bb, topicBytes, queueId, max);
    }

    private static void buildOffsetKeyBB0(final ByteBuffer bb, final byte[] topicBytes, final int queueId,
        final boolean max) {
        bb.putInt(topicBytes.length).put(CTRL_1).put(topicBytes).put(CTRL_1);
        if (max) {
            bb.put(MAX_BYTES);
        } else {
            bb.put(MIN_BYTES);
        }
        bb.put(CTRL_1).putInt(queueId);
        bb.flip();
    }

    private void buildOffsetValueBB(final ByteBuffer bb, final long phyOffset, final long cqOffset) {
        bb.position(0).limit(OFFSET_VALUE_LENGTH);
        buildOffsetValueBB0(bb, phyOffset, cqOffset);
    }

    private ByteBuffer buildOffsetValueBB(final long phyOffset, final long cqOffset) {
        final ByteBuffer bb = ByteBuffer.allocate(OFFSET_VALUE_LENGTH);
        buildOffsetValueBB0(bb, phyOffset, cqOffset);
        return bb;
    }

    private void buildOffsetValueBB0(final ByteBuffer bb, final long phyOffset, final long cqOffset) {
        bb.putLong(phyOffset).putLong(cqOffset);
        bb.flip();
    }

    static class PhyAndCQOffset {
        private final long phyOffset;
        private final long cqOffset;

        public PhyAndCQOffset(final long phyOffset, final long cqOffset) {
            this.phyOffset = phyOffset;
            this.cqOffset = cqOffset;
        }

        public long getPhyOffset() {
            return this.phyOffset;
        }

        public long getCqOffset() {
            return this.cqOffset;
        }

        @Override
        public String toString() {
            return "[cqOffset=" + cqOffset + ", phyOffset=" + phyOffset + "]";
        }
    }
}
