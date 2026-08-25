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
package org.apache.rocketmq.broker.pop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerCache extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final long OFFSET_NOT_EXIST = -1L;

    private final BrokerController brokerController;
    private final PopConsumerKVStore consumerRecordStore;
    private final PopConsumerLockService consumerLockService;
    private final Consumer<PopConsumerRecord> reviveConsumer;

    private final AtomicInteger estimateCacheSize;
    private final ConcurrentMap<String, ConsumerRecords> consumerRecordTable;

    public PopConsumerCache(BrokerController brokerController, PopConsumerKVStore consumerRecordStore,
        PopConsumerLockService popConsumerLockService, Consumer<PopConsumerRecord> reviveConsumer) {

        this.reviveConsumer = reviveConsumer;
        this.brokerController = brokerController;
        this.consumerRecordStore = consumerRecordStore;
        this.consumerLockService = popConsumerLockService;
        this.estimateCacheSize = new AtomicInteger();
        this.consumerRecordTable = new ConcurrentHashMap<>();
    }

    public String getKey(String groupId, String topicId, int queueId) {
        return groupId + "@" + topicId + "@" + queueId;
    }

    public String getKey(PopConsumerRecord consumerRecord) {
        return consumerRecord.getGroupId() + "@" + consumerRecord.getTopicId() + "@" + consumerRecord.getQueueId();
    }

    public int getCacheKeySize() {
        return this.consumerRecordTable.size();
    }

    public int getCacheSize() {
        return this.estimateCacheSize.intValue();
    }

    public boolean isCacheFull() {
        return this.estimateCacheSize.intValue() > brokerController.getBrokerConfig().getPopCkMaxBufferSize();
    }

    public long getMinOffsetInCache(String groupId, String topicId, int queueId) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        return consumerRecords != null ? consumerRecords.getMinOffsetInBuffer() : OFFSET_NOT_EXIST;
    }

    public void setPendingCommitOffset(String groupId, String topicId, int queueId, long offset) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        if (consumerRecords != null) {
            consumerRecords.setPendingCommitOffset(offset);
        }
    }

    public long getPopInFlightMessageCount(String groupId, String topicId, int queueId) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        return consumerRecords != null ? consumerRecords.getInFlightRecordCount() : 0L;
    }

    public void writeRecords(List<PopConsumerRecord> consumerRecordList) {
        this.estimateCacheSize.addAndGet(consumerRecordList.size());
        consumerRecordList.forEach(consumerRecord -> {
            ConsumerRecords consumerRecords = ConcurrentHashMapUtils.computeIfAbsent(consumerRecordTable,
                this.getKey(consumerRecord), k -> new ConsumerRecords(brokerController.getBrokerConfig(),
                    consumerRecord.getGroupId(), consumerRecord.getTopicId(), consumerRecord.getQueueId()));
            assert consumerRecords != null;
            consumerRecords.write(consumerRecord);
        });
    }

    /**
     * Remove the record from the input list then return the content that has not been deleted
     */
    public List<PopConsumerRecord> deleteRecords(List<PopConsumerRecord> consumerRecordList) {
        int total = consumerRecordList.size();
        List<PopConsumerRecord> remain = new ArrayList<>();
        consumerRecordList.forEach(consumerRecord -> {
            ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(consumerRecord));
            if (consumerRecords == null || !consumerRecords.delete(consumerRecord)) {
                remain.add(consumerRecord);
            }
        });
        this.estimateCacheSize.addAndGet(remain.size() - total);
        return remain;
    }

    public int cleanupRecords(Consumer<PopConsumerRecord> consumer) {
        int remain = 0;
        Iterator<Map.Entry<String, ConsumerRecords>> iterator = consumerRecordTable.entrySet().iterator();
        while (iterator.hasNext()) {
            // revive or write record to store
            ConsumerRecords records = iterator.next().getValue();
            boolean timeout = consumerLockService.isLockTimeout(
                records.getGroupId(), records.getTopicId());

            if (timeout) {
                // hold the same lock as PopConsumerService#popAsync to prevent evicting records that a
                // concurrent pop is still writing.
                String lockTopicId = KeyBuilder.parseNormalTopic(records.getTopicId(), records.getGroupId());
                if (!consumerLockService.tryLock(records.getGroupId(), lockTopicId)) {
                    remain += records.getInFlightRecordCount();
                    continue;
                }
                try {
                    records.stageExpiredRecords(Long.MAX_VALUE);
                    List<PopConsumerRecord> writeConsumerRecords =
                        new ArrayList<>(records.getRemoveTreeMap().values());
                    if (!writeConsumerRecords.isEmpty()) {
                        consumerRecordStore.writeRecords(writeConsumerRecords);
                    }
                    records.clearStagedRecords();
                    log.info("PopConsumerOffline, so clean expire records, groupId={}, topic={}, queueId={}, records={}",
                        records.getGroupId(), records.getTopicId(), records.getQueueId(), records.getInFlightRecordCount());
                    iterator.remove();
                } finally {
                    consumerLockService.unlock(records.getGroupId(), lockTopicId);
                }
                commitPendingOffset(records);
                continue;
            }

            long currentTime = System.currentTimeMillis();
            records.stageExpiredRecords(currentTime);
            List<PopConsumerRecord> writeConsumerRecords = new ArrayList<>();
            records.getRemoveTreeMap().values().forEach(record -> {
                if (record.getVisibilityTimeout() <= currentTime) {
                    consumer.accept(record);
                } else {
                    writeConsumerRecords.add(record);
                }
            });

            // write to store and handle it later
            consumerRecordStore.writeRecords(writeConsumerRecords);
            records.clearStagedRecords();

            // commit min offset in buffer to offset store
            long offset = records.getMinOffsetInBuffer();
            if (offset > OFFSET_NOT_EXIST) {
                this.commitOffset("PopConsumerCache",
                    records.getGroupId(), records.getTopicId(), records.getQueueId(), offset);
            } else {
                this.commitPendingOffset(records);
            }

            remain += records.getInFlightRecordCount();
        }
        return remain;
    }

    /**
     * Commit the offset left over by pop once no record of the queue remains in the cache.
     */
    private void commitPendingOffset(ConsumerRecords records) {
        // Read the pending offset before checking the in-flight records, never the other way
        // around. A concurrent pop writes its records into the cache before it updates the pending
        // offset, so this order guarantees that either the check below sees those records, or the
        // pending offset read here is the older one.
        long pendingCommitOffset = records.getPendingCommitOffset();
        if (pendingCommitOffset == OFFSET_NOT_EXIST || records.getInFlightRecordCount() != 0) {
            return;
        }

        String groupId = records.getGroupId();
        String topicId = records.getTopicId();
        int queueId = records.getQueueId();

        ConsumerOffsetManager consumerOffsetManager = brokerController.getConsumerOffsetManager();
        if (consumerOffsetManager.hasOffsetReset(topicId, groupId, queueId)) {
            return;
        }

        if (pendingCommitOffset > consumerOffsetManager.queryOffset(groupId, topicId, queueId)) {
            this.commitOffset("PopConsumerCache", groupId, topicId, queueId, pendingCommitOffset);
        }
    }

    public void commitOffset(String clientHost, String groupId, String topicId, int queueId, long offset) {
        if (!consumerLockService.tryLock(groupId, topicId)) {
            return;
        }
        try {
            ConsumerOffsetManager consumerOffsetManager = brokerController.getConsumerOffsetManager();
            long commit = consumerOffsetManager.queryOffset(groupId, topicId, queueId);
            if (commit != OFFSET_NOT_EXIST && offset < commit) {
                log.info("PopConsumerCache, consumer offset less than store, " +
                    "groupId={}, topicId={}, queueId={}, offset={}", groupId, topicId, queueId, offset);
            }
            consumerOffsetManager.commitOffset(clientHost, groupId, topicId, queueId, offset);
        } finally {
            consumerLockService.unlock(groupId, topicId);
        }
    }

    public void removeRecords(String groupId, String topicId, int queueId) {
        this.consumerRecordTable.remove(this.getKey(groupId, topicId, queueId));
    }

    @Override
    public String getServiceName() {
        return PopConsumerCache.class.getSimpleName();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                this.waitForRunning(TimeUnit.SECONDS.toMillis(1));
                int cacheSize = this.cleanupRecords(reviveConsumer);
                this.estimateCacheSize.set(cacheSize);
            } catch (Exception e) {
                log.error("PopConsumerCacheService revive error", e);
            }
        }
    }

    protected static class ConsumerRecords {

        private final String groupId;
        private final String topicId;
        private final int queueId;
        private final BrokerConfig brokerConfig;
        private final ConcurrentSkipListMap<Long /* offset */, PopConsumerRecord> removeTreeMap;
        private final ConcurrentSkipListMap<Long /* offset */, PopConsumerRecord> recordTreeMap;

        // The consumer offset to commit once no record of this queue is in cache
        private volatile long pendingCommitOffset = OFFSET_NOT_EXIST;

        public ConsumerRecords(BrokerConfig brokerConfig, String groupId, String topicId, int queueId) {
            this.groupId = groupId;
            this.topicId = topicId;
            this.queueId = queueId;
            this.brokerConfig = brokerConfig;
            this.removeTreeMap = new ConcurrentSkipListMap<>();
            this.recordTreeMap = new ConcurrentSkipListMap<>();
        }

        public void write(PopConsumerRecord record) {
            recordTreeMap.put(record.getOffset(), record);
        }

        public boolean delete(PopConsumerRecord record) {
            return recordTreeMap.remove(record.getOffset()) != null;
        }

        public long getMinOffsetInBuffer() {
            Map.Entry<Long, PopConsumerRecord> entry = removeTreeMap.firstEntry();
            if (entry != null) {
                return entry.getKey();
            }
            entry = recordTreeMap.firstEntry();
            return entry != null ? entry.getKey() : OFFSET_NOT_EXIST;
        }

        public int getInFlightRecordCount() {
            return removeTreeMap.size() + recordTreeMap.size();
        }

        public void setPendingCommitOffset(long pendingCommitOffset) {
            this.pendingCommitOffset = pendingCommitOffset;
        }

        public long getPendingCommitOffset() {
            return pendingCommitOffset;
        }

        public void stageExpiredRecords(long currentTime) {
            Iterator<Map.Entry<Long, PopConsumerRecord>>
                iterator = recordTreeMap.entrySet().iterator();

            // refer: org.apache.rocketmq.broker.processor.PopBufferMergeService.scan
            while (iterator.hasNext()) {
                Map.Entry<Long, PopConsumerRecord> entry = iterator.next();
                if (entry.getValue().getVisibilityTimeout() <= currentTime ||
                    entry.getValue().getPopTime() + brokerConfig.getPopCkStayBufferTime() <= currentTime) {
                    removeTreeMap.put(entry.getKey(), entry.getValue());
                    iterator.remove();
                }
            }
        }

        public void clearStagedRecords() {
            removeTreeMap.clear();
        }

        public ConcurrentSkipListMap<Long, PopConsumerRecord> getRemoveTreeMap() {
            return removeTreeMap;
        }

        public String getGroupId() {
            return groupId;
        }

        public String getTopicId() {
            return topicId;
        }

        public int getQueueId() {
            return queueId;
        }

        @Override
        public String toString() {
            return "ConsumerRecords{" +
                "topicId=" + topicId +
                ", groupId=" + groupId +
                ", queueId=" + queueId +
                ", recordTreeMap=" + recordTreeMap.size() +
                '}';
        }
    }
}
