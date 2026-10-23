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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerCache extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final long OFFSET_NOT_EXIST = -1L;

    private final BrokerController brokerController;
    private final PopConsumerRocksdbStore consumerRecordStore;
    private final PopConsumerLockService consumerLockService;
    private final Consumer<PopConsumerRecord> reviveConsumer;

    private final AtomicInteger estimateCacheSize;
    private final ConcurrentMap<String, ConsumerRecords> consumerRecordTable;

    public PopConsumerCache(BrokerController brokerController, PopConsumerRocksdbStore consumerRecordStore,
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

    public long getPopInFlightMessageCount(String groupId, String topicId, int queueId) {
        ConsumerRecords consumerRecords = consumerRecordTable.get(this.getKey(groupId, topicId, queueId));
        return consumerRecords != null ? consumerRecords.getInFlightRecordCount() : 0L;
    }

    private static List<PopConsumerRecord> snapshot(List<PopConsumerRecord> records) {
        List<PopConsumerRecord> copy = new ArrayList<>(records.size());
        records.forEach(record -> copy.add(PopConsumerRecord.decode(record.getValueBytes())));
        return copy;
    }

    public CompletableFuture<Void> writeRecordsAsync(List<PopConsumerRecord> records) {
        return consumerRecordStore.submitCacheRequest(new PopConsumerWriteBatchService.Request(
            PopConsumerWriteBatchService.Type.POP, this, snapshot(records)));
    }

    public CompletableFuture<Void> ackAsync(PopConsumerRecord record) {
        return consumerRecordStore.submitCacheRequest(new PopConsumerWriteBatchService.Request(
            PopConsumerWriteBatchService.Type.ACK, this, snapshot(Collections.singletonList(record))));
    }

    public CompletableFuture<Void> changeAsync(PopConsumerRecord old, List<PopConsumerRecord> puts,
        List<PopConsumerRecord> deletes) {
        PopConsumerWriteBatchService.Request request = new PopConsumerWriteBatchService.Request(
            PopConsumerWriteBatchService.Type.CHANGE, this, snapshot(Collections.singletonList(old)));
        for (PopConsumerRecord record : puts) {
            request.mutations.add(new PopConsumerWriteBatchService.Mutation(record.getKeyBytes(), record.getValueBytes()));
        }
        // Also delete a disk key left by a preceding transfer, even if its write reported failure.
        for (PopConsumerRecord record : deletes) {
            request.mutations.add(new PopConsumerWriteBatchService.Mutation(record.getKeyBytes(), null));
        }
        return consumerRecordStore.submitCacheRequest(request);
    }

    boolean contains(PopConsumerRecord record) {
        ConsumerRecords records = consumerRecordTable.get(getKey(record));
        return records != null && sameCheckpoint(records.recordTreeMap.get(record.getOffset()), record);
    }

    static boolean sameCheckpoint(PopConsumerRecord current, PopConsumerRecord expected) {
        return current != null && current.getPopTime() == expected.getPopTime()
            && current.getInvisibleTime() == expected.getInvisibleTime();
    }

    // Cache mutations are published on the KV writer after the corresponding batch commits.
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
        return cleanupRecordsAsync(consumer).join();
    }

    public CompletableFuture<Integer> cleanupRecordsAsync(Consumer<PopConsumerRecord> consumer) {
        PopConsumerWriteBatchService.Request request = new PopConsumerWriteBatchService.Request(
            PopConsumerWriteBatchService.Type.TRANSFER, this, Collections.emptyList());
        return consumerRecordStore.submitCacheRequest(request).thenApply(ignored -> {
            // Revive may read messages or send retries; never execute it on the KV writer.
            if (!request.expired.isEmpty()) {
                request.expired.forEach(consumer);
            }
            return getCacheSize();
        });
    }

    List<PopConsumerRecord> prepareTransfer(List<PopConsumerRecord> expired, List<String> offlineQueues) {
        List<PopConsumerRecord> writes = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ConsumerRecords> entry : consumerRecordTable.entrySet()) {
            ConsumerRecords records = entry.getValue();
            boolean offline = consumerLockService.isLockTimeout(records.getGroupId(), records.getTopicId());
            if (offline) {
                offlineQueues.add(entry.getKey());
            }
            records.stageExpiredRecords(offline ? Long.MAX_VALUE : now);
            for (PopConsumerRecord record : records.removeTreeMap.values()) {
                if (!offline && !records.pendingDisk.contains(record) && record.getVisibilityTimeout() <= now) {
                    expired.add(record);
                } else {
                    // Keep this marker on failure: the write may have reached disk.
                    records.pendingDisk.add(record);
                    writes.add(record);
                }
            }
        }
        return writes;
    }

    void completeTransfer(List<String> offlineQueues) {
        // The writer commits the entire transfer before processing any later cache operation.
        offlineQueues.forEach(consumerRecordTable::remove);
        for (ConsumerRecords records : consumerRecordTable.values()) {
            records.clearStagedRecords();
            long offset = records.getMinOffsetInBuffer();
            if (offset > OFFSET_NOT_EXIST) {
                commitOffset("PopConsumerCache", records.getGroupId(), records.getTopicId(), records.getQueueId(), offset);
            }
        }
        estimateCacheSize.set(consumerRecordTable.values().stream()
            .mapToInt(ConsumerRecords::getInFlightRecordCount).sum());
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
        removeRecords(getKey(groupId, topicId, queueId));
    }

    void removeRecords(String key) {
        ConsumerRecords removed = this.consumerRecordTable.remove(key);
        if (removed != null) {
            estimateCacheSize.addAndGet(-removed.getInFlightRecordCount());
        }
    }

    public void clearRecords(String groupId, String topicId, int queueId) {
        PopConsumerWriteBatchService.Request request = new PopConsumerWriteBatchService.Request(
            PopConsumerWriteBatchService.Type.CLEAR, this, Collections.emptyList());
        request.cacheKey = getKey(groupId, topicId, queueId);
        // Private future: completion workers may block here without waiting on themselves.
        request.asyncCompletion = false;
        consumerRecordStore.submitCacheRequest(request).join();
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
                this.cleanupRecords(reviveConsumer);
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
        private final Set<PopConsumerRecord> pendingDisk = new HashSet<>();

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
            boolean removed = false;
            for (ConcurrentSkipListMap<Long, PopConsumerRecord> records : Arrays.asList(recordTreeMap, removeTreeMap)) {
                PopConsumerRecord current = records.get(record.getOffset());
                if (sameCheckpoint(current, record) && records.remove(record.getOffset(), current)) {
                    pendingDisk.remove(current);
                    removed = true;
                }
            }
            return removed;
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
            pendingDisk.clear();
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
