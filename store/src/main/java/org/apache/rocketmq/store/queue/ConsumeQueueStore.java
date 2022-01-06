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

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumeQueueStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final MessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final QueueOffsetAssigner queueOffsetAssigner = new QueueOffsetAssigner();
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    // Should be careful, do not change the topic config
    // TopicConfigManager is more suitable here.
    private ConcurrentMap<String, TopicConfig> topicConfigTable;

    public ConsumeQueueStore(MessageStore messageStore, MessageStoreConfig messageStoreConfig) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStoreConfig;
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        return (FileQueueLifeCycle) findOrCreateConsumeQueue(topic, queueId);
    }

    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.correctMinOffset(minCommitLogOffset);
    }

    /**
     * Apply the dispatched request and build the consume queue.
     * This function should be idempotent.
     *
     * @param request
     */
    public void putMessagePositionInfoWrapper(ConsumeQueueInterface consumeQueue, DispatchRequest request) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.putMessagePositionInfoWrapper(request);
    }

    public void putMessagePositionInfoWrapper(DispatchRequest dispatchRequest) {
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        this.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    public boolean load() {
        return loadConsumeQueues() && loadBatchConsumeQueues();
    }

    private boolean loadBatchConsumeQueues() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        TopicConfig topicConfig = this.topicConfigTable == null ? null : this.topicConfigTable.get(topic);

                        // For batch consume queue, the topic config must exist
                        if (topicConfig == null) {
                            log.warn("topic: {} has no topic config.", topic);
                            continue;
                        }

                        if (!Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(Optional.of(topicConfig)))) {
                            log.error("[BUG]topic: {} should be BCQ.", topic);
                        }

                        ConsumeQueueInterface logic = new BatchConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                                this.messageStore);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load batch consume queue all over, OK");

        return true;
    }

    private boolean loadConsumeQueues() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueueInterface logic = new ConsumeQueue(
                                topic,
                                queueId,
                                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                                this.messageStore);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    public void recover(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.recover();
    }

    public long recover() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.recover(logic);
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    public void checkSelf() {
        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Map.Entry<Integer, ConsumeQueueInterface> cq = itNext.next();
                this.checkSelf(cq.getValue());
            }
        }
    }

    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return doFindOrCreateConsumeQueue(topic, queueId);
    }

    private ConsumeQueueInterface doFindOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        Optional<TopicConfig> topicConfig = this.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            newLogic = new BatchConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                    this.messageStore);
            ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        } else {
            newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                    this.messageStore);
            ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    public Long getMaxOffset(String topic, int queueId) {
        return this.queueOffsetAssigner.currentQueueOffset(topic + "-" + queueId);
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.queueOffsetAssigner.setTopicQueueTable(topicQueueTable);
    }

    public void setBatchTopicQueueTable(HashMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetAssigner.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void assignQueueOffset(MessageExtBrokerInner msg, short messageNum) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(msg.getTopic(), msg.getQueueId());
        fileQueueLifeCycle.assignQueueOffset(this.queueOffsetAssigner, msg, messageNum);
    }

    public void removeTopicQueueTable(String topic, Integer queueId) {
        this.queueOffsetAssigner.remove(topic, queueId);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    public void recoverOffsetTable(long minPhyOffset) {
        HashMap<String, Long> cqOffsetTable = new HashMap<>(1024);
        HashMap<String, Long> bcqOffsetTable = new HashMap<>(1024);

        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();

                long maxOffsetInQueue = logic.getMaxOffsetInQueue();
                if (Objects.equals(CQType.BatchCQ, logic.getCQType())) {
                    bcqOffsetTable.put(key, maxOffsetInQueue);
                } else {
                    cqOffsetTable.put(key, maxOffsetInQueue);
                }

                this.correctMinOffset(logic, minPhyOffset);
            }
        }

        this.setTopicQueueTable(cqOffsetTable);
        this.setBatchTopicQueueTable(bcqOffsetTable);
    }

    public void destroy() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.destroy(logic);
            }
        }
    }

    public void cleanExpired(long minCommitLogOffset) {
        Iterator<Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueueInterface> queueTable = next.getValue();
                Iterator<Map.Entry<Integer, ConsumeQueueInterface>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Map.Entry<Integer, ConsumeQueueInterface> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                                nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId(),
                                nextQT.getValue().getMaxPhysicOffset(),
                                nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                                "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                                topic,
                                nextQT.getKey(),
                                minCommitLogOffset,
                                maxCLOffsetInConsumeQueue);

                        removeTopicQueueTable(nextQT.getValue().getTopic(),
                                nextQT.getValue().getQueueId());

                        this.destroy(nextQT.getValue());
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public void truncateDirty(long phyOffset) {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.truncateDirtyLogicFiles(logic, phyOffset);
            }
        }
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        if (this.topicConfigTable == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(this.topicConfigTable.get(topic));
    }
}
