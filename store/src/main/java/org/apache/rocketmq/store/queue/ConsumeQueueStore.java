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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import static java.lang.String.format;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathBatchConsumeQueue;
import static org.apache.rocketmq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

public class ConsumeQueueStore extends AbstractConsumeQueueStore {

    public ConsumeQueueStore(DefaultMessageStore messageStore) {
        super(messageStore);
    }

    @Override
    public void start() {
        log.info("Default ConsumeQueueStore start!");
    }

    @Override
    public boolean load() {
        boolean cqLoadResult = loadConsumeQueues(getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.SimpleCQ);
        boolean bcqLoadResult = loadConsumeQueues(getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()), CQType.BatchCQ);
        return cqLoadResult && bcqLoadResult;
    }

    @Override
    public boolean loadAfterDestroy() {
        return true;
    }

    @Override
    public void recover() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.recover(logic);
            }
        }
    }

    @Override
    public boolean recoverConcurrently() {
        int count = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            count += maps.values().size();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        BlockingQueue<Runnable> recoverQueue = new LinkedBlockingQueue<>();
        final ExecutorService executor = buildExecutorService(recoverQueue, "RecoverConsumeQueueThread_");
        List<FutureTask<Boolean>> result = new ArrayList<>(count);
        try {
            for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
                for (final ConsumeQueueInterface logic : maps.values()) {
                    FutureTask<Boolean> futureTask = new FutureTask<>(() -> {
                        boolean ret = true;
                        try {
                            logic.recover();
                        } catch (Throwable e) {
                            ret = false;
                            log.error("Exception occurs while recover consume queue concurrently, " +
                                "topic={}, queueId={}", logic.getTopic(), logic.getQueueId(), e);
                        } finally {
                            countDownLatch.countDown();
                        }
                        return ret;
                    });

                    result.add(futureTask);
                    executor.submit(futureTask);
                }
            }
            countDownLatch.await();
            for (FutureTask<Boolean> task : result) {
                if (task != null && task.isDone()) {
                    if (!task.get()) {
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception occurs while recover consume queue concurrently", e);
            return false;
        } finally {
            executor.shutdown();
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        consumeQueue.correctMinOffset(minCommitLogOffset);
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest dispatchRequest) {
        ConsumeQueueInterface cq = this.findOrCreateConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        this.putMessagePositionInfoWrapper(cq, dispatchRequest);
    }

    @Override
    public List<ByteBuffer> rangeQuery(String topic, int queueId, long startIndex, int num) {
        return null;
    }

    @Override
    public ByteBuffer get(String topic, int queueId, long startIndex) {
        return null;
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxOffsetInQueue();
        }
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType boundaryType) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            long resultOffset = logic.getOffsetInQueueByTime(timestamp, boundaryType);
            // Make sure the result offset is in valid range.
            resultOffset = Math.max(resultOffset, logic.getMinOffsetInQueue());
            resultOffset = Math.min(resultOffset, logic.getMaxOffsetInQueue());
            return resultOffset;
        }
        return 0;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        return findOrCreateConsumeQueue(topic, queueId);
    }

    public boolean load(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.load();
    }

    private boolean loadConsumeQueues(String storePath, CQType cqType) {
        File dirLogic = new File(storePath);
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

                        queueTypeShouldBe(topic, cqType);

                        ConsumeQueueInterface logic = createConsumeQueueByType(cqType, topic, queueId, storePath);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!this.load(logic)) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load {} all over, OK", cqType);

        return true;
    }

    private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
        if (Objects.equals(CQType.SimpleCQ, cqType)) {
            return new ConsumeQueue(
                topic,
                queueId,
                storePath,
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        } else if (Objects.equals(CQType.BatchCQ, cqType)) {
            return new BatchConsumeQueue(
                topic,
                queueId,
                storePath,
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            throw new RuntimeException(format("queue type %s is not supported.", cqType.toString()));
        }
    }

    private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);

        CQType cqTypeActual = QueueTypeUtils.getCQType(topicConfig);

        if (!Objects.equals(cqTypeExpected, cqTypeActual)) {
            throw new RuntimeException(format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeActual));
        }
    }

    private ExecutorService buildExecutorService(BlockingQueue<Runnable> blockingQueue, String threadNamePrefix) {
        return ThreadUtils.newThreadPoolExecutor(
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            this.messageStore.getBrokerConfig().getRecoverThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            blockingQueue,
            new ThreadFactoryImpl(threadNamePrefix));
    }

    public void recover(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.recover();
    }

    @Override
    public Long getMaxPhyOffsetInConsumeQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMaxPhysicOffset();
        }
        return null;
    }

    @Override
    public long getMaxPhyOffsetInConsumeQueue() {
        long maxPhysicOffset = -1L;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        return maxPhysicOffset;
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueueInterface logic = findOrCreateConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    @Override
    public void checkSelf() {
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : this.consumeQueueTable.entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> cqEntry : topicEntry.getValue().entrySet()) {
                this.checkSelf(cqEntry.getValue());
            }
        }
    }

    @Override
    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    @Override
    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    @Override
    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs,
        long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    @Override
    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    @Override
    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
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

        Optional<TopicConfig> topicConfig = this.messageStore.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            newLogic = new BatchConsumeQueue(
                topic,
                queueId,
                getStorePathBatchConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMapperFileSizeBatchConsumeQueue(),
                this.messageStore);
        } else {
            newLogic = new ConsumeQueue(
                topic,
                queueId,
                getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                this.messageStore);
        }

        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic != null) {
            logic = oldLogic;
        } else {
            logic = newLogic;
        }

        return logic;
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetOperator.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void updateQueueOffset(String topic, int queueId, long offset) {
        String topicQueueKey = topic + "-" + queueId;
        this.queueOffsetOperator.updateQueueOffset(topicQueueKey, offset);
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

    @Override
    public void recoverOffsetTable(long minPhyOffset) {
        ConcurrentMap<String, Long> cqOffsetTable = new ConcurrentHashMap<>(1024);
        ConcurrentMap<String, Long> bcqOffsetTable = new ConcurrentHashMap<>(1024);

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

        // Correct unSubmit consumeOffset
        if (messageStoreConfig.isDuplicationEnable() || messageStore.getBrokerConfig().isEnableControllerMode()) {
            SelectMappedBufferResult lastBuffer = null;
            long startReadOffset = messageStore.getCommitLog().getConfirmOffset() == -1 ? 0 : messageStore.getCommitLog().getConfirmOffset();
            while ((lastBuffer = messageStore.selectOneMessageByOffset(startReadOffset)) != null) {
                try {
                    if (lastBuffer.getStartOffset() > startReadOffset) {
                        startReadOffset = lastBuffer.getStartOffset();
                        continue;
                    }

                    ByteBuffer bb = lastBuffer.getByteBuffer();
                    int magicCode = bb.getInt(bb.position() + 4);
                    if (magicCode == CommitLog.BLANK_MAGIC_CODE) {
                        startReadOffset += bb.getInt(bb.position());
                        continue;
                    } else if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE) {
                        throw new RuntimeException("Unknown magicCode: " + magicCode);
                    }

                    lastBuffer.getByteBuffer().mark();
                    DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(lastBuffer.getByteBuffer(), true, true, true);
                    if (!dispatchRequest.isSuccess())
                        break;
                    lastBuffer.getByteBuffer().reset();

                    MessageExt msg = MessageDecoder.decode(lastBuffer.getByteBuffer(), true, false, false, false, true);
                    if (msg == null)
                        break;

                    String key = msg.getTopic() + "-" + msg.getQueueId();
                    cqOffsetTable.put(key, msg.getQueueOffset() + 1);
                    startReadOffset += msg.getStoreSize();
                } finally {
                    if (lastBuffer != null)
                        lastBuffer.release();
                }

            }
        }

        this.setTopicQueueTable(cqOffsetTable);
        this.setBatchTopicQueueTable(bcqOffsetTable);
    }

    @Override
    public void destroy() {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.destroy(logic);
            }
        }
    }

    @Override
    public void cleanExpired(long minCommitLogOffset) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> next = it.next();
            String topic = next.getKey();
            if (!TopicValidator.isSystemTopic(topic)) {
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

    @Override
    public void truncateDirty(long offsetToTruncate) {
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                this.truncateDirtyLogicFiles(logic, offsetToTruncate);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                totalSize += logic.getTotalSize();
            }
        }
        return totalSize;
    }
}
