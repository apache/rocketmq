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
package org.apache.rocketmq.tieredstore.core;

import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.file.FlatFileInterface;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreDispatcherImpl extends ServiceThread implements MessageStoreDispatcher {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String brokerName;
    protected final MessageStore defaultStore;
    protected final MessageStoreConfig storeConfig;
    protected final TieredMessageStore messageStore;
    protected final FlatFileStore flatFileStore;
    protected final MessageStoreExecutor storeExecutor;
    protected final MessageStoreFilter topicFilter;
    protected final Semaphore semaphore;
    protected final IndexService indexService;

    public MessageStoreDispatcherImpl(TieredMessageStore messageStore) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getStoreConfig();
        this.defaultStore = messageStore.getDefaultStore();
        this.brokerName = storeConfig.getBrokerName();
        this.semaphore = new Semaphore(
            this.storeConfig.getTieredStoreMaxPendingLimit() / 4);
        this.topicFilter = messageStore.getTopicFilter();
        this.flatFileStore = messageStore.getFlatFileStore();
        this.storeExecutor = messageStore.getStoreExecutor();
        this.indexService = messageStore.getIndexService();
    }

    @Override
    public String getServiceName() {
        return MessageStoreDispatcher.class.getSimpleName();
    }

    public void dispatchWithSemaphore(FlatFileInterface flatFile) {
        try {
            if (stopped) {
                return;
            }
            semaphore.acquire();
            this.doScheduleDispatch(flatFile, false)
                .whenComplete((future, throwable) -> semaphore.release());
        } catch (InterruptedException e) {
            semaphore.release();
        }
    }

    @Override
    public void dispatch(DispatchRequest request) {
        // Slave shouldn't dispatch
        if (MessageStoreUtil.isSlave(defaultStore.getMessageStoreConfig())) {
            return;
        }
        if (stopped || topicFilter != null && topicFilter.filterTopic(request.getTopic())) {
            return;
        }
        flatFileStore.computeIfAbsent(
            new MessageQueue(request.getTopic(), brokerName, request.getQueueId()));
    }

    @Override
    public CompletableFuture<Boolean> doScheduleDispatch(FlatFileInterface flatFile, boolean force) {
        if (stopped) {
            return CompletableFuture.completedFuture(true);
        }

        String topic = flatFile.getMessageQueue().getTopic();
        int queueId = flatFile.getMessageQueue().getQueueId();

        // For test scenarios, we set the 'force' variable to true to
        // ensure that the data in the cache is directly committed successfully.
        force = !storeConfig.isTieredStoreGroupCommit() || force;
        if (force) {
            flatFile.getFileLock().lock();
        } else {
            if (!flatFile.getFileLock().tryLock()) {
                return CompletableFuture.completedFuture(false);
            }
        }

        try {
            if (topicFilter != null && topicFilter.filterTopic(flatFile.getMessageQueue().getTopic())) {
                flatFileStore.destroyFile(flatFile.getMessageQueue());
                return CompletableFuture.completedFuture(false);
            }

            long currentOffset = flatFile.getConsumeQueueMaxOffset();
            long commitOffset = flatFile.getConsumeQueueCommitOffset();
            long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
            long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

            // If set to max offset here, some written messages may be lost
            if (!flatFile.isFlatFileInit()) {
                currentOffset = Math.max(minOffsetInQueue,
                    maxOffsetInQueue - storeConfig.getTieredStoreGroupCommitSize());
                flatFile.initOffset(currentOffset);
                return CompletableFuture.completedFuture(true);
            }

            // If the previous commit fails, attempt to trigger a commit directly.
            if (commitOffset < currentOffset) {
                this.commitAsync(flatFile);
                return CompletableFuture.completedFuture(false);
            }

            if (currentOffset < minOffsetInQueue) {
                log.warn("MessageDispatcher#dispatch, current offset is too small, " +
                        "topic={}, queueId={}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
                flatFileStore.destroyFile(flatFile.getMessageQueue());
                flatFileStore.computeIfAbsent(new MessageQueue(topic, brokerName, queueId));
                return CompletableFuture.completedFuture(true);
            }

            if (currentOffset > maxOffsetInQueue) {
                log.warn("MessageDispatcher#dispatch, current offset is too large, " +
                        "topic: {}, queueId: {}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
                return CompletableFuture.completedFuture(false);
            }

            long interval = TimeUnit.HOURS.toMillis(storeConfig.getCommitLogRollingInterval());
            if (flatFile.rollingFile(interval)) {
                log.info("MessageDispatcher#dispatch, rolling file, " +
                        "topic: {}, queueId: {}, offset={}-{}, current={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
            }

            if (currentOffset == maxOffsetInQueue) {
                return CompletableFuture.completedFuture(false);
            }

            long bufferSize = 0L;
            long groupCommitSize = storeConfig.getTieredStoreGroupCommitSize();
            long groupCommitCount = storeConfig.getTieredStoreGroupCommitCount();
            long targetOffset = Math.min(currentOffset + groupCommitCount, maxOffsetInQueue);

            ConsumeQueueInterface consumeQueue = defaultStore.getConsumeQueue(topic, queueId);
            CqUnit cqUnit = consumeQueue.get(currentOffset);
            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());
            boolean timeout = MessageFormatUtil.getStoreTimeStamp(message.getByteBuffer()) +
                storeConfig.getTieredStoreGroupCommitTimeout() < System.currentTimeMillis();
            boolean bufferFull = maxOffsetInQueue - currentOffset > storeConfig.getTieredStoreGroupCommitCount();

            if (!timeout && !bufferFull && !force) {
                log.debug("MessageDispatcher#dispatch hold, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                return CompletableFuture.completedFuture(false);
            } else {
                if (MessageFormatUtil.getStoreTimeStamp(message.getByteBuffer()) +
                    TimeUnit.MINUTES.toMillis(5) < System.currentTimeMillis()) {
                    log.warn("MessageDispatcher#dispatch behind too much, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                        topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                } else {
                    log.info("MessageDispatcher#dispatch, topic={}, queueId={}, offset={}-{}, current={}, remain={}",
                        topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, maxOffsetInQueue - currentOffset);
                }
            }
            message.release();

            long offset = currentOffset;
            for (; offset < targetOffset; offset++) {
                cqUnit = consumeQueue.get(offset);
                bufferSize += cqUnit.getSize();
                if (bufferSize >= groupCommitSize) {
                    break;
                }
                message = defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());

                ByteBuffer byteBuffer = message.getByteBuffer();
                AppendResult result = flatFile.appendCommitLog(message);
                if (!AppendResult.SUCCESS.equals(result)) {
                    break;
                }

                long mappedCommitLogOffset = flatFile.getCommitLogMaxOffset() - byteBuffer.remaining();
                Map<String, String> properties = MessageFormatUtil.getProperties(byteBuffer);

                DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, mappedCommitLogOffset,
                    cqUnit.getSize(), cqUnit.getTagsCode(), MessageFormatUtil.getStoreTimeStamp(byteBuffer),
                    cqUnit.getQueueOffset(), properties.getOrDefault(MessageConst.PROPERTY_KEYS, ""),
                    properties.getOrDefault(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ""),
                    0, 0, new HashMap<>());
                dispatchRequest.setOffsetId(MessageFormatUtil.getOffsetId(byteBuffer));

                result = flatFile.appendConsumeQueue(dispatchRequest);
                if (!AppendResult.SUCCESS.equals(result)) {
                    break;
                }
            }

            // If there are many messages waiting to be uploaded, call the upload logic immediately.
            boolean repeat = timeout || maxOffsetInQueue - offset > storeConfig.getTieredStoreGroupCommitCount();

            if (!flatFile.getDispatchRequestList().isEmpty()) {
                Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                    .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
                    .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, queueId)
                    .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                    .build();
                TieredStoreMetricsManager.messagesDispatchTotal.add(offset - currentOffset, attributes);

                this.commitAsync(flatFile).whenComplete((unused, throwable) -> {
                        if (repeat) {
                            storeExecutor.commonExecutor.submit(() -> dispatchWithSemaphore(flatFile));
                        }
                    }
                );
            }
        } finally {
            flatFile.getFileLock().unlock();
        }
        return CompletableFuture.completedFuture(false);
    }

    public CompletableFuture<Void> commitAsync(FlatFileInterface flatFile) {
        return flatFile.commitAsync().thenAcceptAsync(success -> {
            if (success) {
                if (storeConfig.isMessageIndexEnable()) {
                    flatFile.getDispatchRequestList().forEach(
                        request -> constructIndexFile(flatFile.getTopicId(), request));
                }
                flatFile.release();
            }
        }, MessageStoreExecutor.getInstance().bufferCommitExecutor);
    }

    /**
     * Building indexes with offsetId is no longer supported because offsetId has changed in tiered storage
     */
    public void constructIndexFile(long topicId, DispatchRequest request) {
        Set<String> keySet = new HashSet<>();
        if (StringUtils.isNotBlank(request.getUniqKey())) {
            keySet.add(request.getUniqKey());
        }
        if (StringUtils.isNotBlank(request.getKeys())) {
            keySet.addAll(Arrays.asList(request.getKeys().split(MessageConst.KEY_SEPARATOR)));
        }
        indexService.putKey(request.getTopic(), (int) topicId, request.getQueueId(), keySet,
            request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
    }

    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            flatFileStore.deepCopyFlatFileToList().forEach(this::dispatchWithSemaphore);
            this.waitForRunning(Duration.ofSeconds(20).toMillis());
        }
        log.info("{} service shutdown", this.getServiceName());
    }
}
