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
package org.apache.rocketmq.tieredstore;

import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.container.TieredContainerManager;
import org.apache.rocketmq.tieredstore.container.TieredMessageQueueContainer;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.CQItemBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredDispatcher extends ServiceThread implements CommitLogDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final MessageStore defaultStore;
    private final TieredContainerManager tieredContainerManager;
    private final TieredMessageStoreConfig storeConfig;
    private final String brokerName;

    private ConcurrentMap<TieredMessageQueueContainer, List<DispatchRequest>> dispatchRequestReadMap;
    private ConcurrentMap<TieredMessageQueueContainer, List<DispatchRequest>> dispatchRequestWriteMap;
    private final ReentrantLock dispatchRequestListLock;

    public TieredDispatcher(MessageStore defaultStore, TieredMessageStoreConfig storeConfig) {
        this.defaultStore = defaultStore;
        this.storeConfig = storeConfig;
        this.brokerName = storeConfig.getBrokerName();
        this.tieredContainerManager = TieredContainerManager.getInstance(storeConfig);
        this.dispatchRequestReadMap = new ConcurrentHashMap<>();
        this.dispatchRequestWriteMap = new ConcurrentHashMap<>();
        this.dispatchRequestListLock = new ReentrantLock();

        TieredStoreExecutor.COMMON_SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {
                for (TieredMessageQueueContainer container : tieredContainerManager.getAllMQContainer()) {
                    if (!container.getQueueLock().isLocked()) {
                        TieredStoreExecutor.DISPATCH_EXECUTOR.execute(() -> {
                            try {
                                dispatchByMQContainer(container);
                            } catch (Throwable throwable) {
                                logger.error("[Bug]dispatch failed, can not dispatch by container: topic: {}, queueId: {}", container.getMessageQueue().getTopic(), container.getMessageQueue().getQueueId(), throwable);
                            }
                        });
                    }
                }
            } catch (Throwable ignore) {
            }
        }, 30, 10, TimeUnit.SECONDS);
        TieredStoreExecutor.COMMON_SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {
                for (TieredMessageQueueContainer container : tieredContainerManager.getAllMQContainer()) {
                    container.flushMetadata();
                }
            } catch (Throwable e) {
                logger.error("dispatch by queue container failed: ", e);
            }
        }, 30, 10, TimeUnit.SECONDS);
    }

    @Override
    public void dispatch(DispatchRequest request) {
        if (stopped) {
            return;
        }
        String topic = request.getTopic();
        if (TieredStoreUtil.isSystemTopic(topic)) {
            return;
        }

        TieredMessageQueueContainer container =
            tieredContainerManager.getOrCreateMQContainer(new MessageQueue(topic, brokerName, request.getQueueId()));
        if (container == null) {
            logger.error("[Bug]TieredDispatcher#dispatch: dispatch failed, can not create container: topic: {}, queueId: {}", request.getTopic(), request.getQueueId());
            return;
        }

        // prevent consume queue and index file falling too far
        if (dispatchRequestWriteMap.getOrDefault(container, Collections.emptyList()).size() > storeConfig.getTieredStoreMaxGroupCommitCount()
            || dispatchRequestReadMap.getOrDefault(container, Collections.emptyList()).size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
            return;
        }

        // init dispatch offset
        if (container.getDispatchOffset() == -1) {
            container.initOffset(request.getConsumeQueueOffset());
        }

        if (request.getConsumeQueueOffset() == container.getDispatchOffset()) {
            try {
                if (container.getQueueLock().isLocked() || !container.getQueueLock().tryLock(1, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (Exception e) {
                logger.warn("TieredDispatcher#dispatch: dispatch failed, can not get container lock: topic: {}, queueId: {}", request.getTopic(), request.getQueueId(), e);
                if (container.getQueueLock().isLocked()) {
                    container.getQueueLock().unlock();
                }
                return;
            }

            // double check
            if (request.getConsumeQueueOffset() != container.getDispatchOffset()) {
                container.getQueueLock().unlock();
                return;
            }

            SelectMappedBufferResult message = defaultStore.selectOneMessageByOffset(request.getCommitLogOffset(), request.getMsgSize());
            if (message == null) {
                logger.error("TieredDispatcher#dispatch: dispatch failed, can not get message from next store: topic: {}, queueId: {}, commitLog offset: {}, size: {}",
                    request.getTopic(), request.getQueueId(), request.getCommitLogOffset(), request.getMsgSize());
                container.getQueueLock().unlock();
                return;
            }

            try {
                // drop expired request
                if (request.getConsumeQueueOffset() < container.getDispatchOffset()) {
                    return;
                }
                AppendResult result = container.appendCommitLog(message.getByteBuffer());
                long newCommitLogOffset = container.getCommitLogMaxOffset() - message.getByteBuffer().remaining();
                handleAppendCommitLogResult(result, container, request.getConsumeQueueOffset(),
                    container.getDispatchOffset(), newCommitLogOffset, request.getMsgSize(), request.getTagsCode(), message.getByteBuffer());
                if (result == AppendResult.SUCCESS) {
                    Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, request.getTopic())
                        .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, request.getQueueId())
                        .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, TieredFileSegment.FileSegmentType.COMMIT_LOG.name().toLowerCase())
                        .build();
                    TieredStoreMetricsManager.messagesDispatchTotal.add(1, attributes);
                }
            } catch (Exception throwable) {
                logger.error("TieredDispatcher#dispatch: dispatch failed: topic: {}, queueId: {}, queue offset: {}", request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset(), throwable);
            } finally {
                message.release();
                container.getQueueLock().unlock();
            }
        } else {
            if (!container.getQueueLock().isLocked()) {
                try {
                    TieredStoreExecutor.DISPATCH_EXECUTOR.execute(() -> {
                        try {
                            dispatchByMQContainer(container);
                        } catch (Throwable throwable) {
                            logger.error("[Bug]TieredDispatcher#dispatchByMQContainer: dispatch failed, can not dispatch by container: topic: {}, queueId: {}", topic, container.getMessageQueue().getQueueId(), throwable);
                        }
                    });
                } catch (Throwable ignore) {
                }
            }
        }
    }

    protected void dispatchByMQContainer(TieredMessageQueueContainer container) {
        if (stopped) {
            return;
        }
        if (container.getDispatchOffset() == -1) {
            return;
        }

        // prevent consume queue and index file falling too far
        if (dispatchRequestWriteMap.getOrDefault(container, Collections.emptyList()).size() > storeConfig.getTieredStoreMaxGroupCommitCount()
            || dispatchRequestReadMap.getOrDefault(container, Collections.emptyList()).size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
            return;
        }

        MessageQueue mq = container.getMessageQueue();
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();

        long beforeOffset = container.getDispatchOffset();
        long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
        long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

        if (beforeOffset >= maxOffsetInQueue) {
            return;
        }

        try {
            if (!container.getQueueLock().tryLock(200, TimeUnit.MILLISECONDS)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("TieredDispatcher#dispatchByMQContainer: dispatch failed, can not get container lock: topic: {}, queueId: {}", mq.getTopic(), mq.getQueueId(), e);
            if (container.getQueueLock().isLocked()) {
                container.getQueueLock().unlock();
            }
            return;
        }

        try {
            long queueOffset = container.getDispatchOffset();
            if (minOffsetInQueue > queueOffset) {
                logger.warn("BlobDispatcher#dispatchByMQContainer: message that needs to be dispatched does not exist: topic: {}, queueId: {}, message queue offset: {}, min queue offset: {}",
                    topic, queueId, queueOffset, minOffsetInQueue);
                container.initOffset(minOffsetInQueue);
                queueOffset = minOffsetInQueue;
            }
            beforeOffset = queueOffset;

            // TODO flow control based on message size
            long limit = Math.min(queueOffset + 100000, maxOffsetInQueue);
            ConsumeQueue consumeQueue = (ConsumeQueue) defaultStore.getConsumeQueue(topic, queueId);
            for (; queueOffset < limit; queueOffset++) {
                SelectMappedBufferResult cqItem = consumeQueue.getIndexBuffer(queueOffset);
                if (cqItem == null) {
                    logger.error("[Bug]TieredDispatcher#dispatchByMQContainer: dispatch failed, can not get cq item: topic: {}, queueId: {}, offset: {}", topic, queueId, queueOffset);
                    return;
                }
                long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqItem.getByteBuffer());
                int size = CQItemBufferUtil.getSize(cqItem.getByteBuffer());
                long tagCode = CQItemBufferUtil.getTagCode(cqItem.getByteBuffer());
                cqItem.release();

                SelectMappedBufferResult message = defaultStore.selectOneMessageByOffset(commitLogOffset, size);
                if (message == null) {
                    logger.error("TieredDispatcher#dispatchByMQContainer: dispatch failed, can not get message from next store: topic: {}, queueId: {}, commitLog offset: {}, size: {}",
                        topic, queueId, commitLogOffset, size);
                    break;
                }
                AppendResult result = container.appendCommitLog(message.getByteBuffer(), true);
                long newCommitLogOffset = container.getCommitLogMaxOffset() - message.getByteBuffer().remaining();
                handleAppendCommitLogResult(result, container, queueOffset, container.getDispatchOffset(), newCommitLogOffset, size, tagCode, message.getByteBuffer());
                message.release();
                if (result != AppendResult.SUCCESS) {
                    queueOffset--;
                    break;
                }
            }
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, mq.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, mq.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, TieredFileSegment.FileSegmentType.COMMIT_LOG.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(queueOffset - beforeOffset, attributes);
        } finally {
            container.getQueueLock().unlock();
        }
        // If this queue dispatch falls too far, dispatch again immediately
        if (container.getDispatchOffset() < maxOffsetInQueue && !container.getQueueLock().isLocked()) {
            TieredStoreExecutor.DISPATCH_EXECUTOR.execute(() -> {
                try {
                    dispatchByMQContainer(container);
                } catch (Throwable throwable) {
                    logger.error("[Bug]TieredDispatcher#dispatchByMQContainer: dispatch failed, can not dispatch by container: topic: {}, queueId: {}", topic, queueId, throwable);
                }
            });
        }
    }

    public void handleAppendCommitLogResult(AppendResult result, TieredMessageQueueContainer container,
        long queueOffset,
        long dispatchOffset, long newCommitLogOffset, int size, long tagCode, ByteBuffer message) {
        MessageQueue mq = container.getMessageQueue();
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();
        switch (result) {
            case SUCCESS:
                break;
            case OFFSET_INCORRECT:
                long offset = MessageBufferUtil.getQueueOffset(message);
                if (queueOffset != offset) {
                    logger.error("[Bug]queue offset: {} is not equal to queue offset in message: {}", queueOffset, offset);
                }
                logger.error("[Bug]append message failed, offset is incorrect, maybe because of race: topic: {}, queueId: {}, queue offset: {}, dispatchOffset: {}", topic, queueId, queueOffset, dispatchOffset);
                return;
            case BUFFER_FULL:
                logger.debug("append message failed: topic: {}, queueId: {}, queue offset: {}, result: {}", topic, queueId, queueOffset, result);
                return;
            default:
                logger.info("append message failed: topic: {}, queueId: {}, queue offset: {}, result: {}", topic, queueId, queueOffset, result);
                return;
        }
        dispatchRequestListLock.lock();
        try {
            Map<String, String> properties = MessageBufferUtil.getProperties(message);
            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                queueId,
                newCommitLogOffset,
                size,
                tagCode,
                MessageBufferUtil.getStoreTimeStamp(message),
                queueOffset,
                properties.getOrDefault(MessageConst.PROPERTY_KEYS, ""),
                properties.getOrDefault(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ""),
                0, 0, new HashMap<>());
            dispatchRequest.setOffsetId(MessageBufferUtil.getOffsetId(message));
            List<DispatchRequest> requestList = dispatchRequestWriteMap.computeIfAbsent(container, k -> new ArrayList<>());
            requestList.add(dispatchRequest);
            if (requestList.get(0).getConsumeQueueOffset() >= container.getBuildCQMaxOffset()) {
                wakeup();
            }
        } finally {
            dispatchRequestListLock.unlock();
        }
    }

    public void swapDispatchRequestList() {
        dispatchRequestListLock.lock();
        try {
            dispatchRequestReadMap = dispatchRequestWriteMap;
            dispatchRequestWriteMap = new ConcurrentHashMap<>();
        } finally {
            dispatchRequestListLock.unlock();
        }
    }

    public void sendBackDispatchRequestList() {
        if (!dispatchRequestReadMap.isEmpty()) {
            dispatchRequestListLock.lock();
            try {
                dispatchRequestReadMap.forEach((container, requestList) -> {
                    if (requestList.isEmpty()) {
                        logger.warn("[Bug]TieredDispatcher#sendBackDispatchRequestList: requestList is empty, no need to send back: topic: {}, queueId: {}", container.getMessageQueue().getTopic(), container.getMessageQueue().getQueueId());
                        return;
                    }
                    List<DispatchRequest> requestListToWrite = dispatchRequestWriteMap.computeIfAbsent(container, k -> new ArrayList<>());
                    if (!requestListToWrite.isEmpty() && requestList.get(requestList.size() - 1).getConsumeQueueOffset() > requestListToWrite.get(0).getConsumeQueueOffset()) {
                        logger.warn("[Bug]TieredDispatcher#sendBackDispatchRequestList: dispatch request list is not continuous: topic: {}, queueId: {}, last list max offset: {}, new list min offset: {}",
                            container.getMessageQueue().getTopic(), container.getMessageQueue().getQueueId(),
                            requestList.get(requestList.size() - 1).getConsumeQueueOffset(), requestListToWrite.get(0).getConsumeQueueOffset());
                        requestList.sort(Comparator.comparingLong(DispatchRequest::getConsumeQueueOffset));
                    }
                    requestList.addAll(requestListToWrite);
                    dispatchRequestWriteMap.put(container, requestList);
                });
                dispatchRequestReadMap = new ConcurrentHashMap<>();
            } finally {
                dispatchRequestListLock.unlock();
            }
        }
    }

    public void buildCQAndIndexFile() {
        swapDispatchRequestList();
        Map<MessageQueue, Long> cqMetricsMap = new HashMap<>();
        Map<MessageQueue, Long> ifMetricsMap = new HashMap<>();

        for (Map.Entry<TieredMessageQueueContainer, List<DispatchRequest>> entry : dispatchRequestReadMap.entrySet()) {
            TieredMessageQueueContainer container = entry.getKey();
            List<DispatchRequest> requestList = entry.getValue();
            if (container.isClosed()) {
                requestList.clear();
            }
            MessageQueue messageQueue = container.getMessageQueue();
            Iterator<DispatchRequest> iterator = requestList.iterator();
            while (iterator.hasNext()) {
                DispatchRequest request = iterator.next();

                // remove expired request
                if (request.getConsumeQueueOffset() < container.getConsumeQueueMaxOffset()) {
                    iterator.remove();
                    continue;
                }

                // wait uploading commitLog
                if (container.getBuildCQMaxOffset() < request.getConsumeQueueOffset()) {
                    break;
                }

                // build cq
                AppendResult result = container.appendConsumeQueue(request, true);
                if (result == AppendResult.SUCCESS) {
                    Long count = cqMetricsMap.computeIfAbsent(messageQueue, key -> 0L);
                    cqMetricsMap.put(messageQueue, count + 1);
                } else if (result == AppendResult.OFFSET_INCORRECT) {
                    logger.error("build consumeQueue and indexFile failed, offset is messed up, try to rebuild cq: topic: {}, queue: {}, queue offset: {}, max queue offset: {}"
                        , request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset(), container.getConsumeQueueMaxOffset());
                    container.getQueueLock().lock();
                    try {
                        // rollback dispatch offset, this operation will cause duplicate message in commitLog
                        container.initOffset(container.getConsumeQueueMaxOffset());
                        // clean invalid dispatch request
                        dispatchRequestWriteMap.remove(container);
                        requestList.clear();
                        break;
                    } finally {
                        container.getQueueLock().unlock();
                    }
                } else {
                    logger.warn("build consumeQueue failed, result: {}, topic: {}, queue: {}, queue offset: {}",
                        result, request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
                }

                // build index
                if (storeConfig.isMessageIndexEnable() && result == AppendResult.SUCCESS) {
                    result = container.appendIndexFile(request);
                    switch (result) {
                        case SUCCESS:
                            Long count = ifMetricsMap.computeIfAbsent(messageQueue, key -> 0L);
                            ifMetricsMap.put(messageQueue, count + 1);
                            iterator.remove();
                            break;
                        default:
                            logger.warn("build indexFile failed, result: {}, topic: {}, queue: {}, queue offset: {}",
                                result, request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
                            break;
                    }
                }
            }

            // remove empty list, prevent send back
            if (requestList.isEmpty()) {
                dispatchRequestReadMap.remove(container);
            }
        }
        cqMetricsMap.forEach((messageQueue, count) -> {
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, messageQueue.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, messageQueue.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, TieredFileSegment.FileSegmentType.CONSUME_QUEUE.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(count, attributes);
        });
        ifMetricsMap.forEach((messageQueue, count) -> {
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, messageQueue.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, messageQueue.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, TieredFileSegment.FileSegmentType.INDEX.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(count, attributes);
        });
        sendBackDispatchRequestList();
    }

    @Override
    public String getServiceName() {
        return "TieredStoreDispatcherService";
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(1000);
            try {
                buildCQAndIndexFile();
            } catch (Exception e) {
                logger.error("build consumeQueue and indexFile failed: ", e);
            }
        }
    }
}
