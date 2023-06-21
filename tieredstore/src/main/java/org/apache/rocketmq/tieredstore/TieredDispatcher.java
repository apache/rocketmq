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
import java.util.function.Consumer;
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
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.file.CompositeQueueFlatFile;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.CQItemBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredDispatcher extends ServiceThread implements CommitLogDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final String brokerName;
    private final MessageStore defaultStore;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFlatFileManager tieredFlatFileManager;
    private final ReentrantLock dispatchLock;
    private final ReentrantLock dispatchRequestListLock;

    private ConcurrentMap<CompositeQueueFlatFile, List<DispatchRequest>> dispatchRequestReadMap;
    private ConcurrentMap<CompositeQueueFlatFile, List<DispatchRequest>> dispatchRequestWriteMap;

    public TieredDispatcher(MessageStore defaultStore, TieredMessageStoreConfig storeConfig) {
        this.defaultStore = defaultStore;
        this.storeConfig = storeConfig;
        this.brokerName = storeConfig.getBrokerName();
        this.tieredFlatFileManager = TieredFlatFileManager.getInstance(storeConfig);
        this.dispatchRequestReadMap = new ConcurrentHashMap<>();
        this.dispatchRequestWriteMap = new ConcurrentHashMap<>();
        this.dispatchLock = new ReentrantLock();
        this.dispatchRequestListLock = new ReentrantLock();
        this.initScheduleTask();
    }

    private void initScheduleTask() {
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() -> {
            tieredFlatFileManager.deepCopyFlatFileToList().forEach(flatFile -> {
                if (!flatFile.getCompositeFlatFileLock().isLocked()) {
                    dispatchFlatFile(flatFile);
                }
            });
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

        CompositeQueueFlatFile flatFile =
            tieredFlatFileManager.getOrCreateFlatFileIfAbsent(new MessageQueue(topic, brokerName, request.getQueueId()));

        if (flatFile == null) {
            logger.error("[Bug]TieredDispatcher#dispatch: dispatch failed, " +
                "can not create flatFile: topic: {}, queueId: {}", request.getTopic(), request.getQueueId());
            return;
        }

        // prevent consume queue and index file falling too far
        int groupCommitCount = storeConfig.getTieredStoreMaxGroupCommitCount();
        if (dispatchRequestWriteMap.getOrDefault(flatFile, Collections.emptyList()).size() > groupCommitCount
            || dispatchRequestReadMap.getOrDefault(flatFile, Collections.emptyList()).size() > groupCommitCount) {
            return;
        }

        // init dispatch offset
        if (flatFile.getDispatchOffset() == -1) {
            flatFile.initOffset(request.getConsumeQueueOffset());
        }

        if (request.getConsumeQueueOffset() == flatFile.getDispatchOffset()) {
            try {
                if (flatFile.getCompositeFlatFileLock().isLocked()
                    || !flatFile.getCompositeFlatFileLock().tryLock(3, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (Exception e) {
                logger.warn("TieredDispatcher#dispatch: dispatch failed, " +
                    "can not get flatFile lock: topic: {}, queueId: {}", request.getTopic(), request.getQueueId(), e);
                if (flatFile.getCompositeFlatFileLock().isLocked()) {
                    flatFile.getCompositeFlatFileLock().unlock();
                }
                return;
            }

            // double check
            if (request.getConsumeQueueOffset() != flatFile.getDispatchOffset()) {
                flatFile.getCompositeFlatFileLock().unlock();
                return;
            }

            // obtain message
            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(request.getCommitLogOffset(), request.getMsgSize());

            if (message == null) {
                logger.error("TieredDispatcher#dispatch: dispatch failed, " +
                        "can not get message from next store: topic: {}, queueId: {}, commitLog offset: {}, size: {}",
                    request.getTopic(), request.getQueueId(), request.getCommitLogOffset(), request.getMsgSize());
                flatFile.getCompositeFlatFileLock().unlock();
                return;
            }

            // drop expired request
            try {
                if (request.getConsumeQueueOffset() < flatFile.getDispatchOffset()) {
                    return;
                }
                AppendResult result = flatFile.appendCommitLog(message.getByteBuffer());
                long newCommitLogOffset = flatFile.getCommitLogMaxOffset() - message.getByteBuffer().remaining();
                handleAppendCommitLogResult(result, flatFile, request.getConsumeQueueOffset(), flatFile.getDispatchOffset(),
                    newCommitLogOffset, request.getMsgSize(), request.getTagsCode(), message.getByteBuffer());

                if (result == AppendResult.SUCCESS) {
                    Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, request.getTopic())
                        .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, request.getQueueId())
                        .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                        .build();
                    TieredStoreMetricsManager.messagesDispatchTotal.add(1, attributes);
                }
            } catch (Exception throwable) {
                logger.error("TieredDispatcher#dispatch: dispatch failed: " +
                        "topic: {}, queueId: {}, queue offset: {}", request.getTopic(), request.getQueueId(),
                    request.getConsumeQueueOffset(), throwable);
            } finally {
                message.release();
                flatFile.getCompositeFlatFileLock().unlock();
            }
        } else {
            if (!flatFile.getCompositeFlatFileLock().isLocked()) {
                this.dispatchFlatFileAsync(flatFile);
            }
        }
    }

    // prevent consume queue and index file falling too far
    private boolean detectFallBehind(CompositeQueueFlatFile flatFile) {
        int groupCommitCount = storeConfig.getTieredStoreMaxGroupCommitCount();
        return dispatchRequestWriteMap.getOrDefault(flatFile, Collections.emptyList()).size() > groupCommitCount
            || dispatchRequestReadMap.getOrDefault(flatFile, Collections.emptyList()).size() > groupCommitCount;
    }

    public void dispatchFlatFileAsync(CompositeQueueFlatFile flatFile) {
        this.dispatchFlatFileAsync(flatFile, null);
    }

    public void dispatchFlatFileAsync(CompositeQueueFlatFile flatFile, Consumer<Long> consumer) {
        TieredStoreExecutor.dispatchExecutor.execute(() -> {
            try {
                dispatchFlatFile(flatFile);
            } catch (Throwable throwable) {
                logger.error("[Bug]TieredDispatcher#dispatchFlatFileAsync dispatch failed, " +
                        "can not dispatch, topic: {}, queueId: {}",
                    flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(), throwable);
            }

            if (consumer != null) {
                consumer.accept(flatFile.getDispatchOffset());
            }
        });
    }

    protected void dispatchFlatFile(CompositeQueueFlatFile flatFile) {
        if (stopped) {
            return;
        }

        if (flatFile.getDispatchOffset() == -1) {
            return;
        }

        if (detectFallBehind(flatFile)) {
            return;
        }

        MessageQueue mq = flatFile.getMessageQueue();
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();

        long beforeOffset = flatFile.getDispatchOffset();
        long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
        long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

        if (beforeOffset >= maxOffsetInQueue) {
            return;
        }

        try {
            if (!flatFile.getCompositeFlatFileLock().tryLock(200, TimeUnit.MILLISECONDS)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("TieredDispatcher#dispatchFlatFile: dispatch failed, " +
                "can not get flatFile lock: topic: {}, queueId: {}", mq.getTopic(), mq.getQueueId(), e);
            if (flatFile.getCompositeFlatFileLock().isLocked()) {
                flatFile.getCompositeFlatFileLock().unlock();
            }
            return;
        }

        try {
            long queueOffset = flatFile.getDispatchOffset();
            if (minOffsetInQueue > queueOffset) {
                logger.warn("BlobDispatcher#dispatchFlatFile: " +
                        "message that needs to be dispatched does not exist: " +
                        "topic: {}, queueId: {}, message queue offset: {}, min queue offset: {}",
                    topic, queueId, queueOffset, minOffsetInQueue);
                flatFile.initOffset(minOffsetInQueue);
                queueOffset = minOffsetInQueue;
            }
            beforeOffset = queueOffset;

            // TODO flow control based on message size
            long limit = Math.min(queueOffset + 100000, maxOffsetInQueue);
            ConsumeQueue consumeQueue = (ConsumeQueue) defaultStore.getConsumeQueue(topic, queueId);
            for (; queueOffset < limit; queueOffset++) {
                SelectMappedBufferResult cqItem = consumeQueue.getIndexBuffer(queueOffset);
                if (cqItem == null) {
                    logger.error("[Bug]TieredDispatcher#dispatchFlatFile: dispatch failed, " +
                        "can not get cq item: topic: {}, queueId: {}, offset: {}", topic, queueId, queueOffset);
                    return;
                }
                long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqItem.getByteBuffer());
                int size = CQItemBufferUtil.getSize(cqItem.getByteBuffer());
                long tagCode = CQItemBufferUtil.getTagCode(cqItem.getByteBuffer());
                cqItem.release();

                SelectMappedBufferResult message = defaultStore.selectOneMessageByOffset(commitLogOffset, size);
                if (message == null) {
                    logger.error("TieredDispatcher#dispatchFlatFile: dispatch failed, " +
                            "can not get message from next store: topic: {}, queueId: {}, commitLog offset: {}, size: {}",
                        topic, queueId, commitLogOffset, size);
                    break;
                }
                AppendResult result = flatFile.appendCommitLog(message.getByteBuffer(), true);
                long newCommitLogOffset = flatFile.getCommitLogMaxOffset() - message.getByteBuffer().remaining();
                handleAppendCommitLogResult(result, flatFile, queueOffset, flatFile.getDispatchOffset(), newCommitLogOffset, size, tagCode, message.getByteBuffer());
                message.release();
                if (result != AppendResult.SUCCESS) {
                    queueOffset--;
                    break;
                }
            }
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, mq.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, mq.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(queueOffset - beforeOffset, attributes);
        } finally {
            flatFile.getCompositeFlatFileLock().unlock();
        }

        // If this queue dispatch falls too far, dispatch again immediately
        if (flatFile.getDispatchOffset() < maxOffsetInQueue && !flatFile.getCompositeFlatFileLock().isLocked()) {
            dispatchFlatFileAsync(flatFile);
        }
    }

    public void handleAppendCommitLogResult(AppendResult result, CompositeQueueFlatFile flatFile,
        long queueOffset, long dispatchOffset, long newCommitLogOffset, int size, long tagCode, ByteBuffer message) {
        MessageQueue mq = flatFile.getMessageQueue();
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();

        switch (result) {
            case SUCCESS:
                break;
            case OFFSET_INCORRECT:
                long offset = MessageBufferUtil.getQueueOffset(message);
                if (queueOffset != offset) {
                    logger.error("[Bug]Dispatch append commit log, result={}, offset={}, msg offset={}", queueOffset, offset);
                }
                return;
            case BUFFER_FULL:
                logger.debug("Commitlog buffer full, result={}, topic={}, queueId={}, offset={}",result, topic, queueId, queueOffset);
                return;
            default:
                logger.info("Commitlog append failed, result={}, topic={}, queueId={}, offset={}", result, topic, queueId, queueOffset);
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
            List<DispatchRequest> requestList = dispatchRequestWriteMap.computeIfAbsent(flatFile, k -> new ArrayList<>());
            requestList.add(dispatchRequest);
            if (requestList.get(0).getConsumeQueueOffset() >= flatFile.getConsumeQueueMaxOffset()) {
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
                dispatchRequestReadMap.forEach((flatFile, requestList) -> {
                    if (requestList.isEmpty()) {
                        logger.warn("[Bug]TieredDispatcher#sendBackDispatchRequestList: requestList is empty, no need to send back: topic: {}, queueId: {}", flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId());
                        return;
                    }
                    List<DispatchRequest> requestListToWrite = dispatchRequestWriteMap.computeIfAbsent(flatFile, k -> new ArrayList<>());
                    if (!requestListToWrite.isEmpty() && requestList.get(requestList.size() - 1).getConsumeQueueOffset() > requestListToWrite.get(0).getConsumeQueueOffset()) {
                        logger.warn("[Bug]TieredDispatcher#sendBackDispatchRequestList: dispatch request list is not continuous: topic: {}, queueId: {}, last list max offset: {}, new list min offset: {}",
                            flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(),
                            requestList.get(requestList.size() - 1).getConsumeQueueOffset(), requestListToWrite.get(0).getConsumeQueueOffset());
                        requestList.sort(Comparator.comparingLong(DispatchRequest::getConsumeQueueOffset));
                    }
                    requestList.addAll(requestListToWrite);
                    dispatchRequestWriteMap.put(flatFile, requestList);
                });
                dispatchRequestReadMap = new ConcurrentHashMap<>();
            } finally {
                dispatchRequestListLock.unlock();
            }
        }
    }

    protected void buildConsumeQueueAndIndexFile() {
        swapDispatchRequestList();
        Map<MessageQueue, Long> cqMetricsMap = new HashMap<>();
        Map<MessageQueue, Long> ifMetricsMap = new HashMap<>();

        for (Map.Entry<CompositeQueueFlatFile, List<DispatchRequest>> entry : dispatchRequestReadMap.entrySet()) {
            CompositeQueueFlatFile flatFile = entry.getKey();
            List<DispatchRequest> requestList = entry.getValue();
            if (flatFile.isClosed()) {
                requestList.clear();
            }

            MessageQueue messageQueue = flatFile.getMessageQueue();
            Iterator<DispatchRequest> iterator = requestList.iterator();
            while (iterator.hasNext()) {
                DispatchRequest request = iterator.next();

                // remove expired request
                if (request.getConsumeQueueOffset() < flatFile.getConsumeQueueMaxOffset()) {
                    iterator.remove();
                    continue;
                }

                // wait uploading commitLog
                if (flatFile.getCommitLogDispatchCommitOffset() < request.getConsumeQueueOffset()) {
                    break;
                }

                // build consume queue
                AppendResult result = flatFile.appendConsumeQueue(request, true);

                if (AppendResult.SUCCESS.equals(result)) {
                    long cqCount = cqMetricsMap.computeIfAbsent(messageQueue, key -> 0L);
                    cqMetricsMap.put(messageQueue, cqCount + 1);
                    // build index
                    if (storeConfig.isMessageIndexEnable()) {
                        result = flatFile.appendIndexFile(request);
                        if (AppendResult.SUCCESS.equals(result)) {
                            long ifCount = ifMetricsMap.computeIfAbsent(messageQueue, key -> 0L);
                            ifMetricsMap.put(messageQueue, ifCount + 1);
                            iterator.remove();
                        } else {
                            logger.warn("Build indexFile failed, result: {}, topic: {}, queue: {}, queue offset: {}",
                                result, request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
                        }
                    }
                    continue;
                }

                if (AppendResult.OFFSET_INCORRECT.equals(result)) {
                    logger.error("Build consumeQueue and indexFile failed, offset is messed up, " +
                            "try to rebuild cq: topic: {}, queue: {}, queue offset: {}, max queue offset: {}",
                        request.getTopic(), request.getQueueId(),
                        request.getConsumeQueueOffset(), flatFile.getConsumeQueueMaxOffset());

                    try {
                        flatFile.getCompositeFlatFileLock().lock();
                        // rollback dispatch offset, this operation will cause duplicate message in commitLog
                        flatFile.initOffset(flatFile.getConsumeQueueMaxOffset());
                        // clean invalid dispatch request
                        dispatchRequestWriteMap.remove(flatFile);
                        requestList.clear();
                    } finally {
                        flatFile.getCompositeFlatFileLock().unlock();
                    }
                    break;
                }

                logger.warn("Build consumeQueue failed, result: {}, topic: {}, queue: {}, queue offset: {}",
                    result, request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
            }

            // remove empty list, prevent send back
            if (requestList.isEmpty()) {
                dispatchRequestReadMap.remove(flatFile);
            }
        }

        cqMetricsMap.forEach((messageQueue, count) -> {
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, messageQueue.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, messageQueue.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.CONSUME_QUEUE.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(count, attributes);
        });

        ifMetricsMap.forEach((messageQueue, count) -> {
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, messageQueue.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, messageQueue.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.INDEX.name().toLowerCase())
                .build();
            TieredStoreMetricsManager.messagesDispatchTotal.add(count, attributes);
        });

        sendBackDispatchRequestList();
    }

    // Allow work-stealing
    public void doDispatchTask() {
        try {
            dispatchLock.lock();
            buildConsumeQueueAndIndexFile();
        } catch (Exception e) {
            logger.error("Build consumeQueue and indexFile failed", e);
        } finally {
            dispatchLock.unlock();
        }
    }

    @Override
    public String getServiceName() {
        return "TieredStoreDispatcherService";
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(1000);
            doDispatchTask();
        }
    }
}
