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
    private final ReentrantLock dispatchTaskLock;
    private final ReentrantLock dispatchWriteLock;

    private ConcurrentMap<CompositeQueueFlatFile, List<DispatchRequest>> dispatchRequestReadMap;
    private ConcurrentMap<CompositeQueueFlatFile, List<DispatchRequest>> dispatchRequestWriteMap;

    public TieredDispatcher(MessageStore defaultStore, TieredMessageStoreConfig storeConfig) {
        this.defaultStore = defaultStore;
        this.storeConfig = storeConfig;
        this.brokerName = storeConfig.getBrokerName();
        this.tieredFlatFileManager = TieredFlatFileManager.getInstance(storeConfig);
        this.dispatchRequestReadMap = new ConcurrentHashMap<>();
        this.dispatchRequestWriteMap = new ConcurrentHashMap<>();
        this.dispatchTaskLock = new ReentrantLock();
        this.dispatchWriteLock = new ReentrantLock();
        this.initScheduleTask();
    }

    private void initScheduleTask() {
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() ->
            tieredFlatFileManager.deepCopyFlatFileToList().forEach(flatFile -> {
                if (!flatFile.getCompositeFlatFileLock().isLocked()) {
                    dispatchFlatFile(flatFile);
                }
            }), 30, 10, TimeUnit.SECONDS);
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

        CompositeQueueFlatFile flatFile = tieredFlatFileManager.getOrCreateFlatFileIfAbsent(
            new MessageQueue(topic, brokerName, request.getQueueId()));

        if (flatFile == null) {
            logger.error("[Bug] TieredDispatcher#dispatch: get or create flat file failed, skip this request. ",
                "topic: {}, queueId: {}", request.getTopic(), request.getQueueId());
            return;
        }

        if (detectFallBehind(flatFile)) {
            return;
        }

        // Set cq offset as commitlog first dispatch offset if flat file first init
        if (flatFile.getDispatchOffset() == -1) {
            flatFile.initOffset(request.getConsumeQueueOffset());
        }

        if (request.getConsumeQueueOffset() == flatFile.getDispatchOffset()) {

            // In order to ensure the efficiency of dispatch operation and avoid high dispatch delay,
            // it is not allowed to block for a long time here.
            try {
                // Acquired flat file write lock to append commitlog
                if (flatFile.getCompositeFlatFileLock().isLocked()
                    || !flatFile.getCompositeFlatFileLock().tryLock(3, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (Exception e) {
                logger.warn("Temporarily skip dispatch request because we can not acquired write lock. " +
                    "topic: {}, queueId: {}", request.getTopic(), request.getQueueId(), e);
                if (flatFile.getCompositeFlatFileLock().isLocked()) {
                    flatFile.getCompositeFlatFileLock().unlock();
                }
                return;
            }

            // double check whether the offset matches
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
                doRedispatchRequestToWriteMap(result, flatFile, request.getConsumeQueueOffset(),
                    newCommitLogOffset, request.getMsgSize(), request.getTagsCode(), message.getByteBuffer());

                if (result == AppendResult.SUCCESS) {
                    Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                        .put(TieredStoreMetricsConstant.LABEL_TOPIC, request.getTopic())
                        .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, request.getQueueId())
                        .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE,
                            FileSegmentType.COMMIT_LOG.name().toLowerCase())
                        .build();
                    TieredStoreMetricsManager.messagesDispatchTotal.add(1, attributes);
                }
            } catch (Exception throwable) {
                logger.error("TieredDispatcher#dispatch: dispatch has unexpected problem. " +
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
                logger.error("[Bug] TieredDispatcher#dispatchFlatFileAsync failed, topic: {}, queueId: {}",
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

        if (flatFile.getDispatchOffset() == -1L) {
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

        // perhaps it was caused by local cq file corruption or ha truncation
        if (beforeOffset >= maxOffsetInQueue) {
            return;
        }

        try {
            if (!flatFile.getCompositeFlatFileLock().tryLock(200, TimeUnit.MILLISECONDS)) {
                return;
            }
        } catch (Exception e) {
            logger.warn("TieredDispatcher#dispatchFlatFile: can not acquire flatFile lock, " +
                "topic: {}, queueId: {}", mq.getTopic(), mq.getQueueId(), e);
            if (flatFile.getCompositeFlatFileLock().isLocked()) {
                flatFile.getCompositeFlatFileLock().unlock();
            }
            return;
        }

        try {
            long dispatchOffset = flatFile.getDispatchOffset();
            if (dispatchOffset < minOffsetInQueue) {
                // If the tiered storage feature is turned off midway,
                // it may cause cq discontinuity, resulting in data loss here.
                logger.warn("TieredDispatcher#dispatchFlatFile: dispatch offset is too small, " +
                        "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                    topic, queueId, dispatchOffset, minOffsetInQueue, maxOffsetInQueue);
                flatFile.initOffset(minOffsetInQueue);
                dispatchOffset = minOffsetInQueue;
            }
            beforeOffset = dispatchOffset;

            // flow control by max count, also we could do flow control based on message size
            long maxCount = storeConfig.getTieredStoreGroupCommitCount();
            long upperBound = Math.min(dispatchOffset + maxCount, maxOffsetInQueue);
            ConsumeQueue consumeQueue = (ConsumeQueue) defaultStore.getConsumeQueue(topic, queueId);

            for (; dispatchOffset < upperBound; dispatchOffset++) {
                // get consume queue
                SelectMappedBufferResult cqItem = consumeQueue.getIndexBuffer(dispatchOffset);
                if (cqItem == null) {
                    logger.error("[Bug] TieredDispatcher#dispatchFlatFile: cq item is null, " +
                            "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                        topic, queueId, dispatchOffset, minOffsetInQueue, maxOffsetInQueue);
                    return;
                }
                long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqItem.getByteBuffer());
                int size = CQItemBufferUtil.getSize(cqItem.getByteBuffer());
                long tagCode = CQItemBufferUtil.getTagCode(cqItem.getByteBuffer());
                cqItem.release();

                // get message
                SelectMappedBufferResult message = defaultStore.selectOneMessageByOffset(commitLogOffset, size);
                if (message == null) {
                    logger.error("TieredDispatcher#dispatchFlatFile: get message from next store failed, " +
                            "topic: {}, queueId: {}, commitLog offset: {}, size: {}",
                        topic, queueId, commitLogOffset, size);
                    break;
                }

                // append commitlog will increase dispatch offset here
                AppendResult result = flatFile.appendCommitLog(message.getByteBuffer(), true);
                long newCommitLogOffset = flatFile.getCommitLogMaxOffset() - message.getByteBuffer().remaining();
                doRedispatchRequestToWriteMap(
                    result, flatFile, dispatchOffset, newCommitLogOffset, size, tagCode, message.getByteBuffer());
                message.release();
                if (result != AppendResult.SUCCESS) {
                    dispatchOffset--;
                    break;
                }
            }

            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, mq.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, mq.getQueueId())
                .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                .build();

            TieredStoreMetricsManager.messagesDispatchTotal.add(dispatchOffset - beforeOffset, attributes);
        } finally {
            flatFile.getCompositeFlatFileLock().unlock();
        }

        // If this queue dispatch falls too far, dispatch again immediately
        if (flatFile.getDispatchOffset() < maxOffsetInQueue && !flatFile.getCompositeFlatFileLock().isLocked()) {
            dispatchFlatFileAsync(flatFile);
        }
    }

    // Submit cq to write map if append commitlog success
    public void doRedispatchRequestToWriteMap(AppendResult result, CompositeQueueFlatFile flatFile,
        long queueOffset, long newCommitLogOffset, int size, long tagCode, ByteBuffer message) {

        MessageQueue mq = flatFile.getMessageQueue();
        String topic = mq.getTopic();
        int queueId = mq.getQueueId();

        switch (result) {
            case SUCCESS:
                break;
            case OFFSET_INCORRECT:
                long offset = MessageBufferUtil.getQueueOffset(message);
                if (queueOffset != offset) {
                    logger.error("[Bug] Commitlog offset incorrect, " +
                            "result={}, topic={}, queueId={}, offset={}, msg offset={}",
                        result, topic, queueId, queueOffset, offset);
                }
                return;
            case BUFFER_FULL:
                logger.debug("Commitlog buffer full, result={}, topic={}, queueId={}, offset={}",
                    result, topic, queueId, queueOffset);
                return;
            default:
                logger.info("Commitlog append failed, result={}, topic={}, queueId={}, offset={}",
                    result, topic, queueId, queueOffset);
                return;
        }

        dispatchWriteLock.lock();
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
            List<DispatchRequest> requestList =
                dispatchRequestWriteMap.computeIfAbsent(flatFile, k -> new ArrayList<>());
            requestList.add(dispatchRequest);
            if (requestList.get(0).getConsumeQueueOffset() >= flatFile.getConsumeQueueMaxOffset()) {
                wakeup();
            }
        } finally {
            dispatchWriteLock.unlock();
        }
    }

    public void swapDispatchRequestList() {
        dispatchWriteLock.lock();
        try {
            dispatchRequestReadMap = dispatchRequestWriteMap;
            dispatchRequestWriteMap = new ConcurrentHashMap<>();
        } finally {
            dispatchWriteLock.unlock();
        }
    }

    public void copySurvivorObject() {
        if (dispatchRequestReadMap.isEmpty()) {
            return;
        }

        try {
            dispatchWriteLock.lock();
            dispatchRequestReadMap.forEach((flatFile, requestList) -> {
                String topic = flatFile.getMessageQueue().getTopic();
                int queueId = flatFile.getMessageQueue().getQueueId();
                if (requestList.isEmpty()) {
                    logger.warn("Copy survivor object failed, dispatch request list is empty, " +
                        "topic: {}, queueId: {}", topic, queueId);
                    return;
                }

                List<DispatchRequest> requestListToWrite =
                    dispatchRequestWriteMap.computeIfAbsent(flatFile, k -> new ArrayList<>());

                if (!requestListToWrite.isEmpty()) {
                    long readOffset = requestList.get(requestList.size() - 1).getConsumeQueueOffset();
                    long writeOffset = requestListToWrite.get(0).getConsumeQueueOffset();
                    if (readOffset > writeOffset) {
                        logger.warn("Copy survivor object failed, offset in request list are not continuous. " +
                                "topic: {}, queueId: {}, read offset: {}, write offset: {}",
                            topic, queueId, readOffset, writeOffset);

                        // sort request list according cq offset
                        requestList.sort(Comparator.comparingLong(DispatchRequest::getConsumeQueueOffset));
                    }
                }

                requestList.addAll(requestListToWrite);
                dispatchRequestWriteMap.put(flatFile, requestList);
            });
            dispatchRequestReadMap = new ConcurrentHashMap<>();
        } finally {
            dispatchWriteLock.unlock();
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

                // handle build cq result
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
                            logger.warn("Build index failed, skip this message, " +
                                    "result: {}, topic: {}, queue: {}, request offset: {}",
                                result, request.getTopic(), request.getQueueId(), request.getConsumeQueueOffset());
                        }
                    }
                    continue;
                }

                if (AppendResult.OFFSET_INCORRECT.equals(result)) {
                    logger.error("Consume queue offset incorrect, try to recreated consume queue, " +
                            "result: {}, topic: {}, queue: {}, request offset: {}, current cq offset: {}",
                        result, request.getTopic(), request.getQueueId(),
                        request.getConsumeQueueOffset(), flatFile.getConsumeQueueMaxOffset());

                    try {
                        flatFile.getCompositeFlatFileLock().lock();

                        // reset dispatch offset, this operation will cause duplicate message in commitLog
                        long minOffsetInQueue =
                            defaultStore.getMinOffsetInQueue(request.getTopic(), request.getQueueId());

                        // when dispatch offset is smaller than min offset in local cq
                        // some messages may be lost at this time
                        if (flatFile.getConsumeQueueMaxOffset() < minOffsetInQueue) {
                            // if we use flatFile.destroy() directly will cause manager reference leak.
                            tieredFlatFileManager.destroyCompositeFile(flatFile.getMessageQueue());
                            logger.warn("Found cq max offset is smaller than local cq min offset, " +
                                    "so destroy tiered flat file to recreated, topic: {}, queueId: {}",
                                request.getTopic(), request.getQueueId());
                        } else {
                            flatFile.initOffset(flatFile.getConsumeQueueMaxOffset());
                        }

                        // clean invalid dispatch request
                        dispatchRequestWriteMap.remove(flatFile);
                        requestList.clear();
                    } finally {
                        flatFile.getCompositeFlatFileLock().unlock();
                    }
                    break;
                }

                // other append result
                logger.warn("Append consume queue failed, result: {}, topic: {}, queue: {}, request offset: {}",
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

        copySurvivorObject();
    }

    // Allow work-stealing
    public void doDispatchTask() {
        try {
            dispatchTaskLock.lock();
            buildConsumeQueueAndIndexFile();
        } catch (Exception e) {
            logger.error("Tiered storage do dispatch task failed", e);
        } finally {
            dispatchTaskLock.unlock();
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
