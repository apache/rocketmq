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
package org.apache.rocketmq.tieredstore.container;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.InflightRequestFuture;
import org.apache.rocketmq.tieredstore.common.InflightRequestKey;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.CQItemBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredMessageQueueContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private volatile boolean closed = false;

    private final MessageQueue messageQueue;
    private final int topicId;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredMetadataStore metadataStore;
    private final TieredCommitLog commitLog;
    private final TieredConsumeQueue consumeQueue;
    private final TieredIndexFile indexFile;

    private QueueMetadata queueMetadata;

    private long dispatchOffset;

    private final ReentrantLock queueLock = new ReentrantLock();

    private int readAheadFactor;
    private final Cache<String, Long> groupOffsetCache;
    private final ConcurrentMap<InflightRequestKey, InflightRequestFuture> inFlightRequestMap;

    public TieredMessageQueueContainer(MessageQueue messageQueue, TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {
        this.messageQueue = messageQueue;
        this.storeConfig = storeConfig;
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);

        TopicMetadata topicMetadata = metadataStore.getTopic(messageQueue.getTopic());
        if (topicMetadata == null) {
            // TODO specify reserveTime for each topic
            topicMetadata = metadataStore.addTopic(messageQueue.getTopic(), -1L);
        }
        this.topicId = topicMetadata.getTopicId();

        queueMetadata = metadataStore.getQueue(messageQueue);
        if (queueMetadata == null) {
            queueMetadata = metadataStore.addQueue(messageQueue, -1);
        }
        if (queueMetadata.getMaxOffset() < queueMetadata.getMinOffset()) {
            queueMetadata.setMaxOffset(queueMetadata.getMinOffset());
        }
        this.dispatchOffset = queueMetadata.getMaxOffset();

        this.commitLog = new TieredCommitLog(messageQueue, storeConfig);
        this.consumeQueue = new TieredConsumeQueue(messageQueue, storeConfig);
        if (!consumeQueue.isInitialized() && this.dispatchOffset != -1) {
            consumeQueue.setBaseOffset(this.dispatchOffset * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        }
        this.indexFile = TieredContainerManager.getIndexFile(storeConfig);
        this.readAheadFactor = storeConfig.getReadAheadMinFactor();
        this.inFlightRequestMap = new ConcurrentHashMap<>();
        this.groupOffsetCache = Caffeine.newBuilder()
            .expireAfterWrite(2, TimeUnit.MINUTES)
            .removalListener((key, value, cause) -> {
                if (cause.equals(RemovalCause.EXPIRED)) {
                    inFlightRequestMap.remove(new InflightRequestKey((String) key));
                }
            }).build();
    }

    public boolean isClosed() {
        return closed;
    }

    public ReentrantLock getQueueLock() {
        return queueLock;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public long getCommitLogMinOffset() {
        return commitLog.getMinOffset();
    }

    public long getCommitLogMaxOffset() {
        return commitLog.getMaxOffset();
    }

    public long getCommitLogBeginTimestamp() {
        return commitLog.getBeginTimestamp();
    }

    public long getConsumeQueueBaseOffset() {
        return consumeQueue.getBaseOffset();
    }

    public long getConsumeQueueMinOffset() {
        return consumeQueue.getMinOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE;
    }

    public long getConsumeQueueCommitOffset() {
        return consumeQueue.getCommitOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE;
    }

    public long getConsumeQueueMaxOffset() {
        return consumeQueue.getMaxOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE;
    }

    public long getConsumeQueueEndTimestamp() {
        return consumeQueue.getEndTimestamp();
    }

    // CQ offset
    public long getDispatchOffset() {
        return dispatchOffset;
    }

    public CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset) {
        return readConsumeQueue(queueOffset).thenComposeAsync(cqBuffer -> {
            long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
            int length = CQItemBufferUtil.getSize(cqBuffer);
            return readCommitLog(commitLogOffset, length);
        });
    }

    public long binarySearchInQueueByTime(long timestamp, BoundaryType boundaryType) {
        Pair<Long, Long> pair = consumeQueue.getQueueOffsetInFileByTime(timestamp, boundaryType);
        long minQueueOffset = pair.getLeft();
        long maxQueueOffset = pair.getRight();

        if (maxQueueOffset == -1 || maxQueueOffset < minQueueOffset) {
            return -1L;
        }

        long low = minQueueOffset;
        long high = maxQueueOffset;

        long offset = 0;

        // Handle the following corner cases first:
        // 1. store time of (high) < timestamp
        // 2. store time of (low) > timestamp
        long storeTime;
        // Handle case 1
        ByteBuffer message = getMessageAsync(maxQueueOffset).join();
        storeTime = MessageBufferUtil.getStoreTimeStamp(message);
        if (storeTime < timestamp) {
            switch (boundaryType) {
                case LOWER:
                    return maxQueueOffset + 1;
                case UPPER:
                    return maxQueueOffset;
                default:
                    LOGGER.warn("TieredMessageQueueContainer#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        }

        // Handle case 2
        message = getMessageAsync(minQueueOffset).join();
        storeTime = MessageBufferUtil.getStoreTimeStamp(message);
        if (storeTime > timestamp) {
            switch (boundaryType) {
                case LOWER:
                    return minQueueOffset;
                case UPPER:
                    return 0L;
                default:
                    LOGGER.warn("TieredMessageQueueContainer#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        }

        // Perform binary search
        long midOffset = -1;
        long targetOffset = -1;
        long leftOffset = -1;
        long rightOffset = -1;
        while (high >= low) {
            midOffset = (low + high) / 2;
            message = getMessageAsync(midOffset).join();
            storeTime = MessageBufferUtil.getStoreTimeStamp(message);
            if (storeTime == timestamp) {
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
            // We just found ONE matched record. These next to it might also share the same store-timestamp.
            offset = targetOffset;
            long previousAttempt = targetOffset;
            switch (boundaryType) {
                case LOWER:
                    while (true) {
                        long attempt = previousAttempt - 1;
                        if (attempt < minQueueOffset) {
                            break;
                        }
                        message = getMessageAsync(attempt).join();
                        storeTime = MessageBufferUtil.getStoreTimeStamp(message);
                        if (storeTime == timestamp) {
                            previousAttempt = attempt;
                            continue;
                        }
                        break;
                    }
                    offset = previousAttempt;
                    break;
                case UPPER:
                    while (true) {
                        long attempt = previousAttempt + 1;
                        if (attempt > maxQueueOffset) {
                            break;
                        }

                        message = getMessageAsync(attempt).join();
                        storeTime = MessageBufferUtil.getStoreTimeStamp(message);
                        if (storeTime == timestamp) {
                            previousAttempt = attempt;
                            continue;
                        }
                        break;
                    }
                    offset = previousAttempt;
                    break;
                default:
                    LOGGER.warn("TieredMessageQueueContainer#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        } else {
            // Given timestamp does not have any message records. But we have a range enclosing the
            // timestamp.
            /*
             * Consider the follow case: t2 has no consume queue entry and we are searching offset of
             * t2 for lower and upper boundaries.
             *  --------------------------
             *   timestamp   Consume Queue
             *       t1          1
             *       t1          2
             *       t1          3
             *       t3          4
             *       t3          5
             *   --------------------------
             * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
             * contradictory at first sight, but it does make sense when performing range queries.
             */
            switch (boundaryType) {
                case LOWER: {
                    offset = rightOffset;
                    break;
                }

                case UPPER: {
                    offset = leftOffset;
                    break;
                }
                default: {
                    LOGGER.warn("TieredMessageQueueContainer#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
                }
            }
        }
        return offset;
    }

    public void initOffset(long offset) {
        if (!consumeQueue.isInitialized()) {
            queueMetadata.setMinOffset(offset);
            queueMetadata.setMaxOffset(offset);
        }
        if (!consumeQueue.isInitialized()) {
            consumeQueue.setBaseOffset(offset * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
        }
        dispatchOffset = offset;
    }

    // CQ offset
    public long getBuildCQMaxOffset() {
        return commitLog.getCommitMsgQueueOffset();
    }

    public AppendResult appendCommitLog(ByteBuffer message) {
        return appendCommitLog(message, false);
    }

    public AppendResult appendCommitLog(ByteBuffer message, boolean commit) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        long queueOffset = MessageBufferUtil.getQueueOffset(message);
        if (queueOffset != dispatchOffset) {
            return AppendResult.OFFSET_INCORRECT;
        }

        AppendResult result = commitLog.append(message, commit);
        if (result == AppendResult.SUCCESS) {
            dispatchOffset++;
        }

        return result;
    }

    public AppendResult appendConsumeQueue(DispatchRequest request) {
        return appendConsumeQueue(request, false);
    }

    public AppendResult appendConsumeQueue(DispatchRequest request, boolean commit) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        if (request.getConsumeQueueOffset() != getConsumeQueueMaxOffset()) {
            return AppendResult.OFFSET_INCORRECT;
        }

        return consumeQueue.append(request.getCommitLogOffset(), request.getMsgSize(), request.getTagsCode(), request.getStoreTimestamp(), commit);
    }

    public AppendResult appendIndexFile(DispatchRequest request) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        // building indexes with offsetId is no longer supported because offsetId has changed in tiered storage
//        AppendResult result = indexFile.append(messageQueue, request.getOffsetId(), request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
//        if (result != AppendResult.SUCCESS) {
//            return result;
//        }

        if (StringUtils.isNotBlank(request.getUniqKey())) {
            AppendResult result = indexFile.append(messageQueue, topicId, request.getUniqKey(), request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
            if (result != AppendResult.SUCCESS) {
                return result;
            }
        }

        for (String key : request.getKeys().split(MessageConst.KEY_SEPARATOR)) {
            if (StringUtils.isNotBlank(key)) {
                AppendResult result = indexFile.append(messageQueue, topicId, key, request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
                if (result != AppendResult.SUCCESS) {
                    return result;
                }
            }
        }

        return AppendResult.SUCCESS;
    }

    public CompletableFuture<ByteBuffer> readCommitLog(long offset, int length) {
        return commitLog.readAsync(offset, length);
    }

    public CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset) {
        return readConsumeQueue(queueOffset, 1);
    }

    public CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset, int count) {
        return consumeQueue.readAsync(queueOffset * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE, count * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
    }

    public void flushMetadata() {
        try {
            if (consumeQueue.getCommitOffset() < queueMetadata.getMinOffset()) {
                return;
            }
            queueMetadata.setMaxOffset(consumeQueue.getCommitOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
            metadataStore.updateQueue(queueMetadata);
        } catch (Exception e) {
            LOGGER.error("TieredMessageQueueContainer#flushMetadata: update queue metadata failed: topic: {}, queue: {}", messageQueue.getTopic(), messageQueue.getQueueId(), e);
        }
    }

    public void commitCommitLog() {
        commitLog.commit(true);
    }

    public void commitConsumeQueue() {
        consumeQueue.commit(true);
    }

    public void cleanExpiredFile(long expireTimestamp) {
        commitLog.cleanExpiredFile(expireTimestamp);
        consumeQueue.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        commitLog.destroyExpiredFile();
        consumeQueue.destroyExpiredFile();
    }

    public void commit(boolean sync) {
        commitLog.commit(sync);
        consumeQueue.commit(sync);
    }

    public void increaseReadAheadFactor() {
        readAheadFactor = Math.min(readAheadFactor + 1, storeConfig.getReadAheadMaxFactor());
    }

    public void decreaseReadAheadFactor() {
        readAheadFactor = Math.max(readAheadFactor - 1, storeConfig.getReadAheadMinFactor());
    }

    public void setNotReadAhead() {
        readAheadFactor = 1;
    }

    public int getReadAheadFactor() {
        return readAheadFactor;
    }

    public void recordGroupAccess(String group, long offset) {
        groupOffsetCache.put(group, offset);
    }

    public long getActiveGroupCount(long minOffset, long maxOffset) {
        return groupOffsetCache.asMap()
            .values()
            .stream()
            .filter(offset -> offset >= minOffset && offset <= maxOffset)
            .count();
    }

    public long getActiveGroupCount() {
        return groupOffsetCache.estimatedSize();
    }

    public InflightRequestFuture getInflightRequest(long offset, int batchSize) {
        Optional<InflightRequestFuture> optional = inFlightRequestMap.entrySet()
            .stream()
            .filter(entry -> {
                InflightRequestKey key = entry.getKey();
                return Math.max(key.getOffset(), offset) <= Math.min(key.getOffset() + key.getBatchSize(), offset + batchSize);
            })
            .max(Comparator.comparing(entry -> entry.getKey().getRequestTime()))
            .map(Map.Entry::getValue);
        return optional.orElseGet(() -> new InflightRequestFuture(Long.MAX_VALUE, new ArrayList<>()));
    }

    public InflightRequestFuture getInflightRequest(String group, long offset, int batchSize) {
        InflightRequestFuture future = inFlightRequestMap.get(new InflightRequestKey(group));
        if (future != null && !future.isAllDone()) {
            return future;
        }
        return getInflightRequest(offset, batchSize);
    }

    public void putInflightRequest(String group, long offset, int requestMsgCount,
        List<Pair<Integer, CompletableFuture<Long>>> futureList) {
        InflightRequestKey key = new InflightRequestKey(group, offset, requestMsgCount);
        inFlightRequestMap.remove(key);
        inFlightRequestMap.putIfAbsent(key, new InflightRequestFuture(offset, futureList));
    }

    @Override
    public int hashCode() {
        return messageQueue.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return messageQueue.equals(((TieredMessageQueueContainer) obj).messageQueue);
    }

    public void shutdown() {
        closed = true;
        commitLog.commit(true);
        consumeQueue.commit(true);
        flushMetadata();
    }

    public void destroy() {
        closed = true;
        commitLog.destroy();
        consumeQueue.destroy();
        try {
            metadataStore.deleteFileSegment(messageQueue);
            metadataStore.deleteQueue(messageQueue);
        } catch (Exception e) {
            LOGGER.error("TieredMessageQueueContainer#destroy: clean metadata failed: ", e);
        }
    }
}
