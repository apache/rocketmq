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
package org.apache.rocketmq.tieredstore.file;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.InFlightRequestFuture;
import org.apache.rocketmq.tieredstore.common.InFlightRequestKey;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.CQItemBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.apache.rocketmq.common.BoundaryType;

public class CompositeFlatFile implements CompositeAccess {

    protected static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected volatile boolean closed = false;
    protected int readAheadFactor;

    /**
     * Dispatch offset represents the offset of the messages that have been
     * dispatched to the current chunk, indicating the progress of the message distribution.
     * It's consume queue current offset.
     */
    protected final AtomicLong dispatchOffset;

    protected final ReentrantLock compositeFlatFileLock;
    protected final TieredMessageStoreConfig storeConfig;
    protected final TieredMetadataStore metadataStore;

    protected final String filePath;
    protected final TieredCommitLog commitLog;
    protected final TieredConsumeQueue consumeQueue;
    protected final Cache<String, Long> groupOffsetCache;
    protected final ConcurrentMap<InFlightRequestKey, InFlightRequestFuture> inFlightRequestMap;

    public CompositeFlatFile(TieredFileAllocator fileQueueFactory, String filePath) {
        this.filePath = filePath;
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.readAheadFactor = this.storeConfig.getReadAheadMinFactor();
        this.metadataStore = TieredStoreUtil.getMetadataStore(this.storeConfig);
        this.compositeFlatFileLock = new ReentrantLock();
        this.inFlightRequestMap = new ConcurrentHashMap<>();
        this.commitLog = new TieredCommitLog(fileQueueFactory, filePath);
        this.consumeQueue = new TieredConsumeQueue(fileQueueFactory, filePath);
        this.dispatchOffset = new AtomicLong(
            this.consumeQueue.isInitialized() ? this.getConsumeQueueCommitOffset() : -1L);
        this.groupOffsetCache = this.initOffsetCache();
    }

    private Cache<String, Long> initOffsetCache() {
        return Caffeine.newBuilder()
            .expireAfterWrite(2, TimeUnit.MINUTES)
            .removalListener((key, value, cause) -> {
                if (cause.equals(RemovalCause.EXPIRED)) {
                    inFlightRequestMap.remove(new InFlightRequestKey((String) key));
                }
            }).build();
    }

    public boolean isClosed() {
        return closed;
    }

    public ReentrantLock getCompositeFlatFileLock() {
        return compositeFlatFileLock;
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

    @Override
    public long getCommitLogDispatchCommitOffset() {
        return commitLog.getDispatchCommitOffset();
    }

    public long getConsumeQueueBaseOffset() {
        return consumeQueue.getBaseOffset();
    }

    public long getConsumeQueueMinOffset() {
        long cqOffset = consumeQueue.getMinOffset() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE;
        long effectiveOffset = this.commitLog.getMinConsumeQueueOffset();
        return Math.max(cqOffset, effectiveOffset);
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

    public long getDispatchOffset() {
        return dispatchOffset.get();
    }

    @Override
    public CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset) {
        return getConsumeQueueAsync(queueOffset).thenComposeAsync(cqBuffer -> {
            long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
            int length = CQItemBufferUtil.getSize(cqBuffer);
            return getCommitLogAsync(commitLogOffset, length);
        });
    }

    @Override
    public long getOffsetInConsumeQueueByTime(long timestamp, BoundaryType boundaryType) {
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
                    LOGGER.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
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
                    LOGGER.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
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
                    LOGGER.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
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
                    LOGGER.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
                }
            }
        }
        return offset;
    }

    @Override
    public void initOffset(long offset) {
        if (consumeQueue.isInitialized()) {
            dispatchOffset.set(this.getConsumeQueueCommitOffset());
        } else {
            consumeQueue.setBaseOffset(offset * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
            dispatchOffset.set(offset);
        }
    }

    @Override
    public AppendResult appendCommitLog(ByteBuffer message) {
        return appendCommitLog(message, false);
    }

    @Override
    public AppendResult appendCommitLog(ByteBuffer message, boolean commit) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        AppendResult result = commitLog.append(message, commit);
        if (result == AppendResult.SUCCESS) {
            dispatchOffset.incrementAndGet();
        }
        return result;
    }

    @Override
    public AppendResult appendConsumeQueue(DispatchRequest request) {
        return appendConsumeQueue(request, false);
    }

    @Override
    public AppendResult appendConsumeQueue(DispatchRequest request, boolean commit) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

        if (request.getConsumeQueueOffset() != getConsumeQueueMaxOffset()) {
            return AppendResult.OFFSET_INCORRECT;
        }

        return consumeQueue.append(request.getCommitLogOffset(),
            request.getMsgSize(), request.getTagsCode(), request.getStoreTimestamp(), commit);
    }

    @Override
    public CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length) {
        return commitLog.readAsync(offset, length);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset) {
        return getConsumeQueueAsync(queueOffset, 1);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset, int count) {
        return consumeQueue.readAsync(queueOffset * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE,
            count * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
    }

    @Override
    public void commitCommitLog() {
        commitLog.commit(true);
    }

    @Override
    public void commitConsumeQueue() {
        consumeQueue.commit(true);
    }

    @Override
    public void cleanExpiredFile(long expireTimestamp) {
        commitLog.cleanExpiredFile(expireTimestamp);
        consumeQueue.cleanExpiredFile(expireTimestamp);
    }

    @Override
    public void destroyExpiredFile() {
        commitLog.destroyExpiredFile();
        consumeQueue.destroyExpiredFile();
    }

    @Override
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

    public InFlightRequestFuture getInflightRequest(long offset, int batchSize) {
        Optional<InFlightRequestFuture> optional = inFlightRequestMap.entrySet()
            .stream()
            .filter(entry -> {
                InFlightRequestKey key = entry.getKey();
                return Math.max(key.getOffset(), offset) <= Math.min(key.getOffset() + key.getBatchSize(), offset + batchSize);
            })
            .max(Comparator.comparing(entry -> entry.getKey().getRequestTime()))
            .map(Map.Entry::getValue);
        return optional.orElseGet(() -> new InFlightRequestFuture(Long.MAX_VALUE, new ArrayList<>()));
    }

    public InFlightRequestFuture getInflightRequest(String group, long offset, int batchSize) {
        InFlightRequestFuture future = inFlightRequestMap.get(new InFlightRequestKey(group));
        if (future != null && !future.isAllDone()) {
            return future;
        }
        return getInflightRequest(offset, batchSize);
    }

    public void putInflightRequest(String group, long offset, int requestMsgCount,
        List<Pair<Integer, CompletableFuture<Long>>> futureList) {
        InFlightRequestKey key = new InFlightRequestKey(group, offset, requestMsgCount);
        inFlightRequestMap.remove(key);
        inFlightRequestMap.putIfAbsent(key, new InFlightRequestFuture(offset, futureList));
    }

    @Override
    public int hashCode() {
        return filePath.hashCode();
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
        return StringUtils.equals(filePath, ((CompositeFlatFile) obj).filePath);
    }

    public void shutdown() {
        closed = true;
        commitLog.commit(true);
        consumeQueue.commit(true);
    }

    public void destroy() {
        try {
            closed = true;
            compositeFlatFileLock.lock();
            commitLog.destroy();
            consumeQueue.destroy();
            metadataStore.deleteFileSegment(filePath, FileSegmentType.COMMIT_LOG);
            metadataStore.deleteFileSegment(filePath, FileSegmentType.CONSUME_QUEUE);
        } catch (Exception e) {
            LOGGER.error("CompositeFlatFile#destroy: delete file failed", e);
        } finally {
            compositeFlatFileLock.unlock();
        }
    }
}
