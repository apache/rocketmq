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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.common.GetMessageResultExt;
import org.apache.rocketmq.tieredstore.common.InFlightRequestFuture;
import org.apache.rocketmq.tieredstore.common.MessageCacheKey;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.common.SelectBufferResultWrapper;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.file.CompositeFlatFile;
import org.apache.rocketmq.tieredstore.file.CompositeQueueFlatFile;
import org.apache.rocketmq.tieredstore.file.TieredConsumeQueue;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.index.IndexItem;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.TopicMetadata;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.util.CQItemBufferUtil;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredMessageFetcher implements MessageStoreFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final String brokerName;
    private final TieredMetadataStore metadataStore;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFlatFileManager flatFileManager;
    private final Cache<MessageCacheKey, SelectBufferResultWrapper> readAheadCache;

    public TieredMessageFetcher(TieredMessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.brokerName = storeConfig.getBrokerName();
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        this.flatFileManager = TieredFlatFileManager.getInstance(storeConfig);
        this.readAheadCache = this.initCache(storeConfig);
    }

    private Cache<MessageCacheKey, SelectBufferResultWrapper> initCache(TieredMessageStoreConfig storeConfig) {
        long memoryMaxSize =
            (long) (Runtime.getRuntime().maxMemory() * storeConfig.getReadAheadCacheSizeThresholdRate());

        return Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            .expireAfterWrite(storeConfig.getReadAheadCacheExpireDuration(), TimeUnit.MILLISECONDS)
            .maximumWeight(memoryMaxSize)
            // Using the buffer size of messages to calculate memory usage
            .weigher((MessageCacheKey key, SelectBufferResultWrapper msg) -> msg.getBufferSize())
            .recordStats()
            .build();
    }

    @VisibleForTesting
    public Cache<MessageCacheKey, SelectBufferResultWrapper> getMessageCache() {
        return readAheadCache;
    }

    protected void putMessageToCache(CompositeFlatFile flatFile, SelectBufferResultWrapper result) {
        readAheadCache.put(new MessageCacheKey(flatFile, result.getOffset()), result);
    }

    protected SelectBufferResultWrapper getMessageFromCache(CompositeFlatFile flatFile, long offset) {
        return readAheadCache.getIfPresent(new MessageCacheKey(flatFile, offset));
    }

    protected void recordCacheAccess(CompositeFlatFile flatFile,
        String group, long offset, List<SelectBufferResultWrapper> resultWrapperList) {
        if (!resultWrapperList.isEmpty()) {
            offset = resultWrapperList.get(resultWrapperList.size() - 1).getOffset();
        }
        flatFile.recordGroupAccess(group, offset);
        resultWrapperList.forEach(wrapper -> {
            if (wrapper.incrementAndGet() >= flatFile.getActiveGroupCount()) {
                readAheadCache.invalidate(new MessageCacheKey(flatFile, wrapper.getOffset()));
            }
        });
    }

    private void prefetchMessage(CompositeQueueFlatFile flatFile, String group, int maxCount, long nextBeginOffset) {
        if (maxCount == 1 || flatFile.getReadAheadFactor() == 1) {
            return;
        }

        // make sure there is only one request per group and request range
        int prefetchBatchSize = Math.min(maxCount * flatFile.getReadAheadFactor(), storeConfig.getReadAheadMessageCountThreshold());
        InFlightRequestFuture inflightRequest = flatFile.getInflightRequest(group, nextBeginOffset, prefetchBatchSize);
        if (!inflightRequest.isAllDone()) {
            return;
        }

        synchronized (flatFile) {
            inflightRequest = flatFile.getInflightRequest(nextBeginOffset, maxCount);
            if (!inflightRequest.isAllDone()) {
                return;
            }

            long maxOffsetOfLastRequest = inflightRequest.getLastFuture().join();
            boolean lastRequestIsExpired = getMessageFromCache(flatFile, nextBeginOffset) == null;

            if (lastRequestIsExpired ||
                maxOffsetOfLastRequest != -1L && nextBeginOffset >= inflightRequest.getStartOffset()) {

                long queueOffset;
                if (lastRequestIsExpired) {
                    queueOffset = nextBeginOffset;
                    flatFile.decreaseReadAheadFactor();
                } else {
                    queueOffset = maxOffsetOfLastRequest + 1;
                    flatFile.increaseReadAheadFactor();
                }

                int factor = Math.min(flatFile.getReadAheadFactor(), storeConfig.getReadAheadMessageCountThreshold() / maxCount);
                int flag = 0;
                int concurrency = 1;
                if (factor > storeConfig.getReadAheadBatchSizeFactorThreshold()) {
                    flag = factor % storeConfig.getReadAheadBatchSizeFactorThreshold() == 0 ? 0 : 1;
                    concurrency = factor / storeConfig.getReadAheadBatchSizeFactorThreshold() + flag;
                }
                int requestBatchSize = maxCount * Math.min(factor, storeConfig.getReadAheadBatchSizeFactorThreshold());

                List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
                long nextQueueOffset = queueOffset;
                if (flag == 1) {
                    int firstBatchSize = factor % storeConfig.getReadAheadBatchSizeFactorThreshold() * maxCount;
                    CompletableFuture<Long> future = prefetchMessageThenPutToCache(flatFile, nextQueueOffset, firstBatchSize);
                    futureList.add(Pair.of(firstBatchSize, future));
                    nextQueueOffset += firstBatchSize;
                }
                for (long i = 0; i < concurrency - flag; i++) {
                    CompletableFuture<Long> future = prefetchMessageThenPutToCache(flatFile, nextQueueOffset + i * requestBatchSize, requestBatchSize);
                    futureList.add(Pair.of(requestBatchSize, future));
                }
                flatFile.putInflightRequest(group, queueOffset, maxCount * factor, futureList);
                LOGGER.debug("TieredMessageFetcher#preFetchMessage: try to prefetch messages for later requests: next begin offset: {}, request offset: {}, factor: {}, flag: {}, request batch: {}, concurrency: {}",
                    nextBeginOffset, queueOffset, factor, flag, requestBatchSize, concurrency);
            }
        }
    }

    private CompletableFuture<Long> prefetchMessageThenPutToCache(
        CompositeQueueFlatFile flatFile, long queueOffset, int batchSize) {

        MessageQueue mq = flatFile.getMessageQueue();
        return getMessageFromTieredStoreAsync(flatFile, queueOffset, batchSize)
            .thenApply(result -> {
                if (result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_ONE ||
                    result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    return -1L;
                }
                if (result.getStatus() != GetMessageStatus.FOUND) {
                    LOGGER.warn("MessageFetcher prefetch message then put to cache failed, result: {}, " +
                            "topic: {}, queue: {}, queue offset: {}, batch size: {}",
                        result.getStatus(), mq.getTopic(), mq.getQueueId(), queueOffset, batchSize);
                    return -1L;
                }
                try {
                    List<Long> offsetList = result.getMessageQueueOffset();
                    List<Long> tagCodeList = result.getTagCodeList();
                    List<SelectMappedBufferResult> msgList = result.getMessageMapedList();
                    for (int i = 0; i < offsetList.size(); i++) {
                        SelectMappedBufferResult msg = msgList.get(i);
                        SelectBufferResultWrapper bufferResult = new SelectBufferResultWrapper(
                            msg, offsetList.get(i), tagCodeList.get(i), false);
                        this.putMessageToCache(flatFile, bufferResult);
                    }
                    return offsetList.get(offsetList.size() - 1);
                } catch (Exception e) {
                    LOGGER.error("MessageFetcher prefetch message then put to cache failed, " +
                            "topic: {}, queue: {}, queue offset: {}, batch size: {}",
                        mq.getTopic(), mq.getQueueId(), queueOffset, batchSize, e);
                }
                return -1L;
            });
    }

    public CompletableFuture<GetMessageResultExt> getMessageFromCacheAsync(CompositeQueueFlatFile flatFile,
        String group, long queueOffset, int maxCount, boolean waitInflightRequest) {

        MessageQueue mq = flatFile.getMessageQueue();

        long lastGetOffset = queueOffset - 1;
        List<SelectBufferResultWrapper> resultWrapperList = new ArrayList<>(maxCount);
        for (int i = 0; i < maxCount; i++) {
            lastGetOffset++;
            SelectBufferResultWrapper wrapper = getMessageFromCache(flatFile, lastGetOffset);
            if (wrapper == null) {
                lastGetOffset--;
                break;
            }
            resultWrapperList.add(wrapper);
        }

        // only record cache access count once
        if (waitInflightRequest) {
            Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
                .put(TieredStoreMetricsConstant.LABEL_TOPIC, mq.getTopic())
                .put(TieredStoreMetricsConstant.LABEL_GROUP, group)
                .build();
            TieredStoreMetricsManager.cacheAccess.add(maxCount, attributes);
            TieredStoreMetricsManager.cacheHit.add(resultWrapperList.size(), attributes);
        }

        // If there are no messages in the cache and there are currently requests being pulled.
        // We need to wait for the request to return before continuing.
        if (resultWrapperList.isEmpty() && waitInflightRequest) {
            CompletableFuture<Long> future =
                flatFile.getInflightRequest(group, queueOffset, maxCount).getFuture(queueOffset);
            if (!future.isDone()) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                // to prevent starvation issues, only allow waiting for processing request once
                return future.thenComposeAsync(v -> {
                    LOGGER.debug("MessageFetcher#getMessageFromCacheAsync: wait for response cost: {}ms",
                        stopwatch.elapsed(TimeUnit.MILLISECONDS));
                    return getMessageFromCacheAsync(flatFile, group, queueOffset, maxCount, false);
                }, TieredStoreExecutor.fetchDataExecutor);
            }
        }

        // try to get message from cache again when prefetch request is done
        for (int i = 0; i < maxCount - resultWrapperList.size(); i++) {
            lastGetOffset++;
            SelectBufferResultWrapper wrapper = getMessageFromCache(flatFile, lastGetOffset);
            if (wrapper == null) {
                lastGetOffset--;
                break;
            }
            resultWrapperList.add(wrapper);
        }

        recordCacheAccess(flatFile, group, queueOffset, resultWrapperList);

        if (resultWrapperList.isEmpty()) {
            // If cache miss, pull messages immediately
            LOGGER.info("MessageFetcher cache miss, group: {}, topic: {}, queueId: {}, offset: {}, maxCount: {}",
                group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount);
        } else {
            // If cache hit, return buffer result immediately and asynchronously prefetch messages
            LOGGER.debug("MessageFetcher cache hit, group: {}, topic: {}, queueId: {}, offset: {}, maxCount: {}, resultSize: {}",
                group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount, resultWrapperList.size());

            GetMessageResultExt result = new GetMessageResultExt();
            result.setStatus(GetMessageStatus.FOUND);
            result.setMinOffset(flatFile.getConsumeQueueMinOffset());
            result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());
            result.setNextBeginOffset(queueOffset + resultWrapperList.size());
            resultWrapperList.forEach(wrapper -> result.addMessageExt(
                wrapper.getDuplicateResult(), wrapper.getOffset(), wrapper.getTagCode()));

            if (lastGetOffset < result.getMaxOffset()) {
                this.prefetchMessage(flatFile, group, maxCount, lastGetOffset + 1);
            }
            return CompletableFuture.completedFuture(result);
        }

        CompletableFuture<GetMessageResultExt> resultFuture;
        synchronized (flatFile) {
            int batchSize = maxCount * storeConfig.getReadAheadMinFactor();
            resultFuture = getMessageFromTieredStoreAsync(flatFile, queueOffset, batchSize)
                .thenApply(result -> {
                    if (result.getStatus() != GetMessageStatus.FOUND) {
                        return result;
                    }

                    GetMessageResultExt newResult = new GetMessageResultExt();
                    List<Long> offsetList = result.getMessageQueueOffset();
                    List<Long> tagCodeList = result.getTagCodeList();
                    List<SelectMappedBufferResult> msgList = result.getMessageMapedList();

                    for (int i = 0; i < offsetList.size(); i++) {
                        SelectMappedBufferResult msg = msgList.get(i);
                        SelectBufferResultWrapper bufferResult = new SelectBufferResultWrapper(
                            msg, offsetList.get(i), tagCodeList.get(i), true);
                        this.putMessageToCache(flatFile, bufferResult);
                        if (newResult.getMessageMapedList().size() < maxCount) {
                            newResult.addMessageExt(msg, offsetList.get(i), tagCodeList.get(i));
                        }
                    }

                    newResult.setStatus(GetMessageStatus.FOUND);
                    newResult.setMinOffset(flatFile.getConsumeQueueMinOffset());
                    newResult.setMaxOffset(flatFile.getConsumeQueueCommitOffset());
                    newResult.setNextBeginOffset(queueOffset + newResult.getMessageMapedList().size());
                    return newResult;
                });

            List<Pair<Integer, CompletableFuture<Long>>> futureList = new ArrayList<>();
            CompletableFuture<Long> inflightRequestFuture = resultFuture.thenApply(result ->
                result.getStatus() == GetMessageStatus.FOUND ?
                    result.getMessageQueueOffset().get(result.getMessageQueueOffset().size() - 1) : -1L);
            futureList.add(Pair.of(batchSize, inflightRequestFuture));
            flatFile.putInflightRequest(group, queueOffset, batchSize, futureList);
        }
        return resultFuture;
    }

    public CompletableFuture<GetMessageResultExt> getMessageFromTieredStoreAsync(
        CompositeQueueFlatFile flatFile, long queueOffset, int batchSize) {

        GetMessageResultExt result = new GetMessageResultExt();
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());

        if (queueOffset < result.getMaxOffset()) {
            batchSize = Math.min(batchSize, (int) Math.min(result.getMaxOffset() - queueOffset, Integer.MAX_VALUE));
        } else if (queueOffset == result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
            result.setNextBeginOffset(queueOffset);
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset > result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        }

        LOGGER.info("MessageFetcher#getMessageFromTieredStoreAsync, " +
                "topic: {}, queueId: {}, broker offset: {}-{}, offset: {}, expect: {}",
            flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(),
            result.getMinOffset(), result.getMaxOffset(), queueOffset, batchSize);

        CompletableFuture<ByteBuffer> readConsumeQueueFuture;
        try {
            readConsumeQueueFuture = flatFile.getConsumeQueueAsync(queueOffset, batchSize);
        } catch (TieredStoreException e) {
            switch (e.getErrorCode()) {
                case NO_NEW_DATA:
                    result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
                    result.setNextBeginOffset(queueOffset);
                    return CompletableFuture.completedFuture(result);
                case ILLEGAL_PARAM:
                case ILLEGAL_OFFSET:
                default:
                    result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
                    result.setNextBeginOffset(queueOffset);
                    return CompletableFuture.completedFuture(result);
            }
        }

        CompletableFuture<ByteBuffer> readCommitLogFuture = readConsumeQueueFuture.thenCompose(cqBuffer -> {
            long firstCommitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
            cqBuffer.position(cqBuffer.remaining() - TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
            long lastCommitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
            if (lastCommitLogOffset < firstCommitLogOffset) {
                LOGGER.error("MessageFetcher#getMessageFromTieredStoreAsync, " +
                        "last offset is smaller than first offset, " +
                        "topic: {} queueId: {}, offset: {}, firstOffset: {}, lastOffset: {}",
                    flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(), queueOffset,
                    firstCommitLogOffset, lastCommitLogOffset);
                return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
            }

            // Get the total size of the data by reducing the length limit of cq to prevent OOM
            long length = lastCommitLogOffset - firstCommitLogOffset + CQItemBufferUtil.getSize(cqBuffer);
            while (cqBuffer.limit() > TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE &&
                length > storeConfig.getReadAheadMessageSizeThreshold()) {
                cqBuffer.limit(cqBuffer.position());
                cqBuffer.position(cqBuffer.limit() - TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE);
                length = CQItemBufferUtil.getCommitLogOffset(cqBuffer)
                    - firstCommitLogOffset + CQItemBufferUtil.getSize(cqBuffer);
            }

            return flatFile.getCommitLogAsync(firstCommitLogOffset, (int) length);
        });

        int finalBatchSize = batchSize;
        return readConsumeQueueFuture.thenCombine(readCommitLogFuture, (cqBuffer, msgBuffer) -> {
            List<SelectBufferResult> bufferList = MessageBufferUtil.splitMessageBuffer(cqBuffer, msgBuffer);
            int requestSize = cqBuffer.remaining() / TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE;
            if (bufferList.isEmpty()) {
                result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
                result.setNextBeginOffset(queueOffset + requestSize);
            } else {
                result.setStatus(GetMessageStatus.FOUND);
                result.setNextBeginOffset(queueOffset + requestSize);

                for (SelectBufferResult bufferResult : bufferList) {
                    ByteBuffer slice = bufferResult.getByteBuffer().slice();
                    slice.limit(bufferResult.getSize());
                    SelectMappedBufferResult msg = new SelectMappedBufferResult(bufferResult.getStartOffset(),
                        bufferResult.getByteBuffer(), bufferResult.getSize(), null);
                    result.addMessageExt(msg, MessageBufferUtil.getQueueOffset(slice), bufferResult.getTagCode());
                }
            }
            return result;
        }).exceptionally(e -> {
            MessageQueue mq = flatFile.getMessageQueue();
            LOGGER.warn("MessageFetcher#getMessageFromTieredStoreAsync failed, " +
                "topic: {} queueId: {}, offset: {}, batchSize: {}", mq.getTopic(), mq.getQueueId(), queueOffset, finalBatchSize, e);
            result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
            result.setNextBeginOffset(queueOffset);
            return result;
        });
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(
        String group, String topic, int queueId, long queueOffset, int maxCount, final MessageFilter messageFilter) {

        GetMessageResult result = new GetMessageResult();
        CompositeQueueFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));

        if (flatFile == null) {
            result.setNextBeginOffset(queueOffset);
            result.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
            return CompletableFuture.completedFuture(result);
        }

        // Max queue offset means next message put position
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());

        // Fill result according file offset.
        // Offset range  | Result           | Fix to
        // (-oo, 0]      | no message       | current offset
        // (0, min)      | too small        | min offset
        // [min, max)    | correct          |
        // [max, max]    | overflow one     | max offset
        // (max, +oo)    | overflow badly   | max offset

        if (result.getMaxOffset() <= 0) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setNextBeginOffset(queueOffset);
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset < result.getMinOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            result.setNextBeginOffset(result.getMinOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset == result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset > result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        }

        return getMessageFromCacheAsync(flatFile, group, queueOffset, maxCount, true)
            .thenApply(messageResultExt -> messageResultExt.doFilterMessage(messageFilter));
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        CompositeFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        // read from timestamp to timestamp + length
        int length = MessageBufferUtil.STORE_TIMESTAMP_POSITION + 8;
        return flatFile.getCommitLogAsync(flatFile.getCommitLogMinOffset(), length)
            .thenApply(MessageBufferUtil::getStoreTimeStamp);
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId, long queueOffset) {
        CompositeFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        return flatFile.getConsumeQueueAsync(queueOffset)
            .thenComposeAsync(cqItem -> {
                long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqItem);
                int size = CQItemBufferUtil.getSize(cqItem);
                return flatFile.getCommitLogAsync(commitLogOffset, size);
            }, TieredStoreExecutor.fetchDataExecutor)
            .thenApply(MessageBufferUtil::getStoreTimeStamp)
            .exceptionally(e -> {
                LOGGER.error("TieredMessageFetcher#getMessageStoreTimeStampAsync: " +
                    "get or decode message failed: topic: {}, queue: {}, offset: {}", topic, queueId, queueOffset, e);
                return -1L;
            });
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType type) {
        CompositeFlatFile flatFile = flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return -1L;
        }

        try {
            return flatFile.getOffsetInConsumeQueueByTime(timestamp, type);
        } catch (Exception e) {
            LOGGER.error("TieredMessageFetcher#getOffsetInQueueByTime: " +
                    "get offset in queue by time failed: topic: {}, queue: {}, timestamp: {}, type: {}",
                topic, queueId, timestamp, type, e);
        }
        return -1L;
    }

    @Override
    public CompletableFuture<QueryMessageResult> queryMessageAsync(
        String topic, String key, int maxCount, long begin, long end) {

        IndexService indexStoreService = TieredFlatFileManager.getTieredIndexService(storeConfig);

        long topicId;
        try {
            TopicMetadata topicMetadata = metadataStore.getTopic(topic);
            if (topicMetadata == null) {
                LOGGER.info("MessageFetcher#queryMessageAsync, topic metadata not found, topic: {}", topic);
                return CompletableFuture.completedFuture(new QueryMessageResult());
            }
            topicId = topicMetadata.getTopicId();
        } catch (Exception e) {
            LOGGER.error("MessageFetcher#queryMessageAsync, get topic id failed, topic: {}", topic, e);
            return CompletableFuture.completedFuture(new QueryMessageResult());
        }

        CompletableFuture<List<IndexItem>> future = indexStoreService.queryAsync(topic, key, maxCount, begin, end);

        return future.thenCompose(indexItemList -> {
            List<CompletableFuture<SelectMappedBufferResult>> futureList = new ArrayList<>(maxCount);
            for (IndexItem indexItem : indexItemList) {
                if (topicId != indexItem.getTopicId()) {
                    continue;
                }
                CompositeFlatFile flatFile =
                    flatFileManager.getFlatFile(new MessageQueue(topic, brokerName, indexItem.getQueueId()));
                if (flatFile == null) {
                    continue;
                }
                CompletableFuture<SelectMappedBufferResult> getMessageFuture = flatFile
                    .getCommitLogAsync(indexItem.getOffset(), indexItem.getSize())
                    .thenApply(messageBuffer -> new SelectMappedBufferResult(indexItem.getOffset(), messageBuffer, indexItem.getSize(), null));
                futureList.add(getMessageFuture);
                if (futureList.size() >= maxCount) {
                    break;
                }
            }
            return CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).thenApply(v -> {
                QueryMessageResult result = new QueryMessageResult();
                futureList.forEach(f -> f.thenAccept(result::addMessage));
                return result;
            });
        }).whenComplete((result, throwable) -> {
            if (result != null) {
                LOGGER.info("MessageFetcher#queryMessageAsync, " +
                        "query result: {}, topic: {}, topicId: {}, key: {}, maxCount: {}, timestamp: {}-{}",
                    result.getMessageBufferList().size(), topic, topicId, key, maxCount, begin, end);
            }
        });
    }
}
