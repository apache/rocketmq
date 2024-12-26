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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.GetMessageResultExt;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.exception.TieredStoreException;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.index.IndexItem;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreFetcherImpl implements MessageStoreFetcher {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected static final String CACHE_KEY_FORMAT = "%s@%d@%d";

    private final String brokerName;
    private final MetadataStore metadataStore;
    private final MessageStoreConfig storeConfig;
    private final org.apache.rocketmq.store.config.MessageStoreConfig messageStoreConfig;
    private final TieredMessageStore messageStore;
    private final IndexService indexService;
    private final FlatFileStore flatFileStore;
    private final long memoryMaxSize;
    private final Cache<String /* topic@queueId@offset */, SelectBufferResult> fetcherCache;

    public MessageStoreFetcherImpl(TieredMessageStore messageStore) {
        this(messageStore, messageStore.getStoreConfig(),
            messageStore.getFlatFileStore(), messageStore.getIndexService());
    }

    public MessageStoreFetcherImpl(TieredMessageStore messageStore, MessageStoreConfig storeConfig,
        FlatFileStore flatFileStore, IndexService indexService) {

        this.storeConfig = storeConfig;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();
        this.brokerName = storeConfig.getBrokerName();
        this.flatFileStore = flatFileStore;
        this.messageStore = messageStore;
        this.indexService = indexService;
        this.metadataStore = flatFileStore.getMetadataStore();
        this.memoryMaxSize =
            (long) (Runtime.getRuntime().maxMemory() * storeConfig.getReadAheadCacheSizeThresholdRate());
        this.fetcherCache = this.initCache(storeConfig);
        log.info("MessageStoreFetcher init success, brokerName={}", storeConfig.getBrokerName());
    }

    private Cache<String, SelectBufferResult> initCache(MessageStoreConfig storeConfig) {

        return Caffeine.newBuilder()
            .scheduler(Scheduler.systemScheduler())
            // Clients may repeatedly request messages at the same offset in tiered storage,
            // causing the request queue to become full. Using expire after read or write policy
            // to refresh the cache expiration time.
            .expireAfterAccess(storeConfig.getReadAheadCacheExpireDuration(), TimeUnit.MILLISECONDS)
            .maximumWeight(memoryMaxSize)
            // Using the buffer size of messages to calculate memory usage
            .weigher((String key, SelectBufferResult buffer) -> buffer.getSize())
            .recordStats()
            .build();
    }

    public Cache<String, SelectBufferResult> getFetcherCache() {
        return fetcherCache;
    }

    protected void putMessageToCache(FlatMessageFile flatFile, long offset, SelectBufferResult result) {
        MessageQueue mq = flatFile.getMessageQueue();
        this.fetcherCache.put(String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), offset), result);
    }

    protected SelectBufferResult getMessageFromCache(FlatMessageFile flatFile, long offset) {
        MessageQueue mq = flatFile.getMessageQueue();
        SelectBufferResult buffer = this.fetcherCache.getIfPresent(
            String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), offset));
        // return duplicate buffer here
        if (buffer == null) {
            return null;
        }
        long count = buffer.getAccessCount().incrementAndGet();
        if (count % 1000L == 0L) {
            log.warn("MessageFetcher fetch same offset message too many times, " +
                "topic={}, queueId={}, offset={}, count={}", mq.getTopic(), mq.getQueueId(), offset, count);
        }
        return new SelectBufferResult(
            buffer.getByteBuffer().asReadOnlyBuffer(), buffer.getStartOffset(), buffer.getSize(), buffer.getTagCode());
    }

    protected GetMessageResultExt getMessageFromCache(
        FlatMessageFile flatFile, long offset, int maxCount, MessageFilter messageFilter) {
        GetMessageResultExt result = new GetMessageResultExt();
        int interval = storeConfig.getReadAheadMessageCountThreshold();
        for (long current = offset, end = offset + interval; current < end; current++) {
            SelectBufferResult buffer = getMessageFromCache(flatFile, current);
            if (buffer == null) {
                result.setNextBeginOffset(current);
                break;
            }
            result.setNextBeginOffset(current + 1);
            if (messageFilter != null) {
                if (!messageFilter.isMatchedByConsumeQueue(buffer.getTagCode(), null)) {
                    continue;
                }
                if (!messageFilter.isMatchedByCommitLog(buffer.getByteBuffer().slice(), null)) {
                    continue;
                }
            }
            SelectMappedBufferResult bufferResult = new SelectMappedBufferResult(
                buffer.getStartOffset(), buffer.getByteBuffer(), buffer.getSize(), null);
            result.addMessageExt(bufferResult, current, buffer.getTagCode());
            if (result.getMessageCount() == maxCount) {
                break;
            }
            if (result.getBufferTotalSize() >= messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                break;
            }
        }
        result.setStatus(result.getMessageCount() > 0 ?
            GetMessageStatus.FOUND : GetMessageStatus.NO_MATCHED_MESSAGE);
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());
        return result;
    }

    protected CompletableFuture<Long> fetchMessageThenPutToCache(
        FlatMessageFile flatFile, long queueOffset, int batchSize) {

        MessageQueue mq = flatFile.getMessageQueue();
        return this.getMessageFromTieredStoreAsync(flatFile, queueOffset, batchSize)
            .thenApply(result -> {
                if (result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_ONE ||
                    result.getStatus() == GetMessageStatus.OFFSET_OVERFLOW_BADLY) {
                    return -1L;
                }
                if (result.getStatus() != GetMessageStatus.FOUND) {
                    log.warn("MessageFetcher prefetch message then put to cache failed, result={}, " +
                            "topic={}, queue={}, queue offset={}, batch size={}",
                        result.getStatus(), mq.getTopic(), mq.getQueueId(), queueOffset, batchSize);
                    return -1L;
                }
                List<Long> offsetList = result.getMessageQueueOffset();
                List<Long> tagCodeList = result.getTagCodeList();
                List<SelectMappedBufferResult> msgList = result.getMessageMapedList();
                for (int i = 0; i < offsetList.size(); i++) {
                    SelectMappedBufferResult msg = msgList.get(i);
                    SelectBufferResult bufferResult = new SelectBufferResult(
                        msg.getByteBuffer(), msg.getStartOffset(), msg.getSize(), tagCodeList.get(i));
                    this.putMessageToCache(flatFile, queueOffset + i, bufferResult);
                }
                return offsetList.get(offsetList.size() - 1);
            });
    }

    public CompletableFuture<GetMessageResult> getMessageFromCacheAsync(
        FlatMessageFile flatFile, String group, long queueOffset, int maxCount, MessageFilter messageFilter) {

        MessageQueue mq = flatFile.getMessageQueue();
        GetMessageResultExt result = getMessageFromCache(flatFile, queueOffset, maxCount, messageFilter);

        if (GetMessageStatus.FOUND.equals(result.getStatus())) {
            log.debug("MessageFetcher cache hit, group={}, topic={}, queueId={}, offset={}, maxCount={}, resultSize={}, lag={}",
                group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount,
                result.getMessageCount(), result.getMaxOffset() - result.getNextBeginOffset());
            return CompletableFuture.completedFuture(result);
        }

        // If cache miss, pull messages immediately
        log.debug("MessageFetcher cache miss, group={}, topic={}, queueId={}, offset={}, maxCount={}, lag={}",
            group, mq.getTopic(), mq.getQueueId(), queueOffset, maxCount, result.getMaxOffset() - result.getNextBeginOffset());

        // To optimize the performance of pop consumption
        // Pop revive will cause a large number of random reads,
        // so the amount of pre-fetch message num needs to be reduced.
        int fetchSize = maxCount == 1 ? 32 : storeConfig.getReadAheadMessageCountThreshold();
        return fetchMessageThenPutToCache(flatFile, queueOffset, fetchSize)
            .thenApply(maxOffset -> getMessageFromCache(flatFile, queueOffset, maxCount, messageFilter));
    }

    public CompletableFuture<GetMessageResultExt> getMessageFromTieredStoreAsync(
        FlatMessageFile flatFile, long queueOffset, int batchSize) {

        GetMessageResultExt result = new GetMessageResultExt();
        result.setMinOffset(flatFile.getConsumeQueueMinOffset());
        result.setMaxOffset(flatFile.getConsumeQueueCommitOffset());

        if (queueOffset < result.getMinOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            result.setNextBeginOffset(result.getMinOffset());
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset == result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
            result.setNextBeginOffset(queueOffset);
            return CompletableFuture.completedFuture(result);
        } else if (queueOffset > result.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
            result.setNextBeginOffset(result.getMaxOffset());
            return CompletableFuture.completedFuture(result);
        }

        if (queueOffset < result.getMaxOffset()) {
            batchSize = Math.min(batchSize, (int) Math.min(
                result.getMaxOffset() - queueOffset, storeConfig.getReadAheadMessageCountThreshold()));
        }

        CompletableFuture<ByteBuffer> readConsumeQueueFuture;
        try {
            readConsumeQueueFuture = flatFile.getConsumeQueueAsync(queueOffset, batchSize);
        } catch (TieredStoreException e) {
            switch (e.getErrorCode()) {
                case ILLEGAL_PARAM:
                case ILLEGAL_OFFSET:
                default:
                    result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
                    result.setNextBeginOffset(queueOffset);
                    return CompletableFuture.completedFuture(result);
            }
        }

        int finalBatchSize = batchSize;
        CompletableFuture<ByteBuffer> readCommitLogFuture = readConsumeQueueFuture.thenCompose(cqBuffer -> {

            long firstCommitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer);
            cqBuffer.position(cqBuffer.remaining() - MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
            long lastCommitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer);
            if (lastCommitLogOffset < firstCommitLogOffset) {
                log.error("MessageFetcher#getMessageFromTieredStoreAsync, last offset is smaller than first offset, " +
                        "topic={} queueId={}, offset={}, firstOffset={}, lastOffset={}",
                    flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(), queueOffset,
                    firstCommitLogOffset, lastCommitLogOffset);
                return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
            }

            // Get at least one message
            // Reducing the length limit of cq to prevent OOM
            long length = lastCommitLogOffset - firstCommitLogOffset + MessageFormatUtil.getSizeFromItem(cqBuffer);
            while (cqBuffer.limit() > MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE &&
                length > storeConfig.getReadAheadMessageSizeThreshold()) {
                cqBuffer.limit(cqBuffer.position());
                cqBuffer.position(cqBuffer.limit() - MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
                length = MessageFormatUtil.getCommitLogOffsetFromItem(cqBuffer)
                    - firstCommitLogOffset + MessageFormatUtil.getSizeFromItem(cqBuffer);
            }
            int messageCount = cqBuffer.position() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE + 1;

            log.info("MessageFetcher#getMessageFromTieredStoreAsync, " +
                    "topic={}, queueId={}, broker offset={}-{}, offset={}, expect={}, actually={}, lag={}",
                flatFile.getMessageQueue().getTopic(), flatFile.getMessageQueue().getQueueId(),
                result.getMinOffset(), result.getMaxOffset(), queueOffset, finalBatchSize,
                messageCount, result.getMaxOffset() - queueOffset);

            return flatFile.getCommitLogAsync(firstCommitLogOffset, (int) length);
        });

        return readConsumeQueueFuture.thenCombine(readCommitLogFuture, (cqBuffer, msgBuffer) -> {
            List<SelectBufferResult> bufferList = MessageFormatUtil.splitMessageBuffer(cqBuffer, msgBuffer);
            int requestSize = cqBuffer.remaining() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;

            // not use buffer list size to calculate next offset to prevent split error
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
                    result.addMessageExt(msg, MessageFormatUtil.getQueueOffset(slice), bufferResult.getTagCode());
                }
            }
            return result;
        }).exceptionally(e -> {
            MessageQueue mq = flatFile.getMessageQueue();
            log.warn("MessageFetcher#getMessageFromTieredStoreAsync failed, " +
                "topic={} queueId={}, offset={}, batchSize={}", mq.getTopic(), mq.getQueueId(), queueOffset, finalBatchSize, e);
            result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
            result.setNextBeginOffset(queueOffset);
            return result;
        });
    }

    @Override
    public CompletableFuture<GetMessageResult> getMessageAsync(
        String group, String topic, int queueId, long queueOffset, int maxCount, final MessageFilter messageFilter) {

        GetMessageResult result = new GetMessageResult();
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));

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

        boolean cacheBusy = fetcherCache.estimatedSize() > memoryMaxSize * 0.8;
        if (storeConfig.isReadAheadCacheEnable() && !cacheBusy) {
            return getMessageFromCacheAsync(flatFile, group, queueOffset, maxCount, messageFilter);
        } else {
            return getMessageFromTieredStoreAsync(flatFile, queueOffset, maxCount)
                .thenApply(messageResultExt -> messageResultExt.doFilterMessage(messageFilter));
        }
    }

    @Override
    public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        // read from timestamp to timestamp + length
        int length = MessageFormatUtil.STORE_TIMESTAMP_POSITION + 8;
        return flatFile.getCommitLogAsync(flatFile.getCommitLogMinOffset(), length)
            .thenApply(MessageFormatUtil::getStoreTimeStamp);
    }

    @Override
    public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId, long queueOffset) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return CompletableFuture.completedFuture(-1L);
        }

        return flatFile.getConsumeQueueAsync(queueOffset)
            .thenComposeAsync(cqItem -> {
                long commitLogOffset = MessageFormatUtil.getCommitLogOffsetFromItem(cqItem);
                int size = MessageFormatUtil.getSizeFromItem(cqItem);
                return flatFile.getCommitLogAsync(commitLogOffset, size);
            }, messageStore.getStoreExecutor().bufferFetchExecutor)
            .thenApply(MessageFormatUtil::getStoreTimeStamp)
            .exceptionally(e -> {
                log.error("MessageStoreFetcherImpl#getMessageStoreTimeStampAsync: " +
                    "get or decode message failed, topic={}, queue={}, offset={}", topic, queueId, queueOffset, e);
                return -1L;
            });
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp, BoundaryType type) {
        FlatMessageFile flatFile = flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, queueId));
        if (flatFile == null) {
            return -1L;
        }
        return flatFile.getQueueOffsetByTimeAsync(timestamp, type).join();
    }

    @Override
    public CompletableFuture<QueryMessageResult> queryMessageAsync(
        String topic, String key, int maxCount, long begin, long end) {

        long topicId;
        try {
            TopicMetadata topicMetadata = metadataStore.getTopic(topic);
            if (topicMetadata == null) {
                log.info("MessageFetcher#queryMessageAsync, topic metadata not found, topic={}", topic);
                return CompletableFuture.completedFuture(new QueryMessageResult());
            }
            topicId = topicMetadata.getTopicId();
        } catch (Exception e) {
            log.error("MessageFetcher#queryMessageAsync, get topic id failed, topic={}", topic, e);
            return CompletableFuture.completedFuture(new QueryMessageResult());
        }

        CompletableFuture<List<IndexItem>> future = indexService.queryAsync(topic, key, maxCount, begin, end);

        return future.thenCompose(indexItemList -> {
            List<CompletableFuture<SelectMappedBufferResult>> futureList = new ArrayList<>(maxCount);
            for (IndexItem indexItem : indexItemList) {
                if (topicId != indexItem.getTopicId()) {
                    continue;
                }
                FlatMessageFile flatFile =
                    flatFileStore.getFlatFile(new MessageQueue(topic, brokerName, indexItem.getQueueId()));
                if (flatFile == null) {
                    continue;
                }
                CompletableFuture<SelectMappedBufferResult> getMessageFuture = flatFile
                    .getCommitLogAsync(indexItem.getOffset(), indexItem.getSize())
                    .thenApply(messageBuffer -> new SelectMappedBufferResult(
                        indexItem.getOffset(), messageBuffer, indexItem.getSize(), null));
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
                log.info("MessageFetcher#queryMessageAsync, " +
                        "query result={}, topic={}, topicId={}, key={}, maxCount={}, timestamp={}-{}",
                    result.getMessageBufferList().size(), topic, topicId, key, maxCount, begin, end);
            }
        });
    }
}
