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
package org.apache.rocketmq.broker.pop;

import com.alibaba.fastjson2.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PopConsumerService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private static final long OFFSET_NOT_EXIST = -1L;
    private static final String ROCKSDB_DIRECTORY = "kvStore";
    private static final int[] REWRITE_INTERVALS_IN_SECONDS =
        new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

    private final AtomicBoolean consumerRunning;
    private final BrokerConfig brokerConfig;
    private final BrokerController brokerController;
    private final AtomicLong currentTime;
    private final AtomicLong lastCleanupLockTime;
    private final PopConsumerCache popConsumerCache;
    private final PopConsumerKVStore popConsumerStore;
    private final PopConsumerLockService consumerLockService;
    private final ConcurrentMap<String /* groupId@topicId*/, AtomicLong> requestCountTable;

    public PopConsumerService(BrokerController brokerController) {

        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();

        this.consumerRunning = new AtomicBoolean(false);
        this.requestCountTable = new ConcurrentHashMap<>();
        this.currentTime = new AtomicLong(TimeUnit.SECONDS.toMillis(3));
        this.lastCleanupLockTime = new AtomicLong(System.currentTimeMillis());
        this.consumerLockService = new PopConsumerLockService(TimeUnit.MINUTES.toMillis(2));
        this.popConsumerStore = new PopConsumerRocksdbStore(Paths.get(
            brokerController.getMessageStoreConfig().getStorePathRootDir(), ROCKSDB_DIRECTORY).toString(),
            brokerController.getMessageStoreConfig().getPopRocksdbBlockCacheSize(),
            brokerController.getMessageStoreConfig().getPopRocksdbWriteBufferSize());
        this.popConsumerCache = brokerConfig.isEnablePopBufferMerge() ? new PopConsumerCache(
            brokerController, this.popConsumerStore, this.consumerLockService, this::revive) : null;

        log.info("PopConsumerService init, buffer={}, rocksdb filePath={}",
            brokerConfig.isEnablePopBufferMerge(), this.popConsumerStore.getFilePath());
    }

    /**
     * No external callers, only called by unit tests.
     * In-flight messages are those that have been received from a queue
     * by a consumer but have not yet been deleted. For standard queues,
     * there is a limit on the number of in-flight messages, depending on queue traffic and message backlog.
     */
    public boolean isPopShouldStop(String group, String topic, int queueId) {
        return brokerConfig.isEnablePopMessageThreshold() && popConsumerCache != null &&
            popConsumerCache.getPopInFlightMessageCount(group, topic, queueId) >=
                brokerConfig.getPopInflightMessageThreshold();
    }

    // No external callers, only called by unit tests.
    public long getPendingFilterCount(String groupId, String topicId, int queueId) {
        try {
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);
            long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId);
            return maxOffset - consumeOffset;
        } catch (ConsumeQueueException e) {
            throw new RuntimeException(e);
        }
    }

    // No external callers, only called by unit tests.
    public GetMessageResult recodeRetryMessage(GetMessageResult getMessageResult,
        String topicId, long offset, long popTime, long invisibleTime) {

        if (getMessageResult.getMessageCount() == 0 ||
            getMessageResult.getMessageMapedList().isEmpty()) {
            return getMessageResult;
        }

        GetMessageResult result = new GetMessageResult(getMessageResult.getMessageCount());
        result.setStatus(GetMessageStatus.FOUND);
        String brokerName = brokerConfig.getBrokerName();

        for (SelectMappedBufferResult bufferResult : getMessageResult.getMessageMapedList()) {
            List<MessageExt> messageExtList = MessageDecoder.decodesBatch(
                bufferResult.getByteBuffer(), true, false, true);
            bufferResult.release();
            for (MessageExt messageExt : messageExtList) {
                try {
                    // When override retry message topic to origin topic,
                    // need clear message store size to recode
                    String ckInfo = ExtraInfoUtil.buildExtraInfo(offset, popTime, invisibleTime, 0,
                        messageExt.getTopic(), brokerName, messageExt.getQueueId(), messageExt.getQueueOffset());
                    messageExt.getProperties().putIfAbsent(MessageConst.PROPERTY_POP_CK, ckInfo);
                    messageExt.setTopic(topicId);
                    messageExt.setStoreSize(0);
                    byte[] encode = MessageDecoder.encode(messageExt, false);
                    ByteBuffer buffer = ByteBuffer.wrap(encode);
                    SelectMappedBufferResult tmpResult = new SelectMappedBufferResult(
                        bufferResult.getStartOffset(), buffer, encode.length, null);
                    result.addMessage(tmpResult);
                } catch (Exception e) {
                    log.error("PopConsumerService exception in recode retry message, topic={}", topicId, e);
                }
            }
        }

        return result;
    }

    /**
     * Merge a GetMessageResult into the pop context and commit the consumer offset.
     * No external callers, only called by unit tests.
     *
     * <p>If messages were found:
     * <ul>
     *   <li>For FIFO — the queue is blocked via {@link #setFifoBlocked} so that
     *       subsequent pops on the same queue wait for the ack</li>
     *   <li>The result is appended to the context along with the topic, queue,
     *       and retry type metadata</li>
     * </ul>
     *
     * <p>The consumer offset is then committed:
     * <ul>
     *   <li>For FIFO when no messages found — committed to the next begin offset</li>
     *   <li>For non-FIFO — the pull offset is updated. If buffer merge is enabled,
     *       the offset is clamped to the minimum offset still in the cache to
     *       prevent regression</li>
     * </ul>
     *
     * @param context    the pop context to update
     * @param result     the result from the message store
     * @param topicId    topic name
     * @param queueId    queue id
     * @param retryType  whether this is a retry topic V1/V2
     * @param offset     the original consume offset used for this fetch
     * @return the updated pop context
     */
    public PopConsumerContext handleGetMessageResult(PopConsumerContext context, GetMessageResult result,
        String topicId, int queueId, PopConsumerRecord.RetryType retryType, long offset) {

        if (GetMessageStatus.FOUND.equals(result.getStatus()) && !result.getMessageQueueOffset().isEmpty()) {
            if (context.isFifo()) {
                this.setFifoBlocked(context, context.getGroupId(), topicId, queueId, result.getMessageQueueOffset(), result);
            }
            // build response header here
            context.addGetMessageResult(result, topicId, queueId, retryType, offset);
            if (brokerConfig.isPopConsumerKVServiceLog()) {
                log.info("PopConsumerService pop, time={}, invisible={}, " +
                        "groupId={}, topic={}, queueId={}, offset={}, attemptId={}",
                    context.getPopTime(), context.getInvisibleTime(), context.getGroupId(),
                    topicId, queueId, result.getMessageQueueOffset(), context.getAttemptId());
            }
        }

        long commitOffset = offset;
        if (context.isFifo()) {
            if (!GetMessageStatus.FOUND.equals(result.getStatus())) {
                commitOffset = result.getNextBeginOffset();
            }
        } else {
            this.brokerController.getConsumerOffsetManager().commitPullOffset(
                context.getClientHost(), context.getGroupId(), topicId, queueId, result.getNextBeginOffset());
            if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
                long minOffset = popConsumerCache.getMinOffsetInCache(context.getGroupId(), topicId, queueId);
                if (minOffset != OFFSET_NOT_EXIST) {
                    commitOffset = minOffset;
                }
            }
        }
        this.brokerController.getConsumerOffsetManager().commitOffset(
            context.getClientHost(), context.getGroupId(), topicId, queueId, commitOffset);
        return context;
    }

    /**
     * Retrieve the starting consume offset for a pop request.
     * should be private, no external callers.
     *
     * <p>For FIFO consumers, the offset is read from the regular consumer offset.
     * For non-FIFO consumers, a separate pull offset is used (compatibility with
     * pull consumer switchover).
     *
     * <p>If no offset is stored (first pop), it is initialized via
     * {@code PopMessageProcessor#getInitOffset} based on {@code initMode}
     * (beginning or end of the queue).
     *
     * <p>If a reset offset exists (offset reset command issued), the cache is
     * cleared, FIFO lock unlock, and the reset offset takes effect
     * immediately.
     *
     * @param groupId   consumer group id
     * @param topicId   topic name
     * @param queueId   queue id
     * @param initMode  consume init mode (min/max)
     * @param fifo      whether this is a FIFO ordered consumption
     * @return the consume offset to start popping from
     */
    public long getPopOffset(String groupId, String topicId, int queueId, int initMode, boolean fifo) {

        // For FIFO messages, the pull offset is not used.
        // This preserves compatibility when switching from pull consumer to pop consumer.
        long offset = fifo ?
            this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId) :
            this.brokerController.getConsumerOffsetManager().queryPullOffset(groupId, topicId, queueId);

        // init offset
        if (offset < 0L) {
            try {
                offset = this.brokerController.getPopMessageProcessor()
                    .getInitOffset(topicId, groupId, queueId, initMode, true);
                log.info("PopConsumerService init offset, groupId={}, topicId={}, queueId={}, init={}, offset={}",
                    groupId, topicId, queueId, ConsumeInitMode.MIN == initMode ? "min" : "max", offset);
            } catch (ConsumeQueueException e) {
                throw new RuntimeException(e);
            }
        }

        // get reset offset
        Long resetOffset =
            this.brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topicId, groupId, queueId);
        if (resetOffset != null) {
            this.clearCache(groupId, topicId, queueId);
            this.brokerController.getConsumerOrderInfoManager().clearBlock(topicId, groupId, queueId);
            this.brokerController.getConsumerOffsetManager()
                .commitOffset("ResetPopOffset", groupId, topicId, queueId, resetOffset);
        }

        return resetOffset != null ? resetOffset : offset;
    }

    /**
     * Fetch messages from the store with automatic offset correction.
     * No external callers, except unit tests.
     *
     * <p>If the stored offset is behind the actual consume queue offset
     * ({@code OFFSET_TOO_SMALL}, {@code OFFSET_OVERFLOW_BADLY},
     * {@code OFFSET_FOUND_NULL}), the offset is corrected and a retry is
     * issued with the corrected offset. This prevents duplicate messages
     * when the Pop buffer offset has not yet been committed.
     *
     * @param clientHost the client address
     * @param groupId    consumer group id
     * @param topicId    topic name
     * @param queueId    queue id
     * @param offset     the consume offset to start from
     * @param batchSize  max number of messages
     * @param filter     message filter
     * @return a future completing with the fetch result
     */
    public CompletableFuture<GetMessageResult> getMessageAsync(String clientHost,
        String groupId, String topicId, int queueId, long offset, int batchSize, MessageFilter filter) {

        log.debug("PopConsumerService getMessageAsync, groupId={}, topicId={}, queueId={}, " +
            "offset={}, batchSize={}, filter={}", groupId, topicId, queueId, offset, batchSize, filter != null);

        CompletableFuture<GetMessageResult> getMessageFuture =
            brokerController.getMessageStore().getMessageAsync(groupId, topicId, queueId, offset, batchSize, filter);

        // refer org.apache.rocketmq.broker.processor.PopMessageProcessor#popMsgFromQueue
        return getMessageFuture.thenCompose(result -> {
            if (result == null) {
                return CompletableFuture.completedFuture(null);
            }

            // maybe store offset is not correct.
            if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus()) ||
                GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())) {

                // commit offset, because the offset is not correct
                // If offset in store is greater than cq offset, it will cause duplicate messages,
                // because offset in PopBuffer is not committed.
                this.brokerController.getConsumerOffsetManager().commitOffset(
                    clientHost, groupId, topicId, queueId, result.getNextBeginOffset());

                log.warn("PopConsumerService getMessageAsync, initial offset because store is no correct, " +
                        "groupId={}, topicId={}, queueId={}, batchSize={}, offset={}->{}",
                    groupId, topicId, queueId, batchSize, offset, result.getNextBeginOffset());

                return brokerController.getMessageStore().getMessageAsync(
                    groupId, topicId, queueId, result.getNextBeginOffset(), batchSize, filter);
            }

            return CompletableFuture.completedFuture(result);

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Pop getMessageAsync error", throwable);
            }
        });
    }

    /**
     * Fifo message does not have retry feature in broker
     * No external callers, only called by unit tests.
     */
    public void setFifoBlocked(PopConsumerContext context,
        String groupId, String topicId, int queueId, List<Long> queueOffsetList, GetMessageResult getMessageResult) {
        brokerController.getConsumerOrderInfoManager().update(
            context.getAttemptId(), false, topicId, groupId, queueId,
            context.getPopTime(), context.getInvisibleTime(), queueOffsetList, context.getOrderCountInfoBuilder(), getMessageResult);
    }

    // No external callers, only called by unit tests.
    public boolean isFifoBlocked(PopConsumerContext context, String groupId, String topicId, int queueId) {
        // If server-side reset offset is enabled, and there is a reset offset,
        // then return false to make sure that the reset offset takes effect.
        if (brokerController.getBrokerConfig().isUseServerSideResetOffset() &&
            this.brokerController.getConsumerOffsetManager().hasOffsetReset(topicId, groupId, queueId)) {
            return false;
        }
        return brokerController.getConsumerOrderInfoManager().checkBlock(
            context.getAttemptId(), topicId, groupId, queueId, context.getInvisibleTime());
    }

    /**
     * Fetch messages from a single queue and append them to the pop context.
     * No external callers, except unit tests.
     *
     * <p>Chained via {@link CompletableFuture#thenCompose} from
     * {@link #getMessageFromTopicAsync}. When the batch is already full
     * ({@code remain <= 0}), the pending count is added to the context and
     * the chain stops. Otherwise, messages are fetched from the store and
     * the result is merged into the context via {@link #handleGetMessageResult}.
     *
     * <p>Early termination can occur inside this method when:
     * <ul>
     *   <li>Too many inflight (un-acked) messages exist</li>
     *   <li>A FIFO queue is blocked</li>
     * </ul>
     *
     * @param future    the accumulator future carrying the pop context
     * @param clientHost the client address
     * @param groupId   consumer group id
     * @param topicId   topic name
     * @param queueId   queue id
     * @param batchSize max number of messages still needed
     * @param filter    message filter
     * @param retryType whether this is a retry topic V1/V2
     * @return a future completing with the pop context updated with results
     */
    protected CompletableFuture<PopConsumerContext> getMessageAsync(CompletableFuture<PopConsumerContext> future,
        String clientHost, String groupId, String topicId, int queueId, int batchSize, MessageFilter filter,
        PopConsumerRecord.RetryType retryType) {

        return future.thenCompose(result -> {

            // pop request too much, should not add rest count here
            if (isPopShouldStop(groupId, topicId, queueId)) {
                return CompletableFuture.completedFuture(result);
            }

            // Current requests would calculate the total number of messages
            // waiting to be filtered for new message arrival notifications in
            // the long-polling service, need disregarding the backlog in order
            // consumption scenario. If rest message num including the blocked
            // queue accumulation would lead to frequent unnecessary wake-ups
            // of long-polling requests, resulting unnecessary CPU usage.
            // When client ack message, long-polling request would be notifications
            // by AckMessageProcessor.ackOrderly() and message will not be delayed.
            if (result.isFifo() && isFifoBlocked(result, groupId, topicId, queueId)) {
                // should not add accumulation(max offset - consumer offset) here
                return CompletableFuture.completedFuture(result);
            }

            int remain = batchSize - result.getMessageCount();
            if (remain <= 0) {
                result.addRestCount(this.getPendingFilterCount(groupId, topicId, queueId));
                return CompletableFuture.completedFuture(result);
            } else {
                final long consumeOffset = this.getPopOffset(groupId, topicId, queueId, result.getInitMode(), result.isFifo());
                return getMessageAsync(clientHost, groupId, topicId, queueId, consumeOffset, remain, filter)
                    .thenApply(getMessageResult -> handleGetMessageResult(
                        result, getMessageResult, topicId, queueId, retryType, consumeOffset));
            }
        });
    }

    /**
     * Fetch messages from every read queue of a topic via a CompletableFuture chain.
     *
     * <p>Each queue is visited once. For each queue the
     * {@link #getMessageAsync(CompletableFuture, String, String, String, int, int, MessageFilter, PopConsumerRecord.RetryType)}
     * method is chained via {@link CompletableFuture#thenCompose}. The chain carries
     * the accumulated result through all queues, stopping early when the batch is
     * filled, the queue is blocked, or the inflight threshold is reached.
     *
     * <p>Queue iteration order respects {@code priorityOrderAsc} and uses
     * {@code requestCount} as a round-robin offset for load balancing.
     *
     * @param future       the accumulator future
     * @param clientHost   the client address
     * @param groupId      consumer group id
     * @param topicId      topic name
     * @param requestCount round-robin counter for queue selection
     * @param batchSize    max number of messages to return
     * @param filter       message filter expression
     * @param retryType    whether this is a retry topic V1/V2
     * @return a future completing with the pop result context
     */
    protected CompletableFuture<PopConsumerContext> getMessageFromTopicAsync(CompletableFuture<PopConsumerContext> future,
        String clientHost, String groupId, String topicId, long requestCount, int batchSize, MessageFilter filter,
        PopConsumerRecord.RetryType retryType) {
        // get topic config
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (null == topicConfig) {
            return future;
        }

        // iterate all queues of the topic
        for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
            long index = (brokerController.getBrokerConfig().isPriorityOrderAsc() ?
                topicConfig.getReadQueueNums() - 1 - i : i) + requestCount;
            int current = (int) index % topicConfig.getReadQueueNums();
            future = this.getMessageAsync(future, clientHost, groupId,
                topicId, current, batchSize, filter, retryType);
        }
        return future;
    }

    /**
     * Asynchronously pop messages for the KVStore-based ack path.
     *
     * <p>This method coordinates the full Pop lifecycle:
     * <ol>
     *   <li>Validates topic, group, and acquires the consumer lock</li>
     *   <li>Determines whether to pull from retry topic first
     *       (based on {@code popFromRetryProbability})</li>
     *   <li>Pulls messages from normal topic (and retry topic V1/V2 if configured)</li>
     *   <li>Writes checkpoints to {@link PopConsumerCache} (buffer merge) or
     *       {@link PopConsumerKVStore} (RocksDB)</li>
     *   <li>Re-encodes retry messages if needed</li>
     * </ol>
     *
     * @param clientHost   the client address
     * @param popTime      the pop invocation timestamp
     * @param invisibleTime the message visibility timeout
     * @param groupId      consumer group id
     * @param topicId      topic name
     * @param queueId      queue id (-1 for all queues)
     * @param batchSize    max number of messages to return
     * @param fifo         whether this is a FIFO ordered consumption
     * @param attemptId    attempt id for idempotent consumption
     * @param initMode     consume init mode (min/max)
     * @param filter       message filter expression
     * @return a future that completes with the pop result context
     */
    public CompletableFuture<PopConsumerContext> popAsync(String clientHost, long popTime, long invisibleTime,
        String groupId, String topicId, int queueId, int batchSize, boolean fifo, String attemptId, int initMode,
        MessageFilter filter) {

        // init context params
        PopConsumerContext popConsumerContext =
            new PopConsumerContext(clientHost, popTime, invisibleTime, groupId, fifo, initMode, attemptId);

        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig == null || !consumerLockService.tryLock(groupId, topicId)) {
            return CompletableFuture.completedFuture(popConsumerContext);
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupId);
        if (null == subscriptionGroupConfig || !subscriptionGroupConfig.isConsumeEnable()) {
            return CompletableFuture.completedFuture(popConsumerContext);
        }

        log.debug("PopConsumerService popAsync, groupId={}, topicId={}, queueId={}, " +
                "batchSize={}, invisibleTime={}, fifo={}, attemptId={}, filter={}",
            groupId, topicId, queueId, batchSize, invisibleTime, fifo, attemptId, filter);

        String requestKey = groupId + "@" + topicId;
        String retryTopicV1 = KeyBuilder.buildPopRetryTopicV1(topicId, groupId);
        String retryTopicV2 = KeyBuilder.buildPopRetryTopicV2(topicId, groupId);
        long requestCount = Objects.requireNonNull(ConcurrentHashMapUtils.computeIfAbsent(
            requestCountTable, requestKey, k -> new AtomicLong(0L))).getAndIncrement();
        boolean usePriorityMode = TopicMessageType.PRIORITY.equals(topicConfig.getTopicMessageType())
            && !fifo && requestCount % 100L < subscriptionGroupConfig.getPriorityFactor();
        int probability = usePriorityMode ?
            brokerConfig.getPopFromRetryProbabilityForPriority() : brokerConfig.getPopFromRetryProbability();
        probability = Math.max(0, Math.min(100, probability)); // [51, 100] means always
        boolean preferRetry = probability > 0 && requestCount % (100 / probability) == 0L;
        requestCount = usePriorityMode ? 0 : requestCount; // use requestCount as randomQ

        CompletableFuture<PopConsumerContext> getMessageFuture =
            CompletableFuture.completedFuture(popConsumerContext);

        try {
            // get message from retry topic,
            if (!fifo && preferRetry) {
                // default config of retrieveMessageFromPopRetryTopicV1 is true,
                if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                    getMessageFuture = this.getMessageFromTopicAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV1, requestCount, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                }

                // default config of enableRetryTopicV2 is false
                if (brokerConfig.isEnableRetryTopicV2()) {
                    getMessageFuture = this.getMessageFromTopicAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV2, requestCount, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                }
            }

            // get message from normal topic
            if (queueId != -1) {
                getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                    topicId, queueId, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);
            } else {
                getMessageFuture = this.getMessageFromTopicAsync(getMessageFuture, clientHost, groupId,
                    topicId, requestCount, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);

                // get message from retry topic
                if (!fifo && !preferRetry) {
                    if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                        getMessageFuture = this.getMessageFromTopicAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV1, requestCount, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                    }

                    if (brokerConfig.isEnableRetryTopicV2()) {
                        getMessageFuture = this.getMessageFromTopicAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV2, requestCount, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                    }
                }
            }

            return getMessageFuture.thenCompose(result -> {
                if (result.isFound() && !result.isFifo()) {
                    // write checkpoint to cache or store
                    // default config of enablePopBufferMerge is false
                    if (brokerConfig.isEnablePopBufferMerge() &&
                        popConsumerCache != null && !popConsumerCache.isCacheFull()) {
                        this.popConsumerCache.writeRecords(result.getPopConsumerRecordList());
                    } else {
                        this.popConsumerStore.writeRecords(result.getPopConsumerRecordList());
                    }

                    // format result
                    for (int i = 0; i < result.getGetMessageResultList().size(); i++) {
                        GetMessageResult getMessageResult = result.getGetMessageResultList().get(i);
                        PopConsumerRecord popConsumerRecord = result.getPopConsumerRecordList().get(i);

                        // If the buffer belong retries message, the message needs to be re-encoded.
                        // The buffer should not be re-encoded when popResponseReturnActualRetryTopic
                        // is true or the current topic is not a retry topic.
                        boolean recode = brokerConfig.isPopResponseReturnActualRetryTopic();
                        if (recode && popConsumerRecord.isRetry()) {
                            result.getGetMessageResultList().set(i, this.recodeRetryMessage(
                                getMessageResult, popConsumerRecord.getTopicId(),
                                popConsumerRecord.getQueueId(), result.getPopTime(), invisibleTime));
                        }
                    }
                }
                return CompletableFuture.completedFuture(result);
            }).whenComplete((result, throwable) -> {
                // unlock by consumerLockService
                try {
                    if (throwable != null) {
                        log.error("PopConsumerService popAsync get message error",
                            throwable instanceof CompletionException ? throwable.getCause() : throwable);
                    }
                    if (result.getMessageCount() > 0) {
                        log.debug("PopConsumerService popAsync result, found={}, groupId={}, topicId={}, queueId={}, " +
                                "batchSize={}, invisibleTime={}, fifo={}, attemptId={}, filter={}", result.getMessageCount(),
                            groupId, topicId, queueId, batchSize, invisibleTime, fifo, attemptId, filter);
                    }
                } finally {
                    consumerLockService.unlock(groupId, topicId);
                }
            });
        } catch (Throwable t) {
            log.error("PopConsumerService popAsync error", t);
        }

        return getMessageFuture;
    }

    /**
     * Delete the acked record from the cache and/or RocksDB store.
     *
     * <p>The deletion is a two-step fallback:
     * <ul>
     *   <li>First, the record is deleted from {@link PopConsumerCache} (if buffer
     *       merge is enabled). If the record was present in the cache and removed
     *       successfully, the operation returns immediately without touching RocksDB</li>
     *   <li>If the cache is not enabled or the record was not found in the cache,
     *       deletion falls through to {@link PopConsumerKVStore#deleteRecords}</li>
     * </ul>
     *
     * <p>memo: Notify polling request when receive orderly ack
     *
     * @param popTime       the original pop time of the message
     * @param invisibleTime the original visibility timeout
     * @param groupId       consumer group id
     * @param topicId       topic name
     * @param queueId       queue id
     * @param offset        the acked offset
     * @return a future that completes with {@code true} on success
     */
    public CompletableFuture<Boolean> ackAsync(
        long popTime, long invisibleTime, String groupId, String topicId, int queueId, long offset) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService ack, time={}, invisible={}, groupId={}, topic={}, queueId={}, offset={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset);
        }

        PopConsumerRecord record = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(record)).isEmpty()) {
                return CompletableFuture.completedFuture(true);
            }
        }

        this.popConsumerStore.deleteRecords(Collections.singletonList(record));
        return CompletableFuture.completedFuture(true);
    }

    /**
     * Extend the visibility timeout of a popped message (KVStore path).
     *
     * <p>refer: ChangeInvisibleTimeProcessor.appendCheckPointThenAckOrigin
     * This is the KVStore equivalent of {@code ChangeInvisibleTimeProcessor#appendCheckPointThenAckOrigin}.
     *
     * <p>A new record with the updated timeout is written to the KVStore, and the
     * old record (identified by the original {@code popTime + invisibleTime}) is
     * deleted from the cache and KVStore.
     *
     * <p>If the new and old records have the same visibility timeout (e.g. the
     * consumer extended by the same duration it already had), the delete one is
     * skipped because the write one already overwrites the old record in RocksDB.
     *
     * @param popTime             the original pop time
     * @param invisibleTime       the original visibility timeout
     * @param changedPopTime      the new pop time (typically current time)
     * @param changedInvisibleTime the new visibility timeout
     * @param groupId             consumer group id
     * @param topicId             topic name
     * @param queueId             queue id
     * @param offset              the message offset
     * @param suspend             whether to suspend (nack without incrementing reconsume count)
     */
    public void changeInvisibilityDuration(long popTime, long invisibleTime, long changedPopTime,
                                           long changedInvisibleTime, String groupId, String topicId,
                                           int queueId, long offset, boolean suspend) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService change, time={}, invisible={}, " +
                    "groupId={}, topic={}, queueId={}, offset={}, new time={}, new invisible={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset, changedPopTime, changedInvisibleTime);
        }

        PopConsumerRecord ckRecord = new PopConsumerRecord(
            changedPopTime, groupId, topicId, queueId, 0, changedInvisibleTime, offset, null, suspend);

        PopConsumerRecord ackRecord = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null, suspend);

        // No need to generate new records when the group does not exist,
        // because these retry messages will not be consumed by anyone.
        // default value of popReviveSkipIfGroupAbsent is true
        boolean skipWrite = brokerConfig.isPopReviveSkipIfGroupAbsent() &&
            !brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(groupId);

        if (skipWrite) {
            log.info("PopConsumerService change invisibility skip, time={}, " +
                "groupId={}, topicId={}, queueId={}, offset={}", popTime, groupId, topicId, queueId, offset);
        } else {
            this.popConsumerStore.writeRecords(Collections.singletonList(ckRecord));
        }

        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(ackRecord)).isEmpty()) {
                return;
            }
        }

        // If the new CK has the same key as the old CK (same visibilityTimeout),
        // the write one already overwrites the old record in RocksDB, skip delete
        // to avoid removing the newly written record.
        if (skipWrite || ckRecord.getVisibilityTimeout() != ackRecord.getVisibilityTimeout()) {
            this.popConsumerStore.deleteRecords(Collections.singletonList(ackRecord));
        }
    }

    /**
     * Read the original message from storage for revival.
     * No external callers, except unit tests.
     *
     * <p>Used by {@link #revive(PopConsumerRecord)} when a visibility timeout
     * expires. Delegates to {@link org.apache.rocketmq.broker.EscapeBridge}
     * which can read from either the local store or a remote broker's store.
     *
     * @param consumerRecord the expired record
     * @return a triple of (message, info, needRetry)
     */
    // Use broker escape bridge to support remote read
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(PopConsumerRecord consumerRecord) {
        return this.brokerController.getEscapeBridge().getMessageAsync(consumerRecord.getTopicId(),
            consumerRecord.getOffset(), consumerRecord.getQueueId(), brokerConfig.getBrokerName(), false);
    }

    // No external callers, only called by unit tests.
    public CompletableFuture<Boolean> revive(PopConsumerRecord record) {

        if (brokerConfig.isPopReviveSkipIfGroupAbsent() &&
            !brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(record.getGroupId())) {
            log.info("PopConsumerService skip revive message, record={}", record);
            return CompletableFuture.completedFuture(true);
        }

        return this.getMessageAsync(record)
            .thenCompose(result -> {
                if (result == null) {
                    log.error("PopConsumerService revive error, message may be lost, record={}", record);
                    return CompletableFuture.completedFuture(false);
                }
                // true in triple right means get message needs to be retried
                if (result.getLeft() == null) {
                    log.info("PopConsumerService revive no need retry, record={}", record);
                    return CompletableFuture.completedFuture(!result.getRight());
                }
                return CompletableFuture.completedFuture(this.reviveRetry(record, result.getLeft()));
            });
    }

    // No external callers, only called by unit tests.
    public void clearCache(String groupId, String topicId, int queueId) {
        if (popConsumerCache != null) {
            popConsumerCache.removeRecords(groupId, topicId, queueId);
        }
    }

    // No external callers, only called by unit tests.
    public long revive(AtomicLong currentTime, int maxCount) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long upperTime = System.currentTimeMillis() - 50L;
        List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
                currentTime.get() - TimeUnit.SECONDS.toMillis(3), upperTime, maxCount);
        long scanCostTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // When reading messages from local storage, the current thread is used
        // directly for data retrieval. When reading original messages from remote
        // storage (such as distributed file systems), so concurrency needs to be
        // controlled via semaphore.
        Semaphore semaphore = new Semaphore(brokerConfig.getPopReviveConcurrency());
        Queue<PopConsumerRecord> failureList = new LinkedBlockingQueue<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>(consumerRecords.size());

        // could merge read operation here
        for (PopConsumerRecord record : consumerRecords) {
            CompletableFuture<Boolean> future;
            try {
                semaphore.acquire();
                future = this.revive(record);
            } catch (Exception e) {
                semaphore.release();
                throw new RuntimeException(e);
            }
            futureList.add(future.thenAccept(result -> {
                if (!result) {
                    if (record.getAttemptTimes() < brokerConfig.getPopReviveMaxAttemptTimes()) {
                        long backoffInterval = 1000L * REWRITE_INTERVALS_IN_SECONDS[
                            Math.min(REWRITE_INTERVALS_IN_SECONDS.length - 1, record.getAttemptTimes())];
                        long nextInvisibleTime = record.getInvisibleTime() + backoffInterval;
                        PopConsumerRecord retryRecord = new PopConsumerRecord(System.currentTimeMillis(),
                            record.getGroupId(), record.getTopicId(), record.getQueueId(),
                            record.getRetryFlag(), nextInvisibleTime, record.getOffset(), record.getAttemptId());
                        retryRecord.setAttemptTimes(record.getAttemptTimes() + 1);
                        failureList.add(retryRecord);
                        log.warn("PopConsumerService revive backoff retry, record={}", retryRecord);
                    } else {
                        log.error("PopConsumerService drop record, message may be lost, record={}", record);
                    }
                }
            }).whenComplete((result, ex) -> semaphore.release()));
        }

        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        this.popConsumerStore.writeRecords(new ArrayList<>(failureList));
        this.popConsumerStore.deleteRecords(consumerRecords);
        currentTime.set(consumerRecords.isEmpty() ?
            upperTime : consumerRecords.get(consumerRecords.size() - 1).getVisibilityTimeout());

        if (brokerConfig.isEnablePopBufferMerge()) {
            log.info("PopConsumerService, key size={}, cache size={}, revive count={}, failure count={}, " +
                    "behindInMillis={}, scanInMillis={}, costInMillis={}",
                popConsumerCache.getCacheKeySize(), popConsumerCache.getCacheSize(),
                consumerRecords.size(), failureList.size(), upperTime - currentTime.get(),
                scanCostTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            log.info("PopConsumerService, revive count={}, failure count={}, " +
                    "behindInMillis={}, scanInMillis={}, costInMillis={}",
                consumerRecords.size(), failureList.size(), upperTime - currentTime.get(),
                scanCostTime, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }

        return consumerRecords.size();
    }

    // No external callers, only called by unit tests.
    public void createRetryTopicIfNeeded(String groupId, String retryTopic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(retryTopic);
        if (topicConfig != null && !brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
            return;
        }

        int retryQueueNum = PopAckConstants.retryQueueNum;
        if (brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
            String normalTopic = KeyBuilder.parseNormalTopic(retryTopic, groupId);
            TopicConfig normalConfig = brokerController.getTopicConfigManager().selectTopicConfig(normalTopic); // always exists
            retryQueueNum = normalConfig.getWriteQueueNums();
            if (topicConfig != null && topicConfig.getWriteQueueNums() == normalConfig.getWriteQueueNums()) {
                return;
            }
        }

        topicConfig = new TopicConfig(retryTopic, retryQueueNum, retryQueueNum,
            PermName.PERM_READ | PermName.PERM_WRITE, 0);
        topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
        brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        for (int i = 0; i < retryQueueNum; i++) {
            long offset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, retryTopic, i);
            if (offset < 0) {
                this.brokerController.getConsumerOffsetManager().commitOffset(
                    "InitPopOffset", groupId, retryTopic, i, 0);
            }
        }
    }

    @SuppressWarnings("DuplicatedCode")
    // org.apache.rocketmq.broker.processor.PopReviveService#reviveRetry
    // No external callers, only called by unit tests.
    public boolean reviveRetry(PopConsumerRecord record, MessageExt messageExt) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService revive, time={}, invisible={}, groupId={}, topic={}, queueId={}, offset={}",
                record.getPopTime(), record.getInvisibleTime(), record.getGroupId(), record.getTopicId(),
                record.getQueueId(), record.getOffset());
        }

        boolean retry = StringUtils.startsWith(record.getTopicId(), MixAll.RETRY_GROUP_TOPIC_PREFIX);
        String retryTopic = retry ? record.getTopicId() : KeyBuilder.buildPopRetryTopic(
            record.getTopicId(), record.getGroupId(), brokerConfig.isEnableRetryTopicV2());
        this.createRetryTopicIfNeeded(record.getGroupId(), retryTopic);

        // deep copy here
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(retryTopic);
        msgInner.setBody(messageExt.getBody() != null ? messageExt.getBody() : new byte[] {});
        msgInner.setQueueId(getRetryQueueId(retryTopic, messageExt));
        if (messageExt.getTags() != null) {
            msgInner.setTags(messageExt.getTags());
        } else {
            MessageAccessor.setProperties(msgInner, new HashMap<>());
        }

        msgInner.setBornTimestamp(messageExt.getBornTimestamp());
        msgInner.setFlag(messageExt.getFlag());
        msgInner.setSysFlag(messageExt.getSysFlag());
        msgInner.setBornHost(brokerController.getStoreHost());
        msgInner.setStoreHost(brokerController.getStoreHost());
        if (record.isSuspend()) {
            msgInner.setReconsumeTimes(messageExt.getReconsumeTimes());
        } else {
            msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        }

        msgInner.getProperties().putAll(messageExt.getProperties());

        // set first pop time here
        if (messageExt.getReconsumeTimes() == 0 ||
            msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(record.getPopTime()));
        }
        msgInner.getProperties().put(MessageConst.PROPERTY_ORIGIN_GROUP, record.getGroupId());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        PutMessageResult putMessageResult =
            brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);

        if (putMessageResult.getAppendMessageResult() == null ||
            putMessageResult.getAppendMessageResult().getStatus() != AppendMessageStatus.PUT_OK) {
            log.error("PopConsumerService revive retry msg error, put status={}, ck={}, delay={}ms",
                putMessageResult, JSON.toJSONString(record), System.currentTimeMillis() - record.getVisibilityTimeout());
            return false;
        }

        if (this.brokerController.getBrokerStatsManager() != null) {
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(msgInner.getTopic(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
            this.brokerController.getBrokerStatsManager().incTopicPutSize(
                msgInner.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
        }
        return true;
    }

    private int getRetryQueueId(String retryTopic, MessageExt oriMsg) {
        if (!brokerController.getBrokerConfig().isUseSeparateRetryQueue()) {
            return 0;
        }
        int oriQueueId = oriMsg.getQueueId(); // original qid of normal or retry topic
        if (oriQueueId > brokerController.getTopicConfigManager().selectTopicConfig(retryTopic).getWriteQueueNums() - 1) {
            log.warn("not expected, {}, {}, {}", retryTopic, oriQueueId, oriMsg.getMsgId());
            return 0; // fallback
        }
        return oriQueueId;
    }

    // Export kv store record to revive topic
    // admin service
    @SuppressWarnings("ExtractMethodRecommender")
    public synchronized void transferToFsStore() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            try {
                List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
                    0, Long.MAX_VALUE, brokerConfig.getPopReviveMaxReturnSizePerRead());
                if (consumerRecords == null || consumerRecords.isEmpty()) {
                    break;
                }
                for (PopConsumerRecord record : consumerRecords) {
                    PopCheckPoint ck = new PopCheckPoint();
                    ck.setBitMap(0);
                    ck.setNum((byte) 1);
                    ck.setPopTime(record.getPopTime());
                    ck.setInvisibleTime(record.getInvisibleTime());
                    ck.setStartOffset(record.getOffset());
                    ck.setCId(record.getGroupId());
                    ck.setTopic(record.getTopicId());
                    ck.setQueueId(record.getQueueId());
                    ck.setBrokerName(brokerConfig.getBrokerName());
                    ck.addDiff(0);
                    ck.setRePutTimes(String.valueOf(record.getAttemptTimes()));
                    int reviveQueueId = (int) record.getOffset() % brokerConfig.getReviveQueueNum();
                    MessageExtBrokerInner ckMsg =
                        brokerController.getPopMessageProcessor().buildCkMsg(ck, reviveQueueId);
                    brokerController.getMessageStore().asyncPutMessage(ckMsg).join();
                }
                log.info("PopConsumerStore transfer from kvStore to fsStore, count={}", consumerRecords.size());
                this.popConsumerStore.deleteRecords(consumerRecords);
                this.waitForRunning(1);
            } catch (Throwable t) {
                log.error("PopConsumerStore transfer from kvStore to fsStore failure", t);
            }
        }
        log.info("PopConsumerStore transfer to fsStore finish, cost={}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public String getServiceName() {
        return PopConsumerService.class.getSimpleName();
    }

    @VisibleForTesting
    protected PopConsumerKVStore getPopConsumerStore() {
        return popConsumerStore;
    }

    public PopConsumerLockService getConsumerLockService() {
        return consumerLockService;
    }

    @Override
    public void start() {
        if (!this.popConsumerStore.start()) {
            throw new RuntimeException("PopConsumerStore init error");
        }
        if (this.popConsumerCache != null) {
            this.popConsumerCache.start();
        }
        super.start();
    }

    @Override
    public void shutdown() {
        // Block shutdown thread until write records finish
        super.shutdown();
        do {
            this.waitForRunning(10);
        }
        while (consumerRunning.get());
        if (this.popConsumerCache != null) {
            this.popConsumerCache.shutdown();
        }
        if (this.popConsumerStore != null) {
            this.popConsumerStore.shutdown();
        }
    }

    /**
     * Background thread that periodically revives expired Pop records.
     *
     * <p>Each iteration:
     * <ol>
     *   <li>Calls {@link #revive(AtomicLong, int)} to scan the RocksDB store for
     *       records whose visibility timeout has elapsed, fetch the original
     *       message, and re-publish it to the retry topic</li>
     *   <li>Cleans up stale consumer locks every minute</li>
     *   <li>When the number of revived records is below the batch limit, sleeps
     *       for a short interval to avoid busy-waiting</li>
     * </ol>
     */
    @Override
    public void run() {
        this.consumerRunning.set(true);
        while (!isStopped()) {
            try {
                // to prevent concurrency issues during read and write operations
                long reviveCount = this.revive(this.currentTime,
                    brokerConfig.getPopReviveMaxReturnSizePerRead());

                long current = System.currentTimeMillis();
                if (lastCleanupLockTime.get() + TimeUnit.MINUTES.toMillis(1) < current) {
                    this.consumerLockService.removeTimeout();
                    this.lastCleanupLockTime.set(current);
                }

                if (reviveCount < brokerConfig.getPopReviveMaxReturnSizePerRead()) {
                    this.waitForRunning(500);
                }
            } catch (Exception e) {
                log.error("PopConsumerService revive error", e);
                this.waitForRunning(500);
            }
        }
        this.consumerRunning.set(false);
    }
}
