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

import com.alibaba.fastjson.JSON;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
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
            brokerController.getMessageStoreConfig().getStorePathRootDir(), ROCKSDB_DIRECTORY).toString());
        this.popConsumerCache = brokerConfig.isEnablePopBufferMerge() ? new PopConsumerCache(
            brokerController, this.popConsumerStore, this.consumerLockService, this::revive) : null;

        log.info("PopConsumerService init, buffer={}, rocksdb filePath={}",
            brokerConfig.isEnablePopBufferMerge(), this.popConsumerStore.getFilePath());
    }

    /**
     * In-flight messages are those that have been received from a queue
     * by a consumer but have not yet been deleted. For standard queues,
     * there is a limit on the number of in-flight messages, depending on queue traffic and message backlog.
     */
    public boolean isPopShouldStop(String group, String topic, int queueId) {
        return brokerConfig.isEnablePopMessageThreshold() && popConsumerCache != null &&
            popConsumerCache.getPopInFlightMessageCount(group, topic, queueId) >=
                brokerConfig.getPopInflightMessageThreshold();
    }

    public long getPendingFilterCount(String groupId, String topicId, int queueId) {
        try {
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topicId, queueId);
            long consumeOffset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, queueId);
            return maxOffset - consumeOffset;
        } catch (ConsumeQueueException e) {
            throw new RuntimeException(e);
        }
    }

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

    public PopConsumerContext handleGetMessageResult(PopConsumerContext context, GetMessageResult result,
        String topicId, int queueId, PopConsumerRecord.RetryType retryType, long offset) {

        if (GetMessageStatus.FOUND.equals(result.getStatus()) && !result.getMessageQueueOffset().isEmpty()) {
            if (context.isFifo()) {
                this.setFifoBlocked(context, context.getGroupId(), topicId, queueId, result.getMessageQueueOffset());
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

    public long getPopOffset(String groupId, String topicId, int queueId, int initMode) {
        long offset = this.brokerController.getConsumerOffsetManager().queryPullOffset(groupId, topicId, queueId);
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

    public CompletableFuture<GetMessageResult> getMessageAsync(String clientHost,
        String groupId, String topicId, int queueId, long offset, int batchSize, MessageFilter filter) {

        log.debug("PopConsumerService getMessageAsync, groupId={}, topicId={}, queueId={}, offset={}, batchSize={}, filter={}",
            groupId, topicId, offset, queueId, batchSize, filter != null);

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
     */
    public void setFifoBlocked(PopConsumerContext context,
        String groupId, String topicId, int queueId, List<Long> queueOffsetList) {
        brokerController.getConsumerOrderInfoManager().update(
            context.getAttemptId(), false, topicId, groupId, queueId,
            context.getPopTime(), context.getInvisibleTime(), queueOffsetList, context.getOrderCountInfoBuilder());
    }

    public boolean isFifoBlocked(PopConsumerContext context, String groupId, String topicId, int queueId) {
        return brokerController.getConsumerOrderInfoManager().checkBlock(
            context.getAttemptId(), topicId, groupId, queueId, context.getInvisibleTime());
    }

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
                final long consumeOffset = this.getPopOffset(groupId, topicId, queueId, result.getInitMode());
                return getMessageAsync(clientHost, groupId, topicId, queueId, consumeOffset, remain, filter)
                    .thenApply(getMessageResult -> handleGetMessageResult(
                        result, getMessageResult, topicId, queueId, retryType, consumeOffset));
            }
        });
    }

    public CompletableFuture<PopConsumerContext> popAsync(String clientHost, long popTime, long invisibleTime,
        String groupId, String topicId, int queueId, int batchSize, boolean fifo, String attemptId, int initMode,
        MessageFilter filter) {

        PopConsumerContext popConsumerContext =
            new PopConsumerContext(clientHost, popTime, invisibleTime, groupId, fifo, initMode, attemptId);

        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig == null || !consumerLockService.tryLock(groupId, topicId)) {
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
        boolean preferRetry = requestCount % 5L == 0L;

        CompletableFuture<PopConsumerContext> getMessageFuture =
            CompletableFuture.completedFuture(popConsumerContext);

        try {
            if (!fifo && preferRetry) {
                if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV1, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                }

                if (brokerConfig.isEnableRetryTopicV2()) {
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        retryTopicV2, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                }
            }

            if (queueId != -1) {
                getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                    topicId, queueId, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);
            } else {
                for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                    int current = (int) ((requestCount + i) % topicConfig.getReadQueueNums());
                    getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                        topicId, current, batchSize, filter, PopConsumerRecord.RetryType.NORMAL_TOPIC);
                }

                if (!fifo && !preferRetry) {
                    if (brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                        getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV1, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V1);
                    }

                    if (brokerConfig.isEnableRetryTopicV2()) {
                        getMessageFuture = this.getMessageAsync(getMessageFuture, clientHost, groupId,
                            retryTopicV2, 0, batchSize, filter, PopConsumerRecord.RetryType.RETRY_TOPIC_V2);
                    }
                }
            }

            return getMessageFuture.thenCompose(result -> {
                if (result.isFound() && !result.isFifo()) {
                    if (brokerConfig.isEnablePopBufferMerge() &&
                        popConsumerCache != null && !popConsumerCache.isCacheFull()) {
                        this.popConsumerCache.writeRecords(result.getPopConsumerRecordList());
                    } else {
                        this.popConsumerStore.writeRecords(result.getPopConsumerRecordList());
                    }

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

    // Notify polling request when receive orderly ack
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

    // refer ChangeInvisibleTimeProcessor.appendCheckPointThenAckOrigin
    public void changeInvisibilityDuration(long popTime, long invisibleTime,
        long changedPopTime, long changedInvisibleTime, String groupId, String topicId, int queueId, long offset) {

        if (brokerConfig.isPopConsumerKVServiceLog()) {
            log.info("PopConsumerService change, time={}, invisible={}, " +
                    "groupId={}, topic={}, queueId={}, offset={}, new time={}, new invisible={}",
                popTime, invisibleTime, groupId, topicId, queueId, offset, changedPopTime, changedInvisibleTime);
        }

        PopConsumerRecord ckRecord = new PopConsumerRecord(
            changedPopTime, groupId, topicId, queueId, 0, changedInvisibleTime, offset, null);

        PopConsumerRecord ackRecord = new PopConsumerRecord(
            popTime, groupId, topicId, queueId, 0, invisibleTime, offset, null);

        this.popConsumerStore.writeRecords(Collections.singletonList(ckRecord));

        if (brokerConfig.isEnablePopBufferMerge() && popConsumerCache != null) {
            if (popConsumerCache.deleteRecords(Collections.singletonList(ackRecord)).isEmpty()) {
                return;
            }
        }

        this.popConsumerStore.deleteRecords(Collections.singletonList(ackRecord));
    }

    // Use broker escape bridge to support remote read
    public CompletableFuture<Triple<MessageExt, String, Boolean>> getMessageAsync(PopConsumerRecord consumerRecord) {
        return this.brokerController.getEscapeBridge().getMessageAsync(consumerRecord.getTopicId(),
            consumerRecord.getOffset(), consumerRecord.getQueueId(), brokerConfig.getBrokerName(), false);
    }

    public CompletableFuture<Boolean> revive(PopConsumerRecord record) {
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

    public void clearCache(String groupId, String topicId, int queueId) {
        while (consumerLockService.tryLock(groupId, topicId)) {
        }
        try {
            if (popConsumerCache != null) {
                popConsumerCache.removeRecords(groupId, topicId, queueId);
            }
        } finally {
            consumerLockService.unlock(groupId, topicId);
        }
    }

    public long revive(AtomicLong currentTime, int maxCount) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long upperTime = System.currentTimeMillis() - 50L;
        List<PopConsumerRecord> consumerRecords = this.popConsumerStore.scanExpiredRecords(
                currentTime.get() - TimeUnit.SECONDS.toMillis(3), upperTime, maxCount);
        long scanCostTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Queue<PopConsumerRecord> failureList = new LinkedBlockingQueue<>();
        List<CompletableFuture<?>> futureList = new ArrayList<>(consumerRecords.size());

        // could merge read operation here
        for (PopConsumerRecord record : consumerRecords) {
            futureList.add(this.revive(record).thenAccept(result -> {
                if (!result) {
                    if (record.getAttemptTimes() < brokerConfig.getPopReviveMaxAttemptTimes()) {
                        long backoffInterval = 1000L * REWRITE_INTERVALS_IN_SECONDS[
                            Math.min(REWRITE_INTERVALS_IN_SECONDS.length, record.getAttemptTimes())];
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
            }));
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

    public void createRetryTopicIfNeeded(String groupId, String topicId) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topicId);
        if (topicConfig != null) {
            return;
        }

        topicConfig = new TopicConfig(topicId, 1, 1,
            PermName.PERM_READ | PermName.PERM_WRITE, 0);
        topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
        brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(groupId, topicId, 0);
        if (offset < 0) {
            this.brokerController.getConsumerOffsetManager().commitOffset(
                "InitPopOffset", groupId, topicId, 0, 0);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    // org.apache.rocketmq.broker.processor.PopReviveService#reviveRetry
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
        msgInner.setQueueId(0);
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
        msgInner.setReconsumeTimes(messageExt.getReconsumeTimes() + 1);
        msgInner.getProperties().putAll(messageExt.getProperties());

        // set first pop time here
        if (messageExt.getReconsumeTimes() == 0 ||
            msgInner.getProperties().get(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
            msgInner.getProperties().put(MessageConst.PROPERTY_FIRST_POP_TIME, String.valueOf(record.getPopTime()));
        }
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

    // Export kv store record to revive topic
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
                    ck.setRePutTimes(ck.getRePutTimes());
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
