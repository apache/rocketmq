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
package org.apache.rocketmq.broker.processor;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.lite.LiteMetadataUtil;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLiteLongPollingService;
import org.apache.rocketmq.broker.metrics.LiteConsumerLagCalculator;
import org.apache.rocketmq.broker.offset.MemoryConsumerOrderInfoManager;
import org.apache.rocketmq.broker.pop.PopConsumerLockService;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;

/**
 * Pop lite implementation, support FIFO consuming.
 * This processor uses independent in-memory consumer order info and lock service,
 * along with a specialized long polling service.
 */
public class PopLiteMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);
    private static final String BORN_TIME = "bornTime";
    private static final String ARRIVE_TIME = "liteArriveTime";
    // idle time before the record of a client is cleaned
    @VisibleForTesting
    static final long ATTEMPT_TTL = TimeUnit.MINUTES.toNanos(3);
    // max time a retry waits for queue locks to release its leases
    private static final long RELEASE_LOCK_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(200);

    private final BrokerController brokerController;
    private final PopLiteLongPollingService popLiteLongPollingService;
    private final PopConsumerLockService lockService;
    private final LiteEventDispatcher liteEventDispatcher;
    private final MemoryConsumerOrderInfoManager consumerOrderInfoManager;
    private final PopLiteLockManager popLiteLockManager;
    // clientId -> the latest request of the client, assuming at most one pop with an attemptId in flight per client
    private final Map<String, Attempt> attempts = new ConcurrentHashMap<>();

    public PopLiteMessageProcessor(final BrokerController brokerController, LiteEventDispatcher liteEventDispatcher) {
        this.brokerController = brokerController;
        this.popLiteLongPollingService = new PopLiteLongPollingService(brokerController, this, false);
        this.lockService = new PopConsumerLockService(TimeUnit.MINUTES.toMillis(1));
        this.liteEventDispatcher = liteEventDispatcher;
        this.consumerOrderInfoManager = new MemoryConsumerOrderInfoManager(brokerController);
        this.popLiteLockManager = new PopLiteLockManager();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        // parse request
        final long beginTimeMills = brokerController.getMessageStore().now();
        Channel channel = ctx.channel();
        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }
        RemotingCommand response = RemotingCommand.createResponseCommand(PopLiteMessageResponseHeader.class);
        response.setOpaque(request.getOpaque());

        final PopLiteMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(PopLiteMessageRequestHeader.class, true);
        final PopLiteMessageResponseHeader responseHeader = (PopLiteMessageResponseHeader) response.readCustomHeader();

        // validation
        RemotingCommand preCheckResponse = preCheck(ctx, requestHeader, response);
        if (preCheckResponse != null) {
            return preCheckResponse;
        }

        String clientId = requestHeader.getClientId();
        String group = requestHeader.getConsumerGroup();
        String parentTopic = requestHeader.getTopic();
        long invisibleTime = requestHeader.getInvisibleTime();

        String attemptId = requestHeader.getAttemptId();
        long arriveTime = Long.MAX_VALUE;
        if (StringUtils.isNotEmpty(attemptId)) {
            if (request.isSuspended()) {
                arriveTime = Long.parseLong(request.getExtFields().get(ARRIVE_TIME));
                if (hasNewerRequest(clientId, arriveTime)) {
                    // the client has given up this request, pass the wake-up on to its latest one
                    popLiteLongPollingService.notifyMessageArriving(clientId, true, -1, group);
                    response.setCode(ResponseCode.POLLING_TIMEOUT);
                    response.setRemark(GetMessageStatus.NO_MESSAGE_IN_QUEUE.name());
                    return response;
                }
            } else {
                // overwrite any value carried in by the request
                arriveTime = System.nanoTime();
                request.addExtField(ARRIVE_TIME, String.valueOf(arriveTime));
                if (!onArrival(group, clientId, attemptId, arriveTime)) {
                    response.setCode(ResponseCode.SYSTEM_BUSY);
                    response.setRemark("Lite attempt lease recovery is incomplete or superseded");
                    return response;
                }
            }
        }
        long popTime = System.currentTimeMillis();
        Pair<StringBuilder, GetMessageResult> rst = popByClientId(channel.remoteAddress().toString(), parentTopic,
            group, clientId, popTime, invisibleTime, requestHeader.getMaxMsgNum(), attemptId, arriveTime);

        final GetMessageResult getMessageResult = rst.getObject2();
        if (getMessageResult != null && getMessageResult.getMessageCount() > 0) {
            final byte[] r = readGetMessageResult(getMessageResult);
            brokerController.getBrokerStatsManager().incGroupGetLatency(group, parentTopic, 0,
                (int) (brokerController.getMessageStore().now() - beginTimeMills));
            brokerController.getBrokerStatsManager().incBrokerGetNums(parentTopic, getMessageResult.getMessageCount());
            brokerController.getBrokerStatsManager().incGroupGetNums(group, parentTopic, getMessageResult.getMessageCount());
            brokerController.getBrokerStatsManager().incGroupGetSize(group, parentTopic, getMessageResult.getBufferTotalSize());
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(GetMessageStatus.FOUND.name());
            response.setBody(r);
        } else {
            response.setRemark(GetMessageStatus.NO_MESSAGE_IN_QUEUE.name());
            PollingResult pollingResult = popLiteLongPollingService.polling(ctx, request, requestHeader.getBornTime(),
                requestHeader.getPollTime(), clientId, group);
            if (PollingResult.POLLING_SUC.equals(pollingResult)) {
                return null;
            } else if (PollingResult.POLLING_FULL.equals(pollingResult)) {
                response.setCode(ResponseCode.POLLING_FULL);
            } else {
                response.setCode(ResponseCode.POLLING_TIMEOUT);
            }
        }

        responseHeader.setPopTime(popTime);
        responseHeader.setInvisibleTime(invisibleTime);
        responseHeader.setReviveQid(KeyBuilder.POP_ORDER_REVIVE_QUEUE);
        responseHeader.setOrderCountInfo(rst.getObject1().toString());
        // Since a single read operation potentially retrieving messages from multiple LMQs,
        // we no longer utilize startOffset and msgOffset
        // write response to socket channel
        NettyRemotingAbstract.writeResponse(channel, request, response, null, brokerController.getBrokerMetricsManager().getRemotingMetricsManager());
        return null;
    }

    /**
     * Validate the Pop Lite request:
     * Returns a ready-to-send error {@link RemotingCommand} when failed,
     * or {@code null} when passes all checks and Pop may proceed.
     *
     * <p>The checks are layered as:
     * - timeout
     * - broker permission
     * - batch-size cap
     * - topic existence and readability
     * - topic message type must be {@link TopicMessageType#LITE}
     * - the subscription group exists, is enabled,
     *   and its {@code liteBindTopic} matches the requested topic.
     */
    @VisibleForTesting
    public RemotingCommand preCheck(ChannelHandlerContext ctx,
        PopLiteMessageRequestHeader requestHeader, RemotingCommand response) {
        if (requestHeader.isTimeoutTooMuch()) {
            response.setCode(ResponseCode.POLLING_TIMEOUT);
            response.setRemark(String.format("the broker[%s] pop message is timeout too much",
                brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (!PermName.isReadable(brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] pop message is forbidden",
                brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (requestHeader.getMaxMsgNum() > 32) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("the broker[%s] pop message's num is greater than 32",
                brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOGGER.error("The parentTopic {} not exist, consumer: {} ", requestHeader.getTopic(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic [%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic [%s] pop message is forbidden", requestHeader.getTopic()));
            return response;
        }

        if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("the topic [%s] message type not match", requestHeader.getTopic()));
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] not exist, %s",
                requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        if (!requestHeader.getTopic().equals(subscriptionGroupConfig.getLiteBindTopic())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark("subscription bind topic not match, " + requestHeader.getConsumerGroup());
            return response;
        }

        return null;
    }

    private byte[] readGetMessageResult(GetMessageResult getMessageResult) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(getMessageResult.getBufferTotalSize());
        try {
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                byteBuffer.put(bb);
            }
        } finally {
            getMessageResult.release();
        }
        return byteBuffer.array();
    }

    @VisibleForTesting
    Pair<StringBuilder, GetMessageResult> popByClientId(String clientHost, String parentTopic, String group,
        String clientId, long popTime, long invisibleTime, int maxNum, String attemptId, long arriveTime) {
        GetMessageResult getMessageResult = new GetMessageResult();
        StringBuilder orderCountInfoAll = new StringBuilder();
        AtomicLong total = new AtomicLong(0);

        boolean isExclusiveGroup = LiteMetadataUtil.isSubLiteExclusive(group, brokerController);
        Set<String> processed = new HashSet<>(); // deduplication in one request
        Iterator<String> iterator = liteEventDispatcher.getEventIterator(clientId);
        while (total.get() < maxNum && iterator.hasNext()) {
            String lmqName = iterator.next(); // here event represents a lmq name
            if (null == lmqName) {
                break;
            }
            if (hasNewerRequest(clientId, arriveTime)) {
                // may be an event re-dispatched by the newer request, so it is passed on rather than dropped
                redispatch(group, lmqName);
                break;
            }
            if (processed.contains(lmqName)) {
                // Already read in this pop; skip the duplicate. A FIFO-blocked or empty-read lmq is never marked
                // by popLiteTopic, so its later re-dispatch (ack-unblock or CQ commit) is retried, not deduped away.
                continue;
            }
            // Tombstone check: reject pull if this client was evicted from the liteTopic (exclusive mode)
            if (isExclusiveGroup && brokerController.getLiteSubscriptionRegistry().hasExclusiveEvictionTombstone(clientId, lmqName)) {
                LOGGER.info("popLiteTopic rejected by tombstone: clientId={}, group={}, lmqName={}", clientId, group, lmqName);
                continue;
            }
            Pair<StringBuilder, GetMessageResult> pair = popLiteTopic(parentTopic, clientHost, group, lmqName,
                maxNum - total.get(), popTime, invisibleTime, attemptId, processed, clientId, arriveTime);
            if (null == pair || pair.getObject2().getMessageCount() <= 0) {
                continue;
            }
            GetMessageResult singleResult = pair.getObject2();
            total.addAndGet(singleResult.getMessageCount());
            for (SelectMappedBufferResult mappedBuffer : singleResult.getMessageMapedList()) {
                getMessageResult.addMessage(mappedBuffer);
            }
            if (orderCountInfoAll.length() > 0) {
                orderCountInfoAll.append(";");
            }
            orderCountInfoAll.append(pair.getObject1());
            collectLiteConsumerLagMetrics(group, parentTopic, lmqName, singleResult, maxNum, total);
        }

        return new Pair<>(orderCountInfoAll, getMessageResult);
    }

    /**
     * Record a request as the latest of its client. A retry of the recorded attempt proves its response was lost, so
     * the leases it took are released first.
     */
    @VisibleForTesting
    boolean onArrival(String group, String clientId, String attemptId, long arriveTime) {
        Attempt[] retried = new Attempt[1];
        attempts.compute(clientId, (key, old) -> {
            if (old != null && old.arriveTime > arriveTime) {
                return old;
            }
            Attempt latest = new Attempt(group, attemptId, arriveTime);
            if (old != null && attemptId.equals(old.attemptId) && group.equals(old.group)) {
                // take over the leases left by an unfinished release
                latest.lmqNames.addAll(old.lmqNames);
                old.takenOver = true;
                retried[0] = latest;
            }
            return latest;
        });
        if (retried[0] != null && !releaseAttempt(retried[0], clientId)) {
            return false;
        }
        return !hasNewerRequest(clientId, arriveTime);
    }

    @VisibleForTesting
    boolean hasNewerRequest(String clientId, long arriveTime) {
        Attempt latest = attempts.get(clientId);
        return latest != null && latest.arriveTime > arriveTime;
    }

    /**
     * Called under the queue lock of the lease just taken.
     *
     * @return true if a newer request of the client has arrived, so the caller releases the lease itself
     */
    private boolean registerAttempt(String clientId, String group, String attemptId, String lmqName,
        long arriveTime) {
        Attempt latest = attempts.computeIfPresent(clientId, (key, attempt) -> {
            if (attemptId.equals(attempt.attemptId) && group.equals(attempt.group)
                && attempt.arriveTime <= arriveTime) {
                attempt.lmqNames.add(lmqName);
            }
            return attempt;
        });
        return latest != null && latest.arriveTime > arriveTime;
    }

    /**
     * Release the leases of an attempt and re-dispatch them. It stops once a newer retry of the same attempt takes
     * over, or a queue lock is not taken in time, leaving the rest recorded.
     */
    private boolean releaseAttempt(Attempt attempt, String clientId) {
        long deadline = System.nanoTime() + RELEASE_LOCK_TIMEOUT;
        for (String lmqName : attempt.lmqNames) {
            String lockKey = KeyBuilder.buildPopLiteLockKey(attempt.group, lmqName);
            if (!lockUntil(lockKey, deadline)) {
                break;
            }
            boolean released;
            try {
                // the newer retry may already have re-taken this lease
                if (attempt.takenOver) {
                    break;
                }
                released = consumerOrderInfoManager.releaseQueue(lmqName, attempt.group, 0, attempt.attemptId);
                attempt.lmqNames.remove(lmqName);
            } finally {
                lockService.unlock(lockKey);
            }
            LOGGER.info("lite attempt release lease. clientId={}, group={}, attemptId={}, lmqName={}, released={}",
                clientId, attempt.group, attempt.attemptId, lmqName, released);
            if (released) {
                redispatch(attempt.group, lmqName);
            }
        }
        if (!attempt.lmqNames.isEmpty()) {
            LOGGER.info("lite attempt release stopped. clientId={}, group={}, attemptId={}, takenOver={}, remaining={}",
                clientId, attempt.group, attempt.attemptId, attempt.takenOver, attempt.lmqNames);
        }
        return !attempt.takenOver && attempt.lmqNames.isEmpty();
    }

    private boolean lockUntil(String lockKey, long deadline) {
        while (!lockService.tryLock(lockKey)) {
            if (System.nanoTime() - deadline > 0) {
                return false;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        return true;
    }

    // a failed notification must not fail the pop that has read messages
    private void redispatch(String group, String lmqName) {
        try {
            liteEventDispatcher.dispatch(group, lmqName, 0, -1, -1);
        } catch (Exception e) {
            LOGGER.error("lite redispatch error. group={}, lmqName={}", group, lmqName, e);
        }
    }

    /**
     * Only the records are cleaned, the leases are left to ack or expiry as the batch may still be consumed.
     */
    @VisibleForTesting
    void cleanAttempts(long now) {
        attempts.forEach((clientId, attempt) -> {
            if (now - attempt.arriveTime > ATTEMPT_TTL) {
                attempts.remove(clientId, attempt);
            }
        });
    }

    private static final class Attempt {
        private final String group;
        private final String attemptId;
        private final long arriveTime;
        // leases taken and not released yet
        private final Set<String> lmqNames = ConcurrentHashMap.newKeySet();
        // set once a newer retry of the same attempt inherits the leases
        private volatile boolean takenOver;

        private Attempt(String group, String attemptId, long arriveTime) {
            this.group = group;
            this.attemptId = attemptId;
            this.arriveTime = arriveTime;
        }
    }

    @VisibleForTesting
    Pair<StringBuilder, GetMessageResult> popLiteTopic(String parentTopic, String clientHost, String group,
        String lmqName, long maxNum, long popTime, long invisibleTime, String attemptId, Set<String> processed,
        String clientId, long arriveTime) {
        String lockKey = KeyBuilder.buildPopLiteLockKey(group, lmqName);
        if (!lockService.tryLock(lockKey)) {
            return null;
        }
        boolean redispatch = false;
        GetMessageResult result = null;
        try {
            if (hasNewerRequest(clientId, arriveTime)) {
                // the newer request may already hold this lease
                redispatch = true;
                return null;
            }
            if (isFifoBlocked(attemptId, group, lmqName, invisibleTime)) {
                // Leave this lmq unmarked so a later ack-unblock re-dispatch is retried, not deduped away.
                return null;
            }
            final long consumeOffset = getPopOffset(group, lmqName);
            result = getMessage(clientHost, group, lmqName, consumeOffset, (int) maxNum);
            if (result != null && result.getMessageCount() > 0) {
                // Mark only after messages are read: an empty read (e.g. CQ entry not yet committed) must not
                // dedup away a later re-dispatch of this lmq within the same pop.
                processed.add(lmqName);
            }
            Pair<StringBuilder, GetMessageResult> pair =
                handleGetMessageResult(result, parentTopic, group, lmqName, popTime, invisibleTime, attemptId);
            if (null != pair && pair.getObject2().getMessageCount() > 0 && StringUtils.isNotEmpty(attemptId)
                && registerAttempt(clientId, group, attemptId, lmqName, arriveTime)) {
                // the queue lock proves the lease is still the one just taken
                redispatch = true;
                consumerOrderInfoManager.releaseQueue(lmqName, group, 0, attemptId);
            }
            return pair;
        } catch (Throwable e) {
            LOGGER.error("popLiteTopic error. {}, {}", group, lmqName, e);
            if (result != null) {
                result.release();
            }
        } finally {
            lockService.unlock(lockKey);
            if (redispatch) {
                redispatch(group, lmqName);
            }
        }
        return null;
    }

    public boolean isFifoBlocked(String attemptId, String group, String lmqName, long invisibleTime) {
        if (brokerController.getBrokerConfig().isUseServerSideResetOffset() &&
            this.brokerController.getConsumerOffsetManager().hasOffsetReset(lmqName, group, 0)) {
            return false;
        }
        return consumerOrderInfoManager.checkBlock(attemptId, lmqName, group, 0, invisibleTime);
    }

    public long getPopOffset(String group, String lmqName) {
        long offset = brokerController.getConsumerOffsetManager().queryOffset(group, lmqName, 0);
        if (offset < 0L) {
            try {
                offset = brokerController.getPopMessageProcessor().getInitOffset(lmqName, group, 0, ConsumeInitMode.MAX, true); // reuse code, init as max
                LOGGER.info("init offset, group:{}, topic:{}, offset:{}", group, lmqName, offset);
            } catch (ConsumeQueueException e) {
                throw new RuntimeException(e);
            }
        }
        Long resetOffset = brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(lmqName, group, 0);
        if (resetOffset != null) {
            consumerOrderInfoManager.clearBlock(lmqName, group, 0);
            brokerController.getConsumerOffsetManager().commitOffset("ResetOffset", group, lmqName, 0, resetOffset);
            LOGGER.info("find resetOffset, group:{}, topic:{}, resetOffset:{}", group, lmqName, resetOffset);
            return resetOffset;
        }
        return offset;
    }

    public Pair<StringBuilder, GetMessageResult> handleGetMessageResult(GetMessageResult result, String parentTopic,
        String group, String lmqName, long popTime, long invisibleTime, String attemptId) {
        if (null == result) {
            return null;
        }

        StringBuilder orderCountInfo = new StringBuilder();
        if (GetMessageStatus.FOUND.equals(result.getStatus()) && !result.getMessageQueueOffset().isEmpty()) {
            consumerOrderInfoManager.update(attemptId, false, lmqName, group, 0,
                popTime, invisibleTime, result.getMessageQueueOffset(), orderCountInfo, null);
            recordPopLiteMetrics(result, parentTopic, group);
            orderCountInfo = transformOrderCountInfo(orderCountInfo, result.getMessageCount());
        }
        return new Pair<>(orderCountInfo, result);
    }

    /**
     * For order count information, we use a uniform format of one consume count per offset.
     */
    @VisibleForTesting
    public StringBuilder transformOrderCountInfo(StringBuilder orderCountInfo, int msgCount) {
        if (null == orderCountInfo || orderCountInfo.length() <= 0) {
            return new StringBuilder(String.join(";", Collections.nCopies(msgCount, "0")));
        }
        String infoStr = orderCountInfo.toString();
        String[] infos = infoStr.split(";");
        if (infos.length > 1) {
            // consume count of each offset + ";" + consume count of queueId
            return new StringBuilder(infoStr.substring(0, infoStr.lastIndexOf(";")));
        } else {
            // just consume count of queueId, like "0 0 N"
            String[] split = orderCountInfo.toString().split(MessageConst.KEY_SEPARATOR);
            if (split.length == 3) {
                return new StringBuilder(String.join(";", Collections.nCopies(msgCount, split[2])));
            } else {
                return new StringBuilder(String.join(";", Collections.nCopies(msgCount, "0")));
            }
        }
    }

    @VisibleForTesting
    protected void recordPopLiteMetrics(GetMessageResult result, String parentTopic, String group) {
        Attributes attributes = this.brokerController.getBrokerMetricsManager().newAttributesBuilder()
            .put(LABEL_TOPIC, parentTopic)
            .put(LABEL_CONSUMER_GROUP, group)
            .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(parentTopic) ||
                MixAll.isSysConsumerGroup(group))
            .put(LABEL_IS_RETRY, false)
            .build();
        this.brokerController.getBrokerMetricsManager().getMessagesOutTotal().add(result.getMessageCount(), attributes);
        this.brokerController.getBrokerMetricsManager().getThroughputOutTotal().add(result.getBufferTotalSize(), attributes);
    }

    private void collectLiteConsumerLagMetrics(String group, String topic, String liteTopic,
        GetMessageResult getResult, long maxNum, AtomicLong total) {
        if (!brokerController.getBrokerConfig().isLiteLagLatencyCollectEnable()) {
            return;
        }

        try {
            final LiteConsumerLagCalculator lagCalculator = brokerController.getBrokerMetricsManager()
                .getLiteConsumerLagCalculator();

            if (total.get() < maxNum) {
                // Batch not full, no consume lag
                lagCalculator.removeLagInfo(group, topic, liteTopic);
                return;
            }

            // Batch full, check for potential consume lag
            long storeTimestamp = brokerController.getMessageStore()
                .getMessageStoreTimeStamp(liteTopic, 0, getResult.getNextBeginOffset());
            if (storeTimestamp > 0) {
                lagCalculator.updateLagInfo(group, topic, liteTopic, storeTimestamp);
            } else {
                // no next msg, no consume lag
                lagCalculator.removeLagInfo(group, topic, liteTopic);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to collect lite consumer lag metrics for group={}, topic={}, liteTopic={}",
                group, topic, liteTopic, e);
        }
    }

    // tiered store ensures reading lmq from local storage
    public GetMessageResult getMessage(String clientHost, String group, String lmqName, long offset, int batchSize) {
        GetMessageResult result = brokerController.getMessageStore().getMessage(group, lmqName, 0, offset, batchSize, null);
        if (null == result) {
            return null;
        }
        if (GetMessageStatus.OFFSET_TOO_SMALL.equals(result.getStatus())
            || GetMessageStatus.OFFSET_OVERFLOW_BADLY.equals(result.getStatus())
            || GetMessageStatus.OFFSET_FOUND_NULL.equals(result.getStatus())
            || GetMessageStatus.NO_MATCHED_MESSAGE.equals(result.getStatus())
            || GetMessageStatus.MESSAGE_WAS_REMOVING.equals(result.getStatus())
            || GetMessageStatus.NO_MATCHED_LOGIC_QUEUE.equals(result.getStatus())) {

            long correctOffset = result.getNextBeginOffset(); // >=0
            brokerController.getConsumerOffsetManager().commitOffset("CorrectOffset", group, lmqName, 0, correctOffset);
            LOGGER.warn("correct offset, {}, {}, from {} to {}", group, lmqName, offset, correctOffset);
            return brokerController.getMessageStore().getMessage(group, lmqName, 0, correctOffset, batchSize, null);
        }
        return result;
    }

    public class PopLiteLockManager extends ServiceThread {
        private static final long AUTO_CLEAN_INTERVAL = 5 * 60 * 1000;
        private long lastCleanTime = System.currentTimeMillis();

        @Override
        public String getServiceName() {
            if (brokerController.getBrokerConfig().isInBrokerContainer()) {
                return brokerController.getBrokerIdentity().getIdentifier() + PopLiteLockManager.class.getSimpleName();
            }
            return PopLiteLockManager.class.getSimpleName();
        }

        @Override
        public void run() {
            while (!isStopped()) {
                try {
                    waitForRunning(60000);
                    lockService.removeTimeout();
                    cleanAttempts(System.nanoTime());
                    if (System.currentTimeMillis() - lastCleanTime >= AUTO_CLEAN_INTERVAL) {
                        consumerOrderInfoManager.autoClean();
                        lastCleanTime = System.currentTimeMillis();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    public PopLiteLongPollingService getPopLiteLongPollingService() {
        return popLiteLongPollingService;
    }

    public PopConsumerLockService getLockService() {
        return lockService;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public void startPopLiteLockManager() {
        popLiteLockManager.start();
    }

    public void stopPopLiteLockManager() {
        popLiteLockManager.shutdown();
    }
}
