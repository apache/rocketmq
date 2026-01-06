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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    private final BrokerController brokerController;
    private final PopLiteLongPollingService popLiteLongPollingService;
    private final PopConsumerLockService lockService;
    private final LiteEventDispatcher liteEventDispatcher;
    private final ConsumerOrderInfoManager consumerOrderInfoManager;
    private final PopLiteLockManager popLiteLockManager;

    public PopLiteMessageProcessor(final BrokerController brokerController, LiteEventDispatcher liteEventDispatcher) {
        this.brokerController = brokerController;
        this.popLiteLongPollingService = new PopLiteLongPollingService(brokerController, this, false);
        this.lockService = new PopConsumerLockService(TimeUnit.MINUTES.toMillis(1));
        this.liteEventDispatcher = liteEventDispatcher;
        this.consumerOrderInfoManager = new MemoryConsumerOrderInfoManager(brokerController);
        this.popLiteLockManager = new PopLiteLockManager();
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

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
        RemotingCommand preCheckResponse = preCheck(ctx, requestHeader, response);
        if (preCheckResponse != null) {
            return preCheckResponse;
        }

        String clientId = requestHeader.getClientId();
        String group = requestHeader.getConsumerGroup();
        String parentTopic = requestHeader.getTopic();
        int maxNum = requestHeader.getMaxMsgNum();
        long popTime = System.currentTimeMillis();
        long invisibleTime = requestHeader.getInvisibleTime();

        Pair<StringBuilder, GetMessageResult> rst = popByClientId(channel.remoteAddress().toString(), parentTopic,
            group, clientId, popTime, invisibleTime, maxNum, requestHeader.getAttemptId());

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
        NettyRemotingAbstract.writeResponse(channel, request, response, null, brokerController.getBrokerMetricsManager().getRemotingMetricsManager());
        return null;
    }

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
            LOGGER.error("The parentTopic {} not exist, consumer: {} ", requestHeader.getTopic());
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic [%s] not exist, apply first please! %s", requestHeader.getTopic(),
                FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic [%s] peeking message is forbidden", requestHeader.getTopic()));
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

    public Pair<StringBuilder, GetMessageResult> popByClientId(String clientHost, String parentTopic, String group,
        String clientId, long popTime, long invisibleTime, int maxNum, String attemptId) {
        GetMessageResult getMessageResult = new GetMessageResult();
        StringBuilder orderCountInfoAll = new StringBuilder();
        AtomicLong total = new AtomicLong(0);

        Set<String> processed = new HashSet<>(); // deduplication in one request
        Iterator<String> iterator = liteEventDispatcher.getEventIterator(clientId);
        while (total.get() < maxNum && iterator.hasNext()) {
            String lmqName = iterator.next(); // here event represents a lmq name
            if (null == lmqName) {
                break;
            }
            if (!processed.add(lmqName)) {
                continue; // wait for next pop request or re-fetch in current process, here prefer the former approach
            }
            Pair<StringBuilder, GetMessageResult> pair = popLiteTopic(parentTopic, clientHost, group, lmqName,
                maxNum - total.get(), popTime, invisibleTime, attemptId);
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

    @VisibleForTesting
    public Pair<StringBuilder, GetMessageResult> popLiteTopic(String parentTopic, String clientHost, String group,
        String lmqName, long maxNum, long popTime, long invisibleTime, String attemptId) {
        if (!brokerController.getBrokerConfig().isEnableLiteEventMode()
            && !brokerController.getLiteLifecycleManager().isLmqExist(lmqName)) {
            return null;
        }
        String lockKey = KeyBuilder.buildPopLiteLockKey(group, lmqName);
        if (!lockService.tryLock(lockKey)) {
            return null;
        }
        try {
            if (isFifoBlocked(attemptId, group, lmqName, invisibleTime)) {
                return null;
            }
            final long consumeOffset = getPopOffset(group, lmqName);
            GetMessageResult result = getMessage(clientHost, group, lmqName, consumeOffset, (int) maxNum);
            return handleGetMessageResult(result, parentTopic, group, lmqName, popTime, invisibleTime, attemptId);
        } catch (Throwable e) {
            LOGGER.error("popLiteTopic error. {}, {}", group, lmqName, e);
        } finally {
            lockService.unlock(lockKey);
        }
        return null;
    }

    public boolean isFifoBlocked(String attemptId, String group, String lmqName, long invisibleTime) {
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
