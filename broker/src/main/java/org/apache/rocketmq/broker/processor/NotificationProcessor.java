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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.longpolling.PollingHeader;
import org.apache.rocketmq.broker.longpolling.PollingResult;
import org.apache.rocketmq.broker.longpolling.PopLongPollingService;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.exception.ConsumeQueueException;

public class NotificationProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    private final PopLongPollingService popLongPollingService;
    private static final String BORN_TIME = "bornTime";

    public NotificationProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.popLongPollingService = new PopLongPollingService(brokerController, this, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    // When a new message is written to CommitLog, this method would be called.
    // Suspended long polling will receive notification and be wakeup.
    public void notifyMessageArriving(final String topic, final int queueId, long offset,
        Long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        this.popLongPollingService.notifyMessageArrivingWithRetryTopic(
            topic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }

    public void notifyMessageArriving(final String topic, final int queueId) {
        this.popLongPollingService.notifyMessageArrivingWithRetryTopic(topic, queueId);
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        request.addExtFieldIfNotExist(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        if (Objects.equals(request.getExtFields().get(BORN_TIME), "0")) {
            request.addExtField(BORN_TIME, String.valueOf(System.currentTimeMillis()));
        }
        Channel channel = ctx.channel();

        RemotingCommand response = RemotingCommand.createResponseCommand(NotificationResponseHeader.class);
        final NotificationResponseHeader responseHeader = (NotificationResponseHeader) response.readCustomHeader();
        final NotificationRequestHeader requestHeader =
            (NotificationRequestHeader) request.decodeCommandCustomHeader(NotificationRequestHeader.class);

        response.setOpaque(request.getOpaque());

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the broker[%s] peeking message is forbidden", this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] peeking message is forbidden");
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(errorInfo);
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }
        int randomQ = random.nextInt(100);
        boolean hasMsg = false;
        boolean needRetry = randomQ % 5 == 0;
        BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        if (needRetry) {
            String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
            hasMsg = hasMsgFromTopic(retryTopic, randomQ, requestHeader);
            if (!hasMsg && brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                String retryTopicConfigV1 = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                hasMsg = hasMsgFromTopic(retryTopicConfigV1, randomQ, requestHeader);
            }
        }
        if (!hasMsg) {
            if (requestHeader.getQueueId() < 0) {
                // read all queue
                hasMsg = hasMsgFromTopic(topicConfig, randomQ, requestHeader);
            } else {
                int queueId = requestHeader.getQueueId();
                hasMsg = hasMsgFromQueue(topicConfig.getTopicName(), requestHeader, queueId);
            }
            // if it doesn't have message, fetch retry again
            if (!needRetry && !hasMsg) {
                String retryTopic = KeyBuilder.buildPopRetryTopic(requestHeader.getTopic(), requestHeader.getConsumerGroup(), brokerConfig.isEnableRetryTopicV2());
                hasMsg = hasMsgFromTopic(retryTopic, randomQ, requestHeader);
                if (!hasMsg && brokerConfig.isEnableRetryTopicV2() && brokerConfig.isRetrieveMessageFromPopRetryTopicV1()) {
                    String retryTopicConfigV1 = KeyBuilder.buildPopRetryTopicV1(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                    hasMsg = hasMsgFromTopic(retryTopicConfigV1, randomQ, requestHeader);
                }
            }
        }

        if (!hasMsg) {
            PollingResult pollingResult = popLongPollingService.polling(ctx, request, new PollingHeader(requestHeader));
            if (pollingResult == PollingResult.POLLING_SUC) {
                return null;
            } else if (pollingResult == PollingResult.POLLING_FULL) {
                responseHeader.setPollingFull(true);
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        responseHeader.setHasMsg(hasMsg);
        return response;
    }

    private boolean hasMsgFromTopic(String topicName, int randomQ, NotificationRequestHeader requestHeader)
        throws RemotingCommandException {
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topicName);
        return hasMsgFromTopic(topicConfig, randomQ, requestHeader);
    }

    private boolean hasMsgFromTopic(TopicConfig topicConfig, int randomQ, NotificationRequestHeader requestHeader)
        throws RemotingCommandException {
        boolean hasMsg;
        if (topicConfig != null) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                int queueId = (randomQ + i) % topicConfig.getReadQueueNums();
                hasMsg = hasMsgFromQueue(topicConfig.getTopicName(), requestHeader, queueId);
                if (hasMsg) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasMsgFromQueue(String targetTopic, NotificationRequestHeader requestHeader, int queueId) throws RemotingCommandException {
        if (Boolean.TRUE.equals(requestHeader.getOrder())) {
            if (this.brokerController.getConsumerOrderInfoManager().checkBlock(requestHeader.getAttemptId(), requestHeader.getTopic(), requestHeader.getConsumerGroup(), queueId, 0)) {
                return false;
            }
        }
        long offset = getPopOffset(targetTopic, requestHeader.getConsumerGroup(), queueId);
        try {
            long restNum = this.brokerController.getMessageStore().getMaxOffsetInQueue(targetTopic, queueId) - offset;
            return restNum > 0;
        } catch (ConsumeQueueException e) {
            throw new RemotingCommandException("Failed tp get max offset in queue", e);
        }
    }

    private long getPopOffset(String topic, String cid, int queueId) {
        long offset = this.brokerController.getConsumerOffsetManager().queryOffset(cid, topic, queueId);
        if (offset < 0) {
            offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
        }

        long bufferOffset;
        if (brokerController.getBrokerConfig().isPopConsumerKVServiceEnable()) {
            bufferOffset = this.brokerController.getConsumerOffsetManager().queryPullOffset(cid, topic, queueId);
        } else {
            bufferOffset = this.brokerController.getPopMessageProcessor()
                .getPopBufferMergeService().getLatestOffset(topic, cid, queueId);
        }

        return bufferOffset < 0L ? offset : Math.max(bufferOffset, offset);
    }

    public PopLongPollingService getPopLongPollingService() {
        return popLongPollingService;
    }
}
