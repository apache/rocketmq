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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private final ConcurrentMap<String /* ConsumerGroup */, Integer /* HeartbeatFingerprint */> consumerGroupHeartbeatTable = new ConcurrentHashMap<>();

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            heartbeatData.getClientID(),
            request.getLanguage(),
            request.getVersion()
        );
        int heartbeatFingerprint = heartbeatData.getHeartbeatFingerprint();
        if (heartbeatFingerprint != 0) {
            return heartBeatV2(ctx, heartbeatData, clientChannelInfo, response);
        }
        for (ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
            //Reject the PullConsumer
            if (brokerController.getBrokerConfig().isRejectPullConsumerEnable()) {
                if (ConsumeType.CONSUME_ACTIVELY == consumerData.getConsumeType()) {
                    continue;
                }
            }
            consumerGroupHeartbeatTable.put(consumerData.getGroupName(), heartbeatFingerprint);
            boolean hasOrderTopicSub = false;

            for (final SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                if (this.brokerController.getTopicConfigManager().isOrderTopic(subscriptionData.getTopic())) {
                    hasOrderTopicSub = true;
                    break;
                }
            }

            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager()
                .findSubscriptionGroupConfig(consumerData.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;

            if (null == subscriptionGroupConfig) {
                continue;
            }

            isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
            int topicSysFlag = 0;
            if (consumerData.isUnitMode()) {
                topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
            }
            String newTopic = MixAll.getRetryTopic(consumerData.getGroupName());
            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(),
                PermName.PERM_WRITE | PermName.PERM_READ, hasOrderTopicSub, topicSysFlag);

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                consumerData.getGroupName(),
                clientChannelInfo,
                consumerData.getConsumeType(),
                consumerData.getMessageModel(),
                consumerData.getConsumeFromWhere(),
                consumerData.getSubscriptionDataSet(),
                isNotifyConsumerIdsChangedEnable
            );
            if (changed) {
                LOGGER.info("ClientManageProcessor: registerConsumer info changed, SDK address={}, consumerData={}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), consumerData.toString());
            }

        }

        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.addExtField(MixAll.IS_SUPPORT_HEART_BEAT_V2, Boolean.TRUE.toString());
        response.addExtField(MixAll.IS_SUB_CHANGE, Boolean.TRUE.toString());
        return response;
    }

    private RemotingCommand heartBeatV2(ChannelHandlerContext ctx, HeartbeatData heartbeatData, ClientChannelInfo clientChannelInfo, RemotingCommand response) {
        boolean isSubChange = false;
        for (ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
            //Reject the PullConsumer
            if (brokerController.getBrokerConfig().isRejectPullConsumerEnable()) {
                if (ConsumeType.CONSUME_ACTIVELY == consumerData.getConsumeType()) {
                    continue;
                }
            }
            if (null != consumerGroupHeartbeatTable.get(consumerData.getGroupName()) && consumerGroupHeartbeatTable.get(consumerData.getGroupName()) != heartbeatData.getHeartbeatFingerprint()) {
                isSubChange = true;
            }
            consumerGroupHeartbeatTable.put(consumerData.getGroupName(), heartbeatData.getHeartbeatFingerprint());
            boolean hasOrderTopicSub = false;

            for (final SubscriptionData subscriptionData : consumerData.getSubscriptionDataSet()) {
                if (this.brokerController.getTopicConfigManager().isOrderTopic(subscriptionData.getTopic())) {
                    hasOrderTopicSub = true;
                    break;
                }
            }
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(consumerData.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null == subscriptionGroupConfig) {
                continue;
            }
            isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
            int topicSysFlag = 0;
            if (consumerData.isUnitMode()) {
                topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
            }
            String newTopic = MixAll.getRetryTopic(consumerData.getGroupName());
            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, subscriptionGroupConfig.getRetryQueueNums(), PermName.PERM_WRITE | PermName.PERM_READ, hasOrderTopicSub, topicSysFlag);
            boolean changed = false;
            if (heartbeatData.isWithoutSub()) {
                changed = this.brokerController.getConsumerManager().registerConsumerWithoutSub(consumerData.getGroupName(), clientChannelInfo, consumerData.getConsumeType(), consumerData.getMessageModel(), consumerData.getConsumeFromWhere(), isNotifyConsumerIdsChangedEnable);
            } else {
                changed = this.brokerController.getConsumerManager().registerConsumer(consumerData.getGroupName(), clientChannelInfo, consumerData.getConsumeType(), consumerData.getMessageModel(), consumerData.getConsumeFromWhere(), consumerData.getSubscriptionDataSet(), isNotifyConsumerIdsChangedEnable);
            }
            if (changed) {
                LOGGER.info("heartBeatV2 ClientManageProcessor: registerConsumer info changed, SDK address={}, consumerData={}",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()), consumerData.toString());
            }

        }
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        response.addExtField(MixAll.IS_SUPPORT_HEART_BEAT_V2, Boolean.TRUE.toString());
        response.addExtField(MixAll.IS_SUB_CHANGE, Boolean.valueOf(isSubChange).toString());
        return response;
    }

    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request
                .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            requestHeader.getClientID(),
            request.getLanguage(),
            request.getVersion());
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(),
            CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                LOGGER.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                    requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
