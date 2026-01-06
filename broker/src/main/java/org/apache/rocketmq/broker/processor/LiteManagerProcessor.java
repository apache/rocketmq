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
import io.netty.channel.ChannelHandlerContext;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.lite.LiteMetadataUtil;
import org.apache.rocketmq.broker.lite.LiteSharding;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.body.GetBrokerLiteInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteClientInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteGroupInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetLiteTopicInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.body.GetParentTopicInfoResponseBody;
import org.apache.rocketmq.remoting.protocol.header.GetLiteClientInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteGroupInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetLiteTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetParentTopicInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.TriggerLiteDispatchRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LiteManagerProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LITE_LOGGER_NAME);

    private static final int MAX_RETURN_COUNT = 10000;
    private final BrokerController brokerController;
    private final AbstractLiteLifecycleManager liteLifecycleManager;
    private final LiteSharding liteSharding;

    public LiteManagerProcessor(BrokerController brokerController,
        AbstractLiteLifecycleManager liteLifecycleManager, LiteSharding liteSharding) {
        this.brokerController = brokerController;
        this.liteLifecycleManager = liteLifecycleManager;
        this.liteSharding = liteSharding;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.GET_BROKER_LITE_INFO:
                return this.getBrokerLiteInfo(ctx, request);
            case RequestCode.GET_PARENT_TOPIC_INFO:
                return this.getParentTopicInfo(ctx, request);
            case RequestCode.GET_LITE_TOPIC_INFO:
                return this.getLiteTopicInfo(ctx, request);
            case RequestCode.GET_LITE_CLIENT_INFO:
                return this.getLiteClientInfo(ctx, request);
            case RequestCode.GET_LITE_GROUP_INFO:
                return this.getLiteGroupInfo(ctx, request);
            case RequestCode.TRIGGER_LITE_DISPATCH:
                return this.triggerLiteDispatch(ctx, request);
            default:
                break;
        }
        return null;
    }

    @VisibleForTesting
    protected RemotingCommand getBrokerLiteInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        GetBrokerLiteInfoResponseBody body = new GetBrokerLiteInfoResponseBody();
        body.setStoreType(brokerController.getMessageStoreConfig().getStoreType());
        body.setMaxLmqNum(brokerController.getMessageStoreConfig().getMaxLmqConsumeQueueNum());
        body.setCurrentLmqNum(brokerController.getMessageStore().getQueueStore().getLmqNum());
        body.setLiteSubscriptionCount(brokerController.getLiteSubscriptionRegistry().getActiveSubscriptionNum());
        body.setOrderInfoCount(brokerController.getPopLiteMessageProcessor().getConsumerOrderInfoManager().getOrderInfoCount());
        body.setCqTableSize(brokerController.getMessageStore().getQueueStore().getConsumeQueueTable().size());
        body.setOffsetTableSize(brokerController.getConsumerOffsetManager().getOffsetTable().size());
        body.setEventMapSize(brokerController.getLiteEventDispatcher().getEventMapSize());
        body.setTopicMeta(LiteMetadataUtil.getTopicTtlMap(brokerController));
        body.setGroupMeta(LiteMetadataUtil.getSubscriberGroupMap(brokerController));

        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    @VisibleForTesting
    protected RemotingCommand getParentTopicInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetParentTopicInfoRequestHeader requestHeader =
            request.decodeCommandCustomHeader(GetParentTopicInfoRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String topic = requestHeader.getTopic();
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("Topic [%s] not exist.", topic));
            return response;
        }
        if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("Topic [%s] type not match.", topic));
            return response;
        }

        Map<String, Set<String>> subscriberGroupMap = LiteMetadataUtil.getSubscriberGroupMap(brokerController);

        GetParentTopicInfoResponseBody body = new GetParentTopicInfoResponseBody();
        body.setTopic(topic);
        body.setTtl(topicConfig.getLiteTopicExpiration());
        body.setLmqNum(brokerController.getMessageStore().getQueueStore().getLmqNum());
        body.setLiteTopicCount(liteLifecycleManager.getLiteTopicCount(topic));
        body.setGroups(subscriberGroupMap != null ? subscriberGroupMap.get(topic) : null);

        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    @VisibleForTesting
    protected RemotingCommand getLiteTopicInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetLiteTopicInfoRequestHeader requestHeader =
            request.decodeCommandCustomHeader(GetLiteTopicInfoRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String parentTopic = requestHeader.getParentTopic();
        String liteTopic = requestHeader.getLiteTopic();
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(parentTopic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("Topic [%s] not exist.", parentTopic));
            return response;
        }
        if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("Topic [%s] type not match.", parentTopic));
            return response;
        }

        String lmqName = LiteUtil.toLmqName(parentTopic, liteTopic);
        TopicOffset topicOffset = new TopicOffset();
        long minOffset = 0;
        long lastUpdateTimestamp = 0;
        long maxOffset = liteLifecycleManager.getMaxOffsetInQueue(lmqName);
        if (maxOffset > 0) {
            minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(lmqName, 0);
            lastUpdateTimestamp = brokerController.getMessageStore().getMessageStoreTimeStamp(lmqName, 0, maxOffset - 1);
        }
        topicOffset.setMinOffset(minOffset < 0 ? 0 : minOffset);
        topicOffset.setMaxOffset(maxOffset < 0 ? 0 : maxOffset);
        topicOffset.setLastUpdateTimestamp(lastUpdateTimestamp);

        GetLiteTopicInfoResponseBody body = new GetLiteTopicInfoResponseBody();
        body.setParentTopic(parentTopic);
        body.setLiteTopic(liteTopic);
        body.setSubscriber(brokerController.getLiteSubscriptionRegistry().getSubscriber(lmqName));
        body.setTopicOffset(topicOffset);
        body.setShardingToBroker(brokerController.getBrokerConfig().getBrokerName().equals(
            liteSharding.shardingByLmqName(parentTopic, lmqName)));

        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    @VisibleForTesting
    protected RemotingCommand getLiteClientInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetLiteClientInfoRequestHeader requestHeader =
            request.decodeCommandCustomHeader(GetLiteClientInfoRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String parentTopic = requestHeader.getParentTopic();
        String group = requestHeader.getGroup();
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(parentTopic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("Topic [%s] not exist.", parentTopic));
            return response;
        }
        if (!TopicMessageType.LITE.equals(topicConfig.getTopicMessageType())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("Topic [%s] type not match.", parentTopic));
            return response;
        }
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        if (null == groupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("Group [%s] not exist.", group));
            return response;
        }
        if (!parentTopic.equals(groupConfig.getLiteBindTopic())) {
            response.setCode(ResponseCode.INVALID_PARAMETER);
            response.setRemark(String.format("Subscription [%s]-[%s] not match.", group, parentTopic));
            return response;
        }

        String clientId = requestHeader.getClientId();
        int maxCount = Math.min(requestHeader.getMaxCount(), MAX_RETURN_COUNT);
        Set<String> returnSet = null;
        int liteTopicCount = 0;
        LiteSubscription liteSubscription = brokerController.getLiteSubscriptionRegistry().getLiteSubscription(clientId);
        if (liteSubscription != null && liteSubscription.getLiteTopicSet() != null) {
            Set<String> liteTopicSet = liteSubscription.getLiteTopicSet();
            liteTopicCount = liteTopicSet.size();
            if (maxCount >= liteTopicCount) {
                returnSet = liteTopicSet;
            } else {
                returnSet = new HashSet<>(maxCount);
                int count = 0;
                for (String topic : liteTopicSet) {
                    if (count >= maxCount) {
                        break;
                    }
                    returnSet.add(topic);
                    count++;
                }
            }
        } else {
            liteTopicCount = -1;
        }

        GetLiteClientInfoResponseBody body = new GetLiteClientInfoResponseBody();
        body.setParentTopic(parentTopic);
        body.setGroup(group);
        body.setClientId(clientId);
        body.setLiteTopicCount(liteTopicCount);
        body.setLiteTopicSet(returnSet);
        body.setLastAccessTime(brokerController.getLiteEventDispatcher().getClientLastAccessTime(clientId));

        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    @VisibleForTesting
    protected RemotingCommand getLiteGroupInfo(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final GetLiteGroupInfoRequestHeader requestHeader =
            request.decodeCommandCustomHeader(GetLiteGroupInfoRequestHeader.class);
        final String group = requestHeader.getGroup();
        final String liteTopic = requestHeader.getLiteTopic();
        final int topK = requestHeader.getTopK();
        LOGGER.info("Broker receive request to getLiteGroupInfo, group:{}, liteTopic:{}, caller:{}",
            group, liteTopic, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        if (null == groupConfig) {
            return RemotingCommand.createResponseCommand(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST,
                String.format("Group [%s] not exist.", group));
        }
        if (StringUtils.isEmpty(groupConfig.getLiteBindTopic())) {
            return RemotingCommand.createResponseCommand(ResponseCode.INVALID_PARAMETER,
                String.format("Group [%s] is not a LITE group.", group));
        }
        String bindTopic = groupConfig.getLiteBindTopic();
        GetLiteGroupInfoResponseBody body = new GetLiteGroupInfoResponseBody();
        body.setGroup(group);
        body.setParentTopic(bindTopic);
        body.setLiteTopic(liteTopic);

        if (StringUtils.isEmpty(liteTopic)) {
            Pair<List<LiteLagInfo>, Long> lagCountPair = brokerController.getBrokerMetricsManager()
                .getLiteConsumerLagCalculator()
                .getLagCountTopK(group, topK);

            Pair<List<LiteLagInfo>, Long> lagTimePair = brokerController.getBrokerMetricsManager()
                .getLiteConsumerLagCalculator()
                .getLagTimestampTopK(group, bindTopic, topK);

            body.setLagCountTopK(lagCountPair.getObject1());
            body.setTotalLagCount(lagCountPair.getObject2());
            body.setLagTimestampTopK(lagTimePair.getObject1());
            body.setEarliestUnconsumedTimestamp(lagTimePair.getObject2());
        } else {
            String lmqName = LiteUtil.toLmqName(bindTopic, liteTopic);
            long maxOffset = liteLifecycleManager.getMaxOffsetInQueue(lmqName);
            if (maxOffset > 0) {
                long commitOffset = brokerController.getConsumerOffsetManager().queryOffset(group, lmqName, 0);
                if (commitOffset >= 0) {
                    // lag count and unconsumedTimestamp, reuse total field
                    body.setTotalLagCount(maxOffset - commitOffset);
                    body.setEarliestUnconsumedTimestamp(brokerController.getMessageStore().getMessageStoreTimeStamp(
                        lmqName, 0, commitOffset));

                    OffsetWrapper offsetWrapper = new OffsetWrapper();
                    offsetWrapper.setBrokerOffset(maxOffset);
                    offsetWrapper.setConsumerOffset(commitOffset);
                    if (commitOffset - 1 >= 0) {
                        offsetWrapper.setLastTimestamp(
                            brokerController.getMessageStore().getMessageStoreTimeStamp(lmqName, 0, commitOffset - 1));
                    }
                    body.setLiteTopicOffsetWrapper(offsetWrapper);
                }
            } else {
                body.setTotalLagCount(-1);
                body.setEarliestUnconsumedTimestamp(-1);
            }
        }

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    @VisibleForTesting
    protected RemotingCommand triggerLiteDispatch(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final TriggerLiteDispatchRequestHeader requestHeader =
            request.decodeCommandCustomHeader(TriggerLiteDispatchRequestHeader.class);
        final String group = requestHeader.getGroup();
        final String clientId = requestHeader.getClientId();
        LOGGER.info("Broker receive request to triggerLiteDispatch, group:{}, clientId:{}, caller:{}",
            group, clientId, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        SubscriptionGroupConfig groupConfig =
            brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
        if (null == groupConfig) {
            return RemotingCommand.createResponseCommand(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST,
                String.format("Group [%s] not exist.", group));
        }
        if (StringUtils.isEmpty(groupConfig.getLiteBindTopic())) {
            return RemotingCommand.createResponseCommand(ResponseCode.INVALID_PARAMETER,
                String.format("Group [%s] is not a LITE group.", group));
        }

        if (StringUtils.isNotEmpty(clientId)) {
            brokerController.getLiteEventDispatcher().doFullDispatch(clientId, group);
        } else {
            brokerController.getLiteEventDispatcher().doFullDispatchByGroup(group);
        }

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
