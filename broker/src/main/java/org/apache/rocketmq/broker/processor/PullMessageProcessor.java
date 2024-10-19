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
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filter.ExpressionForRetryMessageFilter;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.longpolling.PullRequest;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.plugin.PullMessageResultHandler;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.ForbiddenType;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.RequestSource;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpc.RpcClientUtils;
import org.apache.rocketmq.remoting.rpc.RpcRequest;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class PullMessageProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private List<ConsumeMessageHook> consumeMessageHookList;
    private PullMessageResultHandler pullMessageResultHandler;
    private final BrokerController brokerController;

    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.pullMessageResultHandler = new DefaultPullMessageResultHandler(brokerController);
    }

    private RemotingCommand rewriteRequestForStaticTopic(PullMessageRequestHeader requestHeader,
        TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            String topic = mappingContext.getTopic();
            Integer globalId = mappingContext.getGlobalId();
            // if the leader? consider the order consumer, which will lock the mq
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d cannot find mapping item in request process of current broker %s", topic, globalId, mappingDetail.getBname()));
            }

            Long globalOffset = requestHeader.getQueueOffset();
            LogicQueueMappingItem mappingItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), globalOffset, true);
            mappingContext.setCurrentItem(mappingItem);

            if (globalOffset < mappingItem.getLogicOffset()) {
                //handleOffsetMoved
                //If the physical queue is reused, we should handle the PULL_OFFSET_MOVED independently
                //Otherwise, we could just transfer it to the physical process
            }
            //below are physical info
            String bname = mappingItem.getBname();
            Integer phyQueueId = mappingItem.getQueueId();
            Long phyQueueOffset = mappingItem.computePhysicalQueueOffset(globalOffset);
            requestHeader.setQueueId(phyQueueId);
            requestHeader.setQueueOffset(phyQueueOffset);
            if (mappingItem.checkIfEndOffsetDecided()
                && requestHeader.getMaxMsgNums() != null) {
                requestHeader.setMaxMsgNums((int) Math.min(mappingItem.getEndOffset() - mappingItem.getStartOffset(), requestHeader.getMaxMsgNums()));
            }

            if (mappingDetail.getBname().equals(bname)) {
                //just let it go, do the local pull process
                return null;
            }

            int sysFlag = requestHeader.getSysFlag();
            requestHeader.setLo(false);
            requestHeader.setBrokerName(bname);
            sysFlag = PullSysFlag.clearSuspendFlag(sysFlag);
            sysFlag = PullSysFlag.clearCommitOffsetFlag(sysFlag);
            requestHeader.setSysFlag(sysFlag);
            RpcRequest rpcRequest = new RpcRequest(RequestCode.PULL_MESSAGE, requestHeader, null);
            RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
            if (rpcResponse.getException() != null) {
                throw rpcResponse.getException();
            }

            PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) rpcResponse.getHeader();
            {
                RemotingCommand rewriteResult = rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, rpcResponse.getCode());
                if (rewriteResult != null) {
                    return rewriteResult;
                }
            }
            return RpcClientUtils.createCommandForRpcResponse(rpcResponse);
        } catch (Throwable t) {
            LOGGER.warn("", t);
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.toString());
        }
    }

    protected RemotingCommand rewriteResponseForStaticTopic(PullMessageRequestHeader requestHeader,
        PullMessageResponseHeader responseHeader,
        TopicQueueMappingContext mappingContext, final int code) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            LogicQueueMappingItem leaderItem = mappingContext.getLeaderItem();

            LogicQueueMappingItem currentItem = mappingContext.getCurrentItem();

            LogicQueueMappingItem earlistItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), 0L, true);

            assert currentItem.getLogicOffset() >= 0;

            long requestOffset = requestHeader.getQueueOffset();
            long nextBeginOffset = responseHeader.getNextBeginOffset();
            long minOffset = responseHeader.getMinOffset();
            long maxOffset = responseHeader.getMaxOffset();
            int responseCode = code;

            //consider the following situations
            // 1. read from slave, currently not supported
            // 2. the middle queue is truncated because of deleting commitlog
            if (code != ResponseCode.SUCCESS) {
                //note the currentItem maybe both the leader and  the earliest
                boolean isRevised = false;
                if (leaderItem.getGen() == currentItem.getGen()) {
                    //read the leader
                    if (requestOffset > maxOffset) {
                        //actually, we need do nothing, but keep the code structure here
                        if (code == ResponseCode.PULL_OFFSET_MOVED) {
                            responseCode = ResponseCode.PULL_OFFSET_MOVED;
                            nextBeginOffset = maxOffset;
                        } else {
                            //maybe current broker is the slave
                            responseCode = code;
                        }
                    } else if (requestOffset < minOffset) {
                        nextBeginOffset = minOffset;
                        responseCode = ResponseCode.PULL_RETRY_IMMEDIATELY;
                    } else {
                        responseCode = code;
                    }
                }
                //note the currentItem maybe both the leader and  the earliest
                if (earlistItem.getGen() == currentItem.getGen()) {
                    //read the earliest one
                    if (requestOffset < minOffset) {
                        if (code == ResponseCode.PULL_OFFSET_MOVED) {
                            responseCode = ResponseCode.PULL_OFFSET_MOVED;
                            nextBeginOffset = minOffset;
                        } else {
                            //maybe read from slave, but we still set it to moved
                            responseCode = ResponseCode.PULL_OFFSET_MOVED;
                            nextBeginOffset = minOffset;
                        }
                    } else if (requestOffset >= maxOffset) {
                        //just move to another item
                        LogicQueueMappingItem nextItem = TopicQueueMappingUtils.findNext(mappingContext.getMappingItemList(), currentItem, true);
                        if (nextItem != null) {
                            isRevised = true;
                            currentItem = nextItem;
                            nextBeginOffset = currentItem.getStartOffset();
                            minOffset = currentItem.getStartOffset();
                            maxOffset = minOffset;
                            responseCode = ResponseCode.PULL_RETRY_IMMEDIATELY;
                        } else {
                            //maybe the next one's logic offset is -1
                            responseCode = ResponseCode.PULL_NOT_FOUND;
                        }
                    } else {
                        //let it go
                        responseCode = code;
                    }
                }

                //read from the middle item, ignore the PULL_OFFSET_MOVED
                if (!isRevised
                    && leaderItem.getGen() != currentItem.getGen()
                    && earlistItem.getGen() != currentItem.getGen()) {
                    if (requestOffset < minOffset) {
                        nextBeginOffset = minOffset;
                        responseCode = ResponseCode.PULL_RETRY_IMMEDIATELY;
                    } else if (requestOffset >= maxOffset) {
                        //just move to another item
                        LogicQueueMappingItem nextItem = TopicQueueMappingUtils.findNext(mappingContext.getMappingItemList(), currentItem, true);
                        if (nextItem != null) {
                            currentItem = nextItem;
                            nextBeginOffset = currentItem.getStartOffset();
                            minOffset = currentItem.getStartOffset();
                            maxOffset = minOffset;
                            responseCode = ResponseCode.PULL_RETRY_IMMEDIATELY;
                        } else {
                            //maybe the next one's logic offset is -1
                            responseCode = ResponseCode.PULL_NOT_FOUND;
                        }
                    } else {
                        responseCode = code;
                    }
                }
            }

            //handle nextBeginOffset
            //the next begin offset should no more than the end offset
            if (currentItem.checkIfEndOffsetDecided()
                && nextBeginOffset >= currentItem.getEndOffset()) {
                nextBeginOffset = currentItem.getEndOffset();
            }
            responseHeader.setNextBeginOffset(currentItem.computeStaticQueueOffsetStrictly(nextBeginOffset));
            //handle min offset
            responseHeader.setMinOffset(currentItem.computeStaticQueueOffsetStrictly(Math.max(currentItem.getStartOffset(), minOffset)));
            //handle max offset
            responseHeader.setMaxOffset(Math.max(currentItem.computeStaticQueueOffsetStrictly(maxOffset),
                TopicQueueMappingDetail.computeMaxOffsetFromMapping(mappingDetail, mappingContext.getGlobalId())));
            //set the offsetDelta
            responseHeader.setOffsetDelta(currentItem.computeOffsetDelta());

            if (code != ResponseCode.SUCCESS) {
                return RemotingCommand.createResponseCommandWithHeader(responseCode, responseHeader);
            } else {
                return null;
            }
        } catch (Throwable t) {
            LOGGER.warn("", t);
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.toString());
        }
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true, true);
    }

    @Override
    public boolean rejectRequest() {
        if (!this.brokerController.getBrokerConfig().isSlaveReadEnable()
            && this.brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            return true;
        }
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request, boolean brokerAllowSuspend, boolean brokerAllowFlowCtrSuspend)
        throws RemotingCommandException {
        final long beginTimeMills = this.brokerController.getMessageStore().now();
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        final PullMessageRequestHeader requestHeader =
            (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        response.setOpaque(request.getOpaque());

        LOGGER.debug("receive PullMessage request command, {}", request);

        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.BROKER_FORBIDDEN);
            response.setRemark(String.format("the broker[%s] pulling message is forbidden",
                this.brokerController.getBrokerConfig().getBrokerIP1()));
            return response;
        }

        if (request.getCode() == RequestCode.LITE_PULL_MESSAGE && !this.brokerController.getBrokerConfig().isLitePullMessageEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.BROKER_FORBIDDEN);
            response.setRemark(
                "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] for lite pull consumer is forbidden");
            return response;
        }

        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark(String.format("subscription group [%s] does not exist, %s", requestHeader.getConsumerGroup(), FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST)));
            return response;
        }

        if (!subscriptionGroupConfig.isConsumeEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.GROUP_FORBIDDEN);
            response.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return response;
        }

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            LOGGER.error("the topic {} not exist, consumer: {}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (!PermName.isReadable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            responseHeader.setForbiddenType(ForbiddenType.TOPIC_FORBIDDEN);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, false);

        {
            RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
            if (rewriteResult != null) {
                return rewriteResult;
            }
        }

        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        ConsumerManager consumerManager = brokerController.getConsumerManager();
        switch (RequestSource.parseInteger(requestHeader.getRequestSource())) {
            case PROXY_FOR_BROADCAST:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_PASSIVELY, MessageModel.BROADCASTING);
                break;
            case PROXY_FOR_STREAM:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING);
                break;
            default:
                consumerManager.compensateBasicConsumerInfo(requestHeader.getConsumerGroup(), ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING);
                break;
        }

        SubscriptionData subscriptionData = null;
        ConsumerFilterData consumerFilterData = null;
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());
        if (hasSubscriptionFlag) {
            try {
                subscriptionData = FilterAPI.build(
                    requestHeader.getTopic(), requestHeader.getSubscription(), requestHeader.getExpressionType()
                );
                consumerManager.compensateSubscribeData(requestHeader.getConsumerGroup(), requestHeader.getTopic(), subscriptionData);

                if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    consumerFilterData = ConsumerFilterManager.build(
                        requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getSubscription(),
                        requestHeader.getExpressionType(), requestHeader.getSubVersion()
                    );
                    assert consumerFilterData != null;
                }
            } catch (Exception e) {
                LOGGER.warn("Parse the consumer's subscription[{}] failed, group: {}", requestHeader.getSubscription(),
                    requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                LOGGER.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable()
                && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                response.setCode(ResponseCode.NO_PERMISSION);
                responseHeader.setForbiddenType(ForbiddenType.BROADCASTING_DISABLE_FORBIDDEN);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] can not consume by broadcast way");
                return response;
            }

            boolean readForbidden = this.brokerController.getSubscriptionGroupManager().getForbidden(//
                subscriptionGroupConfig.getGroupName(), requestHeader.getTopic(), PermName.INDEX_PERM_READ);
            if (readForbidden) {
                response.setCode(ResponseCode.NO_PERMISSION);
                responseHeader.setForbiddenType(ForbiddenType.SUBSCRIPTION_FORBIDDEN);
                response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] is forbidden for topic[" + requestHeader.getTopic() + "]");
                return response;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                LOGGER.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getConsumerGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist" + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return response;
            }

            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                LOGGER.warn("The broker's subscription is not latest, group: {} {}", requestHeader.getConsumerGroup(),
                    subscriptionData.getSubString());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                response.setRemark("the consumer's subscription not latest");
                return response;
            }
            if (!ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                consumerFilterData = this.brokerController.getConsumerFilterManager().get(requestHeader.getTopic(),
                    requestHeader.getConsumerGroup());
                if (consumerFilterData == null) {
                    response.setCode(ResponseCode.FILTER_DATA_NOT_EXIST);
                    response.setRemark("The broker's consumer filter data is not exist!Your expression may be wrong!");
                    return response;
                }
                if (consumerFilterData.getClientVersion() < requestHeader.getSubVersion()) {
                    LOGGER.warn("The broker's consumer filter data is not latest, group: {}, topic: {}, serverV: {}, clientV: {}",
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), consumerFilterData.getClientVersion(), requestHeader.getSubVersion());
                    response.setCode(ResponseCode.FILTER_DATA_NOT_LATEST);
                    response.setRemark("the consumer's consumer filter data not latest");
                    return response;
                }
            }
        }

        if (!ExpressionType.isTagType(subscriptionData.getExpressionType())
            && !this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
            return response;
        }

        MessageFilter messageFilter;
        if (this.brokerController.getBrokerConfig().isFilterSupportRetry()) {
            messageFilter = new ExpressionForRetryMessageFilter(subscriptionData, consumerFilterData,
                this.brokerController.getConsumerFilterManager());
        } else {
            messageFilter = new ExpressionMessageFilter(subscriptionData, consumerFilterData,
                this.brokerController.getConsumerFilterManager());
        }

        final MessageStore messageStore = brokerController.getMessageStore();
        if (this.brokerController.getMessageStore() instanceof DefaultMessageStore) {
            DefaultMessageStore defaultMessageStore = (DefaultMessageStore)this.brokerController.getMessageStore();
            boolean cgNeedColdDataFlowCtr = brokerController.getColdDataCgCtrService().isCgNeedColdDataFlowCtr(requestHeader.getConsumerGroup());
            if (cgNeedColdDataFlowCtr) {
                boolean isMsgLogicCold = defaultMessageStore.getCommitLog()
                    .getColdDataCheckService().isMsgInColdArea(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset());
                if (isMsgLogicCold) {
                    ConsumeType consumeType = this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup()).getConsumeType();
                    if (consumeType == ConsumeType.CONSUME_PASSIVELY) {
                        response.setCode(ResponseCode.SYSTEM_BUSY);
                        response.setRemark("This consumer group is reading cold data. It has been flow control");
                        return response;
                    } else if (consumeType == ConsumeType.CONSUME_ACTIVELY) {
                        if (brokerAllowFlowCtrSuspend) {  // second arrived, which will not be held
                            PullRequest pullRequest = new PullRequest(request, channel, 1000,
                                this.brokerController.getMessageStore().now(), requestHeader.getQueueOffset(), subscriptionData, messageFilter);
                            this.brokerController.getColdDataPullRequestHoldService().suspendColdDataReadRequest(pullRequest);
                            return null;
                        }
                        requestHeader.setMaxMsgNums(1);
                    }
                }
            }
        }

        final boolean useResetOffsetFeature = brokerController.getBrokerConfig().isUseServerSideResetOffset();
        String topic = requestHeader.getTopic();
        String group = requestHeader.getConsumerGroup();
        int queueId = requestHeader.getQueueId();
        Long resetOffset = brokerController.getConsumerOffsetManager().queryThenEraseResetOffset(topic, group, queueId);

        GetMessageResult getMessageResult = null;
        if (useResetOffsetFeature && null != resetOffset) {
            getMessageResult = new GetMessageResult();
            getMessageResult.setStatus(GetMessageStatus.OFFSET_RESET);
            getMessageResult.setNextBeginOffset(resetOffset);
            getMessageResult.setMinOffset(messageStore.getMinOffsetInQueue(topic, queueId));
            getMessageResult.setMaxOffset(messageStore.getMaxOffsetInQueue(topic, queueId));
            getMessageResult.setSuggestPullingFromSlave(false);
        } else {
            long broadcastInitOffset = queryBroadcastPullInitOffset(topic, group, queueId, requestHeader, channel);
            if (broadcastInitOffset >= 0) {
                getMessageResult = new GetMessageResult();
                getMessageResult.setStatus(GetMessageStatus.OFFSET_RESET);
                getMessageResult.setNextBeginOffset(broadcastInitOffset);
            } else {
                SubscriptionData finalSubscriptionData = subscriptionData;
                RemotingCommand finalResponse = response;
                messageStore.getMessageAsync(group, topic, queueId, requestHeader.getQueueOffset(),
                        requestHeader.getMaxMsgNums(), messageFilter)
                    .thenApply(result -> {
                        if (null == result) {
                            finalResponse.setCode(ResponseCode.SYSTEM_ERROR);
                            finalResponse.setRemark("store getMessage return null");
                            return finalResponse;
                        }
                        brokerController.getColdDataCgCtrService().coldAcc(requestHeader.getConsumerGroup(), result.getColdDataSum());
                        return pullMessageResultHandler.handle(
                            result,
                            request,
                            requestHeader,
                            channel,
                            finalSubscriptionData,
                            subscriptionGroupConfig,
                            brokerAllowSuspend,
                            messageFilter,
                            finalResponse,
                            mappingContext,
                            beginTimeMills
                        );
                    })
                    .thenAccept(result -> NettyRemotingAbstract.writeResponse(channel, request, result));
            }
        }

        if (getMessageResult != null) {

            return this.pullMessageResultHandler.handle(
                getMessageResult,
                request,
                requestHeader,
                channel,
                subscriptionData,
                subscriptionGroupConfig,
                brokerAllowSuspend,
                messageFilter,
                response,
                mappingContext,
                beginTimeMills
            );
        }
        return null;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    /**
     * Composes the header of the response message to be sent back to the client
     * @param requestHeader - the header of the request message
     * @param getMessageResult - the result of the GetMessage request
     * @param topicSysFlag - the system flag of the topic
     * @param subscriptionGroupConfig - configuration of the subscription group
     * @param response - the response message to be sent back to the client
     * @param clientAddress - the address of the client
     */
    protected void composeResponseHeader(PullMessageRequestHeader requestHeader, GetMessageResult getMessageResult,
        int topicSysFlag, SubscriptionGroupConfig subscriptionGroupConfig, RemotingCommand response,
        String clientAddress) {
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
        response.setRemark(getMessageResult.getStatus().name());
        responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
        responseHeader.setMinOffset(getMessageResult.getMinOffset());
        // this does not need to be modified since it's not an accurate value under logical queue.
        responseHeader.setMaxOffset(getMessageResult.getMaxOffset());
        responseHeader.setTopicSysFlag(topicSysFlag);
        responseHeader.setGroupSysFlag(subscriptionGroupConfig.getGroupSysFlag());

        switch (getMessageResult.getStatus()) {
            case FOUND:
                response.setCode(ResponseCode.SUCCESS);
                break;
            case MESSAGE_WAS_REMOVING:
            case NO_MATCHED_MESSAGE:
                response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                break;
            case NO_MATCHED_LOGIC_QUEUE:
            case NO_MESSAGE_IN_QUEUE:
                if (0 != requestHeader.getQueueOffset()) {
                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    LOGGER.info("the broker stores no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
                        requestHeader.getQueueOffset(),
                        getMessageResult.getNextBeginOffset(),
                        requestHeader.getTopic(),
                        requestHeader.getQueueId(),
                        requestHeader.getConsumerGroup()
                    );
                } else {
                    response.setCode(ResponseCode.PULL_NOT_FOUND);
                }
                break;
            case OFFSET_FOUND_NULL:
            case OFFSET_OVERFLOW_ONE:
                response.setCode(ResponseCode.PULL_NOT_FOUND);
                break;
            case OFFSET_OVERFLOW_BADLY:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                // XXX: warn and notify me
                LOGGER.info("the request offset: {} over flow badly, fix to {}, broker max offset: {}, consumer: {}",
                    requestHeader.getQueueOffset(), getMessageResult.getNextBeginOffset(), getMessageResult.getMaxOffset(), clientAddress);
                break;
            case OFFSET_RESET:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                LOGGER.info("The queue under pulling was previously reset to start from {}",
                    getMessageResult.getNextBeginOffset());
                break;
            case OFFSET_TOO_SMALL:
                response.setCode(ResponseCode.PULL_OFFSET_MOVED);
                LOGGER.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueOffset(),
                    getMessageResult.getMinOffset(), clientAddress);
                break;
            default:
                assert false;
                break;
        }

        if (this.brokerController.getBrokerConfig().isSlaveReadEnable() && !this.brokerController.getBrokerConfig().isInBrokerContainer()) {
            // consume too slow ,redirect to another machine
            if (getMessageResult.isSuggestPullingFromSlave()) {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getWhichBrokerWhenConsumeSlowly());
            }
            // consume ok
            else {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
            }
        } else {
            responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
        }

        if (this.brokerController.getBrokerConfig().getBrokerId() != MixAll.MASTER_ID && !getMessageResult.isSuggestPullingFromSlave()) {
            if (this.brokerController.getMinBrokerIdInGroup() == MixAll.MASTER_ID) {
                LOGGER.debug("slave redirect pullRequest to master, topic: {}, queueId: {}, consumer group: {}, next: {}, min: {}, max: {}",
                    requestHeader.getTopic(),
                    requestHeader.getQueueId(),
                    requestHeader.getConsumerGroup(),
                    responseHeader.getNextBeginOffset(),
                    responseHeader.getMinOffset(),
                    responseHeader.getMaxOffset()
                );
                responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                if (!getMessageResult.getStatus().equals(GetMessageStatus.FOUND)) {
                    response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                }
            }
        }

    }

    protected void executeConsumeMessageHookBefore(RemotingCommand request, PullMessageRequestHeader requestHeader,
        GetMessageResult getMessageResult, boolean brokerAllowSuspend, int responseCode) {
        if (this.hasConsumeMessageHook()) {
            String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
            String authType = request.getExtFields().get(BrokerStatsManager.ACCOUNT_AUTH_TYPE);
            String ownerParent = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_PARENT);
            String ownerSelf = request.getExtFields().get(BrokerStatsManager.ACCOUNT_OWNER_SELF);

            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getConsumerGroup());
            context.setTopic(requestHeader.getTopic());
            context.setQueueId(requestHeader.getQueueId());
            context.setAccountAuthType(authType);
            context.setAccountOwnerParent(ownerParent);
            context.setAccountOwnerSelf(ownerSelf);
            context.setNamespace(NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic()));

            switch (responseCode) {
                case ResponseCode.SUCCESS:
                    int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                    int incValue = getMessageResult.getMsgCount4Commercial() * commercialBaseCount;

                    context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_SUCCESS);
                    context.setCommercialRcvTimes(incValue);
                    context.setCommercialRcvSize(getMessageResult.getBufferTotalSize());
                    context.setCommercialOwner(owner);

                    context.setRcvStat(BrokerStatsManager.StatsType.RCV_SUCCESS);
                    context.setRcvMsgNum(getMessageResult.getMessageCount());
                    context.setRcvMsgSize(getMessageResult.getBufferTotalSize());
                    context.setCommercialRcvMsgNum(getMessageResult.getMsgCount4Commercial());

                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    if (!brokerAllowSuspend) {

                        context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setCommercialRcvTimes(1);
                        context.setCommercialOwner(owner);

                        context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
                        context.setRcvMsgNum(0);
                        context.setRcvMsgSize(0);
                        context.setCommercialRcvMsgNum(0);
                    }
                    break;
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                case ResponseCode.PULL_OFFSET_MOVED:
                    context.setCommercialRcvStats(BrokerStatsManager.StatsType.RCV_EPOLLS);
                    context.setCommercialRcvTimes(1);
                    context.setCommercialOwner(owner);

                    context.setRcvStat(BrokerStatsManager.StatsType.RCV_EPOLLS);
                    context.setRcvMsgNum(0);
                    context.setRcvMsgSize(0);
                    context.setCommercialRcvMsgNum(0);
                    break;
                default:
                    assert false;
                    break;
            }

            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable ignored) {
                }
            }
        }
    }

    protected void tryCommitOffset(boolean brokerAllowSuspend, PullMessageRequestHeader requestHeader,
        long nextOffset, String clientAddress) {
        this.brokerController.getConsumerOffsetManager().commitPullOffset(clientAddress,
            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), nextOffset);

        boolean storeOffsetEnable = brokerAllowSuspend;
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(clientAddress, requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }
    }

    public void executeRequestWhenWakeup(final Channel channel, final RemotingCommand request) {
        Runnable run = () -> {
            try {
                boolean brokerAllowFlowCtrSuspend = !(request.getExtFields() != null && request.getExtFields().containsKey(ColdDataPullRequestHoldService.NO_SUSPEND_KEY));
                final RemotingCommand response = PullMessageProcessor.this.processRequest(channel, request, false, brokerAllowFlowCtrSuspend);

                if (response != null) {
                    response.setOpaque(request.getOpaque());
                    response.markResponseType();
                    try {
                        NettyRemotingAbstract.writeResponse(channel, request, response, future -> {
                            if (!future.isSuccess()) {
                                LOGGER.error("processRequestWrapper response to {} failed", channel.remoteAddress(), future.cause());
                                LOGGER.error(request.toString());
                                LOGGER.error(response.toString());
                            }
                        });
                    } catch (Throwable e) {
                        LOGGER.error("processRequestWrapper process request over, but response failed", e);
                        LOGGER.error(request.toString());
                        LOGGER.error(response.toString());
                    }
                }
            } catch (RemotingCommandException e1) {
                LOGGER.error("executeRequestWhenWakeup run", e1);
            }
        };
        this.brokerController.getPullMessageExecutor().submit(new RequestTask(run, channel, request));
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    public void setPullMessageResultHandler(PullMessageResultHandler pullMessageResultHandler) {
        this.pullMessageResultHandler = pullMessageResultHandler;
    }

    private boolean isBroadcast(boolean proxyPullBroadcast, ConsumerGroupInfo consumerGroupInfo) {
        return proxyPullBroadcast ||
            consumerGroupInfo != null
                && MessageModel.BROADCASTING.equals(consumerGroupInfo.getMessageModel())
                && ConsumeType.CONSUME_PASSIVELY.equals(consumerGroupInfo.getConsumeType());
    }

    protected void updateBroadcastPulledOffset(String topic, String group, int queueId,
        PullMessageRequestHeader requestHeader, Channel channel, RemotingCommand response, long nextBeginOffset) {

        if (response == null || !this.brokerController.getBrokerConfig().isEnableBroadcastOffsetStore()) {
            return;
        }

        boolean proxyPullBroadcast = Objects.equals(
            RequestSource.PROXY_FOR_BROADCAST.getValue(), requestHeader.getRequestSource());
        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        if (isBroadcast(proxyPullBroadcast, consumerGroupInfo)) {
            long offset = requestHeader.getQueueOffset();
            if (ResponseCode.PULL_OFFSET_MOVED == response.getCode()) {
                offset = nextBeginOffset;
            }
            String clientId;
            if (proxyPullBroadcast) {
                clientId = requestHeader.getProxyFrowardClientId();
            } else {
                ClientChannelInfo clientChannelInfo = consumerGroupInfo.findChannel(channel);
                if (clientChannelInfo == null) {
                    return;
                }
                clientId = clientChannelInfo.getClientId();
            }
            this.brokerController.getBroadcastOffsetManager()
                .updateOffset(topic, group, queueId, offset, clientId, proxyPullBroadcast);
        }
    }

    /**
     * When pull request is not broadcast or not return -1
     */
    protected long queryBroadcastPullInitOffset(String topic, String group, int queueId,
        PullMessageRequestHeader requestHeader, Channel channel) {

        if (!this.brokerController.getBrokerConfig().isEnableBroadcastOffsetStore()) {
            return -1L;
        }

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(group);
        boolean proxyPullBroadcast = Objects.equals(
            RequestSource.PROXY_FOR_BROADCAST.getValue(), requestHeader.getRequestSource());

        if (isBroadcast(proxyPullBroadcast, consumerGroupInfo)) {
            String clientId;
            if (proxyPullBroadcast) {
                clientId = requestHeader.getProxyFrowardClientId();
            } else {
                ClientChannelInfo clientChannelInfo = consumerGroupInfo.findChannel(channel);
                if (clientChannelInfo == null) {
                    return -1;
                }
                clientId = clientChannelInfo.getClientId();
            }

            return this.brokerController.getBroadcastOffsetManager()
                .queryInitOffset(topic, group, queueId, clientId, requestHeader.getQueueOffset(), proxyPullBroadcast);
        }
        return -1L;
    }
}
