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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.auth.converter.AclConverter;
import org.apache.rocketmq.broker.auth.converter.UserConverter;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.InvocationStatus;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageUtil;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.LockCallback;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UnlockCallback;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.constant.FIleReadaheadMode;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ExceptionUtils;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsItem;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.CreateTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupList;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.CheckRocksdbCqWriteProgressRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExchangeHAInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExchangeHAInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllProducerInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerAclConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyBrokerRoleChangedRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGlobalWhiteAddrsConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpc.RpcClientUtils;
import org.apache.rocketmq.remoting.rpc.RpcException;
import org.apache.rocketmq.remoting.rpc.RpcRequest;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.plugin.AbstractPluginMessageStore;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.util.LibC;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_INVOCATION_STATUS;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    protected final BrokerController brokerController;
    protected Set<String> configBlackList = new HashSet<>();

    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        initConfigBlackList();
    }

    private void initConfigBlackList() {
        configBlackList.add("brokerConfigPath");
        configBlackList.add("rocketmqHome");
        configBlackList.add("configBlackList");
        String[] configArray = brokerController.getBrokerConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_TOPIC_LIST:
                return this.updateAndCreateTopicList(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return this.deleteTopic(ctx, request);
            case RequestCode.GET_ALL_TOPIC_CONFIG:
                return this.getAllTopicConfig(ctx, request);
            case RequestCode.GET_TIMER_CHECK_POINT:
                return this.getTimerCheckPoint(ctx, request);
            case RequestCode.GET_TIMER_METRICS:
                return this.getTimerMetrics(ctx, request);
            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(ctx, request);
            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(ctx, request);
            case RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG:
                return this.updateColdDataFlowCtrGroupConfig(ctx, request);
            case RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG:
                return this.removeColdDataFlowCtrGroupConfig(ctx, request);
            case RequestCode.GET_COLD_DATA_FLOW_CTR_INFO:
                return this.getColdDataFlowCtrInfo(ctx);
            case RequestCode.SET_COMMITLOG_READ_MODE:
                return this.setCommitLogReadaheadMode(ctx, request);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return this.searchOffsetByTimestamp(ctx, request);
            case RequestCode.GET_MAX_OFFSET:
                return this.getMaxOffset(ctx, request);
            case RequestCode.GET_MIN_OFFSET:
                return this.getMinOffset(ctx, request);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return this.getEarliestMsgStoretime(ctx, request);
            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return this.getBrokerRuntimeInfo(ctx, request);
            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, request);
            case RequestCode.UNLOCK_BATCH_MQ:
                return this.unlockBatchMQ(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                return this.updateAndCreateSubscriptionGroup(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST:
                return this.updateAndCreateSubscriptionGroupList(ctx, request);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return this.getAllSubscriptionGroup(ctx, request);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return this.deleteSubscriptionGroup(ctx, request);
            case RequestCode.GET_TOPIC_STATS_INFO:
                return this.getTopicStatsInfo(ctx, request);
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return this.getConsumerConnectionList(ctx, request);
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return this.getProducerConnectionList(ctx, request);
            case RequestCode.GET_ALL_PRODUCER_INFO:
                return this.getAllProducerInfo(ctx, request);
            case RequestCode.GET_CONSUME_STATS:
                return this.getConsumeStats(ctx, request);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return this.getAllConsumerOffset(ctx, request);
            case RequestCode.GET_ALL_DELAY_OFFSET:
                return this.getAllDelayOffset(ctx, request);
            case RequestCode.GET_ALL_MESSAGE_REQUEST_MODE:
                return this.getAllMessageRequestMode(ctx, request);
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return this.getConsumerStatus(ctx, request);
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return this.queryTopicConsumeByWho(ctx, request);
            case RequestCode.QUERY_TOPICS_BY_CONSUMER:
                return this.queryTopicsByConsumer(ctx, request);
            case RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER:
                return this.querySubscriptionByConsumer(ctx, request);
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return this.queryConsumeTimeSpan(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return this.getSystemTopicListFromBroker(ctx, request);
            case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                return this.cleanExpiredConsumeQueue();
            case RequestCode.DELETE_EXPIRED_COMMITLOG:
                return this.deleteExpiredCommitLog();
            case RequestCode.CLEAN_UNUSED_TOPIC:
                return this.cleanUnusedTopic();
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);
            case RequestCode.QUERY_CORRECTION_OFFSET:
                return this.queryCorrectionOffset(ctx, request);
            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            case RequestCode.CLONE_GROUP_OFFSET:
                return this.cloneGroupOffset(ctx, request);
            case RequestCode.VIEW_BROKER_STATS_DATA:
                return ViewBrokerStatsData(ctx, request);
            case RequestCode.GET_BROKER_CONSUME_STATS:
                return fetchAllConsumeStatsInBroker(ctx, request);
            case RequestCode.QUERY_CONSUME_QUEUE:
                return queryConsumeQueue(ctx, request);
            case RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS:
                return this.checkRocksdbCqWriteProgress(ctx, request);
            case RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN:
                return this.updateAndGetGroupForbidden(ctx, request);
            case RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG:
                return this.getSubscriptionGroup(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_ACL_CONFIG:
                return updateAndCreateAccessConfig(ctx, request);
            case RequestCode.DELETE_ACL_CONFIG:
                return deleteAccessConfig(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_ACL_INFO:
                return getBrokerAclConfigVersion(ctx, request);
            case RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG:
                return updateGlobalWhiteAddrsConfig(ctx, request);
            case RequestCode.RESUME_CHECK_HALF_MESSAGE:
                return resumeCheckHalfMessage(ctx, request);
            case RequestCode.GET_TOPIC_CONFIG:
                return getTopicConfig(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC:
                return this.updateAndCreateStaticTopic(ctx, request);
            case RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE:
                return this.notifyMinBrokerIdChange(ctx, request);
            case RequestCode.EXCHANGE_BROKER_HA_INFO:
                return this.updateBrokerHaInfo(ctx, request);
            case RequestCode.GET_BROKER_HA_STATUS:
                return this.getBrokerHaStatus(ctx, request);
            case RequestCode.RESET_MASTER_FLUSH_OFFSET:
                return this.resetMasterFlushOffset(ctx, request);
            case RequestCode.GET_BROKER_EPOCH_CACHE:
                return this.getBrokerEpochCache(ctx, request);
            case RequestCode.NOTIFY_BROKER_ROLE_CHANGED:
                return this.notifyBrokerRoleChanged(ctx, request);
            case RequestCode.AUTH_CREATE_USER:
                return this.createUser(ctx, request);
            case RequestCode.AUTH_UPDATE_USER:
                return this.updateUser(ctx, request);
            case RequestCode.AUTH_DELETE_USER:
                return this.deleteUser(ctx, request);
            case RequestCode.AUTH_GET_USER:
                return this.getUser(ctx, request);
            case RequestCode.AUTH_LIST_USER:
                return this.listUser(ctx, request);
            case RequestCode.AUTH_CREATE_ACL:
                return this.createAcl(ctx, request);
            case RequestCode.AUTH_UPDATE_ACL:
                return this.updateAcl(ctx, request);
            case RequestCode.AUTH_DELETE_ACL:
                return this.deleteAcl(ctx, request);
            case RequestCode.AUTH_GET_ACL:
                return this.getAcl(ctx, request);
            case RequestCode.AUTH_LIST_ACL:
                return this.listAcl(ctx, request);
            default:
                return getUnknownCmdResponse(ctx, request);
        }
    }

    /**
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand getSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        GetSubscriptionGroupConfigRequestHeader requestHeader = (GetSubscriptionGroupConfigRequestHeader) request.decodeCommandCustomHeader(GetSubscriptionGroupConfigRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        SubscriptionGroupConfig groupConfig = this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(requestHeader.getGroup());
        if (groupConfig == null) {
            LOGGER.error("No group in this broker, client: {} group: {}", ctx.channel().remoteAddress(), requestHeader.getGroup());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No group in this broker");
            return response;
        }
        String content = JSONObject.toJSONString(groupConfig);
        try {
            response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("UnsupportedEncodingException getSubscriptionGroup: group=" + groupConfig.getGroupName(), e);

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("UnsupportedEncodingException " + e.getMessage());
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    /**
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand updateAndGetGroupForbidden(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UpdateGroupForbiddenRequestHeader requestHeader = (UpdateGroupForbiddenRequestHeader) //
            request.decodeCommandCustomHeader(UpdateGroupForbiddenRequestHeader.class);
        String group = requestHeader.getGroup();
        String topic = requestHeader.getTopic();
        LOGGER.info("updateAndGetGroupForbidden called by {} for object {}@{} readable={}",//
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), group, //
            topic, requestHeader.getReadable());
        SubscriptionGroupManager groupManager = this.brokerController.getSubscriptionGroupManager();
        if (requestHeader.getReadable() != null) {
            groupManager.updateForbidden(group, topic, PermName.INDEX_PERM_READ, !requestHeader.getReadable());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark("");
        GroupForbidden groupForbidden = new GroupForbidden();
        groupForbidden.setGroup(group);
        groupForbidden.setTopic(topic);
        groupForbidden.setReadable(!groupManager.getForbidden(group, topic, PermName.INDEX_PERM_READ));
        response.setBody(groupForbidden.toJson().getBytes(StandardCharsets.UTF_8));
        return response;
    }

    private RemotingCommand checkRocksdbCqWriteProgress(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        CheckRocksdbCqWriteProgressRequestHeader requestHeader = request.decodeCommandCustomHeader(CheckRocksdbCqWriteProgressRequestHeader.class);
        String requestTopic = requestHeader.getTopic();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        MessageStore messageStore = brokerController.getMessageStore();
        DefaultMessageStore defaultMessageStore;
        if (messageStore instanceof AbstractPluginMessageStore) {
            defaultMessageStore = (DefaultMessageStore) ((AbstractPluginMessageStore) messageStore).getNext();
        } else {
            defaultMessageStore = (DefaultMessageStore) messageStore;
        }
        RocksDBMessageStore rocksDBMessageStore = defaultMessageStore.getRocksDBMessageStore();
        if (!defaultMessageStore.getMessageStoreConfig().isRocksdbCQDoubleWriteEnable()) {
            response.setBody(JSON.toJSONBytes(ImmutableMap.of("diffResult", "rocksdbCQWriteEnable is false, checkRocksdbCqWriteProgressCommand is invalid")));
            return response;
        }

        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> cqTable = defaultMessageStore.getConsumeQueueTable();
        StringBuilder diffResult = new StringBuilder();
        try {
            if (StringUtils.isNotBlank(requestTopic)) {
                processConsumeQueuesForTopic(cqTable.get(requestTopic), requestTopic, rocksDBMessageStore, diffResult,false);
                response.setBody(JSON.toJSONBytes(ImmutableMap.of("diffResult", diffResult.toString())));
                return response;
            }
            for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : cqTable.entrySet()) {
                String topic = topicEntry.getKey();
                processConsumeQueuesForTopic(topicEntry.getValue(), topic, rocksDBMessageStore, diffResult,true);
            }
            diffResult.append("check all topic successful, size:").append(cqTable.size());
            response.setBody(JSON.toJSONBytes(ImmutableMap.of("diffResult", diffResult.toString())));

        } catch (Exception e) {
            LOGGER.error("CheckRocksdbCqWriteProgressCommand error", e);
            response.setBody(JSON.toJSONBytes(ImmutableMap.of("diffResult", e.getMessage())));
        }
        return response;
    }

    private void processConsumeQueuesForTopic(ConcurrentMap<Integer, ConsumeQueueInterface> queueMap, String topic, RocksDBMessageStore rocksDBMessageStore, StringBuilder diffResult, boolean checkAll) {
        for (Map.Entry<Integer, ConsumeQueueInterface> queueEntry : queueMap.entrySet()) {
            Integer queueId = queueEntry.getKey();
            ConsumeQueueInterface jsonCq = queueEntry.getValue();
            ConsumeQueueInterface kvCq = rocksDBMessageStore.getConsumeQueue(topic, queueId);
            if (!checkAll) {
                String format = String.format("\n[topic: %s, queue:  %s] \n  kvEarliest : %s |  kvLatest : %s \n fileEarliest: %s | fileEarliest: %s ",
                    topic, queueId, kvCq.getEarliestUnit(), kvCq.getLatestUnit(), jsonCq.getEarliestUnit(), jsonCq.getLatestUnit());
                diffResult.append(format).append("\n");
            }
            long maxFileOffsetInQueue = jsonCq.getMaxOffsetInQueue();
            long minOffsetInQueue = kvCq.getMinOffsetInQueue();
            for (long i = minOffsetInQueue; i < maxFileOffsetInQueue; i++) {
                Pair<CqUnit, Long> fileCqUnit = jsonCq.getCqUnitAndStoreTime(i);
                Pair<CqUnit, Long> kvCqUnit = kvCq.getCqUnitAndStoreTime(i);
                if (fileCqUnit == null || kvCqUnit == null) {
                    diffResult.append(String.format("[topic: %s, queue: %s, offset: %s] \n kv   : %s  \n file : %s  \n",
                        topic, queueId, i, kvCqUnit != null ? kvCqUnit.getObject1() : "null", fileCqUnit != null ? fileCqUnit.getObject1() : "null"));
                    return;
                }
                if (!checkCqUnitEqual(kvCqUnit.getObject1(), fileCqUnit.getObject1())) {
                    String diffInfo = String.format("[topic:%s, queue: %s offset: %s] \n file : %s  \n  kv : %s \n",
                        topic, queueId, i, kvCqUnit.getObject1(), fileCqUnit.getObject1());
                    LOGGER.error(diffInfo);
                    diffResult.append(diffInfo).append(System.lineSeparator());
                    return;
                }
            }
        }
    }
    @Override
    public boolean rejectRequest() {
        return false;
    }

    private synchronized RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        long startTime = System.currentTimeMillis();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
            (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);

        LOGGER.info("Broker receive request to update or create topic={}, caller address={}",
            requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        String topic = requestHeader.getTopic();

        long executionTime;
        try {
            TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(topic);
            if (!result.isValid()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(result.getRemark());
                return response;
            }
            if (brokerController.getBrokerConfig().isValidateSystemTopicWhenUpdateTopic()) {
                if (TopicValidator.isSystemTopic(topic)) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("The topic[" + topic + "] is conflict with system topic.");
                    return response;
                }
            }

            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
            topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
            topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
            topicConfig.setPerm(requestHeader.getPerm());
            topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag() == null ? 0 : requestHeader.getTopicSysFlag());
            topicConfig.setOrder(requestHeader.getOrder());
            String attributesModification = requestHeader.getAttributes();
            topicConfig.setAttributes(AttributeParser.parseToMap(attributesModification));

            if (topicConfig.getTopicMessageType() == TopicMessageType.MIXED
                && !brokerController.getBrokerConfig().isEnableMixedMessageType()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("MIXED message type is not supported.");
                return response;
            }

            if (topicConfig.equals(this.brokerController.getTopicConfigManager().getTopicConfigTable().get(topic))) {
                LOGGER.info("Broker receive request to update or create topic={}, but topicConfig has  no changes , so idempotent, caller address={}",
                    requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                response.setCode(ResponseCode.SUCCESS);
                return response;
            }

            this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
            if (brokerController.getBrokerConfig().isEnableSingleTopicRegister()) {
                this.brokerController.registerSingleTopicAll(topicConfig);
            } else {
                this.brokerController.registerIncrementBrokerData(topicConfig, this.brokerController.getTopicConfigManager().getDataVersion());
            }
            response.setCode(ResponseCode.SUCCESS);
        } catch (Exception e) {
            LOGGER.error("Update / create topic failed for [{}]", request, e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }
        finally {
            executionTime = System.currentTimeMillis() - startTime;
            InvocationStatus status = response.getCode() == ResponseCode.SUCCESS ?
                    InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                    .put(LABEL_INVOCATION_STATUS, status.getName())
                    .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topic))
                    .build();
            BrokerMetricsManager.topicCreateExecuteTime.record(executionTime, attributes);
        }
        LOGGER.info("executionTime of create topic:{} is {} ms" , topic, executionTime);
        return response;
    }

    private synchronized RemotingCommand updateAndCreateTopicList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        long startTime = System.currentTimeMillis();

        final CreateTopicListRequestBody requestBody = CreateTopicListRequestBody.decode(request.getBody(), CreateTopicListRequestBody.class);
        List<TopicConfig> topicConfigList = requestBody.getTopicConfigList();

        StringBuilder builder = new StringBuilder();
        for (TopicConfig topicConfig : topicConfigList) {
            builder.append(topicConfig.getTopicName()).append(";");
        }
        String topicNames = builder.toString();
        LOGGER.info("AdminBrokerProcessor#updateAndCreateTopicList: topicNames: {}, called by {}", topicNames, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        long executionTime;

        try {
            // Valid topics
            for (TopicConfig topicConfig : topicConfigList) {
                String topic = topicConfig.getTopicName();
                TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(topic);
                if (!result.isValid()) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(result.getRemark());
                    return response;
                }
                if (brokerController.getBrokerConfig().isValidateSystemTopicWhenUpdateTopic()) {
                    if (TopicValidator.isSystemTopic(topic)) {
                        response.setCode(ResponseCode.SYSTEM_ERROR);
                        response.setRemark("The topic[" + topic + "] is conflict with system topic.");
                        return response;
                    }
                }
                if (topicConfig.getTopicMessageType() == TopicMessageType.MIXED
                    && !brokerController.getBrokerConfig().isEnableMixedMessageType()) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("MIXED message type is not supported.");
                    return response;
                }
                if (topicConfig.equals(this.brokerController.getTopicConfigManager().getTopicConfigTable().get(topic))) {
                    LOGGER.info("Broker receive request to update or create topic={}, but topicConfig has  no changes , so idempotent, caller address={}",
                        topic, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                    response.setCode(ResponseCode.SUCCESS);
                    return response;
                }
            }

            this.brokerController.getTopicConfigManager().updateTopicConfigList(topicConfigList);
            if (brokerController.getBrokerConfig().isEnableSingleTopicRegister()) {
                for (TopicConfig topicConfig : topicConfigList) {
                    this.brokerController.registerSingleTopicAll(topicConfig);
                }
            } else {
                this.brokerController.registerIncrementBrokerData(topicConfigList, this.brokerController.getTopicConfigManager().getDataVersion());
            }
            response.setCode(ResponseCode.SUCCESS);
        } catch (Exception e) {
            LOGGER.error("Update / create topic failed for [{}]", request, e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
            return response;
        }
        finally {
            executionTime = System.currentTimeMillis() - startTime;
            InvocationStatus status = response.getCode() == ResponseCode.SUCCESS ?
                InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_INVOCATION_STATUS, status.getName())
                .put(LABEL_IS_SYSTEM, TopicValidator.isSystemTopic(topicNames))
                .build();
            BrokerMetricsManager.topicCreateExecuteTime.record(executionTime, attributes);
        }
        LOGGER.info("executionTime of all topics:{} is {} ms" , topicNames, executionTime);
        return response;
    }

    private synchronized RemotingCommand updateAndCreateStaticTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
            (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
        LOGGER.info("Broker receive request to update or create static topic={}, caller address={}", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        final TopicQueueMappingDetail topicQueueMappingDetail = RemotingSerializable.decode(request.getBody(), TopicQueueMappingDetail.class);

        String topic = requestHeader.getTopic();

        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(topic);
        if (!result.isValid()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(result.getRemark());
            return response;
        }
        if (brokerController.getBrokerConfig().isValidateSystemTopicWhenUpdateTopic()) {
            if (TopicValidator.isSystemTopic(topic)) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The topic[" + topic + "] is conflict with system topic.");
                return response;
            }
        }
        boolean force = requestHeader.getForce() != null && requestHeader.getForce();

        TopicConfig topicConfig = new TopicConfig(topic);
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        topicConfig.setPerm(requestHeader.getPerm());
        topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag() == null ? 0 : requestHeader.getTopicSysFlag());

        try {
            this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

            this.brokerController.getTopicQueueMappingManager().updateTopicQueueMapping(topicQueueMappingDetail, force, false, true);

            this.brokerController.registerIncrementBrokerData(topicConfig, this.brokerController.getTopicConfigManager().getDataVersion());
            response.setCode(ResponseCode.SUCCESS);
        } catch (Exception e) {
            LOGGER.error("Update static topic failed for [{}]", request, e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
        }
        return response;
    }

    private synchronized RemotingCommand deleteTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteTopicRequestHeader requestHeader =
            (DeleteTopicRequestHeader) request.decodeCommandCustomHeader(DeleteTopicRequestHeader.class);

        LOGGER.info("AdminBrokerProcessor#deleteTopic: broker receive request to delete topic={}, caller={}",
            requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        String topic = requestHeader.getTopic();

        if (UtilAll.isBlank(topic)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is blank.");
            return response;
        }

        if (brokerController.getBrokerConfig().isValidateSystemTopicWhenUpdateTopic()) {
            if (TopicValidator.isSystemTopic(topic)) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The topic[" + topic + "] is conflict with system topic.");
                return response;
            }
        }

        final Set<String> groups = this.brokerController.getConsumerOffsetManager().whichGroupByTopic(topic);
        // delete pop retry topics first
        try {
            for (String group : groups) {
                final String popRetryTopicV2 = KeyBuilder.buildPopRetryTopic(topic, group, true);
                if (brokerController.getTopicConfigManager().selectTopicConfig(popRetryTopicV2) != null) {
                    deleteTopicInBroker(popRetryTopicV2);
                }
                final String popRetryTopicV1 = KeyBuilder.buildPopRetryTopicV1(topic, group);
                if (brokerController.getTopicConfigManager().selectTopicConfig(popRetryTopicV1) != null) {
                    deleteTopicInBroker(popRetryTopicV1);
                }
            }
            // delete topic
            deleteTopicInBroker(topic);
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private void deleteTopicInBroker(String topic) {
        this.brokerController.getTopicConfigManager().deleteTopicConfig(topic);
        this.brokerController.getTopicQueueMappingManager().delete(topic);
        this.brokerController.getConsumerOffsetManager().cleanOffsetByTopic(topic);
        this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNumByTopicName(topic);
        this.brokerController.getMessageStore().deleteTopics(Sets.newHashSet(topic));
    }

    private synchronized RemotingCommand updateAndCreateAccessConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final CreateAccessConfigRequestHeader requestHeader =
            (CreateAccessConfigRequestHeader) request.decodeCommandCustomHeader(CreateAccessConfigRequestHeader.class);

        PlainAccessConfig accessConfig = new PlainAccessConfig();
        accessConfig.setAccessKey(requestHeader.getAccessKey());
        accessConfig.setSecretKey(requestHeader.getSecretKey());
        accessConfig.setWhiteRemoteAddress(requestHeader.getWhiteRemoteAddress());
        accessConfig.setDefaultTopicPerm(requestHeader.getDefaultTopicPerm());
        accessConfig.setDefaultGroupPerm(requestHeader.getDefaultGroupPerm());
        accessConfig.setTopicPerms(UtilAll.split(requestHeader.getTopicPerms(), ","));
        accessConfig.setGroupPerms(UtilAll.split(requestHeader.getGroupPerms(), ","));
        accessConfig.setAdmin(requestHeader.isAdmin());
        try {

            AccessValidator accessValidator = this.brokerController.getAccessValidatorMap().get(PlainAccessValidator.class);
            if (accessValidator.updateAccessConfig(accessConfig)) {
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                response.markResponseType();
                response.setRemark(null);
                NettyRemotingAbstract.writeResponse(ctx.channel(), request, response);
            } else {
                String errorMsg = "The accessKey[" + requestHeader.getAccessKey() + "] corresponding to accessConfig has been updated failed.";
                LOGGER.warn(errorMsg);
                response.setCode(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED);
                response.setRemark(errorMsg);
                return response;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to generate a proper update accessValidator response", e);
            response.setCode(ResponseCode.UPDATE_AND_CREATE_ACL_CONFIG_FAILED);
            response.setRemark(e.getMessage());
            return response;
        }

        return null;
    }

    private synchronized RemotingCommand deleteAccessConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final DeleteAccessConfigRequestHeader requestHeader =
            (DeleteAccessConfigRequestHeader) request.decodeCommandCustomHeader(DeleteAccessConfigRequestHeader.class);
        LOGGER.info("DeleteAccessConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        try {
            String accessKey = requestHeader.getAccessKey();
            AccessValidator accessValidator = this.brokerController.getAccessValidatorMap().get(PlainAccessValidator.class);
            if (accessValidator.deleteAccessConfig(accessKey)) {
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                response.markResponseType();
                response.setRemark(null);
                NettyRemotingAbstract.writeResponse(ctx.channel(), request, response);
            } else {
                String errorMsg = "The accessKey[" + requestHeader.getAccessKey() + "] corresponding to accessConfig has been deleted failed.";
                LOGGER.warn(errorMsg);
                response.setCode(ResponseCode.DELETE_ACL_CONFIG_FAILED);
                response.setRemark(errorMsg);
                return response;
            }

        } catch (Exception e) {
            LOGGER.error("Failed to generate a proper delete accessValidator response", e);
            response.setCode(ResponseCode.DELETE_ACL_CONFIG_FAILED);
            response.setRemark(e.getMessage());
            return response;
        }

        return null;
    }

    private synchronized RemotingCommand updateGlobalWhiteAddrsConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader =
            (UpdateGlobalWhiteAddrsConfigRequestHeader) request.decodeCommandCustomHeader(UpdateGlobalWhiteAddrsConfigRequestHeader.class);

        try {
            AccessValidator accessValidator = this.brokerController.getAccessValidatorMap().get(PlainAccessValidator.class);
            if (accessValidator.updateGlobalWhiteAddrsConfig(UtilAll.split(requestHeader.getGlobalWhiteAddrs(), ","),
                requestHeader.getAclFileFullPath())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setOpaque(request.getOpaque());
                response.markResponseType();
                response.setRemark(null);
                NettyRemotingAbstract.writeResponse(ctx.channel(), request, response);
            } else {
                String errorMsg = "The globalWhiteAddresses[" + requestHeader.getGlobalWhiteAddrs() + "] has been updated failed.";
                LOGGER.warn(errorMsg);
                response.setCode(ResponseCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED);
                response.setRemark(errorMsg);
                return response;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to generate a proper update globalWhiteAddresses response", e);
            response.setCode(ResponseCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED);
            response.setRemark(e.getMessage());
            return response;
        }

        return null;
    }

    private RemotingCommand getBrokerAclConfigVersion(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerAclConfigResponseHeader.class);

        final GetBrokerAclConfigResponseHeader responseHeader = (GetBrokerAclConfigResponseHeader) response.readCustomHeader();

        try {
            AccessValidator accessValidator = this.brokerController.getAccessValidatorMap().get(PlainAccessValidator.class);

            responseHeader.setVersion(accessValidator.getAclConfigVersion());
            responseHeader.setBrokerAddr(this.brokerController.getBrokerAddr());
            responseHeader.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            responseHeader.setClusterName(this.brokerController.getBrokerConfig().getBrokerClusterName());

            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Exception e) {
            LOGGER.error("Failed to generate a proper getBrokerAclConfigVersion response", e);
        }

        return null;
    }

    private RemotingCommand getUnknownCmdResponse(ChannelHandlerContext ctx, RemotingCommand request) {
        String error = " request type " + request.getCode() + " not supported";
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        return response;
    }

    private RemotingCommand getAllTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetAllTopicConfigResponseHeader.class);
        // final GetAllTopicConfigResponseHeader responseHeader =
        // (GetAllTopicConfigResponseHeader) response.readCustomHeader();

        TopicConfigAndMappingSerializeWrapper topicConfigAndMappingSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();

        topicConfigAndMappingSerializeWrapper.setDataVersion(this.brokerController.getTopicConfigManager().getDataVersion());
        topicConfigAndMappingSerializeWrapper.setTopicConfigTable(this.brokerController.getTopicConfigManager().getTopicConfigTable());

        topicConfigAndMappingSerializeWrapper.setMappingDataVersion(this.brokerController.getTopicQueueMappingManager().getDataVersion());
        topicConfigAndMappingSerializeWrapper.setTopicQueueMappingDetailMap(this.brokerController.getTopicQueueMappingManager().getTopicQueueMappingTable());

        String content = topicConfigAndMappingSerializeWrapper.toJson();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e.getMessage());
                return response;
            }
        } else {
            LOGGER.error("No topic in this broker, client: {}", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getTimerCheckPoint(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "Unknown");
        TimerCheckpoint timerCheckpoint = this.brokerController.getTimerCheckpoint();
        if (null == timerCheckpoint) {
            LOGGER.error("AdminBrokerProcessor#getTimerCheckPoint: checkpoint is null, caller={}", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The checkpoint is null");
            return response;
        }
        response.setBody(TimerCheckpoint.encode(timerCheckpoint).array());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getTimerMetrics(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "Unknown");
        TimerMessageStore timerMessageStore = this.brokerController.getMessageStore().getTimerMessageStore();
        if (null == timerMessageStore) {
            LOGGER.error("The timer message store is null, client: {}", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The timer message store is null");
            return response;
        }
        response.setBody(timerMessageStore.getTimerMetrics().encode().getBytes(StandardCharsets.UTF_8));
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private synchronized RemotingCommand updateColdDataFlowCtrGroupConfig(ChannelHandlerContext ctx,
        RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LOGGER.info("updateColdDataFlowCtrGroupConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    LOGGER.info("updateColdDataFlowCtrGroupConfig new config: {}, client: {}", properties, ctx.channel().remoteAddress());
                    properties.forEach((key, value) -> {
                        try {
                            String consumerGroup = String.valueOf(key);
                            Long threshold = Long.valueOf(String.valueOf(value));
                            this.brokerController.getColdDataCgCtrService()
                                    .addOrUpdateGroupConfig(consumerGroup, threshold);
                        } catch (Exception e) {
                            LOGGER.error("updateColdDataFlowCtrGroupConfig properties on entry error, key: {}, val: {}",
                                    key, value, e);
                        }
                    });
                } else {
                    LOGGER.error("updateColdDataFlowCtrGroupConfig string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("updateColdDataFlowCtrGroupConfig UnsupportedEncodingException", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private synchronized RemotingCommand removeColdDataFlowCtrGroupConfig(ChannelHandlerContext ctx,
        RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LOGGER.info("removeColdDataFlowCtrGroupConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String consumerGroup = new String(body, MixAll.DEFAULT_CHARSET);
                if (consumerGroup != null) {
                    LOGGER.info("removeColdDataFlowCtrGroupConfig, consumerGroup: {} client: {}", consumerGroup, ctx.channel().remoteAddress());
                    this.brokerController.getColdDataCgCtrService().removeGroupConfig(consumerGroup);
                } else {
                    LOGGER.error("removeColdDataFlowCtrGroupConfig string parse error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string parse error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("removeColdDataFlowCtrGroupConfig UnsupportedEncodingException", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getColdDataFlowCtrInfo(ChannelHandlerContext ctx) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LOGGER.info("getColdDataFlowCtrInfo called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        String content = this.brokerController.getColdDataCgCtrService().getColdDataFlowCtrInfo();
        if (content != null) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("getColdDataFlowCtrInfo UnsupportedEncodingException", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand setCommitLogReadaheadMode(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LOGGER.info("setCommitLogReadaheadMode called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        try {
            HashMap<String, String> extFields = request.getExtFields();
            if (null == extFields) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("set commitlog readahead mode param error");
                return response;
            }
            int mode = Integer.parseInt(extFields.get(FIleReadaheadMode.READ_AHEAD_MODE));
            if (mode != LibC.MADV_RANDOM && mode != LibC.MADV_NORMAL) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("set commitlog readahead mode param value error");
                return response;
            }
            MessageStore messageStore = this.brokerController.getMessageStore();
            if (messageStore instanceof DefaultMessageStore) {
                DefaultMessageStore defaultMessageStore = (DefaultMessageStore) messageStore;
                if (mode == LibC.MADV_NORMAL) {
                    defaultMessageStore.getMessageStoreConfig().setDataReadAheadEnable(true);
                } else {
                    defaultMessageStore.getMessageStoreConfig().setDataReadAheadEnable(false);
                }
                defaultMessageStore.getCommitLog().scanFileAndSetReadMode(mode);
            }
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark("set commitlog readahead mode success, mode: " + mode);
        } catch (Exception e) {
            LOGGER.error("set commitlog readahead mode failed", e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("set commitlog readahead mode failed");
        }
        return response;
    }

    private synchronized RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        final String callerAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        LOGGER.info("Broker receive request to update config, caller address={}", callerAddress);

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    LOGGER.info("updateBrokerConfig, new config: [{}] client: {} ", properties, callerAddress);
                    if (validateBlackListConfigExist(properties)) {
                        response.setCode(ResponseCode.NO_PERMISSION);
                        response.setRemark("Can not update config in black list.");
                        return response;
                    }

                    this.brokerController.getConfiguration().update(properties);
                    if (properties.containsKey("brokerPermission")) {
                        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                        this.brokerController.getTopicConfigManager().getDataVersion().nextVersion(stateMachineVersion);
                        this.brokerController.registerBrokerAll(false, false, true);
                    }

                } else {
                    LOGGER.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("AdminBrokerProcessor#updateBrokerConfig: unexpected error, caller={}",
                    callerAddress, e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        String content = this.brokerController.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("AdminBrokerProcessor#getBrokerConfig: unexpected error, caller={}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfiguration().getDataVersionJson());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand rewriteRequestForStaticTopic(SearchOffsetRequestHeader requestHeader,
        TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }

            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            List<LogicQueueMappingItem> mappingItems = mappingContext.getMappingItemList();
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
            }
            //TO DO should make sure the timestampOfOffset is equal or bigger than the searched timestamp
            Long timestamp = requestHeader.getTimestamp();
            long offset = -1;
            for (int i = 0; i < mappingItems.size(); i++) {
                LogicQueueMappingItem item = mappingItems.get(i);
                if (!item.checkIfLogicoffsetDecided()) {
                    continue;
                }
                if (mappingDetail.getBname().equals(item.getBname())) {
                    offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(mappingContext.getTopic(), item.getQueueId(), timestamp, requestHeader.getBoundaryType());
                    if (offset > 0) {
                        offset = item.computeStaticQueueOffsetStrictly(offset);
                        break;
                    }
                } else {
                    requestHeader.setLo(false);
                    requestHeader.setTimestamp(timestamp);
                    requestHeader.setQueueId(item.getQueueId());
                    requestHeader.setBrokerName(item.getBname());
                    RpcRequest rpcRequest = new RpcRequest(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader, null);
                    RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
                    if (rpcResponse.getException() != null) {
                        throw rpcResponse.getException();
                    }
                    SearchOffsetResponseHeader offsetResponseHeader = (SearchOffsetResponseHeader) rpcResponse.getHeader();
                    if (offsetResponseHeader.getOffset() < 0
                        || item.checkIfEndOffsetDecided() && offsetResponseHeader.getOffset() >= item.getEndOffset()) {
                        continue;
                    } else {
                        offset = item.computeStaticQueueOffsetStrictly(offsetResponseHeader.getOffset());
                    }

                }
            }
            final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
            final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
        final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
        final SearchOffsetRequestHeader requestHeader =
            (SearchOffsetRequestHeader) request.decodeCommandCustomHeader(SearchOffsetRequestHeader.class);

        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader);

        RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

        long offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(), requestHeader.getQueueId(),
            requestHeader.getTimestamp(), requestHeader.getBoundaryType());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand rewriteRequestForStaticTopic(GetMaxOffsetRequestHeader requestHeader,
        TopicQueueMappingContext mappingContext) {
        if (mappingContext.getMappingDetail() == null) {
            return null;
        }

        TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
        LogicQueueMappingItem mappingItem = mappingContext.getLeaderItem();
        if (!mappingContext.isLeader()) {
            return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
        }

        try {
            LogicQueueMappingItem maxItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), Long.MAX_VALUE, true);
            assert maxItem != null;
            assert maxItem.getLogicOffset() >= 0;
            requestHeader.setBrokerName(maxItem.getBname());
            requestHeader.setLo(false);
            requestHeader.setQueueId(mappingItem.getQueueId());

            long maxPhysicalOffset = Long.MAX_VALUE;
            if (maxItem.getBname().equals(mappingDetail.getBname())) {
                //current broker
                maxPhysicalOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(mappingContext.getTopic(), mappingItem.getQueueId());
            } else {
                RpcRequest rpcRequest = new RpcRequest(RequestCode.GET_MAX_OFFSET, requestHeader, null);
                RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
                if (rpcResponse.getException() != null) {
                    throw rpcResponse.getException();
                }
                GetMaxOffsetResponseHeader offsetResponseHeader = (GetMaxOffsetResponseHeader) rpcResponse.getHeader();
                maxPhysicalOffset = offsetResponseHeader.getOffset();
            }

            final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
            final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(maxItem.computeStaticQueueOffsetStrictly(maxPhysicalOffset));
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
        final GetMaxOffsetRequestHeader requestHeader =
            (GetMaxOffsetRequestHeader) request.decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader);
        RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private CompletableFuture<RpcResponse> handleGetMinOffsetForStaticTopic(RpcRequest request,
        TopicQueueMappingContext mappingContext) {
        if (mappingContext.getMappingDetail() == null) {
            return null;
        }
        TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
        if (!mappingContext.isLeader()) {
            //this may not
            return CompletableFuture.completedFuture(new RpcResponse(new RpcException(ResponseCode.NOT_LEADER_FOR_QUEUE,
                String.format("%s-%d is not leader in broker %s, request code %d", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname(), request.getCode()))));
        }
        GetMinOffsetRequestHeader requestHeader = (GetMinOffsetRequestHeader) request.getHeader();
        LogicQueueMappingItem mappingItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), 0L, true);
        assert mappingItem != null;
        try {
            requestHeader.setBrokerName(mappingItem.getBname());
            requestHeader.setLo(false);
            requestHeader.setQueueId(mappingItem.getQueueId());
            long physicalOffset;
            //run in local
            if (mappingItem.getBname().equals(mappingDetail.getBname())) {
                physicalOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(mappingDetail.getTopic(), mappingItem.getQueueId());
            } else {
                RpcRequest rpcRequest = new RpcRequest(RequestCode.GET_MIN_OFFSET, requestHeader, null);
                RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
                if (rpcResponse.getException() != null) {
                    throw rpcResponse.getException();
                }
                GetMinOffsetResponseHeader offsetResponseHeader = (GetMinOffsetResponseHeader) rpcResponse.getHeader();
                physicalOffset = offsetResponseHeader.getOffset();
            }
            long offset = mappingItem.computeStaticQueueOffsetLoosely(physicalOffset);

            final GetMinOffsetResponseHeader responseHeader = new GetMinOffsetResponseHeader();
            responseHeader.setOffset(offset);
            return CompletableFuture.completedFuture(new RpcResponse(ResponseCode.SUCCESS, responseHeader, null));
        } catch (Throwable t) {
            LOGGER.error("rewriteRequestForStaticTopic failed", t);
            return CompletableFuture.completedFuture(new RpcResponse(new RpcException(ResponseCode.SYSTEM_ERROR, t.getMessage(), t)));
        }
    }

    private CompletableFuture<RpcResponse> handleGetMinOffset(RpcRequest request) {
        assert request.getCode() == RequestCode.GET_MIN_OFFSET;
        GetMinOffsetRequestHeader requestHeader = (GetMinOffsetRequestHeader) request.getHeader();
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, false);
        CompletableFuture<RpcResponse> rewriteResult = handleGetMinOffsetForStaticTopic(request, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }
        final GetMinOffsetResponseHeader responseHeader = new GetMinOffsetResponseHeader();
        long offset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        responseHeader.setOffset(offset);
        return CompletableFuture.completedFuture(new RpcResponse(ResponseCode.SUCCESS, responseHeader, null));
    }

    private RemotingCommand getMinOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetMinOffsetRequestHeader requestHeader =
            (GetMinOffsetRequestHeader) request.decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);
        try {
            CompletableFuture<RpcResponse> responseFuture = handleGetMinOffset(new RpcRequest(RequestCode.GET_MIN_OFFSET, requestHeader, null));
            RpcResponse rpcResponse = responseFuture.get();
            return RpcClientUtils.createCommandForRpcResponse(rpcResponse);
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

    private RemotingCommand rewriteRequestForStaticTopic(GetEarliestMsgStoretimeRequestHeader requestHeader,
        TopicQueueMappingContext mappingContext) {
        if (mappingContext.getMappingDetail() == null) {
            return null;
        }

        TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
        if (!mappingContext.isLeader()) {
            return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", mappingContext.getTopic(), mappingContext.getGlobalId(), mappingDetail.getBname()));
        }
        LogicQueueMappingItem mappingItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), 0L, true);
        assert mappingItem != null;
        try {
            requestHeader.setBrokerName(mappingItem.getBname());
            requestHeader.setLo(false);
            RpcRequest rpcRequest = new RpcRequest(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader, null);
            //TO DO check if it is in current broker
            RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
            if (rpcResponse.getException() != null) {
                throw rpcResponse.getException();
            }
            GetEarliestMsgStoretimeResponseHeader offsetResponseHeader = (GetEarliestMsgStoretimeResponseHeader) rpcResponse.getHeader();

            final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
            final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
            responseHeader.setTimestamp(offsetResponseHeader.getTimestamp());
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }

    private RemotingCommand getEarliestMsgStoretime(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
        final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
        final GetEarliestMsgStoretimeRequestHeader requestHeader =
            (GetEarliestMsgStoretimeRequestHeader) request.decodeCommandCustomHeader(GetEarliestMsgStoretimeRequestHeader.class);

        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, false);
        RemotingCommand rewriteResult = rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

        long timestamp =
            this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setTimestamp(timestamp);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        byte[] body = kvTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        Set<MessageQueue> lockOKMQSet = new HashSet<>();
        Set<MessageQueue> selfLockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(
            requestBody.getConsumerGroup(),
            requestBody.getMqSet(),
            requestBody.getClientId());
        if (requestBody.isOnlyThisBroker() || !brokerController.getBrokerConfig().isLockInStrictMode()) {
            lockOKMQSet = selfLockOKMQSet;
        } else {
            requestBody.setOnlyThisBroker(true);
            int replicaSize = this.brokerController.getMessageStoreConfig().getTotalReplicas();

            int quorum = replicaSize / 2 + 1;

            if (quorum <= 1) {
                lockOKMQSet = selfLockOKMQSet;
            } else {
                final ConcurrentMap<MessageQueue, Integer> mqLockMap = new ConcurrentHashMap<>();
                for (MessageQueue mq : selfLockOKMQSet) {
                    if (!mqLockMap.containsKey(mq)) {
                        mqLockMap.put(mq, 0);
                    }
                    mqLockMap.put(mq, mqLockMap.get(mq) + 1);
                }

                BrokerMemberGroup memberGroup = this.brokerController.getBrokerMemberGroup();

                if (memberGroup != null) {
                    Map<Long, String> addrMap = new HashMap<>(memberGroup.getBrokerAddrs());
                    addrMap.remove(this.brokerController.getBrokerConfig().getBrokerId());
                    final CountDownLatch countDownLatch = new CountDownLatch(addrMap.size());
                    requestBody.setMqSet(selfLockOKMQSet);
                    requestBody.setOnlyThisBroker(true);
                    for (Long brokerId : addrMap.keySet()) {
                        try {
                            this.brokerController.getBrokerOuterAPI().lockBatchMQAsync(addrMap.get(brokerId),
                                requestBody, 1000, new LockCallback() {
                                    @Override
                                    public void onSuccess(Set<MessageQueue> lockOKMQSet) {
                                        for (MessageQueue mq : lockOKMQSet) {
                                            if (!mqLockMap.containsKey(mq)) {
                                                mqLockMap.put(mq, 0);
                                            }
                                            mqLockMap.put(mq, mqLockMap.get(mq) + 1);
                                        }
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onException(Throwable e) {
                                        LOGGER.warn("lockBatchMQAsync on {} failed, {}", addrMap.get(brokerId), e);
                                        countDownLatch.countDown();
                                    }
                                });
                        } catch (Exception e) {
                            LOGGER.warn("lockBatchMQAsync on {} failed, {}", addrMap.get(brokerId), e);
                            countDownLatch.countDown();
                        }
                    }
                    try {
                        countDownLatch.await(2000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        LOGGER.warn("lockBatchMQ exception on {}, {}", this.brokerController.getBrokerConfig().getBrokerName(), e);
                    }
                }

                for (MessageQueue mq : mqLockMap.keySet()) {
                    if (mqLockMap.get(mq) >= quorum) {
                        lockOKMQSet.add(mq);
                    }
                }
            }
        }

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);

        if (requestBody.isOnlyThisBroker() || !this.brokerController.getBrokerConfig().isLockInStrictMode()) {
            this.brokerController.getRebalanceLockManager().unlockBatch(
                requestBody.getConsumerGroup(),
                requestBody.getMqSet(),
                requestBody.getClientId());
        } else {
            requestBody.setOnlyThisBroker(true);
            BrokerMemberGroup memberGroup = this.brokerController.getBrokerMemberGroup();

            if (memberGroup != null) {
                Map<Long, String> addrMap = memberGroup.getBrokerAddrs();
                for (Long brokerId : addrMap.keySet()) {
                    try {
                        this.brokerController.getBrokerOuterAPI().unlockBatchMQAsync(addrMap.get(brokerId), requestBody, 1000, new UnlockCallback() {
                            @Override
                            public void onSuccess() {

                            }

                            @Override
                            public void onException(Throwable e) {
                                LOGGER.warn("unlockBatchMQ exception on {}, {}", addrMap.get(brokerId), e);
                            }
                        });
                    } catch (Exception e) {
                        LOGGER.warn("unlockBatchMQ exception on {}, {}", addrMap.get(brokerId), e);
                    }
                }
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        long startTime = System.currentTimeMillis();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        LOGGER.info("AdminBrokerProcessor#updateAndCreateSubscriptionGroup called by {}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config = RemotingSerializable.decode(request.getBody(), SubscriptionGroupConfig.class);
        if (config != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        long executionTime = System.currentTimeMillis() - startTime;
        LOGGER.info("executionTime of create subscriptionGroup:{} is {} ms" ,config.getGroupName() ,executionTime);
        InvocationStatus status = response.getCode() == ResponseCode.SUCCESS ?
                InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_INVOCATION_STATUS, status.getName())
                .build();
        BrokerMetricsManager.consumerGroupCreateExecuteTime.record(executionTime, attributes);
        return response;
    }

    private RemotingCommand updateAndCreateSubscriptionGroupList(ChannelHandlerContext ctx, RemotingCommand request) {
        final long startTime = System.nanoTime();

        final SubscriptionGroupList subscriptionGroupList = SubscriptionGroupList.decode(request.getBody(), SubscriptionGroupList.class);
        final List<SubscriptionGroupConfig> groupConfigList = subscriptionGroupList.getGroupConfigList();

        final StringBuilder builder = new StringBuilder();
        for (SubscriptionGroupConfig config : groupConfigList) {
            builder.append(config.getGroupName()).append(";");
        }
        final String groupNames = builder.toString();
        LOGGER.info("AdminBrokerProcessor#updateAndCreateSubscriptionGroupList: groupNames: {}, called by {}",
            groupNames,
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        try {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfigList(groupConfigList);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } finally {
            long executionTime = (System.nanoTime() - startTime) / 1000000L;
            LOGGER.info("executionTime of create updateAndCreateSubscriptionGroupList: {} is {} ms", groupNames, executionTime);
            InvocationStatus status = response.getCode() == ResponseCode.SUCCESS ?
                InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
            Attributes attributes = BrokerMetricsManager.newAttributesBuilder()
                .put(LABEL_INVOCATION_STATUS, status.getName())
                .build();
            BrokerMetricsManager.consumerGroupCreateExecuteTime.record(executionTime, attributes);
        }

        return response;
    }


    private void initConsumerOffset(String clientHost, String groupName, int mode, TopicConfig topicConfig) {
        String topic = topicConfig.getTopicName();
        for (int queueId = 0; queueId < topicConfig.getReadQueueNums(); queueId++) {
            if (this.brokerController.getConsumerOffsetManager().queryOffset(groupName, topic, queueId) > -1) {
                continue;
            }
            long offset = 0;
            if (this.brokerController.getMessageStore().getConsumeQueue(topic, queueId) != null) {
                if (ConsumeInitMode.MAX == mode) {
                    offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                } else if (ConsumeInitMode.MIN == mode) {
                    offset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
                }
            }
            this.brokerController.getConsumerOffsetManager().commitOffset(clientHost, groupName, topic, queueId, offset);
            LOGGER.info("AdminBrokerProcessor#initConsumerOffset: consumerGroup={}, topic={}, queueId={}, offset={}",
                groupName, topic, queueId, offset);
        }
    }

    private RemotingCommand getAllSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        String content = this.brokerController.getSubscriptionGroupManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("UnsupportedEncodingException getAllSubscriptionGroup", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e.getMessage());
                return response;
            }
        } else {
            LOGGER.error("No subscription group in this broker, client:{} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand deleteSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteSubscriptionGroupRequestHeader requestHeader =
            (DeleteSubscriptionGroupRequestHeader) request.decodeCommandCustomHeader(DeleteSubscriptionGroupRequestHeader.class);

        LOGGER.info("AdminBrokerProcessor#deleteSubscriptionGroup, caller={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(requestHeader.getGroupName());

        if (requestHeader.isCleanOffset()) {
            this.brokerController.getConsumerOffsetManager().removeOffset(requestHeader.getGroupName());
            this.brokerController.getPopInflightMessageCounter().clearInFlightMessageNumByGroupName(requestHeader.getGroupName());
        }

        if (this.brokerController.getBrokerConfig().isAutoDeleteUnusedStats()) {
            this.brokerController.getBrokerStatsManager().onGroupDeleted(requestHeader.getGroupName());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getTopicStatsInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicStatsInfoRequestHeader requestHeader =
            (GetTopicStatsInfoRequestHeader) request.decodeCommandCustomHeader(GetTopicStatsInfoRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        TopicStatsTable topicStatsTable = new TopicStatsTable();

        int maxQueueNums = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        for (int i = 0; i < maxQueueNums; i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, i);
            if (min < 0) {
                min = 0;
            }

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            if (max < 0) {
                max = 0;
            }

            long timestamp = 0;
            if (max > 0) {
                timestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
            (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }

    private RemotingCommand getAllProducerInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetAllProducerInfoRequestHeader requestHeader =
            (GetAllProducerInfoRequestHeader) request.decodeCommandCustomHeader(GetAllProducerInfoRequestHeader.class);

        ProducerTableInfo producerTable = this.brokerController.getProducerManager().getProducerTable();
        if (producerTable != null) {
            byte[] body = producerTable.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        return response;
    }

    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
            (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        ProducerConnection bodydata = new ProducerConnection();
        Map<Channel, ClientChannelInfo> channelInfoHashMap =
            this.brokerController.getProducerManager().getGroupChannelTable().get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    private RemotingCommand getConsumeStats(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumeStatsRequestHeader requestHeader =
            (GetConsumeStatsRequestHeader) request.decodeCommandCustomHeader(GetConsumeStatsRequestHeader.class);

        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics = new HashSet<>();
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getConsumerGroup());
        } else {
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                LOGGER.warn("AdminBrokerProcessor#getConsumeStats: topic config does not exist, topic={}", topic);
                continue;
            }

            TopicQueueMappingDetail mappingDetail = this.brokerController.getTopicQueueMappingManager().getTopicQueueMapping(topic);

            {
                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getConsumerGroup(), topic);

                if (null == findSubscriptionData
                    && this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getConsumerGroup()) > 0) {
                    LOGGER.warn(
                        "AdminBrokerProcessor#getConsumeStats: topic does not exist in consumer group's subscription, "
                            + "topic={}, consumer group={}", topic, requestHeader.getConsumerGroup());
                    continue;
                }
            }

            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                if (brokerOffset < 0) {
                    brokerOffset = 0;
                }

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                    requestHeader.getConsumerGroup(), topic, i);

                // the consumerOffset cannot be zero for static topic because of the "double read check" strategy
                // just remain the logic for dynamic topic
                // maybe we should remove it in the future
                if (mappingDetail == null) {
                    if (consumerOffset < 0) {
                        consumerOffset = 0;
                    }
                }

                long pullOffset = this.brokerController.getConsumerOffsetManager().queryPullOffset(
                    requestHeader.getConsumerGroup(), topic, i);

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);
                offsetWrapper.setPullOffset(Math.max(consumerOffset, pullOffset));

                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(requestHeader.getConsumerGroup(), topic);

            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getAllConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.brokerController.getConsumerOffsetManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("get all consumer offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e.getMessage());
                return response;
            }
        } else {
            LOGGER.error("No consumer offset in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No consumer offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getAllDelayOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.brokerController.getScheduleMessageService().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("AdminBrokerProcessor#getAllDelayOffset: unexpected error, caller={}.",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            LOGGER.error("AdminBrokerProcessor#getAllDelayOffset: no delay offset in this broker, caller={}",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No delay offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getAllMessageRequestMode(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.brokerController.getQueryAssignmentProcessor().getMessageRequestModeManager().encode();
        if (content != null && !content.isEmpty()) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("get all message request mode from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            LOGGER.error("No message request mode in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No message request mode in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
            (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        LOGGER.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce());

        if (this.brokerController.getBrokerConfig().isUseServerSideResetOffset()) {
            String topic = requestHeader.getTopic();
            String group = requestHeader.getGroup();
            int queueId = requestHeader.getQueueId();
            long timestamp = requestHeader.getTimestamp();
            Long offset = requestHeader.getOffset();
            return resetOffsetInner(topic, group, queueId, timestamp, offset);
        }

        boolean isC = false;
        LanguageCode language = request.getLanguage();
        switch (language) {
            case CPP:
                isC = true;
                break;
        }
        return this.brokerController.getBroker2Client().resetOffset(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce(), isC);
    }

    private Long searchOffsetByTimestamp(String topic, int queueId, long timestamp) {
        if (timestamp < 0) {
            return brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
        } else {
            return brokerController.getMessageStore().getOffsetInQueueByTime(topic, queueId, timestamp);
        }
    }

    /**
     * Reset consumer offset.
     *
     * @param topic     Required, not null.
     * @param group     Required, not null.
     * @param queueId   if target queue ID is negative, all message queues will be reset; otherwise, only the target queue
     *                  would get reset.
     * @param timestamp if timestamp is negative, offset would be reset to broker offset at the time being; otherwise,
     *                  binary search is performed to locate target offset.
     * @param offset    Target offset to reset to if target queue ID is properly provided.
     * @return Affected queues and their new offset
     */
    private RemotingCommand resetOffsetInner(String topic, String group, int queueId, long timestamp, Long offset) {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);

        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Can not reset offset in slave broker");
            return response;
        }

        Map<Integer, Long> queueOffsetMap = new HashMap<>();

        // Reset offset for all queues belonging to the specified topic
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("Topic " + topic + " does not exist");
            LOGGER.warn("Reset offset failed, topic does not exist. topic={}, group={}", topic, group);
            return response;
        }

        if (!brokerController.getSubscriptionGroupManager().containsSubscriptionGroup(group)) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("Group " + group + " does not exist");
            LOGGER.warn("Reset offset failed, group does not exist. topic={}, group={}", topic, group);
            return response;
        }

        if (queueId >= 0) {
            if (null != offset && -1 != offset) {
                long min = brokerController.getMessageStore().getMinOffsetInQueue(topic, queueId);
                long max = brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                if (min >= 0 && offset < min || offset > max + 1) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(
                        String.format("Target offset %d not in consume queue range [%d-%d]", offset, min, max));
                    return response;
                }
            } else {
                offset = searchOffsetByTimestamp(topic, queueId, timestamp);
            }
            queueOffsetMap.put(queueId, offset);
        } else {
            for (int index = 0; index < topicConfig.getReadQueueNums(); index++) {
                offset = searchOffsetByTimestamp(topic, index, timestamp);
                queueOffsetMap.put(index, offset);
            }
        }

        if (queueOffsetMap.isEmpty()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No queues to reset.");
            LOGGER.warn("Reset offset aborted: no queues to reset");
            return response;
        }

        for (Map.Entry<Integer, Long> entry : queueOffsetMap.entrySet()) {
            brokerController.getConsumerOffsetManager()
                .assignResetOffset(topic, group, entry.getKey(), entry.getValue());
        }

        // Prepare reset result.
        ResetOffsetBody body = new ResetOffsetBody();
        String brokerName = brokerController.getBrokerConfig().getBrokerName();
        for (Map.Entry<Integer, Long> entry : queueOffsetMap.entrySet()) {
            brokerController.getPopInflightMessageCounter().clearInFlightMessageNum(topic, group, entry.getKey());
            body.getOffsetTable().put(new MessageQueue(topic, brokerName, entry.getKey()), entry.getValue());
        }

        LOGGER.info("Reset offset, topic={}, group={}, queues={}", topic, group, body.toJson(false));
        response.setBody(body.encode());
        return response;
    }

    public RemotingCommand getConsumerStatus(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerStatusRequestHeader requestHeader =
            (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        LOGGER.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup());

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getClientAddr());
    }

    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryTopicConsumeByWhoRequestHeader requestHeader =
            (QueryTopicConsumeByWhoRequestHeader) request.decodeCommandCustomHeader(QueryTopicConsumeByWhoRequestHeader.class);

        HashSet<String> groups = this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());

        Set<String> groupInOffset = this.brokerController.getConsumerOffsetManager().whichGroupByTopic(requestHeader.getTopic());
        if (groupInOffset != null && !groupInOffset.isEmpty()) {
            groups.addAll(groupInOffset);
        }

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryTopicsByConsumer(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryTopicsByConsumerRequestHeader requestHeader =
            (QueryTopicsByConsumerRequestHeader) request.decodeCommandCustomHeader(QueryTopicsByConsumerRequestHeader.class);

        Set<String> topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getGroup());

        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);
        topicList.setBrokerAddr(brokerController.getBrokerAddr());
        byte[] body = topicList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand querySubscriptionByConsumer(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QuerySubscriptionByConsumerRequestHeader requestHeader =
            (QuerySubscriptionByConsumerRequestHeader) request.decodeCommandCustomHeader(QuerySubscriptionByConsumerRequestHeader.class);

        SubscriptionData subscriptionData = this.brokerController.getConsumerManager()
            .findSubscriptionData(requestHeader.getGroup(), requestHeader.getTopic());

        QuerySubscriptionResponseBody responseBody = new QuerySubscriptionResponseBody();
        responseBody.setGroup(requestHeader.getGroup());
        responseBody.setTopic(requestHeader.getTopic());
        responseBody.setSubscriptionData(subscriptionData);
        byte[] body = responseBody.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;

    }

    private RemotingCommand queryConsumeTimeSpan(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryConsumeTimeSpanRequestHeader requestHeader =
            (QueryConsumeTimeSpanRequestHeader) request.decodeCommandCustomHeader(QueryConsumeTimeSpanRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        List<QueueTimeSpan> timeSpanSet = new ArrayList<>();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            QueueTimeSpan timeSpan = new QueueTimeSpan();
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            timeSpan.setMessageQueue(mq);

            long minTime = this.brokerController.getMessageStore().getEarliestMessageTime(topic, i);
            timeSpan.setMinTimeStamp(minTime);

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            long maxTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            timeSpan.setMaxTimeStamp(maxTime);

            long consumeTime;
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getGroup(), topic, i);
            if (consumerOffset > 0) {
                consumeTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset - 1);
            } else {
                consumeTime = minTime;
            }
            timeSpan.setConsumeTimeStamp(consumeTime);

            long maxBrokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), i);
            if (consumerOffset < maxBrokerOffset) {
                long nextTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset);
                timeSpan.setDelayTime(System.currentTimeMillis() - nextTime);
            }
            timeSpanSet.add(timeSpan);
        }

        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(timeSpanSet);
        response.setBody(queryConsumeTimeSpanBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getSystemTopicListFromBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        Set<String> topics = TopicValidator.getSystemTopicSet();
        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);
        response.setBody(topicList.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanExpiredConsumeQueue() {
        LOGGER.info("AdminBrokerProcessor#cleanExpiredConsumeQueue: start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        try {
            brokerController.getMessageStore().cleanExpiredConsumerQueue();
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
        LOGGER.info("AdminBrokerProcessor#cleanExpiredConsumeQueue: end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand deleteExpiredCommitLog() {
        LOGGER.warn("invoke deleteExpiredCommitLog start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        brokerController.getMessageStore().executeDeleteFilesManually();
        LOGGER.warn("invoke deleteExpiredCommitLog end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand cleanUnusedTopic() {
        LOGGER.warn("invoke cleanUnusedTopic start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        brokerController.getMessageStore().cleanUnusedTopic(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());
        LOGGER.warn("invoke cleanUnusedTopic end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
            (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getConsumerGroup(),
            requestHeader.getClientId());
    }

    private RemotingCommand queryCorrectionOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryCorrectionOffsetHeader requestHeader =
            (QueryCorrectionOffsetHeader) request.decodeCommandCustomHeader(QueryCorrectionOffsetHeader.class);

        Map<Integer, Long> correctionOffset = this.brokerController.getConsumerOffsetManager()
            .queryMinOffsetInAllGroup(requestHeader.getTopic(), requestHeader.getFilterGroups());

        Map<Integer, Long> compareOffset =
            this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getCompareGroup(), requestHeader.getTopic());

        if (compareOffset != null && !compareOffset.isEmpty()) {
            for (Map.Entry<Integer, Long> entry : compareOffset.entrySet()) {
                Integer queueId = entry.getKey();
                correctionOffset.put(queueId,
                    correctionOffset.get(queueId) > entry.getValue() ? Long.MAX_VALUE : correctionOffset.get(queueId));
            }
        }

        QueryCorrectionOffsetBody body = new QueryCorrectionOffsetBody();
        body.setCorrectionOffsets(correctionOffset);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader) request
            .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        // brokerName
        request.getExtFields().put("brokerName", this.brokerController.getBrokerConfig().getBrokerName());
        // topicSysFlag
        if (StringUtils.isNotEmpty(requestHeader.getTopic())) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().getTopicConfigTable().get(requestHeader.getTopic());
            if (topicConfig != null) {
                request.addExtField("topicSysFlag", String.valueOf(topicConfig.getTopicSysFlag()));
            }
        }
        // groupSysFlag
        if (StringUtils.isNotEmpty(requestHeader.getConsumerGroup())) {
            SubscriptionGroupConfig groupConfig = brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(requestHeader.getConsumerGroup());
            if (groupConfig != null) {
                request.addExtField("groupSysFlag", String.valueOf(groupConfig.getGroupSysFlag()));
            }
        }
        SelectMappedBufferResult selectMappedBufferResult = null;
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(requestHeader.getMsgId());
            selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(messageId.getOffset());

            byte[] body = new byte[selectMappedBufferResult.getSize()];
            selectMappedBufferResult.getByteBuffer().get(body);
            request.setBody(body);
        } catch (UnknownHostException e) {
        } finally {
            if (selectMappedBufferResult != null) {
                selectMappedBufferResult.release();
            }
        }

        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.getConsumerGroup(),
            requestHeader.getClientId());
    }

    private RemotingCommand cloneGroupOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        CloneGroupOffsetRequestHeader requestHeader =
            (CloneGroupOffsetRequestHeader) request.decodeCommandCustomHeader(CloneGroupOffsetRequestHeader.class);

        Set<String> topics;
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getSrcGroup());
        } else {
            topics = new HashSet<>();
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                LOGGER.warn("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            if (!requestHeader.isOffline()) {

                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getSrcGroup(), topic);
                if (this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getSrcGroup()) > 0
                    && findSubscriptionData == null) {
                    LOGGER.warn(
                        "AdminBrokerProcessor#cloneGroupOffset: topic does not exist in consumer group's "
                            + "subscription, topic={}, consumer group={}", topic, requestHeader.getSrcGroup());
                    continue;
                }
            }

            this.brokerController.getConsumerOffsetManager().cloneOffset(requestHeader.getSrcGroup(), requestHeader.getDestGroup(),
                requestHeader.getTopic());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand ViewBrokerStatsData(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ViewBrokerStatsDataRequestHeader requestHeader =
            (ViewBrokerStatsDataRequestHeader) request.decodeCommandCustomHeader(ViewBrokerStatsDataRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        MessageStore messageStore = this.brokerController.getMessageStore();

        StatsItem statsItem = messageStore.getBrokerStatsManager().getStatsItem(requestHeader.getStatsName(), requestHeader.getStatsKey());
        if (null == statsItem) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The stats <%s> <%s> not exist", requestHeader.getStatsName(), requestHeader.getStatsKey()));
            return response;
        }

        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInMinute();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsMinute(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInHour();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsHour(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInDay();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsDay(it);
        }

        response.setBody(brokerStatsData.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand fetchAllConsumeStatsInBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        GetConsumeStatsInBrokerHeader requestHeader =
            (GetConsumeStatsInBrokerHeader) request.decodeCommandCustomHeader(GetConsumeStatsInBrokerHeader.class);
        boolean isOrder = requestHeader.isOrder();
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroups =
            brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();

        List<Map<String/* subscriptionGroupName */, List<ConsumeStats>>> brokerConsumeStatsList =
            new ArrayList<>();

        long totalDiff = 0L;
        long totalInflightDiff = 0L;
        for (String group : subscriptionGroups.keySet()) {
            Map<String, List<ConsumeStats>> subscripTopicConsumeMap = new HashMap<>();
            Set<String> topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(group);
            List<ConsumeStats> consumeStatsList = new ArrayList<>();
            for (String topic : topics) {
                ConsumeStats consumeStats = new ConsumeStats();
                TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
                if (null == topicConfig) {
                    LOGGER.warn(
                        "AdminBrokerProcessor#fetchAllConsumeStatsInBroker: topic config does not exist, topic={}",
                        topic);
                    continue;
                }

                if (isOrder && !topicConfig.isOrder()) {
                    continue;
                }

                {
                    SubscriptionData findSubscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(group, topic);

                    if (null == findSubscriptionData
                        && this.brokerController.getConsumerManager().findSubscriptionDataCount(group) > 0) {
                        LOGGER.warn(
                            "AdminBrokerProcessor#fetchAllConsumeStatsInBroker: topic does not exist in consumer "
                                + "group's subscription, topic={}, consumer group={}", topic, group);
                        continue;
                    }
                }

                for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(topic);
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                    mq.setQueueId(i);
                    OffsetWrapper offsetWrapper = new OffsetWrapper();
                    long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                    if (brokerOffset < 0) {
                        brokerOffset = 0;
                    }
                    long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                        group,
                        topic,
                        i);
                    if (consumerOffset < 0)
                        consumerOffset = 0;

                    offsetWrapper.setBrokerOffset(brokerOffset);
                    offsetWrapper.setConsumerOffset(consumerOffset);

                    long timeOffset = consumerOffset - 1;
                    if (timeOffset >= 0) {
                        long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                        if (lastTimestamp > 0) {
                            offsetWrapper.setLastTimestamp(lastTimestamp);
                        }
                    }
                    consumeStats.getOffsetTable().put(mq, offsetWrapper);
                }
                double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(group, topic);
                consumeTps += consumeStats.getConsumeTps();
                consumeStats.setConsumeTps(consumeTps);
                totalDiff += consumeStats.computeTotalDiff();
                totalInflightDiff += consumeStats.computeInflightTotalDiff();
                consumeStatsList.add(consumeStats);
            }
            subscripTopicConsumeMap.put(group, consumeStatsList);
            brokerConsumeStatsList.add(subscripTopicConsumeMap);
        }
        ConsumeStatsList consumeStats = new ConsumeStatsList();
        consumeStats.setBrokerAddr(brokerController.getBrokerAddr());
        consumeStats.setConsumeStatsList(brokerConsumeStatsList);
        consumeStats.setTotalDiff(totalDiff);
        consumeStats.setTotalInflightDiff(totalInflightDiff);
        response.setBody(consumeStats.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerController.getBrokerAttachedPlugins()) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.buildRuntimeInfo(runtimeInfo);
            }
        }

        this.brokerController.getScheduleMessageService().buildRunningStats(runtimeInfo);
        runtimeInfo.put("brokerActive", String.valueOf(this.brokerController.isSpecialServiceRunning()));
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CURRENT_VERSION));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("dispatchBehindBytes", String.valueOf(this.brokerController.getMessageStore().dispatchBehindBytes()));
        runtimeInfo.put("pageCacheLockTimeMills", String.valueOf(this.brokerController.getMessageStore().lockTimeMills()));

        runtimeInfo.put("earliestMessageTimeStamp", String.valueOf(this.brokerController.getMessageStore().getEarliestMessageTime()));
        runtimeInfo.put("startAcceptSendRequestTimeStamp", String.valueOf(this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp()));

        if (this.brokerController.getMessageStoreConfig().isTimerWheelEnable()) {
            runtimeInfo.put("timerReadBehind", String.valueOf(this.brokerController.getMessageStore().getTimerMessageStore().getDequeueBehind()));
            runtimeInfo.put("timerOffsetBehind", String.valueOf(this.brokerController.getMessageStore().getTimerMessageStore().getEnqueueBehindMessages()));
            runtimeInfo.put("timerCongestNum", String.valueOf(this.brokerController.getMessageStore().getTimerMessageStore().getAllCongestNum()));
            runtimeInfo.put("timerEnqueueTps", String.valueOf(this.brokerController.getMessageStore().getTimerMessageStore().getEnqueueTps()));
            runtimeInfo.put("timerDequeueTps", String.valueOf(this.brokerController.getMessageStore().getTimerMessageStore().getDequeueTps()));
        } else {
            runtimeInfo.put("timerReadBehind", "0");
            runtimeInfo.put("timerOffsetBehind", "0");
            runtimeInfo.put("timerCongestNum", "0");
            runtimeInfo.put("timerEnqueueTps", "0.0");
            runtimeInfo.put("timerDequeueTps", "0.0");
        }
        MessageStore messageStore = this.brokerController.getMessageStore();
        runtimeInfo.put("remainTransientStoreBufferNumbs", String.valueOf(messageStore.remainTransientStoreBufferNumbs()));
        if (this.brokerController.getMessageStore() instanceof DefaultMessageStore && ((DefaultMessageStore) this.brokerController.getMessageStore()).isTransientStorePoolEnable()) {
            runtimeInfo.put("remainHowManyDataToCommit", MixAll.humanReadableByteCount(messageStore.remainHowManyDataToCommit(), false));
        }
        runtimeInfo.put("remainHowManyDataToFlush", MixAll.humanReadableByteCount(messageStore.remainHowManyDataToFlush(), false));

        java.io.File commitLogDir = new java.io.File(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        if (commitLogDir.exists()) {
            runtimeInfo.put("commitLogDirCapacity", String.format("Total : %s, Free : %s.", MixAll.humanReadableByteCount(commitLogDir.getTotalSpace(), false), MixAll.humanReadableByteCount(commitLogDir.getFreeSpace(), false)));
        }

        runtimeInfo.put("sendThreadPoolQueueSize", String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));
        runtimeInfo.put("sendThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        runtimeInfo.put("pullThreadPoolQueueSize", String.valueOf(this.brokerController.getPullThreadPoolQueue().size()));
        runtimeInfo.put("pullThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getPullThreadPoolQueueCapacity()));

        runtimeInfo.put("litePullThreadPoolQueueSize", String.valueOf(brokerController.getLitePullThreadPoolQueue().size()));
        runtimeInfo.put("litePullThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getLitePullThreadPoolQueueCapacity()));

        runtimeInfo.put("queryThreadPoolQueueSize", String.valueOf(this.brokerController.getQueryThreadPoolQueue().size()));
        runtimeInfo.put("queryThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getQueryThreadPoolQueueCapacity()));

        runtimeInfo.put("sendThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4SendThreadPoolQueue()));
        runtimeInfo.put("pullThreadPoolQueueHeadWaitTimeMills", String.valueOf(brokerController.headSlowTimeMills4PullThreadPoolQueue()));
        runtimeInfo.put("queryThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4QueryThreadPoolQueue()));
        runtimeInfo.put("litePullThreadPoolQueueHeadWaitTimeMills", String.valueOf(brokerController.headSlowTimeMills4LitePullThreadPoolQueue()));

        runtimeInfo.put("EndTransactionQueueSize", String.valueOf(this.brokerController.getEndTransactionThreadPoolQueue().size()));
        runtimeInfo.put("EndTransactionThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getEndTransactionPoolQueueCapacity()));

        return runtimeInfo;
    }

    private RemotingCommand callConsumer(
        final int requestCode,
        final RemotingCommand request,
        final String consumerGroup,
        final String clientId) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ClientChannelInfo clientChannelInfo = this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        if (null == clientChannelInfo) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return response;
        }

        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                clientId,
                MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return response;
        }

        try {
            RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, null);
            newRequest.setExtFields(request.getExtFields());
            newRequest.setBody(request.getBody());

            return this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(), newRequest);
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                .setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId, UtilAll.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(
                String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup, clientId, UtilAll.exceptionSimpleDesc(e)));
            return response;
        }
    }

    private RemotingCommand queryConsumeQueue(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        QueryConsumeQueueRequestHeader requestHeader =
            (QueryConsumeQueueRequestHeader) request.decodeCommandCustomHeader(QueryConsumeQueueRequestHeader.class);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ConsumeQueueInterface consumeQueue = this.brokerController.getMessageStore().getConsumeQueue(requestHeader.getTopic(),
            requestHeader.getQueueId());
        if (consumeQueue == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("%d@%s is not exist!", requestHeader.getQueueId(), requestHeader.getTopic()));
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);

        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();
        body.setMaxQueueIndex(consumeQueue.getMaxOffsetInQueue());
        body.setMinQueueIndex(consumeQueue.getMinOffsetInQueue());

        MessageFilter messageFilter = null;
        if (requestHeader.getConsumerGroup() != null) {
            SubscriptionData subscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(
                requestHeader.getConsumerGroup(), requestHeader.getTopic()
            );
            body.setSubscriptionData(subscriptionData);
            if (subscriptionData == null) {
                body.setFilterData(String.format("%s@%s is not online!", requestHeader.getConsumerGroup(), requestHeader.getTopic()));
            } else {
                ConsumerFilterData filterData = this.brokerController.getConsumerFilterManager()
                    .get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                body.setFilterData(JSON.toJSONString(filterData, true));

                messageFilter = new ExpressionMessageFilter(subscriptionData, filterData,
                    this.brokerController.getConsumerFilterManager());
            }
        }

        ReferredIterator<CqUnit> result = consumeQueue.iterateFrom(requestHeader.getIndex());
        if (result == null) {
            response.setRemark(String.format("Index %d of %d@%s is not exist!", requestHeader.getIndex(), requestHeader.getQueueId(), requestHeader.getTopic()));
            return response;
        }
        try {
            List<ConsumeQueueData> queues = new ArrayList<>();
            while (result.hasNext()) {
                CqUnit cqUnit = result.next();
                if (cqUnit.getQueueOffset() - requestHeader.getIndex() >= requestHeader.getCount()) {
                    break;
                }

                ConsumeQueueData one = new ConsumeQueueData();
                one.setPhysicOffset(cqUnit.getPos());
                one.setPhysicSize(cqUnit.getSize());
                one.setTagsCode(cqUnit.getTagsCode());

                if (cqUnit.getCqExtUnit() == null && cqUnit.isTagsCodeValid()) {
                    queues.add(one);
                    continue;
                }

                if (cqUnit.getCqExtUnit() != null) {
                    ConsumeQueueExt.CqExtUnit cqExtUnit = cqUnit.getCqExtUnit();
                    one.setExtendDataJson(JSON.toJSONString(cqExtUnit));
                    if (cqExtUnit.getFilterBitMap() != null) {
                        one.setBitMap(BitsArray.create(cqExtUnit.getFilterBitMap()).toString());
                    }
                    if (messageFilter != null) {
                        one.setEval(messageFilter.isMatchedByConsumeQueue(cqExtUnit.getTagsCode(), cqExtUnit));
                    }
                } else {
                    one.setMsg("Cq extend not exist!addr: " + one.getTagsCode());
                }

                queues.add(one);
            }
            body.setQueueData(queues);
        } finally {
            result.release();
        }
        response.setBody(body.encode());
        return response;
    }

    private RemotingCommand resumeCheckHalfMessage(ChannelHandlerContext ctx,
        RemotingCommand request)
        throws RemotingCommandException {
        final ResumeCheckHalfMessageRequestHeader requestHeader = (ResumeCheckHalfMessageRequestHeader) request
            .decodeCommandCustomHeader(ResumeCheckHalfMessageRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        SelectMappedBufferResult selectMappedBufferResult = null;
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(requestHeader.getMsgId());
            selectMappedBufferResult = this.brokerController.getMessageStore()
                .selectOneMessageByOffset(messageId.getOffset());
            MessageExt msg = MessageDecoder.decode(selectMappedBufferResult.getByteBuffer());
            msg.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(0));
            PutMessageResult putMessageResult = this.brokerController.getMessageStore()
                .putMessage(toMessageExtBrokerInner(msg));
            if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                LOGGER.info(
                    "Put message back to RMQ_SYS_TRANS_HALF_TOPIC. real topic={}",
                    msg.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            } else {
                LOGGER.error("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed.");
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed.");
            }
        } catch (Exception e) {
            LOGGER.error("Exception was thrown when putting message back to RMQ_SYS_TRANS_HALF_TOPIC.");
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Exception was thrown when putting message back to RMQ_SYS_TRANS_HALF_TOPIC.");
        } finally {
            if (selectMappedBufferResult != null) {
                selectMappedBufferResult.release();
            }
        }
        return response;
    }

    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(0);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }

    private RemotingCommand getTopicConfig(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        GetTopicConfigRequestHeader requestHeader = (GetTopicConfigRequestHeader) request.decodeCommandCustomHeader(GetTopicConfigRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().getTopicConfigTable().get(requestHeader.getTopic());
        if (topicConfig == null) {
            LOGGER.error("No topic in this broker, client: {} topic: {}", ctx.channel().remoteAddress(), requestHeader.getTopic());
            //be care of the response code, should set "not-exist" explicitly
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("No topic in this broker. topic: " + requestHeader.getTopic());
            return response;
        }
        TopicQueueMappingDetail topicQueueMappingDetail = null;
        if (Boolean.TRUE.equals(requestHeader.getLo())) {
            topicQueueMappingDetail = this.brokerController.getTopicQueueMappingManager().getTopicQueueMapping(requestHeader.getTopic());
        }
        String content = JSONObject.toJSONString(new TopicConfigAndQueueMapping(topicConfig, topicQueueMappingDetail));
        try {
            response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("UnsupportedEncodingException getTopicConfig: topic=" + topicConfig.getTopicName(), e);

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("UnsupportedEncodingException " + e.getMessage());
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand notifyMinBrokerIdChange(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        NotifyMinBrokerIdChangeRequestHeader requestHeader = (NotifyMinBrokerIdChangeRequestHeader) request.decodeCommandCustomHeader(NotifyMinBrokerIdChangeRequestHeader.class);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        LOGGER.warn("min broker id changed, prev {}, new {}", this.brokerController.getMinBrokerIdInGroup(), requestHeader.getMinBrokerId());

        this.brokerController.updateMinBroker(requestHeader.getMinBrokerId(), requestHeader.getMinBrokerAddr(),
            requestHeader.getOfflineBrokerAddr(),
            requestHeader.getHaBrokerAddr());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand updateBrokerHaInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(ExchangeHAInfoResponseHeader.class);

        ExchangeHAInfoRequestHeader requestHeader = (ExchangeHAInfoRequestHeader) request.decodeCommandCustomHeader(ExchangeHAInfoRequestHeader.class);
        if (requestHeader.getMasterHaAddress() != null) {
            this.brokerController.getMessageStore().updateHaMasterAddress(requestHeader.getMasterHaAddress());
            this.brokerController.getMessageStore().updateMasterAddress(requestHeader.getMasterAddress());
            if (this.brokerController.getMessageStore().getMasterFlushedOffset() == 0
                && this.brokerController.getMessageStoreConfig().isSyncMasterFlushOffsetWhenStartup()) {
                LOGGER.info("Set master flush offset in slave to {}", requestHeader.getMasterFlushOffset());
                this.brokerController.getMessageStore().setMasterFlushedOffset(requestHeader.getMasterFlushOffset());
            }
        } else if (this.brokerController.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID) {
            final ExchangeHAInfoResponseHeader responseHeader = (ExchangeHAInfoResponseHeader) response.readCustomHeader();
            responseHeader.setMasterHaAddress(this.brokerController.getHAServerAddr());
            responseHeader.setMasterFlushOffset(this.brokerController.getMessageStore().getBrokerInitMaxOffset());
            responseHeader.setMasterAddress(this.brokerController.getBrokerAddr());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand getBrokerHaStatus(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HARuntimeInfo runtimeInfo = this.brokerController.getMessageStore().getHARuntimeInfo();

        if (runtimeInfo != null) {
            byte[] body = runtimeInfo.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Can not get HARuntimeInfo, may be duplicationEnable is true");
        }

        return response;
    }

    private RemotingCommand getBrokerEpochCache(ChannelHandlerContext ctx, RemotingCommand request) {
        final ReplicasManager replicasManager = this.brokerController.getReplicasManager();
        assert replicasManager != null;
        final BrokerConfig brokerConfig = this.brokerController.getBrokerConfig();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        if (!brokerConfig.isEnableControllerMode()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("this request only for controllerMode ");
            return response;
        }
        final EpochEntryCache entryCache = new EpochEntryCache(brokerConfig.getBrokerClusterName(),
            brokerConfig.getBrokerName(), brokerConfig.getBrokerId(), replicasManager.getEpochEntries(), this.brokerController.getMessageStore().getMaxPhyOffset());

        response.setBody(entryCache.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand resetMasterFlushOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        if (this.brokerController.getBrokerConfig().getBrokerId() != MixAll.MASTER_ID) {

            ResetMasterFlushOffsetHeader requestHeader = (ResetMasterFlushOffsetHeader) request.decodeCommandCustomHeader(ResetMasterFlushOffsetHeader.class);

            if (requestHeader.getMasterFlushOffset() != null) {
                this.brokerController.getMessageStore().setMasterFlushedOffset(requestHeader.getMasterFlushOffset());
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand notifyBrokerRoleChanged(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        NotifyBrokerRoleChangedRequestHeader requestHeader = (NotifyBrokerRoleChangedRequestHeader) request.decodeCommandCustomHeader(NotifyBrokerRoleChangedRequestHeader.class);
        SyncStateSet syncStateSetInfo = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        LOGGER.info("Receive notifyBrokerRoleChanged request, try to change brokerRole, request:{}", requestHeader);

        final ReplicasManager replicasManager = this.brokerController.getReplicasManager();
        if (replicasManager != null) {
            try {
                replicasManager.changeBrokerRole(requestHeader.getMasterBrokerId(), requestHeader.getMasterAddress(), requestHeader.getMasterEpoch(), requestHeader.getSyncStateSetEpoch(), syncStateSetInfo.getSyncStateSet());
            } catch (Exception e) {
                throw new RemotingCommandException(e.getMessage());
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    private RemotingCommand createUser(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CreateUserRequestHeader requestHeader = request.decodeCommandCustomHeader(CreateUserRequestHeader.class);
        if (StringUtils.isEmpty(requestHeader.getUsername())) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The username is blank");
            return response;
        }

        UserInfo userInfo = RemotingSerializable.decode(request.getBody(), UserInfo.class);
        userInfo.setUsername(requestHeader.getUsername());
        User user = UserConverter.convertUser(userInfo);

        if (user.getUserType() == UserType.SUPER && isNotSuperUserLogin(request)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The super user can only be create by super user");
            return response;
        }

        this.brokerController.getAuthenticationMetadataManager().createUser(user)
            .thenAccept(nil -> response.setCode(ResponseCode.SUCCESS))
            .exceptionally(ex -> {
                LOGGER.error("create user {} error", user.getUsername(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand updateUser(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        UpdateUserRequestHeader requestHeader = request.decodeCommandCustomHeader(UpdateUserRequestHeader.class);
        if (StringUtils.isEmpty(requestHeader.getUsername())) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The username is blank");
            return response;
        }

        UserInfo userInfo = RemotingSerializable.decode(request.getBody(), UserInfo.class);
        userInfo.setUsername(requestHeader.getUsername());
        User user = UserConverter.convertUser(userInfo);

        if (user.getUserType() == UserType.SUPER && isNotSuperUserLogin(request)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The super user can only be update by super user");
            return response;
        }

        this.brokerController.getAuthenticationMetadataManager().getUser(requestHeader.getUsername())
            .thenCompose(old -> {
                if (old == null) {
                    throw new AuthenticationException("The user is not exist");
                }
                if (old.getUserType() == UserType.SUPER && isNotSuperUserLogin(request)) {
                    throw new AuthenticationException("The super user can only be update by super user");
                }
                return this.brokerController.getAuthenticationMetadataManager().updateUser(old);
            }).thenAccept(nil -> response.setCode(ResponseCode.SUCCESS))
            .exceptionally(ex -> {
                LOGGER.error("update user {} error", requestHeader.getUsername(), ex);
                return handleAuthException(response, ex);
            })
            .join();
        return response;
    }

    private RemotingCommand deleteUser(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        DeleteUserRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteUserRequestHeader.class);

        this.brokerController.getAuthenticationMetadataManager().getUser(requestHeader.getUsername())
            .thenCompose(user -> {
                if (user == null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (user.getUserType() == UserType.SUPER && isNotSuperUserLogin(request)) {
                    throw new AuthenticationException("The super user can only be update by super user");
                }
                return this.brokerController.getAuthenticationMetadataManager().deleteUser(requestHeader.getUsername());
            }).thenAccept(nil -> response.setCode(ResponseCode.SUCCESS))
            .exceptionally(ex -> {
                LOGGER.error("delete user {} error", requestHeader.getUsername(), ex);
                return handleAuthException(response, ex);
            })
            .join();
        return response;
    }

    private RemotingCommand getUser(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        GetUserRequestHeader requestHeader = request.decodeCommandCustomHeader(GetUserRequestHeader.class);

        if (StringUtils.isBlank(requestHeader.getUsername())) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The username is blank");
            return response;
        }

        this.brokerController.getAuthenticationMetadataManager().getUser(requestHeader.getUsername())
            .thenAccept(user -> {
                response.setCode(ResponseCode.SUCCESS);
                if (user != null) {
                    UserInfo userInfo = UserConverter.convertUser(user);
                    response.setBody(JSON.toJSONString(userInfo).getBytes(StandardCharsets.UTF_8));
                }
            })
            .exceptionally(ex -> {
                LOGGER.error("get user {} error", requestHeader.getUsername(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand listUser(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ListUsersRequestHeader requestHeader = request.decodeCommandCustomHeader(ListUsersRequestHeader.class);

        this.brokerController.getAuthenticationMetadataManager().listUser(requestHeader.getFilter())
            .thenAccept(users -> {
                response.setCode(ResponseCode.SUCCESS);
                if (CollectionUtils.isNotEmpty(users)) {
                    List<UserInfo> userInfos = UserConverter.convertUsers(users);
                    response.setBody(JSON.toJSONString(userInfos).getBytes(StandardCharsets.UTF_8));
                }
            })
            .exceptionally(ex -> {
                LOGGER.error("list user by {} error", requestHeader.getFilter(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand createAcl(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CreateAclRequestHeader requestHeader = request.decodeCommandCustomHeader(CreateAclRequestHeader.class);
        Subject subject = Subject.of(requestHeader.getSubject());

        AclInfo aclInfo = RemotingSerializable.decode(request.getBody(), AclInfo.class);
        if (aclInfo == null || CollectionUtils.isEmpty(aclInfo.getPolicies())) {
            throw new AuthorizationException("The body of acl is null");
        }

        Acl acl = AclConverter.convertAcl(aclInfo);
        if (acl != null && acl.getSubject() == null) {
            acl.setSubject(subject);
        }

        this.brokerController.getAuthorizationMetadataManager().createAcl(acl)
            .thenAccept(nil -> response.setCode(ResponseCode.SUCCESS))
            .exceptionally(ex -> {
                LOGGER.error("create acl for {} error", requestHeader.getSubject(), ex);
                return handleAuthException(response, ex);
            })
            .join();
        return response;
    }

    private RemotingCommand updateAcl(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        UpdateAclRequestHeader requestHeader = request.decodeCommandCustomHeader(UpdateAclRequestHeader.class);
        Subject subject = Subject.of(requestHeader.getSubject());

        AclInfo aclInfo = RemotingSerializable.decode(request.getBody(), AclInfo.class);
        if (aclInfo == null || CollectionUtils.isEmpty(aclInfo.getPolicies())) {
            throw new AuthorizationException("The body of acl is null");
        }

        Acl acl = AclConverter.convertAcl(aclInfo);
        if (acl != null && acl.getSubject() == null) {
            acl.setSubject(subject);
        }

        this.brokerController.getAuthorizationMetadataManager().updateAcl(acl)
            .thenAccept(nil -> response.setCode(ResponseCode.SUCCESS))
            .exceptionally(ex -> {
                LOGGER.error("update acl for {} error", requestHeader.getSubject(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand deleteAcl(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        DeleteAclRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteAclRequestHeader.class);

        Subject subject = Subject.of(requestHeader.getSubject());

        PolicyType policyType = PolicyType.getByName(requestHeader.getPolicyType());

        Resource resource = Resource.of(requestHeader.getResource());

        this.brokerController.getAuthorizationMetadataManager().deleteAcl(subject, policyType, resource)
            .thenAccept(nil -> {
                response.setCode(ResponseCode.SUCCESS);
            })
            .exceptionally(ex -> {
                LOGGER.error("delete acl for {} error", requestHeader.getSubject(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand getAcl(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        GetAclRequestHeader requestHeader = request.decodeCommandCustomHeader(GetAclRequestHeader.class);

        Subject subject = Subject.of(requestHeader.getSubject());

        this.brokerController.getAuthorizationMetadataManager().getAcl(subject)
            .thenAccept(acl -> {
                response.setCode(ResponseCode.SUCCESS);
                if (acl != null) {
                    AclInfo aclInfo = AclConverter.convertAcl(acl);
                    String body = JSON.toJSONString(aclInfo);
                    response.setBody(body.getBytes(StandardCharsets.UTF_8));
                }
            })
            .exceptionally(ex -> {
                LOGGER.error("get acl for {} error", requestHeader.getSubject(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private RemotingCommand listAcl(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ListAclsRequestHeader requestHeader = request.decodeCommandCustomHeader(ListAclsRequestHeader.class);

        this.brokerController.getAuthorizationMetadataManager()
            .listAcl(requestHeader.getSubjectFilter(), requestHeader.getResourceFilter())
            .thenAccept(acls -> {
                response.setCode(ResponseCode.SUCCESS);
                if (CollectionUtils.isNotEmpty(acls)) {
                    List<AclInfo> aclInfos = AclConverter.convertAcls(acls);
                    String body = JSON.toJSONString(aclInfos);
                    response.setBody(body.getBytes(StandardCharsets.UTF_8));
                }
            })
            .exceptionally(ex -> {
                LOGGER.error("list acl error, subjectFilter:{}, resourceFilter:{}", requestHeader.getSubjectFilter(), requestHeader.getResourceFilter(), ex);
                return handleAuthException(response, ex);
            })
            .join();

        return response;
    }

    private boolean isNotSuperUserLogin(RemotingCommand request) {
        String accessKey = request.getExtFields().get("AccessKey");
        // if accessKey is null, it may be authentication is not enabled.
        if (StringUtils.isEmpty(accessKey)) {
            return false;
        }
        return !this.brokerController.getAuthenticationMetadataManager()
            .isSuperUser(accessKey).join();
    }

    private Void handleAuthException(RemotingCommand response, Throwable ex) {
        Throwable throwable = ExceptionUtils.getRealException(ex);
        if (throwable instanceof AuthenticationException || throwable instanceof AuthorizationException) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(throwable.getMessage());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("An system error occurred, please try again later.");
            LOGGER.error("An system error occurred when processing auth admin request.", ex);
        }
        return null;
    }

    private boolean validateSlave(RemotingCommand response) {
        if (this.brokerController.getMessageStoreConfig().getBrokerRole().equals(BrokerRole.SLAVE)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Can't modify topic or subscription group from slave broker, " +
                "please execute it from master broker.");
            return true;
        }
        return false;
    }

    private boolean validateBlackListConfigExist(Properties properties) {
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkCqUnitEqual(CqUnit cqUnit1, CqUnit cqUnit2) {
        if (cqUnit1.getQueueOffset() != cqUnit2.getQueueOffset()) {
            return false;
        }
        if (cqUnit1.getSize() != cqUnit2.getSize()) {
            return false;
        }
        if (cqUnit1.getPos() != cqUnit2.getPos()) {
            return false;
        }
        if (cqUnit1.getBatchNum() != cqUnit2.getBatchNum()) {
            return false;
        }
        return cqUnit1.getTagsCode() == cqUnit2.getTagsCode();
    }
}
