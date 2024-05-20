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
package org.apache.rocketmq.client.impl;

import com.alibaba.fastjson.JSON;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.OffsetNotFoundException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.rpchook.NamespaceRpcHook;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.constant.FIleReadaheadMode;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.namesrv.DefaultTopAddressing;
import org.apache.rocketmq.common.namesrv.NameServerUpdateCallback;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.HeartbeatV2Result;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.remoting.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.EpochEntryCache;
import org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.RatelimitInfo;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.AddBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateRatelimitRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAccessConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteRatelimitRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllProducerInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerAclConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
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
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.HeartbeatRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGlobalWhiteAddrsConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateRatelimitRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook;
import org.apache.rocketmq.remoting.rpchook.StreamTypeRPCHook;

import static org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode.SUCCESS;

public class MQClientAPIImpl implements NameServerUpdateCallback {
    private final static Logger log = LoggerFactory.getLogger(MQClientAPIImpl.class);
    private static boolean sendSmartMsg =
        Boolean.parseBoolean(System.getProperty("org.apache.rocketmq.client.sendSmartMsg", "true"));

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private String nameSrvAddr = null;
    private ClientConfig clientConfig;

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor,
        RPCHook rpcHook, final ClientConfig clientConfig) {
        this(nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig, null);
    }

    public MQClientAPIImpl(final NettyClientConfig nettyClientConfig,
        final ClientRemotingProcessor clientRemotingProcessor,
        RPCHook rpcHook, final ClientConfig clientConfig, final ChannelEventListener channelEventListener) {
        this.clientConfig = clientConfig;
        topAddressing = new DefaultTopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
        topAddressing.registerChangeCallBack(this);
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, channelEventListener);
        this.clientRemotingProcessor = clientRemotingProcessor;

        this.remotingClient.registerRPCHook(new NamespaceRpcHook(clientConfig));
        // Inject stream rpc hook first to make reserve field signature
        if (clientConfig.isEnableStreamRequestType()) {
            this.remotingClient.registerRPCHook(new StreamTypeRPCHook());
        }
        this.remotingClient.registerRPCHook(rpcHook);
        this.remotingClient.registerRPCHook(new DynamicalExtFieldRPCHook());
        this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);

        this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this.clientRemotingProcessor, null);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (!UtilAll.isBlank(addrs)) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    @Override
    public String onNameServerAddressChange(String namesrvAddress) {
        if (namesrvAddress != null) {
            if (!namesrvAddress.equals(this.nameSrvAddr)) {
                log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + namesrvAddress);
                this.updateNameServerAddressList(namesrvAddress);
                this.nameSrvAddr = namesrvAddress;
                return nameSrvAddr;
            }
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        this.remotingClient.updateNameServerAddressList(list);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public Set<MessageQueueAssignment> queryAssignment(final String addr, final String topic,
        final String consumerGroup, final String clientId, final String strategyName,
        final MessageModel messageModel, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        QueryAssignmentRequestBody requestBody = new QueryAssignmentRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(consumerGroup);
        requestBody.setClientId(clientId);
        requestBody.setMessageModel(messageModel);
        requestBody.setStrategyName(strategyName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_ASSIGNMENT, null);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryAssignmentResponseBody queryAssignmentResponseBody = QueryAssignmentResponseBody.decode(response.getBody(), QueryAssignmentResponseBody.class);
                return queryAssignmentResponseBody.getMessageQueueAssignments();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config,
        final long timeoutMillis) throws RemotingException, InterruptedException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }

    public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
        final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        Validators.checkTopicConfig(topicConfig);

        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        requestHeader.setAttributes(AttributeParser.parseToString(topicConfig.getAttributes()));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void createPlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig,
        final long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
        requestHeader.setAccessKey(plainAccessConfig.getAccessKey());
        requestHeader.setSecretKey(plainAccessConfig.getSecretKey());
        requestHeader.setAdmin(plainAccessConfig.isAdmin());
        requestHeader.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        requestHeader.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        requestHeader.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        requestHeader.setTopicPerms(UtilAll.join(plainAccessConfig.getTopicPerms(), ","));
        requestHeader.setGroupPerms(UtilAll.join(plainAccessConfig.getGroupPerms(), ","));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteAccessConfig(final String addr, final String accessKey, final long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
        requestHeader.setAccessKey(accessKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void updateGlobalWhiteAddrsConfig(final String addr, final String globalWhiteAddrs, String aclFileFullPath,
        final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrsConfigRequestHeader();
        requestHeader.setGlobalWhiteAddrs(globalWhiteAddrs);
        requestHeader.setAclFileFullPath(aclFileFullPath);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ClusterAclVersionInfo getBrokerClusterAclInfo(final String addr,
        final long timeoutMillis) throws RemotingCommandException, InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetBrokerAclConfigResponseHeader responseHeader =
                    (GetBrokerAclConfigResponseHeader) response.decodeCommandCustomHeader(GetBrokerAclConfigResponseHeader.class);

                ClusterAclVersionInfo clusterAclVersionInfo = new ClusterAclVersionInfo();
                clusterAclVersionInfo.setClusterName(responseHeader.getClusterName());
                clusterAclVersionInfo.setBrokerName(responseHeader.getBrokerName());
                clusterAclVersionInfo.setBrokerAddr(responseHeader.getBrokerAddr());
                clusterAclVersionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion.class));
                HashMap<String, Object> dataVersionMap = JSON.parseObject(responseHeader.getAllAclFileVersion(), HashMap.class);
                Map<String, DataVersion> allAclConfigDataVersion = new HashMap<>(dataVersionMap.size(), 1);
                for (Map.Entry<String, Object> entry : dataVersionMap.entrySet()) {
                    allAclConfigDataVersion.put(entry.getKey(), DataVersion.fromJson(JSON.toJSONString(entry.getValue()), DataVersion.class));
                }
                clusterAclVersionInfo.setAllAclConfigDataVersion(allAclConfigDataVersion);
                return clusterAclVersionInfo;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

    }

    public SendResult sendMessage(
        final String addr,
        final String brokerName,
        final Message msg,
        final SendMessageRequestHeader requestHeader,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final SendMessageContext context,
        final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
    }

    public SendResult sendMessage(
        final String addr,
        final String brokerName,
        final Message msg,
        final SendMessageRequestHeader requestHeader,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final MQClientInstance instance,
        final int retryTimesWhenSendFailed,
        final SendMessageContext context,
        final DefaultMQProducerImpl producer
    ) throws RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request = null;
        String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
        boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
        if (isReply) {
            if (sendSmartMsg) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
            }
        } else {
            if (sendSmartMsg || msg instanceof MessageBatch) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        request.setBody(msg.getBody());

        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();
                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                    retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    private SendResult sendMessageSync(
        final String addr,
        final String brokerName,
        final Message msg,
        final long timeoutMillis,
        final RemotingCommand request
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processSendResponse(brokerName, msg, response, addr);
    }

    void execRpcHooksAfterRequest(ResponseFuture responseFuture) {
        if (this.remotingClient instanceof NettyRemotingClient) {
            NettyRemotingClient remotingClient = (NettyRemotingClient) this.remotingClient;
            RemotingCommand response = responseFuture.getResponseCommand();
            remotingClient.doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel()), responseFuture.getRequestCommand(), response);
        }
    }

    private void sendMessageAsync(
        final String addr,
        final String brokerName,
        final Message msg,
        final long timeoutMillis,
        final RemotingCommand request,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final MQClientInstance instance,
        final int retryTimesWhenSendFailed,
        final AtomicInteger times,
        final SendMessageContext context,
        final DefaultMQProducerImpl producer
    ) {
        final long beginStartTime = System.currentTimeMillis();
        try {
            this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    long cost = System.currentTimeMillis() - beginStartTime;
                    if (null == sendCallback) {
                        try {
                            SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                            if (context != null && sendResult != null) {
                                context.setSendResult(sendResult);
                                context.getProducer().executeSendMessageHookAfter(context);
                            }
                        } catch (Throwable e) {
                        }

                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, false, true);
                        return;
                    }

                    try {
                        SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                        assert sendResult != null;
                        if (context != null) {
                            context.setSendResult(sendResult);
                            context.getProducer().executeSendMessageHookAfter(context);
                        }

                        try {
                            sendCallback.onSuccess(sendResult);
                        } catch (Throwable e) {
                        }

                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, false, true);
                    } catch (Exception e) {
                        producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, true, true);
                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, e, context, false, producer);
                    }
                }

                @Override
                public void operationFail(Throwable throwable) {
                    producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, true, true);
                    long cost = System.currentTimeMillis() - beginStartTime;
                    if (throwable instanceof RemotingSendRequestException) {
                        MQClientException ex = new MQClientException("send request failed", throwable);
                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                    } else if (throwable instanceof RemotingTimeoutException) {
                        MQClientException ex = new MQClientException("wait response timeout, cost=" + cost, throwable);
                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                    } else {
                        MQClientException ex = new MQClientException("unknow reseaon", throwable);
                        onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                    }
                }
            });
        } catch (Exception ex) {
            long cost = System.currentTimeMillis() - beginStartTime;
            producer.updateFaultItem(brokerName, cost, true, false);
            onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                retryTimesWhenSendFailed, times, ex, context, true, producer);
        }
    }

    private void onExceptionImpl(final String brokerName,
        final Message msg,
        final long timeoutMillis,
        final RemotingCommand request,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final MQClientInstance instance,
        final int timesTotal,
        final AtomicInteger curTimes,
        final Exception e,
        final SendMessageContext context,
        final boolean needRetry,
        final DefaultMQProducerImpl producer
    ) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            String retryBrokerName = brokerName;//by default, it will send to the same broker
            if (topicPublishInfo != null) { //select one message queue accordingly, in order to determine which broker to send
                MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName, false);
                retryBrokerName = instance.getBrokerNameFromMessageQueue(mqChosen);
            }
            String addr = instance.findBrokerAddressInPublish(retryBrokerName);
            log.warn("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}", tmp, msg.getTopic(), addr,
                retryBrokerName, e);
            request.setOpaque(RemotingCommand.createNewRequestId());
            sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                timesTotal, curTimes, context, producer);
        } else {

            if (context != null) {
                context.setException(e);
                context.getProducer().executeSendMessageHookAfter(context);
            }

            try {
                sendCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    protected SendResult processSendResponse(
        final String brokerName,
        final Message msg,
        final RemotingCommand response,
        final String addr
    ) throws MQBrokerException, RemotingCommandException {
        SendStatus sendStatus;
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            }
            case ResponseCode.SUCCESS: {
                sendStatus = SendStatus.SEND_OK;
                break;
            }
            default: {
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }
        }

        SendMessageResponseHeader responseHeader =
            (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

        //If namespace not null , reset Topic without namespace.
        String topic = msg.getTopic();
        if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
            topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
        }

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

        String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
        if (msg instanceof MessageBatch && responseHeader.getBatchUniqId() == null) {
            // This means it is not an inner batch
            StringBuilder sb = new StringBuilder();
            for (Message message : (MessageBatch) msg) {
                sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
            }
            uniqMsgId = sb.toString();
        }
        SendResult sendResult = new SendResult(sendStatus,
            uniqMsgId,
            responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
        sendResult.setTransactionId(responseHeader.getTransactionId());
        String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
        if (regionId == null || regionId.isEmpty()) {
            regionId = MixAll.DEFAULT_TRACE_REGION_ID;
        }
        sendResult.setRegionId(regionId);
        String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
        sendResult.setTraceOn(!Boolean.FALSE.toString().equals(traceOn));
        return sendResult;
    }

    public PullResult pullMessage(
        final String addr,
        final PullMessageRequestHeader requestHeader,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request;
        if (PullSysFlag.hasLitePullFlag(requestHeader.getSysFlag())) {
            request = RemotingCommand.createRequestCommand(RequestCode.LITE_PULL_MESSAGE, requestHeader);
        } else {
            request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        }

        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                break;
        }

        return null;
    }

    public void popMessageAsync(
        final String brokerName, final String addr, final PopMessageRequestHeader requestHeader,
        final long timeoutMillis, final PopCallback popCallback
    ) throws RemotingException, InterruptedException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                try {
                    PopResult popResult = MQClientAPIImpl.this.processPopResponse(brokerName, response, requestHeader.getTopic(), requestHeader);
                    popCallback.onSuccess(popResult);
                } catch (Exception e) {
                    popCallback.onException(e);
                }
            }
            @Override
            public void operationFail(Throwable throwable) {
                popCallback.onException(throwable);
            }
        });
    }

    public void ackMessageAsync(
        final String addr,
        final long timeOut,
        final AckCallback ackCallback,
        final AckMessageRequestHeader requestHeader
    ) throws RemotingException, MQBrokerException, InterruptedException {
        ackMessageAsync(addr, timeOut, ackCallback, requestHeader, null);
    }

    public void batchAckMessageAsync(
        final String addr,
        final long timeOut,
        final AckCallback ackCallback,
        final String topic,
        final String consumerGroup,
        final List<String> extraInfoList
    ) throws RemotingException, MQBrokerException, InterruptedException {
        String brokerName = null;
        Map<String, BatchAck> batchAckMap = new HashMap<>();
        for (String extraInfo : extraInfoList) {
            String[] extraInfoData = ExtraInfoUtil.split(extraInfo);
            if (brokerName == null) {
                brokerName = ExtraInfoUtil.getBrokerName(extraInfoData);
            }
            String mergeKey = ExtraInfoUtil.getRetry(extraInfoData) + "@" +
                ExtraInfoUtil.getQueueId(extraInfoData) + "@" +
                ExtraInfoUtil.getCkQueueOffset(extraInfoData) + "@" +
                ExtraInfoUtil.getPopTime(extraInfoData);
            BatchAck bAck = batchAckMap.computeIfAbsent(mergeKey, k -> {
                BatchAck newBatchAck = new BatchAck();
                newBatchAck.setConsumerGroup(consumerGroup);
                newBatchAck.setTopic(topic);
                newBatchAck.setRetry(ExtraInfoUtil.getRetry(extraInfoData));
                newBatchAck.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfoData));
                newBatchAck.setQueueId(ExtraInfoUtil.getQueueId(extraInfoData));
                newBatchAck.setReviveQueueId(ExtraInfoUtil.getReviveQid(extraInfoData));
                newBatchAck.setPopTime(ExtraInfoUtil.getPopTime(extraInfoData));
                newBatchAck.setInvisibleTime(ExtraInfoUtil.getInvisibleTime(extraInfoData));
                newBatchAck.setBitSet(new BitSet());
                return newBatchAck;
            });
            bAck.getBitSet().set((int) (ExtraInfoUtil.getQueueOffset(extraInfoData) - ExtraInfoUtil.getCkQueueOffset(extraInfoData)));
        }

        BatchAckMessageRequestBody requestBody = new BatchAckMessageRequestBody();
        requestBody.setBrokerName(brokerName);
        requestBody.setAcks(new ArrayList<>(batchAckMap.values()));
        batchAckMessageAsync(addr, timeOut, ackCallback, requestBody);
    }

    public void batchAckMessageAsync(
        final String addr,
        final long timeOut,
        final AckCallback ackCallback,
        final BatchAckMessageRequestBody requestBody
    ) throws RemotingException, MQBrokerException, InterruptedException {
        ackMessageAsync(addr, timeOut, ackCallback, null, requestBody);
    }

    protected void ackMessageAsync(
        final String addr,
        final long timeOut,
        final AckCallback ackCallback,
        final AckMessageRequestHeader requestHeader,
        final BatchAckMessageRequestBody requestBody
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request;
        if (requestHeader != null) {
            request = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        } else {
            request = RemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);
            if (requestBody != null) {
                request.setBody(requestBody.encode());
            }
        }
        this.remotingClient.invokeAsync(addr, request, timeOut, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                AckResult ackResult = new AckResult();
                if (ResponseCode.SUCCESS == response.getCode()) {
                    ackResult.setStatus(AckStatus.OK);
                } else {
                    ackResult.setStatus(AckStatus.NO_EXIST);
                }
                ackCallback.onSuccess(ackResult);
            }

            @Override
            public void operationFail(Throwable throwable) {
                ackCallback.onException(throwable);
            }
        });
    }

    public void changeInvisibleTimeAsync(//
        final String brokerName,
        final String addr, //
        final ChangeInvisibleTimeRequestHeader requestHeader,//
        final long timeoutMillis,
        final AckCallback ackCallback
    ) throws RemotingException, MQBrokerException, InterruptedException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                try {
                    ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.decodeCommandCustomHeader(ChangeInvisibleTimeResponseHeader.class);
                    AckResult ackResult = new AckResult();
                    if (ResponseCode.SUCCESS == response.getCode()) {
                        ackResult.setStatus(AckStatus.OK);
                        ackResult.setPopTime(responseHeader.getPopTime());
                        ackResult.setExtraInfo(ExtraInfoUtil
                            .buildExtraInfo(requestHeader.getOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                responseHeader.getReviveQid(), requestHeader.getTopic(), brokerName, requestHeader.getQueueId()) + MessageConst.KEY_SEPARATOR
                            + requestHeader.getOffset());
                    } else {
                        ackResult.setStatus(AckStatus.NO_EXIST);
                    }
                    ackCallback.onSuccess(ackResult);
                } catch (Exception e) {
                    ackCallback.onException(e);
                }
            }

            @Override
            public void operationFail(Throwable throwable) {
                ackCallback.onException(throwable);
            }
        });
    }

    private void pullMessageAsync(
        final String addr,
        final RemotingCommand request,
        final long timeoutMillis,
        final PullCallback pullCallback
    ) throws RemotingException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationComplete(ResponseFuture responseFuture) {

            }

            @Override
            public void operationSucceed(RemotingCommand response) {
                try {
                    PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response, addr);
                    pullCallback.onSuccess(pullResult);
                } catch (Exception e) {
                    pullCallback.onException(e);
                }
            }

            @Override
            public void operationFail(Throwable throwable) {
                pullCallback.onException(throwable);
            }
        });
    }

    private PullResult pullMessageSync(
        final String addr,
        final RemotingCommand request,
        final long timeoutMillis
    ) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response, addr);
    }

    private PullResult processPullResponse(
        final RemotingCommand response,
        final String addr) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        PullMessageResponseHeader responseHeader =
            (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
            responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody(), responseHeader.getOffsetDelta());
    }

    private PopResult processPopResponse(final String brokerName, final RemotingCommand response, String topic,
        CommandCustomHeader requestHeader) throws MQBrokerException, RemotingCommandException {
        PopStatus popStatus = PopStatus.NO_NEW_MSG;
        List<MessageExt> msgFoundList = null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                popStatus = PopStatus.FOUND;
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                msgFoundList = MessageDecoder.decodesBatch(
                    byteBuffer,
                    clientConfig.isDecodeReadBody(),
                    clientConfig.isDecodeDecompressBody(),
                    true);
                break;
            case ResponseCode.POLLING_FULL:
                popStatus = PopStatus.POLLING_FULL;
                break;
            case ResponseCode.POLLING_TIMEOUT:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                popStatus = PopStatus.POLLING_NOT_FOUND;
                break;
            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PopResult popResult = new PopResult(popStatus, msgFoundList);
        PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) response.decodeCommandCustomHeader(PopMessageResponseHeader.class);
        popResult.setRestNum(responseHeader.getRestNum());
        if (popStatus != PopStatus.FOUND) {
            return popResult;
        }
        // it is a pop command if pop time greater than 0, we should set the check point info to extraInfo field
        Map<String, Long> startOffsetInfo = null;
        Map<String, List<Long>> msgOffsetInfo = null;
        Map<String, Integer> orderCountInfo = null;
        if (requestHeader instanceof PopMessageRequestHeader) {
            popResult.setInvisibleTime(responseHeader.getInvisibleTime());
            popResult.setPopTime(responseHeader.getPopTime());
            startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
            msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
            orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
        }
        Map<String/*topicMark@queueId*/, List<Long>/*msg queueOffset*/> sortMap
            = buildQueueOffsetSortedMap(topic, msgFoundList);
        Map<String, String> map = new HashMap<>(5);
        for (MessageExt messageExt : msgFoundList) {
            if (requestHeader instanceof PopMessageRequestHeader) {
                if (startOffsetInfo == null) {
                    // we should set the check point info to extraInfo field , if the command is popMsg
                    // find pop ck offset
                    String key = messageExt.getTopic() + messageExt.getQueueId();
                    if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
                        map.put(key, ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(),
                            messageExt.getTopic(), brokerName, messageExt.getQueueId()));

                    }
                    messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
                } else {
                    if (messageExt.getProperty(MessageConst.PROPERTY_POP_CK) == null) {
                        final String queueIdKey;
                        final String queueOffsetKey;
                        final int index;
                        final Long msgQueueOffset;
                        if (MixAll.isLmq(topic) && messageExt.getReconsumeTimes() == 0 && StringUtils.isNotEmpty(
                            messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
                            // process LMQ
                            String[] queues = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)
                                .split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
                            String[] queueOffsets = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET)
                                .split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
                            long offset = Long.parseLong(queueOffsets[ArrayUtils.indexOf(queues, topic)]);
                            // LMQ topic has only 1 queue, which queue id is 0
                            queueIdKey = ExtraInfoUtil.getStartOffsetInfoMapKey(topic, MixAll.LMQ_QUEUE_ID);
                            queueOffsetKey = ExtraInfoUtil.getQueueOffsetMapKey(topic, MixAll.LMQ_QUEUE_ID, offset);
                            index = sortMap.get(queueIdKey).indexOf(offset);
                            msgQueueOffset = msgOffsetInfo.get(queueIdKey).get(index);
                            if (msgQueueOffset != offset) {
                                log.warn("Queue offset[{}] of msg is strange, not equal to the stored in msg, {}",
                                    msgQueueOffset, messageExt);
                            }
                            messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                                ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(queueIdKey), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                    responseHeader.getReviveQid(), topic, brokerName, 0, msgQueueOffset)
                            );
                        } else {
                            queueIdKey = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                            queueOffsetKey = ExtraInfoUtil.getQueueOffsetMapKey(messageExt.getTopic(), messageExt.getQueueId(), messageExt.getQueueOffset());
                            index = sortMap.get(queueIdKey).indexOf(messageExt.getQueueOffset());
                            msgQueueOffset = msgOffsetInfo.get(queueIdKey).get(index);
                            if (msgQueueOffset != messageExt.getQueueOffset()) {
                                log.warn("Queue offset[{}] of msg is strange, not equal to the stored in msg, {}", msgQueueOffset, messageExt);
                            }
                            messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                                ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(queueIdKey), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                    responseHeader.getReviveQid(), messageExt.getTopic(), brokerName, messageExt.getQueueId(), msgQueueOffset)
                            );
                        }
                        if (((PopMessageRequestHeader) requestHeader).isOrder() && orderCountInfo != null) {
                            Integer count = orderCountInfo.get(queueOffsetKey);
                            if (count == null) {
                                count = orderCountInfo.get(queueIdKey);
                            }
                            if (count != null && count > 0) {
                                messageExt.setReconsumeTimes(count);
                            }
                        }
                    }
                }
                messageExt.getProperties().computeIfAbsent(
                    MessageConst.PROPERTY_FIRST_POP_TIME, k -> String.valueOf(responseHeader.getPopTime()));
            }
            messageExt.setBrokerName(brokerName);
            messageExt.setTopic(NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace()));
        }
        return popResult;
    }

    /**
     * Build queue offset sorted map
     *
     * @param topic pop consumer topic
     * @param msgFoundList popped message list
     * @return sorted map, key is topicMark@queueId, value is sorted msg queueOffset list
     */
    private static Map<String, List<Long>> buildQueueOffsetSortedMap(String topic, List<MessageExt> msgFoundList) {
        Map<String/*topicMark@queueId*/, List<Long>/*msg queueOffset*/> sortMap = new HashMap<>(16);
        for (MessageExt messageExt : msgFoundList) {
            final String key;
            if (MixAll.isLmq(topic) && messageExt.getReconsumeTimes() == 0
                && StringUtils.isNotEmpty(messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
                // process LMQ
                String[] queues = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)
                    .split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
                String[] queueOffsets = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET)
                    .split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
                // LMQ topic has only 1 queue, which queue id is 0
                key = ExtraInfoUtil.getStartOffsetInfoMapKey(topic, MixAll.LMQ_QUEUE_ID);
                sortMap.putIfAbsent(key, new ArrayList<>(4));
                sortMap.get(key).add(Long.parseLong(queueOffsets[ArrayUtils.indexOf(queues, topic)]));
                continue;
            }
            // Value of POP_CK is used to determine whether it is a pop retry,
            // cause topic could be rewritten by broker.
            key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(),
                messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getQueueId());
            if (!sortMap.containsKey(key)) {
                sortMap.put(key, new ArrayList<>(4));
            }
            sortMap.get(key).add(messageExt.getQueueOffset());
        }
        return sortMap;
    }

    public MessageExt viewMessage(final String addr, final String topic, final long phyoffset, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                //If namespace not null , reset Topic without namespace.
                if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
                    messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
                }
                return messageExt;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    @Deprecated
    public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
        final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                SearchOffsetResponseHeader responseHeader =
                    (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long searchOffset(final String addr, final MessageQueue messageQueue, final long timestamp,
        final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        // default return lower boundary offset when there are more than one offsets.
        return searchOffset(addr, messageQueue, timestamp, BoundaryType.LOWER, timeoutMillis);
    }

    public long searchOffset(final String addr, final MessageQueue messageQueue, final long timestamp,
        final BoundaryType boundaryType, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBrokerName(messageQueue.getBrokerName());
        requestHeader.setTimestamp(timestamp);
        requestHeader.setBoundaryType(boundaryType);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                SearchOffsetResponseHeader responseHeader =
                    (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getMaxOffset(final String addr, final MessageQueue messageQueue, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBrokerName(messageQueue.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMaxOffsetResponseHeader responseHeader =
                    (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public List<String> getConsumerIdListByGroup(
        final String addr,
        final String consumerGroup,
        final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        MQBrokerException, InterruptedException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerListByGroupResponseBody body =
                        GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                    return body.getConsumerIdList();
                }
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getMinOffset(final String addr, final MessageQueue messageQueue, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(messageQueue.getTopic());
        requestHeader.setQueueId(messageQueue.getQueueId());
        requestHeader.setBrokerName(messageQueue.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetMinOffsetResponseHeader responseHeader =
                    (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

                return responseHeader.getOffset();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long getEarliestMsgStoretime(final String addr, final MessageQueue mq, final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(mq.getTopic());
        requestHeader.setQueueId(mq.getQueueId());
        requestHeader.setBrokerName(mq.getBrokerName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetEarliestMsgStoretimeResponseHeader responseHeader =
                    (GetEarliestMsgStoretimeResponseHeader) response.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);

                return responseHeader.getTimestamp();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public long queryConsumerOffset(
        final String addr,
        final QueryConsumerOffsetRequestHeader requestHeader,
        final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumerOffsetResponseHeader responseHeader =
                    (QueryConsumerOffsetResponseHeader) response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
                return responseHeader.getOffset();
            }
            case ResponseCode.QUERY_NOT_FOUND: {
                throw new OffsetNotFoundException(response.getCode(), response.getRemark(), addr);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateConsumerOffset(
        final String addr,
        final UpdateConsumerOffsetRequestHeader requestHeader,
        final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateConsumerOffsetOneway(
        final String addr,
        final UpdateConsumerOffsetRequestHeader requestHeader,
        final long timeoutMillis
    ) throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException,
        InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
    }

    public int sendHeartbeat(
        final String addr,
        final HeartbeatData heartbeatData,
        final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return response.getVersion();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public HeartbeatV2Result sendHeartbeatV2(
        final String addr,
        final HeartbeatData heartbeatData,
        final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getExtFields() != null) {
                    return new HeartbeatV2Result(response.getVersion(), Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUB_CHANGE)), Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUPPORT_HEART_BEAT_V2)));
                }
                return new HeartbeatV2Result(response.getVersion(), false, false);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void unregisterClient(
        final String addr,
        final String clientID,
        final String producerGroup,
        final String consumerGroup,
        final long timeoutMillis
    ) throws RemotingException, MQBrokerException, InterruptedException {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void endTransactionOneway(
        final String addr,
        final EndTransactionRequestHeader requestHeader,
        final String remark,
        final long timeoutMillis
    ) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    public void queryMessage(
        final String addr,
        final QueryMessageRequestHeader requestHeader,
        final long timeoutMillis,
        final InvokeCallback invokeCallback,
        final Boolean isUnqiueKey
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());
        this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis,
            invokeCallback);
    }

    public boolean registerClient(final String addr, final HeartbeatData heartbeat, final long timeoutMillis)
        throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());

        request.setBody(heartbeat.encode());
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        return response.getCode() == ResponseCode.SUCCESS;
    }

    public void consumerSendMessageBack(
        final String addr,
        final String brokerName,
        final MessageExt msg,
        final String consumerGroup,
        final int delayLevel,
        final long timeoutMillis,
        final int maxConsumeRetryTimes
    ) throws RemotingException, MQBrokerException, InterruptedException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
        requestHeader.setBrokerName(brokerName);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public Set<MessageQueue> lockBatchMQ(
        final String addr,
        final LockBatchRequestBody requestBody,
        final long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, new LockBatchMqRequestHeader());

        request.setBody(requestBody.encode());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
                Set<MessageQueue> messageQueues = responseBody.getLockOKMQSet();
                return messageQueues;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void unlockBatchMQ(
        final String addr,
        final UnlockBatchRequestBody requestBody,
        final long timeoutMillis,
        final boolean oneway
    ) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, new UnlockBatchMqRequestHeader());

        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        } else {
            RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    public TopicStatsTable getTopicStatsInfo(final String addr, final String topic,
        final long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicStatsTable topicStatsTable = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
                return topicStatsTable;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQBrokerException {
        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
    }

    public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic,
        final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQBrokerException {
        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                ConsumeStats consumeStats = ConsumeStats.decode(response.getBody(), ConsumeStats.class);
                return consumeStats;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ProducerConnection getProducerConnectionList(final String addr, final String producerGroup,
        final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ProducerTableInfo getAllProducerInfo(final String addr, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        GetAllProducerInfoRequestHeader requestHeader = new GetAllProducerInfoRequestHeader();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ProducerTableInfo.decode(response.getBody(), ProducerTableInfo.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public ConsumerConnection getConsumerConnectionList(final String addr, final String consumerGroup,
        final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public KVTable getBrokerRuntimeInfo(final String addr, final long timeoutMillis) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void addBroker(final String addr, final String brokerConfigPath, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        AddBrokerRequestHeader requestHeader = new AddBrokerRequestHeader();
        requestHeader.setConfigPath(brokerConfigPath);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                return;
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void removeBroker(final String addr, String clusterName, String brokerName, long brokerId,
        final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemoveBrokerRequestHeader requestHeader = new RemoveBrokerRequestHeader();
        requestHeader.setBrokerClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerId(brokerId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                return;
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateBrokerConfig(final String addr, final Properties properties, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException, MQClientException, UnsupportedEncodingException {
        Validators.checkBrokerConfig(properties);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient
                .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    public Properties getBrokerConfig(final String addr, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateColdDataFlowCtrGroupConfig(final String addr, final Properties properties, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null);
        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(
                MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public void removeColdDataFlowCtrGroupConfig(final String addr, final String consumerGroup, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null);
        if (consumerGroup != null && consumerGroup.length() > 0) {
            request.setBody(consumerGroup.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(
                MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public String getColdDataFlowCtrInfo(final String addr, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (null != response.getBody() && response.getBody().length > 0) {
                    return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
                }
                return null;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public String setCommitLogReadAheadMode(final String addr, final String mode, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_COMMITLOG_READ_MODE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put(FIleReadaheadMode.READ_AHEAD_MODE, mode);
        request.setExtFields(extFields);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (null != response.getRemark() && response.getRemark().length() > 0) {
                    return response.getRemark();
                }
                return null;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public ClusterInfo getBrokerClusterInfo(
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, timeoutMillis, false);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis,
        boolean allowTopicNotExist) throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }

                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getTopicListFromNameServer(final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicList.decode(body, TopicList.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName,
        final long timeoutMillis) throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                WipeWritePermOfBrokerResponseHeader responseHeader =
                    (WipeWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
                return responseHeader.getWipeTopicCount();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public int addWritePermOfBroker(final String nameSrvAddr, String brokerName, final long timeoutMillis)
        throws RemotingCommandException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(nameSrvAddr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                AddWritePermOfBrokerResponseHeader responseHeader =
                    (AddWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(AddWritePermOfBrokerResponseHeader.class);
                return responseHeader.getAddTopicCount();
            }
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInBroker(final String addr, final String topic, final long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String topic, final long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteTopicInNameServer(final String addr, final String clusterName, final String topic,
        final long timeoutMillis)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setClusterName(clusterName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void deleteSubscriptionGroup(final String addr, final String groupName, final boolean removeOffset,
        final long timeoutMillis)
        throws RemotingException, InterruptedException, MQClientException {
        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupName);
        requestHeader.setCleanOffset(removeOffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public String getKVConfigValue(final String namespace, final String key, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GetKVConfigResponseHeader responseHeader =
                    (GetKVConfigResponseHeader) response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
                return responseHeader.getValue();
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }

            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                switch (response.getCode()) {
                    case ResponseCode.SUCCESS: {
                        break;
                    }
                    default:
                        errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return KVTable.decode(response.getBody(), KVTable.class);
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
        final long timestamp, final boolean isForce, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
        final long timestamp, int queueId, Long offset, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setOffset(offset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET,
            requestHeader);

        RemotingCommand response = remotingClient.invokeSync(
            MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (null != response.getBody()) {
                    return ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class).getOffsetTable();
                }
                break;
            }
            case ResponseCode.TOPIC_NOT_EXIST:
            case ResponseCode.SUBSCRIPTION_NOT_EXIST:
            case ResponseCode.SYSTEM_ERROR:
                log.warn("Invoke broker to reset offset error code={}, remark={}",
                    response.getCode(), response.getRemark());
                break;
            default:
                break;
        }
        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group,
        final long timestamp, final boolean isForce, final long timeoutMillis, boolean isC)
        throws RemotingException, MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);
        // offset is -1 means offset is null
        requestHeader.setOffset(-1L);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        if (isC) {
            request.setLanguage(LanguageCode.CPP);
        }
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
                    return body.getOffsetTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(final String addr, final String topic,
        final String group,
        final String clientAddr,
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
                    return body.getConsumerTable();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public GroupList queryTopicConsumeByWho(final String addr, final String topic, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                GroupList groupList = GroupList.decode(response.getBody(), GroupList.class);
                return groupList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList queryTopicsByConsumer(final String addr, final String group, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        QueryTopicsByConsumerRequestHeader requestHeader = new QueryTopicsByConsumerRequestHeader();
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                return topicList;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public SubscriptionData querySubscriptionByConsumer(final String addr, final String group, final String topic,
        final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        QuerySubscriptionByConsumerRequestHeader requestHeader = new QuerySubscriptionByConsumerRequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QuerySubscriptionResponseBody subscriptionResponseBody =
                    QuerySubscriptionResponseBody.decode(response.getBody(), QuerySubscriptionResponseBody.class);
                return subscriptionResponseBody.getSubscriptionData();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String addr, final String topic, final String group,
        final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException,
        MQBrokerException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
                return consumeTimeSpanBody.getConsumeTimeSpanSet();
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicList(
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty()
                        && !UtilAll.isBlank(topicList.getBrokerAddr())) {
                        TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr(), timeoutMillis);
                        if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
                            topicList.getTopicList().addAll(tmp.getTopicList());
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getSystemTopicListFromBroker(final String addr, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(body, TopicList.class);
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanExpiredConsumeQueue(final String addr,
        long timeoutMillis) throws MQClientException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean deleteExpiredCommitLog(final String addr, long timeoutMillis) throws MQClientException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_EXPIRED_COMMITLOG, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public boolean cleanUnusedTopicByAddr(final String addr,
        long timeoutMillis) throws MQClientException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumerRunningInfo getConsumerRunningInfo(final String addr, String consumerGroup, String clientId,
        boolean jstack,
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumerRunningInfo info = ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final String addr,
        String consumerGroup,
        String clientId,
        String topic,
        String msgId,
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    ConsumeMessageDirectlyResult info = ConsumeMessageDirectlyResult.decode(body, ConsumeMessageDirectlyResult.class);
                    return info;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Map<Integer, Long> queryCorrectionOffset(final String addr, final String topic, final String group,
        Set<String> filterGroup,
        long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
        requestHeader.setCompareGroup(group);
        requestHeader.setTopic(topic);
        if (filterGroup != null) {
            StringBuilder sb = new StringBuilder();
            String splitor = "";
            for (String s : filterGroup) {
                sb.append(splitor).append(s);
                splitor = ",";
            }
            requestHeader.setFilterGroups(sb.toString());
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                if (response.getBody() != null) {
                    QueryCorrectionOffsetBody body = QueryCorrectionOffsetBody.decode(response.getBody(), QueryCorrectionOffsetBody.class);
                    return body.getCorrectionOffsets();
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getUnitTopicList(final boolean containRetry, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                                it.remove();
                            }
                        }
                    }

                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubTopicList(final boolean containRetry, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                                it.remove();
                            }
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public TopicList getHasUnitSubUnUnitTopicList(final boolean containRetry, final long timeoutMillis)
        throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);
        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
                    if (!containRetry) {
                        Iterator<String> it = topicList.getTopicList().iterator();
                        while (it.hasNext()) {
                            String topic = it.next();
                            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                                it.remove();
                            }
                        }
                    }
                    return topicList;
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void cloneGroupOffset(final String addr, final String srcGroup, final String destGroup, final String topic,
        final boolean isOffline,
        final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
        throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);
        RemotingCommand response = this.remotingClient
            .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return BrokerStatsData.decode(body, BrokerStatsData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public Set<String> getClusterList(String topic,
        long timeoutMillis) {
        return Collections.EMPTY_SET;
    }

    public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, boolean isOrder,
        long timeoutMillis) throws MQClientException,
        RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
        requestHeader.setIsOrder(isOrder);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);
        RemotingCommand response = this.remotingClient
            .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return ConsumeStatsList.decode(body, ConsumeStatsList.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr,
        long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = this.remotingClient
            .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public SubscriptionGroupConfig getSubscriptionGroupConfig(final String brokerAddr, String group,
        long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetSubscriptionGroupConfigRequestHeader header = new GetSubscriptionGroupConfigRequestHeader();
        header.setGroup(group);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG, header);
        RemotingCommand response = this.remotingClient
            .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), SubscriptionGroupConfig.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public TopicConfigSerializeWrapper getAllTopicConfig(final String addr,
        long timeoutMillis) throws RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void updateNameServerConfig(final Properties properties, final List<String> nameServers, long timeoutMillis)
        throws UnsupportedEncodingException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQClientException {
        String str = MixAll.properties2String(properties);
        if (str == null || str.length() < 1) {
            return;
        }
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
            this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    break;
                }
                default:
                    errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Map<String, Properties> getNameServerConfig(final List<String> nameServers, long timeoutMillis)
        throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException,
        MQClientException, UnsupportedEncodingException {
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ?
            this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<>(4);
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

            assert response != null;

            if (ResponseCode.SUCCESS == response.getCode()) {
                configMap.put(nameServer, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
            } else {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }
        return configMap;
    }

    public QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr, final String topic,
        final int queueId,
        final long index, final int count, final String consumerGroup,
        final long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {

        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setIndex(index);
        requestHeader.setCount(count);
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS == response.getCode()) {
            return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    public void checkClientInBroker(final String brokerAddr, final String consumerGroup,
        final String clientId, final SubscriptionData subscriptionData,
        final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);

        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;

        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
    }

    public boolean resumeCheckHalfMessage(final String addr, String topic, String msgId,
        final long timeoutMillis) throws RemotingException, InterruptedException {
        ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return true;
            }
            default:
                log.error("Failed to resume half message check logic. Remark={}", response.getRemark());
                return false;
        }
    }

    public void setMessageRequestMode(final String brokerAddr, final String topic, final String consumerGroup,
        final MessageRequestMode mode, final int popShareQueueNum, final long timeoutMillis)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingConnectException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_MESSAGE_REQUEST_MODE, null);

        SetMessageRequestModeRequestBody requestBody = new SetMessageRequestModeRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(consumerGroup);
        requestBody.setMode(mode);
        requestBody.setPopShareQueueNum(popShareQueueNum);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
    }

    public TopicConfigAndQueueMapping getTopicConfig(final String brokerAddr, String topic,
        long timeoutMillis) throws InterruptedException,
        RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicConfigRequestHeader header = new GetTopicConfigRequestHeader();
        header.setTopic(topic);
        header.setLo(true);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, header);
        RemotingCommand response = this.remotingClient
            .invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), TopicConfigAndQueueMapping.class);
            }
            //should check the exist
            case ResponseCode.TOPIC_NOT_EXIST: {
                //should return null?
                break;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void createStaticTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
        final TopicQueueMappingDetail topicQueueMappingDetail, boolean force,
        final long timeoutMillis) throws RemotingException, InterruptedException, MQBrokerException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        requestHeader.setForce(force);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC, requestHeader);
        request.setBody(topicQueueMappingDetail.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * @param addr
     * @param requestHeader
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     * @throws MQBrokerException
     */
    public GroupForbidden updateAndGetGroupForbidden(String addr, UpdateGroupForbiddenRequestHeader requestHeader,
        long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
            request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), GroupForbidden.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    public void resetMasterFlushOffset(final String brokerAddr, final long masterFlushOffset)
        throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        ResetMasterFlushOffsetHeader requestHeader = new ResetMasterFlushOffsetHeader();
        requestHeader.setMasterFlushOffset(masterFlushOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESET_MASTER_FLUSH_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    public HARuntimeInfo getBrokerHAStatus(final String brokerAddr, final long timeoutMillis)
        throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
        InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_HA_STATUS, null);
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, timeoutMillis);
        assert response != null;

        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return HARuntimeInfo.decode(response.getBody(), HARuntimeInfo.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public GetMetaDataResponseHeader getControllerMetaData(
        final String controllerAddress) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);
        final RemotingCommand response = this.remotingClient.invokeSync(controllerAddress, request, 3000);
        assert response != null;
        if (response.getCode() == SUCCESS) {
            return (GetMetaDataResponseHeader) response.decodeCommandCustomHeader(GetMetaDataResponseHeader.class);
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public BrokerReplicasInfo getInSyncStateData(final String controllerAddress,
        final List<String> brokers) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, RemotingCommandException {
        // Get controller leader address.
        final GetMetaDataResponseHeader controllerMetaData = getControllerMetaData(controllerAddress);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        final String leaderAddress = controllerMetaData.getControllerLeaderAddress();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, null);
        final byte[] body = RemotingSerializable.encode(brokers);
        request.setBody(body);
        RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), BrokerReplicasInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public EpochEntryCache getBrokerEpochCache(
        String brokerAddr) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_EPOCH_CACHE, null);
        final RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), EpochEntryCache.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public Map<String, Properties> getControllerConfig(final List<String> controllerServers,
        final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, MQClientException, UnsupportedEncodingException {
        List<String> invokeControllerServers = (controllerServers == null || controllerServers.isEmpty()) ?
            this.remotingClient.getNameServerAddressList() : controllerServers;
        if (invokeControllerServers == null || invokeControllerServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONTROLLER_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<>(4);
        for (String controller : invokeControllerServers) {
            RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);

            assert response != null;

            if (ResponseCode.SUCCESS == response.getCode()) {
                configMap.put(controller, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
            } else {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }
        return configMap;
    }

    public void updateControllerConfig(final Properties properties, final List<String> controllers,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException, UnsupportedEncodingException,
        RemotingSendRequestException, RemotingTimeoutException, MQClientException {
        String str = MixAll.properties2String(properties);
        if (str.length() < 1 || controllers == null || controllers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONTROLLER_CONFIG, null);
        request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String controller : controllers) {
            RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);
            assert response != null;
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    break;
                }
                default:
                    errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    public Pair<ElectMasterResponseHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName,
        String brokerName,
        Long brokerId) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {

        //get controller leader address
        final GetMetaDataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        final String leaderAddress = controllerMetaData.getControllerLeaderAddress();
        ElectMasterRequestHeader electRequestHeader = ElectMasterRequestHeader.ofAdminTrigger(clusterName, brokerName, brokerId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, electRequestHeader);
        final RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                BrokerMemberGroup brokerMemberGroup = RemotingSerializable.decode(response.getBody(), BrokerMemberGroup.class);
                ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.decodeCommandCustomHeader(ElectMasterResponseHeader.class);
                return new Pair<>(responseHeader, brokerMemberGroup);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void cleanControllerBrokerData(String controllerAddr, String clusterName,
        String brokerName, String brokerControllerIdsToClean, boolean isCleanLivingBroker)
        throws RemotingException, InterruptedException, MQBrokerException {

        //get controller leader address
        final GetMetaDataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
        assert controllerMetaData != null;
        assert controllerMetaData.getControllerLeaderAddress() != null;
        final String leaderAddress = controllerMetaData.getControllerLeaderAddress();

        CleanControllerBrokerDataRequestHeader cleanHeader = new CleanControllerBrokerDataRequestHeader(clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_BROKER_DATA, cleanHeader);

        final RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void createUser(String addr, UserInfo userInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        CreateUserRequestHeader requestHeader = new CreateUserRequestHeader(userInfo.getUsername());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, requestHeader);
        request.setBody(RemotingSerializable.encode(userInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateUser(String addr, UserInfo userInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UpdateUserRequestHeader requestHeader = new UpdateUserRequestHeader(userInfo.getUsername());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, requestHeader);
        request.setBody(RemotingSerializable.encode(userInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void deleteUser(String addr, String username, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        DeleteUserRequestHeader requestHeader = new DeleteUserRequestHeader(username);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public UserInfo getUser(String addr, String username, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetUserRequestHeader requestHeader = new GetUserRequestHeader(username);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_USER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), UserInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<UserInfo> listUser(String addr, String filter, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        ListUsersRequestHeader requestHeader = new ListUsersRequestHeader(filter);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_USER, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decodeList(response.getBody(), UserInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void createAcl(String addr, AclInfo aclInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        CreateAclRequestHeader requestHeader = new CreateAclRequestHeader(aclInfo.getSubject());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_ACL, requestHeader);
        request.setBody(RemotingSerializable.encode(aclInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateAcl(String addr, AclInfo aclInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UpdateAclRequestHeader requestHeader = new UpdateAclRequestHeader(aclInfo.getSubject());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_ACL, requestHeader);
        request.setBody(RemotingSerializable.encode(aclInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void deleteAcl(String addr, String subject, String resource, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        DeleteAclRequestHeader requestHeader = new DeleteAclRequestHeader(subject, resource);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_ACL, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public AclInfo getAcl(String addr, String subject, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetAclRequestHeader requestHeader = new GetAclRequestHeader(subject);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_ACL, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decode(response.getBody(), AclInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<AclInfo> listAcl(String addr, String subjectFilter, String resourceFilter, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        ListAclsRequestHeader requestHeader = new ListAclsRequestHeader(subjectFilter, resourceFilter);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_ACL, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decodeList(response.getBody(), AclInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void createRatelimit(String addr, RatelimitInfo ratelimitInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        CreateRatelimitRequestHeader requestHeader = new CreateRatelimitRequestHeader(ratelimitInfo.getName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CREATE_RATELIMIT, requestHeader);
        request.setBody(RemotingSerializable.encode(ratelimitInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void updateRatelimit(String addr, RatelimitInfo ratelimitInfo, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        UpdateRatelimitRequestHeader requestHeader = new UpdateRatelimitRequestHeader(ratelimitInfo.getName());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_RATELIMIT, requestHeader);
        request.setBody(RemotingSerializable.encode(ratelimitInfo));
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public void deleteRatelimit(String addr, String name, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        DeleteRatelimitRequestHeader requestHeader = new DeleteRatelimitRequestHeader(name);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_RATELIMIT, requestHeader);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    public List<RatelimitInfo> listRatelimit(String addr, long millis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LIST_RATELIMIT, null);
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, millis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return RemotingSerializable.decodeList(response.getBody(), RatelimitInfo.class);
            }
            default:
                break;
        }
        throw new MQBrokerException(response.getCode(), response.getRemark());
    }
}
