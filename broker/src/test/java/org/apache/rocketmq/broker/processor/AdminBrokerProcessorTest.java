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
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Environment;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.config.v1.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.config.v1.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.TopicQueueId;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.constant.FIleReadaheadMode;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.AclInfo;
import org.apache.rocketmq.remoting.protocol.body.CreateTopicListRequestBody;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UserInfo;
import org.apache.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExchangeHAInfoResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListAclsRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ListUsersRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import org.apache.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateAclRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateUserRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.apache.rocketmq.store.util.LibC;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest {

    private AdminBrokerProcessor adminBrokerProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
        new MessageStoreConfig(), null);

    @Mock
    private MessageStore messageStore;

    @Mock
    private SendMessageProcessor sendMessageProcessor;

    @Mock
    private ConcurrentMap<TopicQueueId, LongAdder> inFlyWritingCounterMap;

    private Set<String> systemTopicSet;
    private String topic;

    @Mock
    private SocketAddress socketAddress;
    @Mock
    private BrokerStats brokerStats;
    @Mock
    private TopicConfigManager topicConfigManager;
    @Mock
    private ConsumerManager consumerManager;
    @Mock
    private ConsumerOffsetManager consumerOffsetManager;
    @Mock
    private DefaultMessageStore defaultMessageStore;
    @Mock
    private ScheduleMessageService scheduleMessageService;
    @Mock
    private AuthenticationMetadataManager authenticationMetadataManager;
    @Mock
    private AuthorizationMetadataManager authorizationMetadataManager;

    @Mock
    private TimerMessageStore timerMessageStore;

    @Mock
    private TimerMetrics timerMetrics;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Mock
    private CommitLog commitLog;

    @Mock
    private Broker2Client broker2Client;

    @Mock
    private ClientChannelInfo clientChannelInfo;

    @Before
    public void init() throws Exception {
        brokerController.setMessageStore(messageStore);
        brokerController.setAuthenticationMetadataManager(authenticationMetadataManager);
        brokerController.setAuthorizationMetadataManager(authorizationMetadataManager);
        Field field = BrokerController.class.getDeclaredField("broker2Client");
        field.setAccessible(true);
        field.set(brokerController, broker2Client);

        //doReturn(sendMessageProcessor).when(brokerController).getSendMessageProcessor();

        adminBrokerProcessor = new AdminBrokerProcessor(brokerController);

        systemTopicSet = Sets.newHashSet(
            TopicValidator.RMQ_SYS_SELF_TEST_TOPIC,
            TopicValidator.RMQ_SYS_BENCHMARK_TOPIC,
            TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
            TopicValidator.RMQ_SYS_OFFSET_MOVED_EVENT,
            TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC,
            this.brokerController.getBrokerConfig().getBrokerClusterName(),
            this.brokerController.getBrokerConfig().getBrokerClusterName() + "_" + MixAll.REPLY_TOPIC_POSTFIX);
        if (this.brokerController.getBrokerConfig().isTraceTopicEnable()) {
            systemTopicSet.add(this.brokerController.getBrokerConfig().getMsgTraceTopicName());
        }
        when(handlerContext.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));

        topic = "FooBar" + System.nanoTime();

        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig(topic));
        brokerController.getMessageStoreConfig().setTimerWheelEnable(false);
    }

    @After
    public void destroy() {
        if (notToBeExecuted()) {
            return;
        }
        if (brokerController.getSubscriptionGroupManager() != null) {
            brokerController.getSubscriptionGroupManager().stop();
        }
        if (brokerController.getTopicConfigManager() != null) {
            brokerController.getTopicConfigManager().stop();
        }
        if (brokerController.getConsumerOffsetManager() != null) {
            brokerController.getConsumerOffsetManager().stop();
        }
    }

    private void initRocksdbTopicManager() {
        if (notToBeExecuted()) {
            return;
        }
        RocksDBTopicConfigManager rocksDBTopicConfigManager = new RocksDBTopicConfigManager(brokerController);
        brokerController.setTopicConfigManager(rocksDBTopicConfigManager);
        rocksDBTopicConfigManager.load();
    }

    private void initRocksdbSubscriptionManager() {
        if (notToBeExecuted()) {
            return;
        }
        RocksDBSubscriptionGroupManager rocksDBSubscriptionGroupManager = new RocksDBSubscriptionGroupManager(brokerController);
        brokerController.setSubscriptionGroupManager(rocksDBSubscriptionGroupManager);
        rocksDBSubscriptionGroupManager.load();
    }

    @Test
    public void testProcessRequest_success() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createUpdateBrokerConfigCommand();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_fail() throws RemotingCommandException, UnknownHostException {
        RemotingCommand request = createResumeCheckHalfMessageCommand();
        when(messageStore.selectOneMessageByOffset(any(Long.class))).thenReturn(createSelectMappedBufferResult());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testUpdateAndCreateTopicInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testUpdateAndCreateTopic();
    }

    @Test
    public void testUpdateAndCreateTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildCreateTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        //test validate error topic
        String topic = "";
        RemotingCommand request = buildCreateTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        topic = "TEST_CREATE_TOPIC";
        request = buildCreateTopicRequest(topic);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateAndCreateTopicList() throws RemotingCommandException {
        List<String> systemTopicList = new ArrayList<>(systemTopicSet);
        RemotingCommand request = buildCreateTopicListRequest(systemTopicList);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("The topic[" + systemTopicList.get(0) + "] is conflict with system topic.");

        List<String> inValidTopicList = new ArrayList<>();
        inValidTopicList.add("");
        request = buildCreateTopicListRequest(inValidTopicList);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        List<String> topicList = new ArrayList<>();
        topicList.add("TEST_CREATE_TOPIC");
        topicList.add("TEST_CREATE_TOPIC1");
        request = buildCreateTopicListRequest(topicList);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        //test no changes
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteTopicInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testDeleteTopic();
    }

    @Test
    public void testDeleteTopic() throws Exception {
        //test system topic
        for (String topic : systemTopicSet) {
            RemotingCommand request = buildDeleteTopicRequest(topic);
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
            assertThat(response.getRemark()).isEqualTo("The topic[" + topic + "] is conflict with system topic.");
        }

        String topic = "TEST_DELETE_TOPIC";
        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteWithPopRetryTopic() throws Exception {
        String topic = "topicA";
        String anotherTopic = "another_topicA";
        BrokerConfig brokerConfig = new BrokerConfig();

        topicConfigManager = mock(TopicConfigManager.class);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        final ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        topicConfigTable.put(topic, new TopicConfig());
        topicConfigTable.put(KeyBuilder.buildPopRetryTopic(topic, "cid1", brokerConfig.isEnableRetryTopicV2()), new TopicConfig());

        topicConfigTable.put(anotherTopic, new TopicConfig());
        topicConfigTable.put(KeyBuilder.buildPopRetryTopic(anotherTopic, "cid2", brokerConfig.isEnableRetryTopicV2()), new TopicConfig());
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigTable);
        when(topicConfigManager.selectTopicConfig(anyString())).thenAnswer(invocation -> {
            final String selectTopic = invocation.getArgument(0);
            return topicConfigManager.getTopicConfigTable().get(selectTopic);
        });

        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.whichGroupByTopic(topic)).thenReturn(Sets.newHashSet("cid1"));

        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        verify(topicConfigManager).deleteTopicConfig(topic);
        verify(topicConfigManager).deleteTopicConfig(KeyBuilder.buildPopRetryTopic(topic, "cid1", brokerConfig.isEnableRetryTopicV2()));
        verify(messageStore, times(2)).deleteTopics(anySet());
    }

    @Test
    public void testGetAllTopicConfigInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testGetAllTopicConfig();
    }

    @Test
    public void testGetAllTopicConfig() throws Exception {
        GetAllTopicConfigResponseHeader getAllTopicConfigResponseHeader = new GetAllTopicConfigResponseHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, getAllTopicConfigResponseHeader);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateBrokerConfig() throws Exception {
        handlerContext = mock(ChannelHandlerContext.class);
        channel = mock(Channel.class);
        when(handlerContext.channel()).thenReturn(channel);
        socketAddress = mock(SocketAddress.class);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("key", "value");
        request.setBody(bodyMap.toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerConfig() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testProcessRequest_UpdateConfigPath() throws RemotingCommandException {
        final RemotingCommand updateConfigRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        Properties properties = new Properties();

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        // Update allowed value
        properties.setProperty("allAckInSyncStateSet", "true");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        //update disallowed value
        properties.clear();
        properties.setProperty("brokerConfigPath", "test/path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list.");

        //update disallowed value
        properties.clear();
        properties.setProperty("configBlackList", "test;path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = adminBrokerProcessor.processRequest(ctx, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list.");
    }

    @Test
    public void testSearchOffsetByTimestamp() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getOffsetInQueueByTime(anyString(), anyInt(), anyLong(), any(BoundaryType.class))).thenReturn(Long.MIN_VALUE);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        SearchOffsetRequestHeader searchOffsetRequestHeader = new SearchOffsetRequestHeader();
        searchOffsetRequestHeader.setTopic("topic");
        searchOffsetRequestHeader.setQueueId(0);
        searchOffsetRequestHeader.setTimestamp(System.currentTimeMillis());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, searchOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        request.addExtField("timestamp", System.currentTimeMillis() + "");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetMaxOffset() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getMaxOffsetInQueue(anyString(), anyInt())).thenReturn(Long.MIN_VALUE);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        GetMaxOffsetRequestHeader getMaxOffsetRequestHeader = new GetMaxOffsetRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, getMaxOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetMinOffset() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getMinOffsetInQueue(anyString(), anyInt())).thenReturn(Long.MIN_VALUE);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        GetMinOffsetRequestHeader getMinOffsetRequestHeader = new GetMinOffsetRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, getMinOffsetRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetEarliestMsgStoretime() throws Exception {
        messageStore = mock(MessageStore.class);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        GetEarliestMsgStoretimeRequestHeader getEarliestMsgStoretimeRequestHeader = new GetEarliestMsgStoretimeRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, getEarliestMsgStoretimeRequestHeader);
        request.addExtField("topic", "topic");
        request.addExtField("queueId", "0");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerRuntimeInfo() throws Exception {
        brokerStats = mock(BrokerStats.class);
        when(brokerController.getBrokerStats()).thenReturn(brokerStats);
        when(brokerStats.getMsgPutTotalYesterdayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgPutTotalTodayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgPutTotalTodayNow()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgGetTotalTodayMorning()).thenReturn(Long.MIN_VALUE);
        when(brokerStats.getMsgGetTotalTodayNow()).thenReturn(Long.MIN_VALUE);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testLockBatchMQ() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        LockBatchRequestBody lockBatchRequestBody = new LockBatchRequestBody();
        lockBatchRequestBody.setClientId("1111");
        lockBatchRequestBody.setConsumerGroup("group");
        request.setBody(JSON.toJSON(lockBatchRequestBody).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUnlockBatchMQ() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        UnlockBatchRequestBody unlockBatchRequestBody = new UnlockBatchRequestBody();
        unlockBatchRequestBody.setClientId("11111");
        unlockBatchRequestBody.setConsumerGroup("group");
        request.setBody(JSON.toJSON(unlockBatchRequestBody).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testUpdateAndCreateSubscriptionGroup();
    }

    @Test
    public void testUpdateAndCreateSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setBrokerId(1);
        subscriptionGroupConfig.setGroupName("groupId");
        subscriptionGroupConfig.setConsumeEnable(Boolean.TRUE);
        subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.TRUE);
        subscriptionGroupConfig.setRetryMaxTimes(111);
        subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.TRUE);
        request.setBody(JSON.toJSON(subscriptionGroupConfig).toString().getBytes());
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testGetAllSubscriptionGroup();
    }

    @Test
    public void testGetAllSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteSubscriptionGroupInRocksdb() throws Exception {
        initRocksdbSubscriptionManager();
        testDeleteSubscriptionGroup();
    }

    @Test
    public void testDeleteSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, null);
        request.addExtField("groupName", "GID-Group-Name");
        request.addExtField("removeOffset", "true");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetTopicStatsInfo() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, null);
        request.addExtField("topic", "topicTest");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        topicConfigManager = mock(TopicConfigManager.class);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("topicTest");
        when(topicConfigManager.selectTopicConfig(anyString())).thenReturn(topicConfig);
        RemotingCommand responseSuccess = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(responseSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumerConnectionList() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, null);
        request.addExtField("consumerGroup", "GID-group-test");
        consumerManager = mock(ConsumerManager.class);
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo("GID-group-test", ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(consumerManager.getConsumerGroupInfo(anyString())).thenReturn(consumerGroupInfo);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetProducerConnectionList() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, null);
        request.addExtField("producerGroup", "ProducerGroupId");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testGetAllProducerInfo() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumeStats() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, null);
        request.addExtField("topic", "topicTest");
        request.addExtField("consumerGroup", "GID-test");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllConsumerOffset() throws RemotingCommandException {
        consumerOffsetManager = mock(ConsumerOffsetManager.class);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        ConsumerOffsetManager consumerOffset = new ConsumerOffsetManager();
        when(consumerOffsetManager.encode()).thenReturn(JSON.toJSONString(consumerOffset, false));
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllDelayOffset() throws Exception {
        defaultMessageStore = mock(DefaultMessageStore.class);
        scheduleMessageService = mock(ScheduleMessageService.class);
//        when(brokerController.getMessageStore()).thenReturn(defaultMessageStore);
        when(brokerController.getScheduleMessageService()).thenReturn(scheduleMessageService);
        when(scheduleMessageService.encode()).thenReturn("content");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetTopicConfigInRocksdb() throws Exception {
        if (notToBeExecuted()) {
            return;
        }
        initRocksdbTopicManager();
        testGetTopicConfig();
    }

    @Test
    public void testGetTopicConfig() throws Exception {
        String topic = "foobar";

        brokerController.getTopicConfigManager().getTopicConfigTable().put(topic, new TopicConfig(topic));

        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getBody()).isNotEmpty();
        }
        {
            GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
            requestHeader.setTopic("aaaaaaa");
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);
            request.makeCustomHeaderToNet();
            RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
            assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
            assertThat(response.getRemark()).contains("No topic in this broker.");
        }
    }

    @Test
    public void testCreateUser() throws RemotingCommandException {
        when(authenticationMetadataManager.createUser(any(User.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        CreateUserRequestHeader createUserRequestHeader = new CreateUserRequestHeader();
        createUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, createUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        UserInfo userInfo = UserInfo.of("abc", "123", UserType.NORMAL.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        createUserRequestHeader = new CreateUserRequestHeader();
        createUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, createUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        userInfo = UserInfo.of("super", "123", UserType.SUPER.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testUpdateUser() throws RemotingCommandException {
        when(authenticationMetadataManager.updateUser(any(User.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));
        when(authenticationMetadataManager.getUser(eq("super"))).thenReturn(CompletableFuture.completedFuture(User.of("super", "123", UserType.SUPER)));

        UpdateUserRequestHeader updateUserRequestHeader = new UpdateUserRequestHeader();
        updateUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, updateUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        UserInfo userInfo = UserInfo.of("abc", "123", UserType.NORMAL.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        updateUserRequestHeader = new UpdateUserRequestHeader();
        updateUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, updateUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        userInfo = UserInfo.of("super", "123", UserType.SUPER.getName());
        request.setBody(JSON.toJSONBytes(userInfo));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testDeleteUser() throws RemotingCommandException {
        when(authenticationMetadataManager.deleteUser(any(String.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));
        when(authenticationMetadataManager.getUser(eq("super"))).thenReturn(CompletableFuture.completedFuture(User.of("super", "123", UserType.SUPER)));

        DeleteUserRequestHeader deleteUserRequestHeader = new DeleteUserRequestHeader();
        deleteUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, deleteUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(true));
        deleteUserRequestHeader = new DeleteUserRequestHeader();
        deleteUserRequestHeader.setUsername("super");
        request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, deleteUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(authenticationMetadataManager.isSuperUser(eq("rocketmq"))).thenReturn(CompletableFuture.completedFuture(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
    }

    @Test
    public void testGetUser() throws RemotingCommandException {
        when(authenticationMetadataManager.getUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(User.of("abc", "123", UserType.NORMAL)));

        GetUserRequestHeader getUserRequestHeader = new GetUserRequestHeader();
        getUserRequestHeader.setUsername("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_USER, getUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        UserInfo userInfo = JSON.parseObject(new String(response.getBody()), UserInfo.class);
        assertThat(userInfo.getUsername()).isEqualTo("abc");
        assertThat(userInfo.getPassword()).isEqualTo("123");
        assertThat(userInfo.getUserType()).isEqualTo("Normal");
    }

    @Test
    public void testListUser() throws RemotingCommandException {
        when(authenticationMetadataManager.listUser(eq("abc"))).thenReturn(CompletableFuture.completedFuture(Arrays.asList(User.of("abc", "123", UserType.NORMAL))));

        ListUsersRequestHeader listUserRequestHeader = new ListUsersRequestHeader();
        listUserRequestHeader.setFilter("abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_USER, listUserRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        List<UserInfo> userInfo = JSON.parseArray(new String(response.getBody()), UserInfo.class);
        assertThat(userInfo.get(0).getUsername()).isEqualTo("abc");
        assertThat(userInfo.get(0).getPassword()).isEqualTo("123");
        assertThat(userInfo.get(0).getUserType()).isEqualTo("Normal");
    }

    @Test
    public void testCreateAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.createAcl(any(Acl.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        CreateAclRequestHeader createAclRequestHeader = new CreateAclRequestHeader();
        createAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_ACL, createAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        AclInfo aclInfo = AclInfo.of("User:abc", Arrays.asList("Topic:*"), Arrays.asList("PUB"), Arrays.asList("192.168.0.1"), "Grant");
        request.setBody(JSON.toJSONBytes(aclInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.updateAcl(any(Acl.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        UpdateAclRequestHeader updateAclRequestHeader = new UpdateAclRequestHeader();
        updateAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_ACL, updateAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        AclInfo aclInfo = AclInfo.of("User:abc", Arrays.asList("Topic:*"), Arrays.asList("PUB"), Arrays.asList("192.168.0.1"), "Grant");
        request.setBody(JSON.toJSONBytes(aclInfo));
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteAcl() throws RemotingCommandException {
        when(authorizationMetadataManager.deleteAcl(any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(null));

        DeleteAclRequestHeader deleteAclRequestHeader = new DeleteAclRequestHeader();
        deleteAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_ACL, deleteAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAcl() throws RemotingCommandException {
        Acl aclInfo = Acl.of(User.of("abc"), Arrays.asList(Resource.of("Topic:*")), Arrays.asList(Action.PUB), Environment.of("192.168.0.1"), Decision.ALLOW);
        when(authorizationMetadataManager.getAcl(any(Subject.class))).thenReturn(CompletableFuture.completedFuture(aclInfo));

        GetAclRequestHeader getAclRequestHeader = new GetAclRequestHeader();
        getAclRequestHeader.setSubject("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_ACL, getAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        AclInfo aclInfoData = JSON.parseObject(new String(response.getBody()), AclInfo.class);
        assertThat(aclInfoData.getSubject()).isEqualTo("User:abc");
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getResource()).isEqualTo("Topic:*");
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getActions()).containsAll(Arrays.asList(Action.PUB.getName()));
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getSourceIps()).containsAll(Arrays.asList("192.168.0.1"));
        assertThat(aclInfoData.getPolicies().get(0).getEntries().get(0).getDecision()).isEqualTo("Allow");
    }

    @Test
    public void testListAcl() throws RemotingCommandException {
        Acl aclInfo = Acl.of(User.of("abc"), Arrays.asList(Resource.of("Topic:*")), Arrays.asList(Action.PUB), Environment.of("192.168.0.1"), Decision.ALLOW);
        when(authorizationMetadataManager.listAcl(any(), any())).thenReturn(CompletableFuture.completedFuture(Arrays.asList(aclInfo)));

        ListAclsRequestHeader listAclRequestHeader = new ListAclsRequestHeader();
        listAclRequestHeader.setSubjectFilter("User:abc");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_ACL, listAclRequestHeader);
        request.setVersion(441);
        request.addExtField("AccessKey", "rocketmq");
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        List<AclInfo> aclInfoData = JSON.parseArray(new String(response.getBody()), AclInfo.class);
        assertThat(aclInfoData.get(0).getSubject()).isEqualTo("User:abc");
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getResource()).isEqualTo("Topic:*");
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getActions()).containsAll(Arrays.asList(Action.PUB.getName()));
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getSourceIps()).containsAll(Arrays.asList("192.168.0.1"));
        assertThat(aclInfoData.get(0).getPolicies().get(0).getEntries().get(0).getDecision()).isEqualTo("Allow");
    }

    @Test
    public void testGetTimeCheckPoint() throws RemotingCommandException {
        when(this.brokerController.getTimerCheckpoint()).thenReturn(null);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TIMER_CHECK_POINT, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("The checkpoint is null");

        when(this.brokerController.getTimerCheckpoint()).thenReturn(new TimerCheckpoint());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }
    @Test
    public void testGetTimeMetrics() throws RemotingCommandException, IOException {
        when(this.brokerController.getMessageStore().getTimerMessageStore()).thenReturn(null);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TIMER_METRICS, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        when(this.brokerController.getMessageStore().getTimerMessageStore()).thenReturn(timerMessageStore);
        when(this.timerMessageStore.getTimerMetrics()).thenReturn(timerMetrics);
        when(this.timerMetrics.encode()).thenReturn(new TimerMetrics.TimerMetricsSerializeWrapper().toJson(false));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateColdDataFlowCtrGroupConfig() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        request.setBody("consumerGroup1=1".getBytes());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        request.setBody("".getBytes());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testRemoveColdDataFlowCtrGroupConfig() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        request.setBody("consumerGroup1".getBytes());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetColdDataFlowCtrInfo() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testSetCommitLogReadAheadMode() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_COMMITLOG_READ_MODE, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        HashMap<String, String> extfields = new HashMap<>();
        extfields.put(FIleReadaheadMode.READ_AHEAD_MODE, String.valueOf(LibC.MADV_DONTNEED));
        request.setExtFields(extfields);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        extfields.clear();
        extfields.put(FIleReadaheadMode.READ_AHEAD_MODE, String.valueOf(LibC.MADV_NORMAL));
        request.setExtFields(extfields);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        this.brokerController.setMessageStore(defaultMessageStore);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        when(this.defaultMessageStore.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(this.defaultMessageStore.getCommitLog()).thenReturn(commitLog);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetUnknownCmdResponse() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(10000, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void testGetAllMessageRequestMode() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_MESSAGE_REQUEST_MODE, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testResetOffset() throws RemotingCommandException {
        ResetOffsetRequestHeader requestHeader =
                createRequestHeader("topic","group",-1,false,-1,-1);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);

        this.brokerController.getTopicConfigManager().getTopicConfigTable().put("topic", new TopicConfig("topic"));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);

        this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().put("group", new SubscriptionGroupConfig());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        requestHeader.setQueueId(0);
        request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        requestHeader.setOffset(2L);
        request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testGetConsumerStatus() throws RemotingCommandException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setGroup("group");
        requestHeader.setTopic("topic");
        requestHeader.setClientAddr("");
        RemotingCommand request = RemotingCommand
                .createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);
        RemotingCommand responseCommand = RemotingCommand.createResponseCommand(null);
        responseCommand.setCode(ResponseCode.SUCCESS);
        when(broker2Client.getConsumeStatus(anyString(),anyString(),anyString())).thenReturn(responseCommand);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testQueryTopicConsumeByWho() throws RemotingCommandException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic("topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);
        request.makeCustomHeaderToNet();
        HashSet<String> groups = new HashSet<>();
        groups.add("group");
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(consumerManager.queryTopicConsumeByWho(anyString())).thenReturn(groups);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(RemotingSerializable.decode(response.getBody(), GroupList.class)
                .getGroupList().contains("group"))
                .isEqualTo(groups.contains("group"));
    }

    @Test
    public void testQueryTopicByConsumer() throws RemotingCommandException {
        QueryTopicsByConsumerRequestHeader requestHeader = new QueryTopicsByConsumerRequestHeader();
        requestHeader.setGroup("group");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);
        request.makeCustomHeaderToNet();
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testQuerySubscriptionByConsumer() throws RemotingCommandException {
        QuerySubscriptionByConsumerRequestHeader requestHeader = new QuerySubscriptionByConsumerRequestHeader();
        requestHeader.setGroup("group");
        requestHeader.setTopic("topic");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);
        request.makeCustomHeaderToNet();
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(consumerManager.findSubscriptionData(anyString(),anyString())).thenReturn(null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetSystemTopicListFromBroker() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testCleanExpiredConsumeQueue() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteExpiredCommitLog() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_EXPIRED_COMMITLOG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testCleanUnusedTopic() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConsumerRunningInfo() throws RemotingCommandException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        when(brokerController.getConsumerManager()).thenReturn(consumerManager);
        when(consumerManager.findChannel(anyString(),anyString())).thenReturn(null);
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setClientId("client");
        requestHeader.setConsumerGroup("group");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        when(consumerManager.findChannel(anyString(),anyString())).thenReturn(clientChannelInfo);
        when(clientChannelInfo.getVersion()).thenReturn(MQVersion.Version.V3_0_0_SNAPSHOT.ordinal());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        when(clientChannelInfo.getVersion()).thenReturn(MQVersion.Version.V5_2_3.ordinal());
        when(brokerController.getBroker2Client()).thenReturn(broker2Client);
        when(clientChannelInfo.getChannel()).thenReturn(channel);
        RemotingCommand responseCommand = RemotingCommand.createResponseCommand(null);
        responseCommand.setCode(ResponseCode.SUCCESS);
        when(broker2Client.callClient(any(Channel.class),any(RemotingCommand.class))).thenReturn(responseCommand);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(broker2Client.callClient(any(Channel.class),any(RemotingCommand.class))).thenThrow(new RemotingTimeoutException("timeout"));
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.CONSUME_MSG_TIMEOUT);
    }

    @Test
    public void testQueryCorrectionOffset() throws RemotingCommandException {
        Map<Integer, Long> correctionOffsetMap = new HashMap<>();
        correctionOffsetMap.put(0, 100L);
        correctionOffsetMap.put(1, 200L);
        Map<Integer, Long> compareOffsetMap = new HashMap<>();
        compareOffsetMap.put(0, 80L);
        compareOffsetMap.put(1, 300L);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);
        when(consumerOffsetManager.queryMinOffsetInAllGroup(anyString(),anyString())).thenReturn(correctionOffsetMap);
        when(consumerOffsetManager.queryOffset(anyString(),anyString())).thenReturn(compareOffsetMap);
        QueryCorrectionOffsetHeader queryCorrectionOffsetHeader = new QueryCorrectionOffsetHeader();
        queryCorrectionOffsetHeader.setTopic("topic");
        queryCorrectionOffsetHeader.setCompareGroup("group");
        queryCorrectionOffsetHeader.setFilterGroups("");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, queryCorrectionOffsetHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        QueryCorrectionOffsetBody body = RemotingSerializable.decode(response.getBody(), QueryCorrectionOffsetBody.class);
        Map<Integer, Long> correctionOffsets = body.getCorrectionOffsets();
        assertThat(correctionOffsets.get(0)).isEqualTo(Long.MAX_VALUE);
        assertThat(correctionOffsets.get(1)).isEqualTo(200L);
    }

    @Test
    public void testNotifyMinBrokerIdChange() throws RemotingCommandException {
        NotifyMinBrokerIdChangeRequestHeader requestHeader = new NotifyMinBrokerIdChangeRequestHeader();
        requestHeader.setMinBrokerId(1L);
        requestHeader.setMinBrokerAddr("127.0.0.1:10912");
        requestHeader.setOfflineBrokerAddr("127.0.0.1:10911");
        requestHeader.setHaBrokerAddr("");
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateBrokerHaInfo() throws RemotingCommandException {
        ExchangeHAInfoResponseHeader requestHeader = new ExchangeHAInfoResponseHeader();
        requestHeader.setMasterAddress("127.0.0.1:10911");
        requestHeader.setMasterFlushOffset(0L);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.EXCHANGE_BROKER_HA_INFO, requestHeader);
        request.makeCustomHeaderToNet();
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(brokerController.getMessageStore()).thenReturn(messageStore);
        requestHeader.setMasterHaAddress("127.0.0.1:10912");
        request = RemotingCommand.createRequestCommand(RequestCode.EXCHANGE_BROKER_HA_INFO, requestHeader);
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        when(messageStore.getMasterFlushedOffset()).thenReturn(0L);
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerHaStatus() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_HA_STATUS,null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);

        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(messageStore.getHARuntimeInfo()).thenReturn(new HARuntimeInfo());
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testResetMasterFlushOffset() throws RemotingCommandException {
        ResetMasterFlushOffsetHeader requestHeader = new ResetMasterFlushOffsetHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESET_MASTER_FLUSH_OFFSET,requestHeader);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        requestHeader.setMasterFlushOffset(0L);
        request.makeCustomHeaderToNet();
        response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private ResetOffsetRequestHeader createRequestHeader(String topic,String group,long timestamp,boolean force,long offset,int queueId) {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(force);
        requestHeader.setOffset(offset);
        requestHeader.setQueueId(queueId);
        return requestHeader;
    }

    private RemotingCommand buildCreateTopicRequest(String topic) {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setTopicFilterType(TopicFilterType.SINGLE_TAG.name());
        requestHeader.setReadQueueNums(8);
        requestHeader.setWriteQueueNums(8);
        requestHeader.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand buildCreateTopicListRequest(List<String> topicList) {
        List<TopicConfig> topicConfigList = new ArrayList<>();
        for (String topic:topicList) {
            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicFilterType(TopicFilterType.SINGLE_TAG);
            topicConfig.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
            topicConfig.setTopicSysFlag(0);
            topicConfig.setOrder(false);
            topicConfigList.add(topicConfig);
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, null);
        CreateTopicListRequestBody createTopicListRequestBody = new CreateTopicListRequestBody(topicConfigList);
        request.setBody(createTopicListRequestBody.encode());
        return request;
    }

    private RemotingCommand buildDeleteTopicRequest(String topic) {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);
        request.makeCustomHeaderToNet();
        return request;
    }

    private MessageExt createDefaultMessageExt() {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("12345678");
        messageExt.setQueueId(0);
        messageExt.setCommitLogOffset(123456789L);
        messageExt.setQueueOffset(1234);
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_QUEUE_ID, "0");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_REAL_TOPIC, "testTopic");
        MessageAccessor.putProperty(messageExt, MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, "15");
        return messageExt;
    }

    private SelectMappedBufferResult createSelectMappedBufferResult() {
        SelectMappedBufferResult result = new SelectMappedBufferResult(0, ByteBuffer.allocate(1024), 0, new DefaultMappedFile());
        return result;
    }

    private ResumeCheckHalfMessageRequestHeader createResumeCheckHalfMessageRequestHeader() {
        ResumeCheckHalfMessageRequestHeader header = new ResumeCheckHalfMessageRequestHeader();
        header.setTopic("topic");
        header.setMsgId("C0A803CA00002A9F0000000000031367");
        return header;
    }

    private RemotingCommand createResumeCheckHalfMessageCommand() {
        ResumeCheckHalfMessageRequestHeader header = createResumeCheckHalfMessageRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return request;
    }

    private RemotingCommand createUpdateBrokerConfigCommand() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        request.makeCustomHeaderToNet();
        return request;
    }

    private boolean notToBeExecuted() {
        return MixAll.isMac();
    }
}
