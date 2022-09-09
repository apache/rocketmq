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
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.TopicQueueId;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminBrokerProcessorTest {

    private AdminBrokerProcessor adminBrokerProcessor;

    @Mock
    private ChannelHandlerContext handlerContext;

    @Mock
    private Channel channel;

    @Spy
    private BrokerController
        brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(), new NettyClientConfig(),
        new MessageStoreConfig());

    @Mock
    private MessageStore messageStore;

    @Mock
    private SendMessageProcessor sendMessageProcessor;

    @Mock
    private ConcurrentMap<TopicQueueId, LongAdder> inFlyWritingCouterMap;

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

    @Before
    public void init() throws Exception {
        brokerController.setMessageStore(messageStore);

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
    public void testUpdateAndCreateTopicOnSlave() throws Exception {
        // setup
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SLAVE);
        defaultMessageStore = mock(DefaultMessageStore.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        // test on slave
        String topic = "TEST_CREATE_TOPIC";
        RemotingCommand request = buildCreateTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("Can't modify topic or subscription group from slave broker, " +
            "please execute it from master broker.");
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
    public void testDeleteTopicOnSlave() throws Exception {
        // setup
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SLAVE);
        defaultMessageStore = mock(DefaultMessageStore.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        String topic = "TEST_DELETE_TOPIC";
        RemotingCommand request = buildDeleteTopicRequest(topic);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("Can't modify topic or subscription group from slave broker, " +
            "please execute it from master broker.");
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
    public void testSearchOffsetByTimestamp() throws Exception {
        messageStore = mock(MessageStore.class);
        when(messageStore.getOffsetInQueueByTime(anyString(), anyInt(), anyLong())).thenReturn(Long.MIN_VALUE);
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
    public void testUpdateAndCreateSubscriptionGroupOnSlave() throws RemotingCommandException {
        // Setup
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SLAVE);
        defaultMessageStore = mock(DefaultMessageStore.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        // Test
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
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("Can't modify topic or subscription group from slave broker, " +
            "please execute it from master broker.");
    }

    @Test
    public void testGetAllSubscriptionGroup() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
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
    public void testDeleteSubscriptionGroupOnSlave() throws RemotingCommandException {
        // Setup
        MessageStoreConfig messageStoreConfig = mock(MessageStoreConfig.class);
        when(messageStoreConfig.getBrokerRole()).thenReturn(BrokerRole.SLAVE);
        defaultMessageStore = mock(DefaultMessageStore.class);
        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);

        // Test
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, null);
        request.addExtField("groupName", "GID-Group-Name");
        request.addExtField("removeOffset", "true");
        RemotingCommand response = adminBrokerProcessor.processRequest(handlerContext, request);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).isEqualTo("Can't modify topic or subscription group from slave broker, " +
            "please execute it from master broker.");
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
}
