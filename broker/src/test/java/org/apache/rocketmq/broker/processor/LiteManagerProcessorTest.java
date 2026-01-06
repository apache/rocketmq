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

import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.lite.AbstractLiteLifecycleManager;
import org.apache.rocketmq.broker.lite.LiteEventDispatcher;
import org.apache.rocketmq.broker.lite.LiteSharding;
import org.apache.rocketmq.broker.lite.LiteSubscriptionRegistry;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.metrics.LiteConsumerLagCalculator;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.MemoryConsumerOrderInfoManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.entity.ClientGroup;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.common.lite.LiteSubscription;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
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
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteManagerProcessorTest {

    @Mock
    private BrokerController brokerController;

    @Mock
    private AbstractLiteLifecycleManager liteLifecycleManager;

    @Mock
    private LiteSharding liteSharding;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private MessageStoreConfig messageStoreConfig;

    @Mock
    private MessageStore messageStore;

    @Mock
    private ConsumeQueueStoreInterface consumeQueueStore;

    @Mock
    private TopicConfigManager topicConfigManager;

    @Mock
    private SubscriptionGroupManager subscriptionGroupManager;

    @Mock
    private LiteSubscriptionRegistry liteSubscriptionRegistry;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Mock
    private BrokerMetricsManager brokerMetricsManager;

    @Mock
    private LiteConsumerLagCalculator liteConsumerLagCalculator;

    @Mock
    private LiteEventDispatcher liteEventDispatcher;

    @Mock
    private PopLiteMessageProcessor popLiteMessageProcessor;

    private LiteManagerProcessor processor;

    @Before
    public void setUp() {
        processor = new LiteManagerProcessor(brokerController, liteLifecycleManager, liteSharding);

        when(brokerController.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        when(brokerController.getMessageStore()).thenReturn(messageStore);
        when(brokerController.getTopicConfigManager()).thenReturn(topicConfigManager);
        when(brokerController.getSubscriptionGroupManager()).thenReturn(subscriptionGroupManager);
        when(brokerController.getLiteSubscriptionRegistry()).thenReturn(liteSubscriptionRegistry);
        when(brokerController.getBrokerMetricsManager()).thenReturn(brokerMetricsManager);
        when(brokerController.getLiteEventDispatcher()).thenReturn(liteEventDispatcher);
        when(brokerController.getPopLiteMessageProcessor()).thenReturn(popLiteMessageProcessor);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);

        ConsumerOrderInfoManager consumerOrderInfoManager = new MemoryConsumerOrderInfoManager(brokerController);
        when(popLiteMessageProcessor.getConsumerOrderInfoManager()).thenReturn(consumerOrderInfoManager);

        when(messageStore.getQueueStore()).thenReturn(consumeQueueStore);
        when(consumeQueueStore.getConsumeQueueTable()).thenReturn(new ConcurrentHashMap<>());
        when(brokerMetricsManager.getLiteConsumerLagCalculator()).thenReturn(liteConsumerLagCalculator);

        when(consumerOffsetManager.getOffsetTable()).thenReturn(new ConcurrentHashMap<>());
    }

    @Test
    public void testProcessRequest_GetBrokerLiteInfo() throws Exception {
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(RequestCode.GET_BROKER_LITE_INFO);

        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigTable);

        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>();
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroupTable);

        RemotingCommand response = processor.processRequest(ctx, request);

        assertNotNull(response);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProcessRequest_UnsupportedRequestCode() throws Exception {
        RemotingCommand request = mock(RemotingCommand.class);
        when(request.getCode()).thenReturn(99999);

        assertNull(processor.processRequest(ctx, request));
    }

    @Test
    public void testGetBrokerLiteInfo() throws RemotingCommandException {
        when(messageStoreConfig.getStoreType()).thenReturn("RocksDB");
        when(messageStoreConfig.getMaxLmqConsumeQueueNum()).thenReturn(10000);
        when(consumeQueueStore.getLmqNum()).thenReturn(100);
        when(liteSubscriptionRegistry.getActiveSubscriptionNum()).thenReturn(50);

        ConcurrentHashMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<>();
        topicConfigMap.put("SYSTEM_TOPIC", new TopicConfig("SYSTEM_TOPIC"));
        when(topicConfigManager.getTopicConfigTable()).thenReturn(topicConfigMap);

        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupMap = new ConcurrentHashMap<>();
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("test_group");
        config.setLiteBindTopic("test_topic");
        subscriptionGroupMap.put("test_group", config);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroupMap);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_LITE_INFO, null);

        RemotingCommand response = processor.getBrokerLiteInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetBrokerLiteInfoResponseBody body = GetBrokerLiteInfoResponseBody.decode(response.getBody(), GetBrokerLiteInfoResponseBody.class);
        assertEquals("RocksDB", body.getStoreType());
        assertEquals(10000, body.getMaxLmqNum());
        assertEquals(100, body.getCurrentLmqNum());
        assertEquals(50, body.getLiteSubscriptionCount());
        assertNotNull(body.getTopicMeta());
        assertNotNull(body.getGroupMeta());
    }

    @Test
    public void testGetParentTopicInfo_TopicNotExist() throws RemotingCommandException {
        GetParentTopicInfoRequestHeader requestHeader = new GetParentTopicInfoRequestHeader();
        requestHeader.setTopic("nonexistent_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PARENT_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        when(topicConfigManager.selectTopicConfig("nonexistent_topic")).thenReturn(null);

        RemotingCommand response = processor.getParentTopicInfo(ctx, request);

        assertEquals(ResponseCode.TOPIC_NOT_EXIST, response.getCode());
        assertTrue(response.getRemark().contains("nonexistent_topic"));
    }

    @Test
    public void testGetParentTopicInfo_InvalidTopicType() throws RemotingCommandException {
        GetParentTopicInfoRequestHeader requestHeader = new GetParentTopicInfoRequestHeader();
        requestHeader.setTopic("invalid_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PARENT_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("invalid_topic");
        topicConfig.setTopicMessageType(TopicMessageType.NORMAL);

        when(topicConfigManager.selectTopicConfig("invalid_topic")).thenReturn(topicConfig);

        RemotingCommand response = processor.getParentTopicInfo(ctx, request);

        assertEquals(ResponseCode.INVALID_PARAMETER, response.getCode());
        assertTrue(response.getRemark().contains("invalid_topic"));
    }

    @Test
    public void testGetParentTopicInfo_Success() throws RemotingCommandException {
        GetParentTopicInfoRequestHeader requestHeader = new GetParentTopicInfoRequestHeader();
        requestHeader.setTopic("parent_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PARENT_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("parent_topic");
        topicConfig.setTopicMessageType(TopicMessageType.LITE);
        topicConfig.setLiteTopicExpiration(3600);

        when(topicConfigManager.selectTopicConfig("parent_topic")).thenReturn(topicConfig);
        when(consumeQueueStore.getLmqNum()).thenReturn(200);
        when(liteLifecycleManager.getLiteTopicCount("parent_topic")).thenReturn(10);

        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptionGroupMap = new ConcurrentHashMap<>();
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("test_group");
        config.setLiteBindTopic("parent_topic");
        subscriptionGroupMap.put("test_group", config);
        when(subscriptionGroupManager.getSubscriptionGroupTable()).thenReturn(subscriptionGroupMap);

        RemotingCommand response = processor.getParentTopicInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetParentTopicInfoResponseBody body = GetParentTopicInfoResponseBody.decode(response.getBody(), GetParentTopicInfoResponseBody.class);
        assertEquals("parent_topic", body.getTopic());
        assertEquals(3600, body.getTtl());
        assertEquals(200, body.getLmqNum());
        assertEquals(10, body.getLiteTopicCount());
        assertTrue(body.getGroups().contains("test_group"));
    }

    @Test
    public void testGetLiteTopicInfo_ParentTopicNotExist() throws RemotingCommandException {
        GetLiteTopicInfoRequestHeader requestHeader = new GetLiteTopicInfoRequestHeader();
        requestHeader.setParentTopic("nonexistent_parent");
        requestHeader.setLiteTopic("lite_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        when(topicConfigManager.selectTopicConfig("nonexistent_parent")).thenReturn(null);

        RemotingCommand response = processor.getLiteTopicInfo(ctx, request);

        assertEquals(ResponseCode.TOPIC_NOT_EXIST, response.getCode());
        assertTrue(response.getRemark().contains("nonexistent_parent"));
    }

    @Test
    public void testGetLiteTopicInfo_InvalidParentTopicType() throws RemotingCommandException {
        GetLiteTopicInfoRequestHeader requestHeader = new GetLiteTopicInfoRequestHeader();
        requestHeader.setParentTopic("invalid_parent");
        requestHeader.setLiteTopic("lite_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("invalid_parent");
        topicConfig.setTopicMessageType(TopicMessageType.NORMAL);

        when(topicConfigManager.selectTopicConfig("invalid_parent")).thenReturn(topicConfig);

        RemotingCommand response = processor.getLiteTopicInfo(ctx, request);

        assertEquals(ResponseCode.INVALID_PARAMETER, response.getCode());
        assertTrue(response.getRemark().contains("invalid_parent"));
    }

    @Test
    public void testGetLiteTopicInfo_Success() throws RemotingCommandException {
        GetLiteTopicInfoRequestHeader requestHeader = new GetLiteTopicInfoRequestHeader();
        requestHeader.setParentTopic("parent_topic");
        requestHeader.setLiteTopic("lite_topic");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_TOPIC_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("parent_topic");
        topicConfig.setTopicMessageType(TopicMessageType.LITE);

        String lmqName = LiteUtil.toLmqName("parent_topic", "lite_topic");
        long maxOffset = 100L;
        long minOffset = 10L;
        long lastUpdateTimestamp = System.currentTimeMillis();

        when(topicConfigManager.selectTopicConfig("parent_topic")).thenReturn(topicConfig);
        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);
        when(messageStore.getMinOffsetInQueue(lmqName, 0)).thenReturn(minOffset);
        when(messageStore.getMessageStoreTimeStamp(lmqName, 0, maxOffset - 1)).thenReturn(lastUpdateTimestamp);
        Set<ClientGroup> subscribers = new HashSet<>();
        subscribers.add(new ClientGroup("clientId1", "group1"));
        when(liteSubscriptionRegistry.getSubscriber(lmqName)).thenReturn(subscribers);
        when(brokerController.getBrokerConfig()).thenReturn(mock(BrokerConfig.class));
        when(brokerController.getBrokerConfig().getBrokerName()).thenReturn("broker1");
        when(liteSharding.shardingByLmqName("parent_topic", lmqName)).thenReturn("broker1");

        RemotingCommand response = processor.getLiteTopicInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteTopicInfoResponseBody body = GetLiteTopicInfoResponseBody.decode(response.getBody(), GetLiteTopicInfoResponseBody.class);
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("lite_topic", body.getLiteTopic());
        assertEquals(subscribers, body.getSubscriber());

        TopicOffset topicOffset = body.getTopicOffset();
        assertEquals(minOffset, topicOffset.getMinOffset());
        assertEquals(maxOffset, topicOffset.getMaxOffset());
        assertEquals(lastUpdateTimestamp, topicOffset.getLastUpdateTimestamp());
        assertTrue(body.isShardingToBroker());
    }

    @Test
    public void testGetLiteClientInfo_ParentTopicNotExist() throws RemotingCommandException {
        GetLiteClientInfoRequestHeader requestHeader = new GetLiteClientInfoRequestHeader();
        requestHeader.setParentTopic("nonexistent_parent");
        requestHeader.setGroup("group1");
        requestHeader.setClientId("client1");
        requestHeader.setMaxCount(100);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_CLIENT_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        when(topicConfigManager.selectTopicConfig("nonexistent_parent")).thenReturn(null);

        RemotingCommand response = processor.getLiteClientInfo(ctx, request);

        assertEquals(ResponseCode.TOPIC_NOT_EXIST, response.getCode());
        assertTrue(response.getRemark().contains("nonexistent_parent"));
    }

    @Test
    public void testGetLiteClientInfo_GroupNotExist() throws RemotingCommandException {
        GetLiteClientInfoRequestHeader requestHeader = new GetLiteClientInfoRequestHeader();
        requestHeader.setParentTopic("parent_topic");
        requestHeader.setGroup("nonexistent_group");
        requestHeader.setClientId("client1");
        requestHeader.setMaxCount(100);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_CLIENT_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("parent_topic");
        topicConfig.setTopicMessageType(TopicMessageType.LITE);

        when(topicConfigManager.selectTopicConfig("parent_topic")).thenReturn(topicConfig);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("nonexistent_group")).thenReturn(null);

        RemotingCommand response = processor.getLiteClientInfo(ctx, request);

        assertEquals(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, response.getCode());
        assertTrue(response.getRemark().contains("nonexistent_group"));
    }

    @Test
    public void testGetLiteClientInfo_NoSubscription() throws RemotingCommandException {
        GetLiteClientInfoRequestHeader requestHeader = new GetLiteClientInfoRequestHeader();
        requestHeader.setParentTopic("parent_topic");
        requestHeader.setGroup("group1");
        requestHeader.setClientId("client1");
        requestHeader.setMaxCount(100);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_CLIENT_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("parent_topic");
        topicConfig.setTopicMessageType(TopicMessageType.LITE);

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("group1");
        groupConfig.setLiteBindTopic("parent_topic");

        when(topicConfigManager.selectTopicConfig("parent_topic")).thenReturn(topicConfig);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("group1")).thenReturn(groupConfig);
        when(liteSubscriptionRegistry.getLiteSubscription("client1")).thenReturn(null);

        RemotingCommand response = processor.getLiteClientInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteClientInfoResponseBody body = GetLiteClientInfoResponseBody.decode(response.getBody(), GetLiteClientInfoResponseBody.class);
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("group1", body.getGroup());
        assertEquals("client1", body.getClientId());
        assertEquals(-1, body.getLiteTopicCount());
        assertNull(body.getLiteTopicSet());
    }

    @Test
    public void testGetLiteClientInfo_WithSubscription() throws RemotingCommandException {
        GetLiteClientInfoRequestHeader requestHeader = new GetLiteClientInfoRequestHeader();
        requestHeader.setParentTopic("parent_topic");
        requestHeader.setGroup("group1");
        requestHeader.setClientId("client1");
        requestHeader.setMaxCount(100);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_CLIENT_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName("parent_topic");
        topicConfig.setTopicMessageType(TopicMessageType.LITE);

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("group1");
        groupConfig.setLiteBindTopic("parent_topic");

        Set<String> liteTopicSet = new HashSet<>();
        liteTopicSet.add("lite_topic1");
        liteTopicSet.add("lite_topic2");

        LiteSubscription liteSubscription = new LiteSubscription();
        liteSubscription.setLiteTopicSet(liteTopicSet);

        when(topicConfigManager.selectTopicConfig("parent_topic")).thenReturn(topicConfig);
        when(subscriptionGroupManager.findSubscriptionGroupConfig("group1")).thenReturn(groupConfig);
        when(liteSubscriptionRegistry.getLiteSubscription("client1")).thenReturn(liteSubscription);

        RemotingCommand response = processor.getLiteClientInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteClientInfoResponseBody body = GetLiteClientInfoResponseBody.decode(response.getBody(), GetLiteClientInfoResponseBody.class);
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("group1", body.getGroup());
        assertEquals("client1", body.getClientId());
        assertEquals(2, body.getLiteTopicCount());
        assertEquals(liteTopicSet, body.getLiteTopicSet());
    }

    @Test
    public void testGetLiteGroupInfo_GroupNotExist() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("nonexistent_group");
        requestHeader.setLiteTopic("");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        when(subscriptionGroupManager.findSubscriptionGroupConfig("nonexistent_group")).thenReturn(null);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, response.getCode());
        assertTrue(response.getRemark().contains("nonexistent_group"));
    }

    @Test
    public void testGetLiteGroupInfo_NotLiteGroup() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("normal_group");
        requestHeader.setLiteTopic("");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("normal_group");
        groupConfig.setLiteBindTopic("");

        when(subscriptionGroupManager.findSubscriptionGroupConfig("normal_group")).thenReturn(groupConfig);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.INVALID_PARAMETER, response.getCode());
        assertTrue(response.getRemark().contains("normal_group"));
        assertTrue(response.getRemark().contains("not a LITE group"));
    }

    @Test
    public void testGetLiteGroupInfo_GetTopKInfo() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("lite_group");
        requestHeader.setLiteTopic("");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("lite_group");
        groupConfig.setLiteBindTopic("parent_topic");

        List<LiteLagInfo> lagCountList = new ArrayList<>();
        LiteLagInfo lagCountInfo = new LiteLagInfo();
        lagCountInfo.setLiteTopic("topic1");
        lagCountInfo.setLagCount(100L);
        lagCountList.add(lagCountInfo);
        Pair<List<LiteLagInfo>, Long> lagCountPair = new Pair<>(lagCountList, 100L);

        List<LiteLagInfo> lagTimeList = new ArrayList<>();
        LiteLagInfo lagTimeInfo = new LiteLagInfo();
        lagTimeInfo.setLiteTopic("topic1");
        lagTimeInfo.setEarliestUnconsumedTimestamp(System.currentTimeMillis());
        lagTimeList.add(lagTimeInfo);
        Pair<List<LiteLagInfo>, Long> lagTimePair = new Pair<>(lagTimeList, System.currentTimeMillis());

        when(subscriptionGroupManager.findSubscriptionGroupConfig("lite_group")).thenReturn(groupConfig);
        when(liteConsumerLagCalculator.getLagCountTopK("lite_group", 10)).thenReturn(lagCountPair);
        when(liteConsumerLagCalculator.getLagTimestampTopK("lite_group", "parent_topic", 10)).thenReturn(lagTimePair);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteGroupInfoResponseBody body = GetLiteGroupInfoResponseBody.decode(response.getBody(), GetLiteGroupInfoResponseBody.class);
        assertEquals("lite_group", body.getGroup());
        assertEquals("parent_topic", body.getParentTopic());
        assertTrue(StringUtils.isEmpty(body.getLiteTopic()));
        List<LiteLagInfo> actualLagCountList = body.getLagCountTopK();
        assertEquals(lagCountList.size(), actualLagCountList.size());
        for (int i = 0; i < lagCountList.size(); i++) {
            LiteLagInfo expected = lagCountList.get(i);
            LiteLagInfo actual = actualLagCountList.get(i);
            assertEquals(expected.getLiteTopic(), actual.getLiteTopic());
            assertEquals(expected.getLagCount(), actual.getLagCount());
        }
        assertEquals(Long.valueOf(100L), Long.valueOf(body.getTotalLagCount()));
        List<LiteLagInfo> actualLagTimeList = body.getLagTimestampTopK();
        assertEquals(lagTimeList.size(), actualLagTimeList.size());
        for (int i = 0; i < lagTimeList.size(); i++) {
            LiteLagInfo expected = lagTimeList.get(i);
            LiteLagInfo actual = actualLagTimeList.get(i);
            assertEquals(expected.getLiteTopic(), actual.getLiteTopic());
            assertEquals(expected.getEarliestUnconsumedTimestamp(), actual.getEarliestUnconsumedTimestamp());
        }
    }

    @Test
    public void testGetLiteGroupInfo_SpecificLiteTopic_WithMessages() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("lite_group");
        requestHeader.setLiteTopic("specific_lite_topic");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("lite_group");
        groupConfig.setLiteBindTopic("parent_topic");

        String lmqName = LiteUtil.toLmqName("parent_topic", "specific_lite_topic");
        long maxOffset = 100L;
        long commitOffset = 50L;
        long messageTimestamp = System.currentTimeMillis() - 10000;

        when(subscriptionGroupManager.findSubscriptionGroupConfig("lite_group")).thenReturn(groupConfig);
        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);
        when(consumerOffsetManager.queryOffset("lite_group", lmqName, 0)).thenReturn(commitOffset);
        when(messageStore.getMessageStoreTimeStamp(lmqName, 0, commitOffset)).thenReturn(messageTimestamp);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteGroupInfoResponseBody body = GetLiteGroupInfoResponseBody.decode(response.getBody(), GetLiteGroupInfoResponseBody.class);
        assertEquals("lite_group", body.getGroup());
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("specific_lite_topic", body.getLiteTopic());
        assertEquals(maxOffset - commitOffset, body.getTotalLagCount());
        assertEquals(messageTimestamp, body.getEarliestUnconsumedTimestamp());
    }

    @Test
    public void testGetLiteGroupInfo_SpecificLiteTopic_WithoutMessages() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("lite_group");
        requestHeader.setLiteTopic("specific_lite_topic");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("lite_group");
        groupConfig.setLiteBindTopic("parent_topic");

        String lmqName = LiteUtil.toLmqName("parent_topic", "specific_lite_topic");
        long maxOffset = 0L;

        when(subscriptionGroupManager.findSubscriptionGroupConfig("lite_group")).thenReturn(groupConfig);
        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteGroupInfoResponseBody body = GetLiteGroupInfoResponseBody.decode(response.getBody(), GetLiteGroupInfoResponseBody.class);
        assertEquals("lite_group", body.getGroup());
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("specific_lite_topic", body.getLiteTopic());
        assertEquals(-1, body.getTotalLagCount());
        assertEquals(-1L, body.getEarliestUnconsumedTimestamp());
    }

    @Test
    public void testGetLiteGroupInfo_SpecificLiteTopic_ZeroCommitOffset() throws RemotingCommandException {
        GetLiteGroupInfoRequestHeader requestHeader = new GetLiteGroupInfoRequestHeader();
        requestHeader.setGroup("lite_group");
        requestHeader.setLiteTopic("specific_lite_topic");
        requestHeader.setTopK(10);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_LITE_GROUP_INFO, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName("lite_group");
        groupConfig.setLiteBindTopic("parent_topic");

        String lmqName = LiteUtil.toLmqName("parent_topic", "specific_lite_topic");
        long maxOffset = 100L;
        long commitOffset = 0L;

        when(subscriptionGroupManager.findSubscriptionGroupConfig("lite_group")).thenReturn(groupConfig);
        when(liteLifecycleManager.getMaxOffsetInQueue(lmqName)).thenReturn(maxOffset);
        when(consumerOffsetManager.queryOffset("lite_group", lmqName, 0)).thenReturn(commitOffset);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);

        RemotingCommand response = processor.getLiteGroupInfo(ctx, request);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        GetLiteGroupInfoResponseBody body = GetLiteGroupInfoResponseBody.decode(response.getBody(), GetLiteGroupInfoResponseBody.class);
        assertEquals("lite_group", body.getGroup());
        assertEquals("parent_topic", body.getParentTopic());
        assertEquals("specific_lite_topic", body.getLiteTopic());
        assertEquals(maxOffset - commitOffset, body.getTotalLagCount());
        assertEquals(0, body.getEarliestUnconsumedTimestamp());
    }

    @Test
    public void testTriggerLiteDispatch() throws Exception {
        String group = "group";
        String clientId = "clientId";
        TriggerLiteDispatchRequestHeader requestHeader;

        // with clientId
        requestHeader = new TriggerLiteDispatchRequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setClientId(clientId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.TRIGGER_LITE_DISPATCH, requestHeader);
        request.makeCustomHeaderToNet();

        SubscriptionGroupConfig groupConfig = new SubscriptionGroupConfig();
        groupConfig.setGroupName(group);
        groupConfig.setLiteBindTopic("parent_topic");
        when(subscriptionGroupManager.findSubscriptionGroupConfig(group)).thenReturn(groupConfig);

        RemotingCommand response = processor.triggerLiteDispatch(ctx, request);

        assertNotNull(response);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId, group);
        verify(liteEventDispatcher, never()).doFullDispatchByGroup(group);

        // without clientId
        requestHeader = new TriggerLiteDispatchRequestHeader();
        requestHeader.setGroup(group);
        request = RemotingCommand.createRequestCommand(RequestCode.TRIGGER_LITE_DISPATCH, requestHeader);
        request.makeCustomHeaderToNet();

        response = processor.triggerLiteDispatch(ctx, request);

        assertNotNull(response);
        assertEquals(ResponseCode.SUCCESS, response.getCode());
        verify(liteEventDispatcher, times(1)).doFullDispatch(clientId, group);
        verify(liteEventDispatcher, times(1)).doFullDispatchByGroup(group);
    }
}
