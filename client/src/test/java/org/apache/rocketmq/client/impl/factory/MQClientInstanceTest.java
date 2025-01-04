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
package org.apache.rocketmq.client.impl.factory;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MQClientInstanceTest {

    @Mock
    private MQClientAPIImpl mQClientAPIImpl;

    @Mock
    private RemotingClient remotingClient;

    @Mock
    private ClientConfig clientConfig;

    private final MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());

    private final String topic = "FooBar";

    private final String group = "FooBarGroup";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final String defaultBroker = "BrokerA";

    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    @Before
    public void init() throws Exception {
        when(mQClientAPIImpl.getRemotingClient()).thenReturn(remotingClient);
        FieldUtils.writeDeclaredField(mqClientInstance, "brokerAddrTable", brokerAddrTable, true);
        FieldUtils.writeDeclaredField(mqClientInstance, "mQClientAPIImpl", mQClientAPIImpl, true);
        FieldUtils.writeDeclaredField(mqClientInstance, "consumerTable", consumerTable, true);
        FieldUtils.writeDeclaredField(mqClientInstance, "clientConfig", clientConfig, true);
        FieldUtils.writeDeclaredField(mqClientInstance, "topicRouteTable", topicRouteTable, true);
    }

    @Test
    public void testFindBrokerAddressInSubscribe() {
        // dledger normal case
        String brokerName = "BrokerA";
        HashMap<Long, String> addrMap = new HashMap<>();
        addrMap.put(0L, "127.0.0.1:10911");
        addrMap.put(1L, "127.0.0.1:10912");
        addrMap.put(2L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMap);
        long brokerId = 1;
        FindBrokerResult brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();

        // dledger case, when node n0 was voted as the leader
        brokerName = "BrokerB";
        HashMap<Long, String> addrMapNew = new HashMap<>();
        addrMapNew.put(0L, "127.0.0.1:10911");
        addrMapNew.put(2L, "127.0.0.1:10912");
        addrMapNew.put(3L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMapNew);
        brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        assertThat(brokerResult).isNotNull();
        assertThat(brokerResult.getBrokerAddr()).isEqualTo("127.0.0.1:10912");
        assertThat(brokerResult.isSlave()).isTrue();
    }

    @Test
    public void testRegisterProducer() {
        boolean flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterProducer(group);
        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterConsumer() {
        boolean flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testConsumerRunningInfoWhenConsumersIsEmptyOrNot() throws RemotingException, InterruptedException, MQBrokerException {
        MQConsumerInner mockConsumerInner = mock(MQConsumerInner.class);
        ConsumerRunningInfo mockConsumerRunningInfo = mock(ConsumerRunningInfo.class);
        when(mockConsumerInner.consumerRunningInfo()).thenReturn(mockConsumerRunningInfo);
        when(mockConsumerInner.consumeType()).thenReturn(ConsumeType.CONSUME_PASSIVELY);
        Properties properties = new Properties();
        when(mockConsumerRunningInfo.getProperties()).thenReturn(properties);
        mqClientInstance.unregisterConsumer(group);

        ConsumerRunningInfo runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNull();
        boolean flag = mqClientInstance.registerConsumer(group, mockConsumerInner);
        assertThat(flag).isTrue();

        runningInfo = mqClientInstance.consumerRunningInfo(group);
        assertThat(runningInfo).isNotNull();
        assertThat(mockConsumerInner.consumerRunningInfo().getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).isNotNull();

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testRegisterAdminExt() {
        boolean flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();

        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isFalse();

        mqClientInstance.unregisterAdminExt(group);
        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        assertThat(flag).isTrue();
    }

    @Test
    public void testTopicRouteData2TopicPublishInfo() {
        TopicPublishInfo actual = MQClientInstance.topicRouteData2TopicPublishInfo(topic, createTopicRouteData());
        assertThat(actual.isHaveTopicRouterInfo()).isFalse();
        assertThat(actual.getMessageQueueList().size()).isEqualTo(4);
    }

    @Test
    public void testTopicRouteData2TopicPublishInfoWithOrderTopicConf() {
        TopicRouteData topicRouteData = createTopicRouteData();
        when(topicRouteData.getOrderTopicConf()).thenReturn("127.0.0.1:4");
        TopicPublishInfo actual = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
        assertFalse(actual.isHaveTopicRouterInfo());
        assertEquals(4, actual.getMessageQueueList().size());
    }

    @Test
    public void testTopicRouteData2TopicPublishInfoWithTopicQueueMappingByBroker() {
        TopicRouteData topicRouteData = createTopicRouteData();
        when(topicRouteData.getTopicQueueMappingByBroker()).thenReturn(Collections.singletonMap(topic, new TopicQueueMappingInfo()));
        TopicPublishInfo actual = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
        assertFalse(actual.isHaveTopicRouterInfo());
        assertEquals(0, actual.getMessageQueueList().size());
    }

    @Test
    public void testTopicRouteData2TopicSubscribeInfo() {
        TopicRouteData topicRouteData = createTopicRouteData();
        when(topicRouteData.getTopicQueueMappingByBroker()).thenReturn(Collections.singletonMap(topic, new TopicQueueMappingInfo()));
        Set<MessageQueue> actual = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testParseOffsetTableFromBroker() {
        Map<MessageQueue, Long> offsetTable = new HashMap<>();
        offsetTable.put(new MessageQueue(), 0L);
        Map<MessageQueue, Long> actual = mqClientInstance.parseOffsetTableFromBroker(offsetTable, "defaultNamespace");
        assertNotNull(actual);
        assertEquals(1, actual.size());
    }

    @Test
    public void testCheckClientInBroker() throws MQClientException, RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, InterruptedException {
        doThrow(new MQClientException("checkClientInBroker exception", null)).when(mQClientAPIImpl).checkClientInBroker(
                any(),
                any(),
                any(),
                any(SubscriptionData.class),
                anyLong());
        topicRouteTable.put(topic, createTopicRouteData());
        MQConsumerInner mqConsumerInner = createMQConsumerInner();
        mqConsumerInner.subscriptions().clear();
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setExpressionType("type");
        mqConsumerInner.subscriptions().add(subscriptionData);
        consumerTable.put(group, mqConsumerInner);
        Throwable thrown = assertThrows(MQClientException.class, mqClientInstance::checkClientInBroker);
        assertTrue(thrown.getMessage().contains("checkClientInBroker exception"));
    }

    @Test
    public void testSendHeartbeatToBrokerV1() {
        consumerTable.put(group, createMQConsumerInner());
        assertTrue(mqClientInstance.sendHeartbeatToBroker(0L, defaultBroker, defaultBrokerAddr));
    }

    @Test
    public void testSendHeartbeatToBrokerV2() {
        consumerTable.put(group, createMQConsumerInner());
        when(clientConfig.isUseHeartbeatV2()).thenReturn(true);
        assertFalse(mqClientInstance.sendHeartbeatToBroker(0L, defaultBroker, defaultBrokerAddr));
    }

    @Test
    public void testSendHeartbeatToAllBrokerWithLockV1() {
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        assertTrue(mqClientInstance.sendHeartbeatToAllBrokerWithLock());
    }

    @Test
    public void testSendHeartbeatToAllBrokerWithLockV2() {
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        when(clientConfig.isUseHeartbeatV2()).thenReturn(true);
        assertFalse(mqClientInstance.sendHeartbeatToAllBrokerWithLock());
    }

    @Test
    public void testUpdateTopicRouteInfoFromNameServer() throws RemotingException, InterruptedException, MQClientException {
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        DefaultMQProducer defaultMQProducer = mock(DefaultMQProducer.class);
        TopicRouteData topicRouteData = createTopicRouteData();
        when(mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(anyLong())).thenReturn(topicRouteData);
        assertFalse(mqClientInstance.updateTopicRouteInfoFromNameServer(topic, true, defaultMQProducer));
    }

    @Test
    public void testFindBrokerAddressInAdmin() {
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        FindBrokerResult actual = mqClientInstance.findBrokerAddressInAdmin(defaultBroker);
        assertNotNull(actual);
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }

    @Test
    public void testFindBrokerAddressInSubscribeWithOneBroker() throws IllegalAccessException {
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        ConcurrentMap<String, HashMap<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
        HashMap<String, Integer> addressMap = new HashMap<>();
        addressMap.put(defaultBrokerAddr, 0);
        brokerVersionTable.put(defaultBroker, addressMap);
        FieldUtils.writeDeclaredField(mqClientInstance, "brokerVersionTable", brokerVersionTable, true);
        FindBrokerResult actual = mqClientInstance.findBrokerAddressInSubscribe(defaultBroker, 1L, false);
        assertNotNull(actual);
        assertEquals(defaultBrokerAddr, actual.getBrokerAddr());
    }

    @Test
    public void testFindConsumerIdList() {
        topicRouteTable.put(topic, createTopicRouteData());
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        List<String> actual = mqClientInstance.findConsumerIdList(topic, group);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testQueryAssignment() throws MQBrokerException, RemotingException, InterruptedException {
        topicRouteTable.put(topic, createTopicRouteData());
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        Set<MessageQueueAssignment> actual = mqClientInstance.queryAssignment(topic, group, "", MessageModel.CLUSTERING, 1000);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testResetOffset() throws IllegalAccessException {
        topicRouteTable.put(topic, createTopicRouteData());
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        Map<MessageQueue, Long> offsetTable = new HashMap<>();
        offsetTable.put(createMessageQueue(), 0L);
        mqClientInstance.resetOffset(topic, group, offsetTable);
        Field consumerTableField = FieldUtils.getDeclaredField(mqClientInstance.getClass(), "consumerTable", true);
        ConcurrentMap<String, MQConsumerInner> consumerTable = (ConcurrentMap<String, MQConsumerInner>) consumerTableField.get(mqClientInstance);
        DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) consumerTable.get(group);
        verify(consumer).suspend();
        verify(consumer).resume();
        verify(consumer, times(1))
                .updateConsumeOffset(
                        any(MessageQueue.class),
                        eq(0L));
    }

    @Test
    public void testGetConsumerStatus() {
        topicRouteTable.put(topic, createTopicRouteData());
        brokerAddrTable.put(defaultBroker, createBrokerAddrMap());
        consumerTable.put(group, createMQConsumerInner());
        Map<MessageQueue, Long> actual = mqClientInstance.getConsumerStatus(topic, group);
        assertNotNull(actual);
        assertEquals(0, actual.size());
    }

    @Test
    public void testGetAnExistTopicRouteData() {
        topicRouteTable.put(topic, createTopicRouteData());
        TopicRouteData actual = mqClientInstance.getAnExistTopicRouteData(topic);
        assertNotNull(actual);
        assertNotNull(actual.getQueueDatas());
        assertNotNull(actual.getBrokerDatas());
    }

    @Test
    public void testConsumeMessageDirectly() {
        consumerTable.put(group, createMQConsumerInner());
        assertNull(mqClientInstance.consumeMessageDirectly(createMessageExt(), group, defaultBroker));
    }

    @Test
    public void testQueryTopicRouteData() {
        consumerTable.put(group, createMQConsumerInner());
        topicRouteTable.put(topic, createTopicRouteData());
        TopicRouteData actual = mqClientInstance.queryTopicRouteData(topic);
        assertNotNull(actual);
        assertNotNull(actual.getQueueDatas());
        assertNotNull(actual.getBrokerDatas());
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setBody("body".getBytes(StandardCharsets.UTF_8));
        result.setTopic(topic);
        result.setBrokerName(defaultBroker);
        result.putUserProperty("key", "value");
        result.getProperties().put(MessageConst.PROPERTY_PRODUCER_GROUP, group);
        result.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "TX1");
        long curTime = System.currentTimeMillis();
        result.setBornTimestamp(curTime - 1000);
        result.getProperties().put(MessageConst.PROPERTY_POP_CK, curTime + " " + curTime + " " + curTime + " " + curTime);
        result.setKeys("keys");
        result.setSysFlag(MessageSysFlag.INNER_BATCH_FLAG);
        result.setSysFlag(result.getSysFlag() | MessageSysFlag.NEED_UNWRAP_FLAG);
        SocketAddress bornHost = new InetSocketAddress("127.0.0.1", 12911);
        SocketAddress storeHost = new InetSocketAddress("127.0.0.1", 10911);
        result.setBornHost(bornHost);
        result.setStoreHost(storeHost);
        return result;
    }

    private MessageQueue createMessageQueue() {
        MessageQueue result = new MessageQueue();
        result.setQueueId(0);
        result.setBrokerName(defaultBroker);
        result.setTopic(topic);
        return result;
    }

    private TopicRouteData createTopicRouteData() {
        TopicRouteData result = mock(TopicRouteData.class);
        when(result.getBrokerDatas()).thenReturn(createBrokerDatas());
        when(result.getQueueDatas()).thenReturn(createQueueDatas());
        return result;
    }

    private HashMap<Long, String> createBrokerAddrMap() {
        HashMap<Long, String> result = new HashMap<>();
        result.put(0L, defaultBrokerAddr);
        return result;
    }

    private MQConsumerInner createMQConsumerInner() {
        DefaultMQPushConsumerImpl result = mock(DefaultMQPushConsumerImpl.class);
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        subscriptionDataSet.add(subscriptionData);
        when(result.subscriptions()).thenReturn(subscriptionDataSet);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueMap = new ConcurrentHashMap<>();
        ProcessQueue processQueue = new ProcessQueue();
        processQueueMap.put(createMessageQueue(), processQueue);
        when(rebalanceImpl.getProcessQueueTable()).thenReturn(processQueueMap);
        when(result.getRebalanceImpl()).thenReturn(rebalanceImpl);
        OffsetStore offsetStore = mock(OffsetStore.class);
        when(result.getOffsetStore()).thenReturn(offsetStore);
        ConsumeMessageService consumeMessageService = mock(ConsumeMessageService.class);
        when(result.getConsumeMessageService()).thenReturn(consumeMessageService);
        return result;
    }

    private List<QueueData> createQueueDatas() {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(defaultBroker);
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        return Collections.singletonList(queueData);
    }

    private List<BrokerData> createBrokerDatas() {
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(defaultBroker);
        String defaultCluster = "defaultCluster";
        brokerData.setCluster(defaultCluster);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, defaultBrokerAddr);
        brokerData.setBrokerAddrs(brokerAddrs);
        return Collections.singletonList(brokerData);
    }
}
