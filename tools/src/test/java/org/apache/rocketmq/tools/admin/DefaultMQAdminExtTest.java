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
package org.apache.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.admin.common.AdminToolResult;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.apache.rocketmq.tools.utils.MQAdminTestUtils;
import org.assertj.core.util.Maps;
import org.junit.*;
import org.apache.rocketmq.tools.admin.common.AdminToolResult;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.assertj.core.util.Maps;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQAdminExtTest {
    private static final String broker1Addr = "127.0.0.1:45676";
    private static final String broker1Name = "default-broker";
    private static final String cluster = "default-cluster";
    private static final String broker2Name = "broker-test";
    private static final String broker2Addr = "127.0.0.2:45676";
    private static final String topic1 = "%RETRY%default-consumer-group";
    private static final String topic2 = "%RETRY%default-consumer-group_1";
    private static final int NAME_SERVER_PORT = 45677;
    private static final int BROKER_PORT = 45676;
    protected static int topicCreateTime = 30 * 1000;
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance;
    private static MQClientAPIImpl mQClientAPIImpl;
    private static Properties properties = new Properties();
    private static TopicList topicList = new TopicList();
    private static TopicRouteData topicRouteData = new TopicRouteData();
    private static KVTable kvTable = new KVTable();
    private static ClusterInfo clusterInfo = new ClusterInfo();
    private static ServerResponseMocker brokerMocker = mock(ServerResponseMocker.class);
    private static ServerResponseMocker nameServerMocker = mock(ServerResponseMocker.class);
    
    @BeforeClass
    public static void init() throws Exception {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt("Test_Admin_Ext");
        defaultMQAdminExt.setNamesrvAddr("127.0.0.1:" + NAME_SERVER_PORT);
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 40000);
        mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
        
        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        properties.setProperty("maxMessageSize", "5000000");
        properties.setProperty("flushDelayOffsetInterval", "15000");
        properties.setProperty("serverSocketRcvBufSize", "655350");
        when(mQClientAPIImpl.getBrokerConfig(anyString(), anyLong())).thenReturn(properties);

        Set<String> topicSet = new HashSet<>();
        topicSet.add("topic_one");
        topicSet.add("topic_two");
        topicList.setTopicList(topicSet);
        when(mQClientAPIImpl.getTopicListFromNameServer(anyLong())).thenReturn(topicList);

        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1234l, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster(cluster);
        brokerData.setBrokerName(broker1Name);
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        HashMap<String, String> result = new HashMap<>();
        result.put("id", "1234");
        result.put("brokerName", "default-broker");
        kvTable.setTable(result);
        when(mQClientAPIImpl.getBrokerRuntimeInfo(anyString(), anyLong())).thenReturn(kvTable);

        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put(cluster, brokerData);
        brokerAddrTable.put("broker-test", new BrokerData());
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(new HashMap<String, Set<String>>());
        when(mQClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);
        when(mQClientAPIImpl.cleanExpiredConsumeQueue(anyString(), anyLong())).thenReturn(true);

        Set<String> clusterList = new HashSet<>();
        clusterList.add("default-cluster-one");
        clusterList.add("default-cluster-two");
        when(mQClientAPIImpl.getClusterList(anyString(), anyLong())).thenReturn(clusterList);

        GroupList groupList = new GroupList();
        HashSet<String> groups = new HashSet<>();
        groups.add("consumer-group-one");
        groups.add("consumer-group-two");
        groupList.setGroupList(groups);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        when(mQClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);

        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptions = new ConcurrentHashMap<>();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setBrokerId(1234);
        subscriptionGroupConfig.setGroupName("Consumer-group-one");
        subscriptions.put("Consumer-group-one", subscriptionGroupConfig);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptions);
        when(mQClientAPIImpl.getAllSubscriptionGroup(anyString(), anyLong())).thenReturn(subscriptionGroupWrapper);

        String topicListConfig = "topicListConfig";
        when(mQClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenReturn(topicListConfig);

        KVTable kvTable = new KVTable();
        HashMap<String, String> kv = new HashMap<>();
        kv.put("broker-name", "broker-one");
        kv.put("cluster-name", "default-cluster");
        kvTable.setTable(kv);
        when(mQClientAPIImpl.getKVListByNamespace(anyString(), anyLong())).thenReturn(kvTable);

//        ConsumeStats consumeStats = new ConsumeStats();
//        consumeStats.setConsumeTps(1234);
//        MessageQueue messageQueue = new MessageQueue();
//        OffsetWrapper offsetWrapper = new OffsetWrapper();
//        HashMap<MessageQueue, OffsetWrapper> stats = new HashMap<>();
//        stats.put(messageQueue, offsetWrapper);
//        consumeStats.setOffsetTable(stats);
//        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), anyString(), anyLong())).thenReturn(consumeStats);

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerConnection.setMessageModel(MessageModel.CLUSTERING);
        HashSet<Connection> connections = new HashSet<>();
        connections.add(new Connection());
        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<String, SubscriptionData>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        ProducerConnection producerConnection = new ProducerConnection();
        Connection connection = new Connection();
        connection.setClientAddr("127.0.0.1:9898");
        connection.setClientId("PID_12345");
        HashSet<Connection> connectionSet = new HashSet<Connection>();
        connectionSet.add(connection);
        producerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getProducerConnectionList(anyString(), anyString(), anyLong())).thenReturn(producerConnection);

        when(mQClientAPIImpl.wipeWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(6);
        when(mQClientAPIImpl.addWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(7);

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(new HashMap<MessageQueue, TopicOffset>());

        Map<String, Map<MessageQueue, Long>> consumerStatus = new HashMap<>();
        when(mQClientAPIImpl.invokeBrokerToGetConsumerStatus(anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(consumerStatus);

        List<QueueTimeSpan> queueTimeSpanList = new ArrayList<>();
        when(mQClientAPIImpl.queryConsumeTimeSpan(anyString(), anyString(), anyString(), anyLong())).thenReturn(queueTimeSpanList);

        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), anyString(), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(new ConcurrentHashMap<String, TopicConfig>() {
            {
                put("topic_test_examine_topicConfig", new TopicConfig("topic_test_examine_topicConfig"));
            }
        });
        //when(mQClientAPIImpl.getAllTopicConfig(anyString(),anyLong())).thenReturn(topicConfigSerializeWrapper);
        when(mQClientAPIImpl.getTopicConfig(anyString(), anyString(), anyLong())).thenReturn(new TopicConfigAndQueueMapping(new TopicConfig("topic_test_examine_topicConfig"), null));
    }

    @AfterClass
    public static void terminate() throws Exception {
        if (defaultMQAdminExtImpl != null)
            defaultMQAdminExt.shutdown();
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testUpdateBrokerConfig() throws InterruptedException, RemotingConnectException, UnsupportedEncodingException, RemotingTimeoutException, MQBrokerException, RemotingSendRequestException {
        Properties result = defaultMQAdminExt.getBrokerConfig("127.0.0.1:10911");
        assertThat(result.getProperty("maxMessageSize")).isEqualTo("5000000");
        assertThat(result.getProperty("flushDelayOffsetInterval")).isEqualTo("15000");
        assertThat(result.getProperty("serverSocketRcvBufSize")).isEqualTo("655350");
    }

    @Test
    public void testFetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        assertThat(topicList.getTopicList().size()).isEqualTo(2);
        assertThat(topicList.getTopicList()).contains("topic_one");
    }

    @Test
    public void testFetchBrokerRuntimeStats() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        KVTable brokerStats = defaultMQAdminExt.fetchBrokerRuntimeStats("127.0.0.1:10911");
        assertThat(brokerStats.getTable().get("id")).isEqualTo(String.valueOf(MixAll.MASTER_ID));
        assertThat(brokerStats.getTable().get("brokerName")).isEqualTo("default-broker");
    }

    @Test
    public void testExamineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        Map<String, BrokerData> brokerList = clusterInfo.getBrokerAddrTable();
        assertThat(brokerList.get("default-broker").getBrokerName()).isEqualTo("default-broker");
        assertThat(brokerList.containsKey("broker-test")).isTrue();

        HashMap<String, Set<String>> clusterMap = new HashMap<>();
        Set<String> brokers = new HashSet<>();
        brokers.add("default-broker");
        brokers.add("broker-test");
        clusterMap.put("default-cluster", brokers);
        ClusterInfo cInfo = mock(ClusterInfo.class);
        when(cInfo.getClusterAddrTable()).thenReturn(clusterMap);
        Map<String, Set<String>> clusterAddress = cInfo.getClusterAddrTable();
        assertThat(clusterAddress.containsKey("default-cluster")).isTrue();
        assertThat(clusterAddress.get("default-cluster").size()).isEqualTo(2);
    }

    @Ignore
    @Test
    public void testExamineConsumeStats() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats("default-consumer-group", "unit-test");
        assertThat(consumeStats.getConsumeTps()).isGreaterThanOrEqualTo(1234);
    }

    @Test
    public void testExamineConsumerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo("default-consumer-group");
        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);

        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);
    }

    @Test
    public void testexamineConsumerConnectionInfoConcurrent() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
    
        awaitUtilDefaultMQAdminExtStart(cluster);
    
        defaultMQAdminExt.setNamesrvAddr("localhost:"+NAME_SERVER_PORT);
    
        TopicConfig topicConfig = new TopicConfig(topic1, 1, 1, 6, 0);
        defaultMQAdminExt.createAndUpdateTopicConfig(broker1Addr, topicConfig);
    
        defaultMQAdminExt.setCreateTopicKey(broker1Name);
        defaultMQAdminExt.createTopic(broker1Name,topic1, 8,new HashMap<>());
        defaultMQAdminExt.createTopic(broker1Name,topic2,8,new HashMap<>());
        
        AdminToolResult<ConsumerConnection> adminToolResult = defaultMQAdminExt.examineConsumerConnectionInfoConcurrent("default-consumer-group");
        ConsumerConnection consumerConnection = adminToolResult.getData();
        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);

        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);
    }

    @Test
    public void testExamineProducerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ProducerConnection producerConnection = defaultMQAdminExt.examineProducerConnectionInfo("default-producer-group", "unit-test");
        assertThat(producerConnection.getConnectionSet().size()).isEqualTo(1);
    }

    @Test
    public void testWipeWritePermOfBroker() throws InterruptedException, RemotingCommandException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, RemotingConnectException {
        int result = defaultMQAdminExt.wipeWritePermOfBroker("127.0.0.1:9876", "default-broker");
        assertThat(result).isEqualTo(6);
    }

    @Test
    public void testAddWritePermOfBroker() throws InterruptedException, RemotingCommandException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, RemotingConnectException {
        int result = defaultMQAdminExt.addWritePermOfBroker("127.0.0.1:9876", "default-broker");
        assertThat(result).isEqualTo(7);
    }

    @Test
    public void testExamineTopicRouteInfo() throws RemotingException, MQClientException, InterruptedException {
        TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo("UnitTest");
        assertThat(topicRouteData.getBrokerDatas().get(0).getBrokerName()).isEqualTo("default-broker");
        assertThat(topicRouteData.getBrokerDatas().get(0).getCluster()).isEqualTo("default-cluster");
    }

    @Test
    public void testGetNameServerAddressList() {
        List<String> result = new ArrayList<>();
        result.add("default-name-one");
        result.add("default-name-two");
        when(mqClientInstance.getMQClientAPIImpl().getNameServerAddressList()).thenReturn(result);
        List<String> nameList = defaultMQAdminExt.getNameServerAddressList();
        assertThat(nameList.get(0)).isEqualTo("default-name-one");
        assertThat(nameList.get(1)).isEqualTo("default-name-two");
    }

    @Test
    public void testPutKVConfig() throws RemotingException, MQClientException, InterruptedException {
        String topicConfig = defaultMQAdminExt.getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest");
        assertThat(topicConfig).isEqualTo("topicListConfig");
        KVTable kvs = defaultMQAdminExt.getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        assertThat(kvs.getTable().get("broker-name")).isEqualTo("broker-one");
        assertThat(kvs.getTable().get("cluster-name")).isEqualTo("default-cluster");
    }

    @Test
    public void testQueryTopicConsumeByWho() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        GroupList groupList = defaultMQAdminExt.queryTopicConsumeByWho("UnitTest");
        assertThat(groupList.getGroupList().contains("consumer-group-two")).isTrue();
    }

    @Test
    public void testQueryConsumeTimeSpan() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<QueueTimeSpan> result = defaultMQAdminExt.queryConsumeTimeSpan("unit-test", "default-broker-group");
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testCleanExpiredConsumerQueue() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean result = defaultMQAdminExt.cleanExpiredConsumerQueue("default-cluster");
        assertThat(result).isFalse();
    }

    @Test
    public void testCleanExpiredConsumerQueueByAddr() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean clean = defaultMQAdminExt.cleanExpiredConsumerQueueByAddr("127.0.0.1:10911");
        assertThat(clean).isTrue();
    }

    @Test
    public void testCleanUnusedTopic() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean result = defaultMQAdminExt.cleanUnusedTopic("default-cluster");
        assertThat(result).isFalse();
    }

    @Test
    public void testGetConsumerRunningInfo() throws RemotingException, MQClientException, InterruptedException {
        ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo("consumer-group", "cid_123", false);
        assertThat(consumerRunningInfo.getJstack()).isEqualTo("test");
    }

    @Test
    public void testMessageTrackDetail() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("msgId");
        messageExt.setTopic("unit-test");
        List<MessageTrack> messageTrackList = defaultMQAdminExt.messageTrackDetail(messageExt);
        assertThat(messageTrackList.size()).isEqualTo(2);
    }

    @Test
    public void testGetConsumeStatus() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Map<String, Map<MessageQueue, Long>> result = defaultMQAdminExt.getConsumeStatus("unit-test", "default-broker-group", "127.0.0.1:10911");
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testGetTopicClusterList() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Set<String> result = defaultMQAdminExt.getTopicClusterList("unit-test");
        assertThat(result.size()).isEqualTo(0);
    }

    @Test
    public void testGetClusterList() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        Set<String> clusterlist = defaultMQAdminExt.getClusterList("UnitTest");
        assertThat(clusterlist.contains("default-cluster-one")).isTrue();
        assertThat(clusterlist.contains("default-cluster-two")).isTrue();
    }

    @Test
    public void testFetchConsumeStatsInBroker() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        ConsumeStatsList result = new ConsumeStatsList();
        result.setBrokerAddr("127.0.0.1:10911");
        when(mqClientInstance.getMQClientAPIImpl().fetchConsumeStatsInBroker("127.0.0.1:10911", false, 10000)).thenReturn(result);
        ConsumeStatsList consumeStatsList = defaultMQAdminExt.fetchConsumeStatsInBroker("127.0.0.1:10911", false, 10000);
        assertThat(consumeStatsList.getBrokerAddr()).isEqualTo("127.0.0.1:10911");
    }

    @Test
    public void testGetAllSubscriptionGroup() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup("127.0.0.1:10911", 10000);
        assertThat(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").getBrokerId()).isEqualTo(1234);
        assertThat(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").getGroupName()).isEqualTo("Consumer-group-one");
        assertThat(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").isConsumeBroadcastEnable()).isTrue();
    }

    @Test
    public void testMaxOffset() throws Exception {
        when(mQClientAPIImpl.getMaxOffset(anyString(), anyString(), anyInt(), anyLong())).thenReturn(100L);

        assertThat(defaultMQAdminExt.maxOffset(new MessageQueue(topic1, broker1Name, 0))).isEqualTo(100L);
    }

    @Test
    public void testSearchOffset() throws Exception {
        when(mQClientAPIImpl.searchOffset(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(101L);

        assertThat(defaultMQAdminExt.searchOffset(new MessageQueue(topic1, broker1Name, 0), System.currentTimeMillis())).isEqualTo(101L);
    }

    @Test
    public void testExamineTopicConfig() throws MQBrokerException, RemotingException, InterruptedException {
        TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig("127.0.0.1:10911", "topic_test_examine_topicConfig");
        assertThat(topicConfig.getTopicName().equals("topic_test_examine_topicConfig"));
    }
    
    
    protected static void awaitUtilDefaultMQAdminExtStart(String cluster) {
        await().atMost(Duration.ofSeconds(10000))
            .until(() -> {
                boolean done = true;
                try {
                    brokerMocker = startOneBroker();
                    nameServerMocker = startNameServer(cluster);
                    defaultMQAdminExtImpl.start();
                } catch (Exception ex){
                    done = false;
                }
                return done;
            });
    }
    
    private static ServerResponseMocker startNameServer(String cluster) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + NAME_SERVER_PORT);
        ClusterInfo clusterInfo = new ClusterInfo();
        
        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(broker1Name);
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(1L, "127.0.0.1:" + BROKER_PORT);
        brokerData.setBrokerAddrs(brokerAddress);
        brokerData.setCluster(cluster);
        brokerAddressTable.put(broker1Name, brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddressTable);
        
        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add(broker1Name);
        clusterAddressTable.put(cluster, brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddressTable);
        
        // start name server
        return ServerResponseMocker.startServer(NAME_SERVER_PORT, clusterInfo.encode());
    }
    
    
    private static ServerResponseMocker startOneBroker() {
        ConsumerConnection consumerConnection = new ConsumerConnection();
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection connection = mock(Connection.class);
        connectionSet.add(connection);
        consumerConnection.setConnectionSet(connectionSet);
        // start broker
        return ServerResponseMocker.startServer(BROKER_PORT, consumerConnection.encode());
    }
}