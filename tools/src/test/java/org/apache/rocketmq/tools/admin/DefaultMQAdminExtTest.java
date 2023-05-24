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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.protocol.body.ProducerInfo;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.admin.api.TrackType;
import org.assertj.core.util.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQAdminExtTest {
    private static final String BROKER1_ADDR = "127.0.0.1:10911";
    private static final String BROKER1_NAME = "default-broker";
    private static final String CLUSTER = "default-cluster";
    private static final String BROKER2_NAME = "broker-test";
    private static final String BROKER2_ADDR = "127.0.0.2:10911";
    private static final String TOPIC1 = "topic_one";
    private static final String TOPIC2 = "topic_two";
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;
    private static Properties properties = new Properties();
    private static TopicList topicList = new TopicList();
    private static TopicRouteData topicRouteData = new TopicRouteData();
    private static KVTable kvTable = new KVTable();
    private static ClusterInfo clusterInfo = new ClusterInfo();

    @BeforeClass
    public static void init() throws Exception {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

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
        topicSet.add(TOPIC1);
        topicSet.add(TOPIC2);
        topicList.setTopicList(topicSet);
        when(mQClientAPIImpl.getTopicListFromNameServer(anyLong())).thenReturn(topicList);

        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER1_ADDR);
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster(CLUSTER);
        brokerData.setBrokerName(BROKER1_NAME);
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        brokerDatas.add(new BrokerData(CLUSTER, BROKER2_NAME, (HashMap<Long, String>) Maps.newHashMap(MixAll.MASTER_ID, BROKER2_ADDR)));
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(new ArrayList<>());
        topicRouteData.setFilterServerTable(new HashMap<>());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        HashMap<String, String> result = new HashMap<>();
        result.put("id", String.valueOf(MixAll.MASTER_ID));
        result.put("brokerName", BROKER1_NAME);
        kvTable.setTable(result);
        when(mQClientAPIImpl.getBrokerRuntimeInfo(anyString(), anyLong())).thenReturn(kvTable);

        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put(BROKER1_NAME, brokerData);
        brokerAddrTable.put(BROKER2_NAME, new BrokerData());
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(new HashMap<>());
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
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        ProducerConnection producerConnection = new ProducerConnection();
        Connection connection = new Connection();
        connection.setClientAddr("127.0.0.1:9898");
        connection.setClientId("PID_12345");
        HashSet<Connection> connectionSet = new HashSet<>();
        connectionSet.add(connection);
        producerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getProducerConnectionList(anyString(), anyString(), anyLong())).thenReturn(producerConnection);

        ProducerTableInfo producerTableInfo = new ProducerTableInfo(new HashMap<>());
        producerTableInfo.getData().put("test-producer-group", Arrays.asList(new ProducerInfo(
                "xxxx-client-id",
                "127.0.0.1:18978",
                LanguageCode.JAVA,
                400,
                System.currentTimeMillis()

        )));
        when(mQClientAPIImpl.getAllProducerInfo(anyString(), anyLong())).thenReturn(producerTableInfo);

        when(mQClientAPIImpl.wipeWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(6);
        when(mQClientAPIImpl.addWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(7);

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(new HashMap<>());

        Map<String, Map<MessageQueue, Long>> consumerStatus = new HashMap<>();
        when(mQClientAPIImpl.invokeBrokerToGetConsumerStatus(anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(consumerStatus);

        List<QueueTimeSpan> queueTimeSpanList = new ArrayList<>();
        when(mQClientAPIImpl.queryConsumeTimeSpan(anyString(), anyString(), anyString(), anyLong())).thenReturn(queueTimeSpanList);

        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");
        consumerRunningInfo.setMqTable(new TreeMap<>());
        consumerRunningInfo.setStatusTable(new TreeMap<>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<>());
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
        ConsumerConnection connection = new ConsumerConnection();
        connection.setMessageModel(MessageModel.BROADCASTING);
        HashSet<Connection> connections = new HashSet<>();
        connections.add(new Connection());
        connection.setConnectionSet(connections);
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(new ConsumeStats());
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong()))
            .thenReturn(new ConsumerConnection()).thenReturn(connection);
        // CONSUMER_NOT_ONLINE
        try {
            defaultMQAdminExt.examineConsumeStats("default-consumer-group", "unit-test");
        } catch (Exception e) {
            assertThat(e instanceof MQClientException).isTrue();
            assertThat(((MQClientException) e).getResponseCode()).isEqualTo(ResponseCode.CONSUMER_NOT_ONLINE);
        }
        // BROADCAST_CONSUMPTION
        try {
            defaultMQAdminExt.examineConsumeStats("default-consumer-group", "unit-test");
        } catch (Exception e) {
            assertThat(e instanceof MQClientException).isTrue();
            assertThat(((MQClientException) e).getResponseCode()).isEqualTo(ResponseCode.BROADCAST_CONSUMPTION);
        }
    }

    @Test
    public void testExamineConsumerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo("default-consumer-group");
        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);

        consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo("default-consumer-group", "127.0.0.1:10911");
        assertThat(consumerConnection.getConsumeType()).isEqualTo(ConsumeType.CONSUME_PASSIVELY);
        assertThat(consumerConnection.getMessageModel()).isEqualTo(MessageModel.CLUSTERING);
    }

    @Test
    public void testExamineProducerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ProducerConnection producerConnection = defaultMQAdminExt.examineProducerConnectionInfo("default-producer-group", "unit-test");
        assertThat(producerConnection.getConnectionSet().size()).isEqualTo(1);
    }

    @Test
    public void testGetAllProducerInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ProducerTableInfo producerTableInfo = defaultMQAdminExt.getAllProducerInfo("127.0.0.1:10911");
        assertThat(producerTableInfo.getData().size()).isEqualTo(1);
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

        ConsumerConnection connection = new ConsumerConnection();
        connection.setMessageModel(MessageModel.BROADCASTING);
        connection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        HashSet<Connection> connections = new HashSet<>();
        connections.add(new Connection());
        connection.setConnectionSet(connections);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(connection);
        ConsumeStats consumeStats = new ConsumeStats();
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), isNull(), anyLong())).thenReturn(consumeStats);
        List<MessageTrack> broadcastMessageTracks = defaultMQAdminExt.messageTrackDetail(messageExt);
        assertThat(broadcastMessageTracks.size()).isEqualTo(2);
        assertThat(broadcastMessageTracks.get(0).getTrackType()).isEqualTo(TrackType.CONSUME_BROADCASTING);
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
    @Ignore
    public void testMaxOffset() throws Exception {
        when(mQClientAPIImpl.getMaxOffset(anyString(), any(MessageQueue.class), anyLong())).thenReturn(100L);
        assertThat(defaultMQAdminExt.maxOffset(new MessageQueue(TOPIC1, BROKER1_NAME, 0))).isEqualTo(100L);
    }

    @Test
    @Ignore
    public void testSearchOffset() throws Exception {
        when(mQClientAPIImpl.searchOffset(anyString(), any(MessageQueue.class), anyLong(), anyLong())).thenReturn(101L);
        assertThat(defaultMQAdminExt.searchOffset(new MessageQueue(TOPIC1, BROKER1_NAME, 0), System.currentTimeMillis())).isEqualTo(101L);
    }

    @Test
    public void testExamineTopicConfig() throws MQBrokerException, RemotingException, InterruptedException {
        TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig("127.0.0.1:10911", "topic_test_examine_topicConfig");
        assertThat(topicConfig.getTopicName().equals("topic_test_examine_topicConfig")).isTrue();
    }
}
