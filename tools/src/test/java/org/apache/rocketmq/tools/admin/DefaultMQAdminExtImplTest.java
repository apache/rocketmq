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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.admin.RollbackStats;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.GroupForbidden;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.api.BrokerOperatorResult;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.admin.common.AdminToolResult;
import org.apache.rocketmq.tools.admin.common.AdminToolsResultCodeEnum;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMQAdminExtImplTest {

    private DefaultMQAdminExtImpl defaultMQAdminExtImpl;

    @Mock
    private DefaultMQAdminExt defaultMQAdminExt;

    @Mock
    private MQClientInstance mqClientInstance;

    @Mock
    private MQClientAPIImpl mqClientAPIImpl;

    @Mock
    private MQAdminImpl mqAdminImpl;

    private final String defaultTopic = "defaultTopic";

    private final String defaultCluster = "cluster";

    private final String defaultBroker = "broker1";

    private final String defaultGroup = "consumerGroup";

    private final String defaultBrokerAddr = "127.0.0.1:10911";

    private final long timeoutMillis = 3000L;

    private final String defaultMsgId = "AC1A43AC00002A9F00008F214319C26B";

    @Before
    public void init() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException, MQBrokerException {
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, timeoutMillis);
        FieldUtils.writeDeclaredField(defaultMQAdminExtImpl, "mqClientInstance", mqClientInstance, true);
        FieldUtils.writeDeclaredField(defaultMQAdminExtImpl, "mqClientInstance", mqClientInstance, true);
        FieldUtils.writeDeclaredField(defaultMQAdminExtImpl, "threadPoolExecutor", Executors.newFixedThreadPool(1), true);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPIImpl);
        when(mqClientInstance.getMQAdminImpl()).thenReturn(mqAdminImpl);
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(createTopicRouteData());
    }

    @Test
    public void testExamineTopicStats() throws Exception {
        TopicStatsTable topicStatsTable = mock(TopicStatsTable.class);
        Map<MessageQueue, TopicOffset> offsetTable = new ConcurrentHashMap<>();
        offsetTable.put(new MessageQueue(), new TopicOffset());
        when(topicStatsTable.getOffsetTable()).thenReturn(offsetTable);
        when(mqClientAPIImpl.getTopicStatsInfo(any(), any(), anyLong())).thenReturn(topicStatsTable);
        TopicStatsTable actual = defaultMQAdminExtImpl.examineTopicStats(defaultTopic);
        assertNotNull(actual);
        assertEquals(offsetTable.size(), actual.getOffsetTable().size());
    }

    @Test
    public void testExamineTopicStatsConcurrentTopicRouteDataNull() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(null);
        AdminToolResult<TopicStatsTable> actual = defaultMQAdminExtImpl.examineTopicStatsConcurrent(defaultTopic);
        assertNotNull(actual);
        assertEquals(200, actual.getCode());
        assertEquals(0, actual.getData().getOffsetTable().size());
    }

    @Test
    public void testExamineTopicStatsConcurrentBrokerDataEmpty() throws Exception {
        TopicRouteData topicRouteData = mock(TopicRouteData.class);
        when(topicRouteData.getBrokerDatas()).thenReturn(new ArrayList<>());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(topicRouteData);
        AdminToolResult<TopicStatsTable> actual = defaultMQAdminExtImpl.examineTopicStatsConcurrent(defaultTopic);
        assertNotNull(actual);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
        assertEquals(0, actual.getData().getOffsetTable().size());
    }

    @Test
    public void testExamineTopicStatsConcurrent() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(mqClientAPIImpl).getTopicStatsInfo(any(), any(), anyLong());
        latch.await(1000, TimeUnit.MILLISECONDS);
        AdminToolResult<TopicStatsTable> actual = defaultMQAdminExtImpl.examineTopicStatsConcurrent(defaultTopic);
        assertNotNull(actual);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
        assertEquals(0, actual.getData().getOffsetTable().size());
    }

    @Test
    public void testExamineTopicStatsConcurrentException() throws Exception {
        doThrow(new MQBrokerException(ResponseCode.SYSTEM_ERROR, "Test Exception")).when(mqClientAPIImpl).getTopicStatsInfo(any(), any(), anyLong());
        assertNotNull(defaultMQAdminExtImpl.examineTopicStatsConcurrent(defaultTopic));
    }

    @Test
    public void testExamineConsumeStatsConcurrentTopicRouteInfoNotExist() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(null);
        AdminToolResult<ConsumeStats> result = defaultMQAdminExtImpl.examineConsumeStatsConcurrent(defaultGroup, defaultTopic);
        assertEquals(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST.getCode(), result.getCode());
    }

    @Test
    public void testExamineConsumeStatsConcurrent() throws Exception {
        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger fail = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(10);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<BrokerData> brokerDataList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BrokerData bd = new BrokerData();
            bd.setBrokerName("brokerName" + i);
            bd.setCluster(defaultCluster);
            bd.setBrokerAddrs(createBrokerAddrs());
            brokerDataList.add(bd);
        }
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(brokerDataList);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    Thread.sleep(100);
                    if (count.incrementAndGet() % 2 == 0) {
                        success.incrementAndGet();
                    } else {
                        throw new RemotingException("Test Exception");
                    }
                    latch.countDown();
                } catch (Exception e) {
                    fail.incrementAndGet();
                }
            });
        }
        latch.await(3000, TimeUnit.MILLISECONDS);
        executorService.shutdown();
        assertEquals(5, success.get());
        assertEquals(5, fail.get());
        assertEquals(10, count.get());
    }

    @Test
    public void testExamineConsumeStatsConcurrentEmptyOffsetTable() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRouteData());
        when(mqClientAPIImpl.getConsumeStats(anyString(), anyString(), anyString(), anyLong())).thenReturn(new ConsumeStats());
        AdminToolResult<ConsumeStats> actual = defaultMQAdminExtImpl.examineConsumeStatsConcurrent(defaultGroup, defaultTopic);
        assertEquals(AdminToolsResultCodeEnum.CONSUMER_NOT_ONLINE.getCode(), actual.getCode());
    }

    @Test
    public void testViewMessageValidMsgIdReturnsMessageExt() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        MessageExt expected = createMessageExt();
        when(mqAdminImpl.viewMessage(anyString(), anyString())).thenReturn(expected);
        when(mqClientInstance.getMQAdminImpl()).thenReturn(mqAdminImpl);
        MessageExt actual = defaultMQAdminExtImpl.viewMessage(expected.getTopic(), expected.getMsgId());
        assertNotNull(actual);
        assertEquals(expected.getMsgId(), actual.getMsgId());
        assertEquals(expected.getTopic(), actual.getTopic());
    }

    @Test
    public void testViewMessageInvalidMsgIdQueriesByUniqKey() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        MessageExt expected = createMessageExt();
        expected.setMsgId("invalidMsgId");
        when(mqAdminImpl.queryMessageByUniqKey(anyString(), anyString())).thenReturn(expected);
        when(mqClientInstance.getMQAdminImpl()).thenReturn(mqAdminImpl);
        MessageExt actual = defaultMQAdminExtImpl.viewMessage(expected.getTopic(), expected.getMsgId());
        assertNotNull(actual);
        assertEquals(expected.getMsgId(), actual.getMsgId());
        assertEquals(expected.getTopic(), actual.getTopic());
    }

    @Test
    public void testViewMessageExceptionInDecodeLogsWarningAndQueriesByUniqKey() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        MessageExt expected = createMessageExt();
        expected.setMsgId("exceptionMsgId");
        when(mqAdminImpl.queryMessageByUniqKey(anyString(), anyString())).thenReturn(expected);
        MessageExt actual = defaultMQAdminExtImpl.viewMessage(expected.getTopic(), expected.getMsgId());
        assertNotNull(actual);
        assertEquals(expected.getMsgId(), actual.getMsgId());
        assertEquals(expected.getTopic(), actual.getTopic());
    }

    @Test
    public void testQueryMessageInvalidMsgIdReturnsMessageExt() throws Exception {
        MessageExt expected = createMessageExt();
        expected.setMsgId("invalidMsgId");
        when(mqAdminImpl.queryMessageByUniqKey(anyString(), anyString(), anyString())).thenReturn(expected);
        MessageExt actual = defaultMQAdminExtImpl.queryMessage(defaultCluster, expected.getTopic(), expected.getMsgId());
        assertNotNull(actual);
        assertEquals(expected.getMsgId(), actual.getMsgId());
        assertEquals(expected.getTopic(), actual.getTopic());
    }

    @Test
    public void testQueryMessageRemotingException() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        when(mqAdminImpl.viewMessage(anyString(), anyString())).thenThrow(new RemotingException("Test Exception"));
        assertNull(defaultMQAdminExtImpl.queryMessage(null, defaultTopic, defaultMsgId));
    }

    @Test
    public void testDeleteTopicValidInput() throws Exception {
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(defaultMQAdminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put(defaultCluster, new HashSet<>(Arrays.asList("broker1", "broker2")));
        when(clusterInfo.getClusterAddrTable()).thenReturn(clusterAddrTable);
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName(defaultBroker);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, defaultBrokerAddr);
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put(defaultBroker, brokerData);
        when(clusterInfo.getBrokerAddrTable()).thenReturn(brokerAddrTable);
        List<String> nsAddrs = new ArrayList<>();
        nsAddrs.add("127.0.0.1:9876");
        when(mqClientAPIImpl.getNameServerAddressList()).thenReturn(nsAddrs);
        List<String> kvNamespaceToDeleteList = new ArrayList<>();
        kvNamespaceToDeleteList.add("namespace");
        FieldUtils.writeDeclaredField(defaultMQAdminExtImpl, "kvNamespaceToDeleteList", kvNamespaceToDeleteList, true);
        defaultMQAdminExtImpl.deleteTopic(defaultTopic, defaultCluster);
        verify(mqClientAPIImpl, times(1)).deleteTopicInBroker(any(), any(), anyLong());
        verify(mqClientAPIImpl, times(1)).deleteTopicInNameServer(any(), any(), anyLong());
        verify(mqClientAPIImpl, times(1)).deleteKVConfigValue(any(), any(), anyLong());
    }

    @Test
    public void testDeleteTopicInBrokerConcurrent() throws InterruptedException, RemotingException, MQClientException {
        Set<String> addrs = Collections.singleton(defaultBrokerAddr);
        doNothing().when(mqClientAPIImpl).deleteTopicInBroker(anyString(), anyString(), anyLong());
        AdminToolResult<BrokerOperatorResult> result = defaultMQAdminExtImpl.deleteTopicInBrokerConcurrent(addrs, defaultTopic);
        assertNotNull(result);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), result.getCode());
        BrokerOperatorResult brokerResult = result.getData();
        List<String> successList = brokerResult.getSuccessList();
        List<String> failureList = brokerResult.getFailureList();
        assertEquals(1, successList.size());
        assertEquals(0, failureList.size());
        assertEquals(addrs.iterator().next(), successList.get(0));
    }

    @Test
    public void testDeleteTopicInBrokerConcurrentAllFailures() throws InterruptedException, RemotingException, MQClientException {
        Set<String> addrs = new HashSet<>(Collections.singleton(defaultBrokerAddr));
        String anotherAddr = "anotherBrokerAddr:10911";
        addrs.add(anotherAddr);
        doThrow(new RuntimeException("deleteTopic error")).when(mqClientAPIImpl).deleteTopicInBroker(anyString(), anyString(), anyLong());
        AdminToolResult<BrokerOperatorResult> result = defaultMQAdminExtImpl.deleteTopicInBrokerConcurrent(addrs, defaultTopic);
        assertNotNull(result);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), result.getCode());
        BrokerOperatorResult brokerResult = result.getData();
        List<String> successList = brokerResult.getSuccessList();
        List<String> failureList = brokerResult.getFailureList();
        assertEquals(0, successList.size());
        assertEquals(2, failureList.size());
        assertTrue(failureList.contains(defaultBrokerAddr));
        assertTrue(failureList.contains(anotherAddr));
    }

    @Test
    public void testResetOffsetByTimestampOldThrowException() {
        String topic = "nonExistentTopic";
        long timestamp = System.currentTimeMillis();
        assertThrows(NullPointerException.class, () -> defaultMQAdminExtImpl.resetOffsetByTimestampOld(defaultGroup, topic, timestamp, false));
    }

    @Test
    public void testResetOffsetByTimestampOldValidInputShouldProcessCorrectly() throws Exception {
        long timestamp = System.currentTimeMillis();
        ConsumeStats consumeStats = mock(ConsumeStats.class);
        Map<MessageQueue, OffsetWrapper> offsetTable = new ConcurrentHashMap<>();
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        offsetWrapper.setBrokerOffset(5L);
        offsetWrapper.setConsumerOffset(5L);
        offsetTable.put(new MessageQueue(defaultTopic, defaultBroker, 0), offsetWrapper);
        when(consumeStats.getOffsetTable()).thenReturn(offsetTable);
        when(mqClientAPIImpl.getConsumeStats(any(), any(), anyLong())).thenReturn(consumeStats);
        List<RollbackStats> rollbackStatsList = defaultMQAdminExtImpl.resetOffsetByTimestampOld(defaultGroup, defaultTopic, timestamp, false);
        assertNotNull(rollbackStatsList);
        assertEquals(1, rollbackStatsList.size());
        RollbackStats rollbackStats = rollbackStatsList.get(0);
        assertEquals(defaultBroker, rollbackStats.getBrokerName());
        assertEquals(0, rollbackStats.getQueueId());
        assertEquals(5L, rollbackStats.getBrokerOffset());
        assertEquals(5L, rollbackStats.getConsumerOffset());
    }

    @Test
    public void testResetOffsetNew() throws Exception {
        defaultMQAdminExtImpl.resetOffsetNew(defaultGroup, defaultTopic, timeoutMillis);
        verify(mqClientAPIImpl, times(1)).invokeBrokerToResetOffset(
                anyString(),
                anyString(),
                anyString(),
                anyLong(),
                anyBoolean(),
                anyLong(),
                anyBoolean());
    }

    @Test
    public void testResetOffsetNewConcurrent() {
        AdminToolResult<BrokerOperatorResult> actual = defaultMQAdminExtImpl.resetOffsetNewConcurrent(defaultGroup, defaultTopic, timeoutMillis);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
    }

    @Test
    public void testResetOffsetNewConcurrentTopicRouteInfoNotExist() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(null);
        AdminToolResult<BrokerOperatorResult> actual = defaultMQAdminExtImpl.resetOffsetNewConcurrent(defaultGroup, defaultTopic, timeoutMillis);
        assertEquals(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST.getCode(), actual.getCode());
    }

    @Test
    public void testResetOffsetNewConcurrentException() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenThrow(new MQClientException(ResponseCode.SYSTEM_ERROR, "Test Exception"));
        AdminToolResult<BrokerOperatorResult> actual = defaultMQAdminExtImpl.resetOffsetNewConcurrent(defaultGroup, defaultTopic, timeoutMillis);
        assertEquals(AdminToolsResultCodeEnum.MQ_CLIENT_ERROR.getCode(), actual.getCode());
    }

    @Test
    public void testCreateOrUpdateOrderConfClusterConfig() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String key = "key1";
        String value = "value1";
        doNothing().when(mqClientAPIImpl).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, true);
        verify(mqClientAPIImpl, times(1)).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    public void testCreateOrUpdateOrderConfNonClusterConfig() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String key = "key1";
        String value = "value1:value2";
        String oldOrderConfs = "key1:value1;key2:value2";
        when(mqClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenReturn(oldOrderConfs);
        doNothing().when(mqClientAPIImpl).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, false);
        verify(mqClientAPIImpl, times(1)).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    public void testCreateOrUpdateOrderConfExceptionInPut() throws RemotingException, InterruptedException, MQClientException {
        String key = "key1";
        String value = "value1:value2";
        String oldOrderConfs = "key1:value1;key2:value2";
        when(mqClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenReturn(oldOrderConfs);
        doThrow(new RemotingException("Test Exception")).when(mqClientAPIImpl).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
        assertThrows(RemotingException.class, () -> defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, false));
    }

    @Test
    public void testCreateOrUpdateOrderConfNoOldConfs() throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        String key = "key1";
        String value = "value1:value2";
        when(mqClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenReturn(null);
        doNothing().when(mqClientAPIImpl).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, false);
        verify(mqClientAPIImpl, times(1)).putKVConfigValue(anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    public void testCreateOrUpdateOrderConfExceptionInGet() throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        String key = "key1";
        String value = "value1:value2";
        when(mqClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenThrow(new RemotingException("Test Exception"));
        defaultMQAdminExtImpl.createOrUpdateOrderConf(key, value, false);
        verify(mqClientAPIImpl, times(1)).getKVConfigValue(anyString(), anyString(), anyLong());
    }

    @Test
    public void testQuerySubscriptionValidInput() throws InterruptedException, MQBrokerException, RemotingException, MQClientException {
        when(mqClientAPIImpl.querySubscriptionByConsumer(anyString(), anyString(), anyString(), anyLong())).thenReturn(new SubscriptionData());
        assertNotNull(defaultMQAdminExtImpl.querySubscription("group", "topic"));
    }

    @Test
    public void testQueryTopicsByConsumer() throws Exception {
        TopicList expected = new TopicList();
        expected.getTopicList().add(defaultTopic);
        when(mqClientAPIImpl.queryTopicsByConsumer(anyString(), anyString(), anyLong())).thenReturn(expected);
        TopicList actual = defaultMQAdminExtImpl.queryTopicsByConsumer(defaultGroup);
        assertEquals(1, actual.getTopicList().size());
        assertEquals(expected.getTopicList().iterator().next(), actual.getTopicList().iterator().next());
        verify(mqClientAPIImpl, times(1)).queryTopicsByConsumer(anyString(), anyString(), anyLong());
    }

    @Test
    public void testQueryTopicsByConsumerRemotingTimeoutException() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(mqClientAPIImpl.queryTopicsByConsumer(anyString(), anyString(), anyLong())).thenThrow(new RemotingTimeoutException("Test Exception"));
        assertThrows(RemotingTimeoutException.class, () -> defaultMQAdminExtImpl.queryTopicsByConsumer(defaultGroup));
        verify(mqClientAPIImpl, times(1)).queryTopicsByConsumer(anyString(), anyString(), anyLong());
    }

    @Test
    public void testQueryTopicsByConsumerMQBrokerException() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(mqClientAPIImpl.queryTopicsByConsumer(anyString(), anyString(), anyLong())).thenThrow(new MQBrokerException(ResponseCode.SYSTEM_ERROR, "Test Exception"));
        assertThrows(MQBrokerException.class, () -> defaultMQAdminExtImpl.queryTopicsByConsumer(defaultGroup));
        verify(mqClientAPIImpl, times(1)).queryTopicsByConsumer(anyString(), anyString(), anyLong());
    }

    @Test
    public void testQueryTopicsByConsumerMQClientException() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(mqClientAPIImpl.queryTopicsByConsumer(anyString(), anyString(), anyLong())).thenThrow(new MQBrokerException(ResponseCode.SYSTEM_ERROR, "Test Exception"));
        assertThrows(MQBrokerException.class, () -> defaultMQAdminExtImpl.queryTopicsByConsumer(defaultGroup));
        verify(mqClientAPIImpl, times(1)).queryTopicsByConsumer(anyString(), anyString(), anyLong());
    }

    @Test
    public void testQueryTopicsByConsumerNoBrokers() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(new ArrayList<>());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(topicRouteData);
        TopicList actual = defaultMQAdminExtImpl.queryTopicsByConsumer(defaultGroup);
        assertEquals(0, actual.getTopicList().size());
    }

    @Test
    public void testQueryTopicsByConsumerConcurrentTopicRouteDataNull() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(null);
        AdminToolResult<TopicList> actual = defaultMQAdminExtImpl.queryTopicsByConsumerConcurrent(defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST.getCode(), actual.getCode());
        assertEquals("router info not found.", actual.getErrorMsg());
    }

    @Test
    public void testQueryTopicsByConsumerConcurrentNoBrokers() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(new ArrayList<>());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(any(), anyLong())).thenReturn(topicRouteData);
        AdminToolResult<TopicList> actual = defaultMQAdminExtImpl.queryTopicsByConsumerConcurrent(defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST.getCode(), actual.getCode());
        assertEquals("router info not found.", actual.getErrorMsg());
    }

    @Test
    public void testQueryTopicsByConsumerConcurrent() throws Exception {
        TopicList expectedTopicList = new TopicList();
        expectedTopicList.setTopicList(new HashSet<>(Arrays.asList(defaultTopic, "topic2")));
        when(mqClientAPIImpl.queryTopicsByConsumer(any(), any(), anyLong())).thenReturn(expectedTopicList);
        AdminToolResult<TopicList> result = defaultMQAdminExtImpl.queryTopicsByConsumerConcurrent(defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), result.getCode());
        Set<String> actual = result.getData().getTopicList();
        assertFalse(actual.isEmpty());
        assertTrue(actual.containsAll(expectedTopicList.getTopicList()));
    }

    @Test
    public void testQueryConsumeTimeSpanConcurrentTopicRouteDataNull() throws Exception {
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(null);
        AdminToolResult<List<QueueTimeSpan>> actual = defaultMQAdminExtImpl.queryConsumeTimeSpanConcurrent(defaultTopic, defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
        assertEquals(0, actual.getData().size());
    }

    @Test
    public void testQueryConsumeTimeSpanConcurrentNoBrokers() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(new ArrayList<>());
        when(mqClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        AdminToolResult<List<QueueTimeSpan>> actual = defaultMQAdminExtImpl.queryConsumeTimeSpanConcurrent(defaultTopic, defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
        assertEquals(0, actual.getData().size());
    }

    @Test
    public void testQueryConsumeTimeSpanConcurrent() throws Exception {
        List<QueueTimeSpan> spans = new ArrayList<>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(1000L);
        queueTimeSpan.setMaxTimeStamp(2000L);
        spans.add(queueTimeSpan);
        when(mqClientAPIImpl.queryConsumeTimeSpan(anyString(), anyString(), anyString(), anyLong())).thenReturn(spans);
        AdminToolResult<List<QueueTimeSpan>> actual = defaultMQAdminExtImpl.queryConsumeTimeSpanConcurrent(defaultTopic, defaultGroup);
        assertEquals(AdminToolsResultCodeEnum.SUCCESS.getCode(), actual.getCode());
        assertEquals(1, actual.getData().size());
    }

    @Test
    public void testDeleteExpiredCommitLog() throws Exception {
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.retrieveAllAddrByCluster(defaultCluster)).thenReturn(new String[]{"addr1", "addr2"});
        when(mqClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);
        when(mqClientAPIImpl.deleteExpiredCommitLog(anyString(), anyLong())).thenReturn(true);
        boolean actual = defaultMQAdminExtImpl.deleteExpiredCommitLog(defaultCluster);
        assertTrue(actual);
        verify(mqClientAPIImpl, times(2)).deleteExpiredCommitLog(anyString(), anyLong());
    }

    @Test
    public void testDeleteExpiredCommitLogByCluster() throws Exception {
        ClusterInfo clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.retrieveAllAddrByCluster(defaultCluster)).thenReturn(new String[]{"addr1", "addr2"});
        when(mqClientAPIImpl.deleteExpiredCommitLog(anyString(), anyLong())).thenReturn(true);
        boolean actual = defaultMQAdminExtImpl.deleteExpiredCommitLogByCluster(clusterInfo, defaultCluster);
        assertTrue(actual);
        verify(mqClientAPIImpl, times(2)).deleteExpiredCommitLog(anyString(), anyLong());
    }

    @Test
    public void testDeleteExpiredCommitLogByAddr() throws Exception {
        when(mqClientAPIImpl.deleteExpiredCommitLog(defaultBrokerAddr, timeoutMillis)).thenReturn(true);
        boolean actual = defaultMQAdminExtImpl.deleteExpiredCommitLogByAddr(defaultBrokerAddr);
        assertTrue(actual);
        verify(mqClientAPIImpl, times(1)).deleteExpiredCommitLog(defaultBrokerAddr, timeoutMillis);
    }

    @Test
    public void testConsumeMessageDirectly() throws Exception {
        String clientId = "clientId";
        MessageExt messageExt = createMessageExt();
        when(mqAdminImpl.viewMessage(defaultTopic, defaultMsgId)).thenReturn(messageExt);
        ConsumeMessageDirectlyResult consumeMessageDirectlyResult = mock(ConsumeMessageDirectlyResult.class);
        when(mqClientAPIImpl.consumeMessageDirectly(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyLong()))
                .thenReturn(consumeMessageDirectlyResult);
        ConsumeMessageDirectlyResult actual = defaultMQAdminExtImpl.consumeMessageDirectly(defaultGroup, clientId, defaultTopic, defaultMsgId);
        assertNotNull(actual);
        assertNull(actual.getRemark());
        assertFalse(actual.isAutoCommit());
        assertFalse(actual.isOrder());
    }

    @Test
    public void testMessageTrackDetailConcurrent() throws Exception {
        MessageExt messageExt = createMessageExt();
        GroupList groupList = mock(GroupList.class);
        HashSet<String> groupSet = new HashSet<>();
        groupSet.add(defaultGroup);
        when(groupList.getGroupList()).thenReturn(groupSet);
        when(mqClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);
        ConsumerConnection consumerConnection = mock(ConsumerConnection.class);
        when(mqClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);
        List<MessageTrack> actual = defaultMQAdminExtImpl.messageTrackDetailConcurrent(messageExt);
        assertEquals(1, actual.size());
    }

//    @Test
//    public void testConsumedConcurrent() throws Exception {
//        ConsumeStats consumeStats = mock(ConsumeStats.class);
//        ClusterInfo ci = mock(ClusterInfo.class);
//        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
//        BrokerData brokerData = mock(BrokerData.class);
//        HashMap<Long, String> brokerAddress = new HashMap<>();
//        brokerAddress.put(0L, defaultBrokerAddr);
//        when(brokerData.getBrokerAddrs()).thenReturn(brokerAddress);
//        brokerAddrTable.put(defaultBroker, brokerData);
//        when(ci.getBrokerAddrTable()).thenReturn(brokerAddrTable);
//        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
//        OffsetWrapper offsetWrapper = new OffsetWrapper();
//        offsetWrapper.setConsumerOffset(1L);
//        offsetTable.put(new MessageQueue(defaultTopic, defaultBroker, 0), offsetWrapper);
//        when(consumeStats.getOffsetTable()).thenReturn(offsetTable);
////        when(mqClientAPIImpl.getConsumeStats(any(), any(), any(), anyLong())).thenReturn(consumeStats);
//        when(mqClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(ci);
////        assertTrue(defaultMQAdminExtImpl.consumedConcurrent(createMessageExt(), defaultGroup));
//    }

    @Test
    public void testCloneGroupOffsetValidInput() throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        String srcGroup = "srcGroup";
        String destGroup = "destGroup";
        boolean isOffline = false;
        defaultMQAdminExtImpl.cloneGroupOffset(srcGroup, destGroup, defaultTopic, isOffline);
        verify(mqClientAPIImpl, times(1)).cloneGroupOffset(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyBoolean(),
                anyLong());
    }

    @Test
    public void testGetUserSubscriptionGroup() throws Exception {
        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap<>();
        SubscriptionGroupConfig groupConfig1 = new SubscriptionGroupConfig();
        groupConfig1.setGroupName("CID_RMQ_SYS_GROUP");
        SubscriptionGroupConfig groupConfig2 = new SubscriptionGroupConfig();
        groupConfig2.setGroupName("DEFAULT_CONSUMER");
        SubscriptionGroupConfig groupConfig3 = new SubscriptionGroupConfig();
        groupConfig3.setGroupName("SYS_CONSUMER_GROUP");
        subscriptionGroupTable.put(groupConfig1.getGroupName(), groupConfig1);
        subscriptionGroupTable.put(groupConfig2.getGroupName(), groupConfig2);
        subscriptionGroupTable.put(groupConfig3.getGroupName(), groupConfig3);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptionGroupTable);
        when(mqClientAPIImpl.getAllSubscriptionGroup(any(), anyLong())).thenReturn(subscriptionGroupWrapper);
        SubscriptionGroupWrapper actual = defaultMQAdminExtImpl.getUserSubscriptionGroup(defaultBrokerAddr, timeoutMillis);
        assertEquals(1, actual.getSubscriptionGroupTable().size());
        assertTrue(actual.getSubscriptionGroupTable().containsKey("SYS_CONSUMER_GROUP"));
    }

    @Test
    public void testGetUserTopicConfig() throws Exception {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<>();
        topicConfigMap.put("Topic1", new TopicConfig("Topic1", 1, 1, 0));
        topicConfigMap.put("Topic2", new TopicConfig("Topic2", 1, 1, 1));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigMap);
        TopicList topicList = new TopicList();
        Set<String> topicSet = new HashSet<>();
        topicSet.add("Topic2");
        topicList.setTopicList(topicSet);
        when(mqClientAPIImpl.getAllTopicConfig(any(), anyLong())).thenReturn(topicConfigSerializeWrapper);
        when(mqClientAPIImpl.getSystemTopicListFromBroker(anyString(), anyLong())).thenReturn(topicList);
        TopicConfigSerializeWrapper actual = defaultMQAdminExtImpl.getUserTopicConfig("brokerAddr", false, timeoutMillis);
        assertEquals(1, actual.getTopicConfigTable().size());
    }

    @Test
    public void testUpdateConsumeOffset() throws Exception {
        doNothing().when(mqClientAPIImpl).updateConsumerOffset(any(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
        defaultMQAdminExtImpl.updateConsumeOffset(defaultBrokerAddr, defaultGroup, createMessageQueue(), 1L);
        verify(mqClientAPIImpl, times(1)).updateConsumerOffset(any(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
    }

    @Test
    public void testUpdateConsumeOffsetException() throws MQBrokerException, RemotingException, InterruptedException {
        doThrow(new RemotingException("Test exception")).when(mqClientAPIImpl).updateConsumerOffset(anyString(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
        assertThrows(RemotingException.class,
                () -> defaultMQAdminExtImpl.updateConsumeOffset(defaultBrokerAddr, defaultGroup, createMessageQueue(), 1L));
    }

    @Test
    public void testResetOffsetByQueueId() throws Exception {
        long resetOffset = 100;
        doNothing().when(mqClientAPIImpl).updateConsumerOffset(any(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
        Map<MessageQueue, Long> result = new HashMap<>();
        result.put(createMessageQueue(), resetOffset);
        when(mqClientAPIImpl.invokeBrokerToResetOffset(anyString(), anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyLong())).thenReturn(result);
        defaultMQAdminExtImpl.resetOffsetByQueueId(defaultBrokerAddr, defaultGroup, defaultTopic, 0, resetOffset);
        verify(mqClientAPIImpl, times(1)).updateConsumerOffset(any(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
        verify(mqClientAPIImpl, times(1)).invokeBrokerToResetOffset(
                anyString(),
                anyString(),
                anyString(),
                anyLong(),
                anyInt(),
                anyLong(),
                anyLong());
    }

    @Test
    public void testResetOffsetByQueueIdThrowsException() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        doNothing().when(mqClientAPIImpl).updateConsumerOffset(any(), any(UpdateConsumerOffsetRequestHeader.class), anyLong());
        when(mqClientAPIImpl.invokeBrokerToResetOffset(
                anyString(),
                anyString(),
                anyString(),
                anyLong(),
                anyInt(),
                anyLong(),
                anyLong()))
                .thenThrow(new MQClientException(1, "Exception"));
        assertThrows(MQBrokerException.class,
                () -> defaultMQAdminExtImpl.resetOffsetByQueueId(defaultBrokerAddr, defaultGroup, defaultTopic, 0, 100));
    }

    @Test
    public void testUpdateAndGetGroupReadForbidden() throws RemotingException, InterruptedException, MQBrokerException {
        boolean readable = true;
        GroupForbidden expectedResponse = new GroupForbidden();
        expectedResponse.setGroup(defaultGroup);
        expectedResponse.setTopic(defaultTopic);
        expectedResponse.setReadable(readable);
        when(mqClientAPIImpl.updateAndGetGroupForbidden(any(), any(UpdateGroupForbiddenRequestHeader.class), anyLong())).thenReturn(expectedResponse);
        GroupForbidden actual = defaultMQAdminExtImpl.updateAndGetGroupReadForbidden(defaultBrokerAddr, defaultGroup, defaultTopic, readable);
        assertNotNull(actual);
        assertEquals(defaultGroup, actual.getGroup());
        assertEquals(defaultTopic, actual.getTopic());
        assertEquals(readable, actual.getReadable());
    }

    @Test
    public void testUpdateAndGetGroupReadForbiddenException() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        when(mqClientAPIImpl.updateAndGetGroupForbidden(any(), any(UpdateGroupForbiddenRequestHeader.class), anyLong()))
                .thenThrow(new MQBrokerException(ResponseCode.SYSTEM_ERROR, "Test Exception"));
        assertThrows(MQBrokerException.class,
                () -> defaultMQAdminExtImpl.updateAndGetGroupReadForbidden(defaultBrokerAddr, defaultGroup, defaultTopic, true));
    }

    private HashMap<Long, String> createBrokerAddrs() {
        HashMap<Long, String> result = new HashMap<>();
        result.put(0L, defaultBrokerAddr);
        return result;
    }

    private TopicRouteData createTopicRouteData() {
        BrokerData bd = new BrokerData(defaultCluster, defaultBroker, new HashMap<>());
        bd.setBrokerAddrs(new HashMap<>());
        bd.getBrokerAddrs().put(0L, defaultBrokerAddr);
        QueueData qd = new QueueData();
        qd.setBrokerName(defaultBroker);
        qd.setPerm(PermName.PERM_WRITE);
        qd.setReadQueueNums(1);
        qd.setTopicSysFlag(0);
        qd.setWriteQueueNums(1);
        TopicRouteData result = new TopicRouteData();
        result.getBrokerDatas().add(bd);
        result.getQueueDatas().add(qd);
        return result;
    }

    private MessageQueue createMessageQueue() {
        return new MessageQueue(defaultTopic, defaultBroker, 0);
    }

    private MessageExt createMessageExt() {
        MessageExt result = new MessageExt();
        result.setMsgId(defaultMsgId);
        result.setTopic(defaultTopic);
        result.setQueueId(0);
        InetAddress inetAddress = mock(InetAddress.class);
        InetSocketAddress address = new InetSocketAddress(inetAddress, 10911);
        when(inetAddress.getHostAddress()).thenReturn("127.0.0.1");
        result.setStoreHost(address);
        return result;
    }
}
