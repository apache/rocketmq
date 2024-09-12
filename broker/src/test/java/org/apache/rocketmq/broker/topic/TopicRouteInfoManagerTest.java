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
package org.apache.rocketmq.broker.topic;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicRouteInfoManagerTest {
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private BrokerOuterAPI brokerOuterAPI;
    
    @Mock
    private TopicPublishInfo topicPublishInfo;
    
    @Mock
    private TopicRouteData topicRouteData;
    
    @Mock
    private Lock lockNamesrv;
    
    private TopicRouteInfoManager topicRouteInfoManager;
    
    private final String defaultTopic = "defaultTopic";
    
    private final String defaultBroker = "defaultBroker";
    
    private final long defaultTimeout = 3000L;
    
    private final String masterAddress = "127.0.0.1:10911";
    
    private final String slaveAddress = "127.0.0.1:10912";
    
    private final long defaultBrokerId = MixAll.MASTER_ID;
    
    private final boolean defaultOnlyThisBroker = false;
    
    @Before
    public void setUp() {
        topicRouteInfoManager = new TopicRouteInfoManager(brokerController);
        when(brokerController.getBrokerOuterAPI()).thenReturn(brokerOuterAPI);
    }
    
    @Test
    public void testUpdateTopicRouteInfoFromNameServerTopicRouteDataNull() throws MQBrokerException, RemotingException, InterruptedException {
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(null);
        topicRouteInfoManager.updateTopicRouteInfoFromNameServer(defaultTopic, true, true);
        verify(brokerOuterAPI, times(1)).getTopicRouteInfoFromNameServer(defaultTopic, defaultTimeout);
    }
    
    @Test
    public void testUpdateTopicRouteInfoFromNameServerException() throws MQBrokerException, RemotingException, InterruptedException {
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenThrow(new RemotingException("Test exception"));
        topicRouteInfoManager.updateTopicRouteInfoFromNameServer(defaultTopic, true, true);
        verify(brokerOuterAPI, times(1)).getTopicRouteInfoFromNameServer(defaultTopic, defaultTimeout);
    }
    
    @Test
    public void testUpdateTopicRouteInfoFromNameServerSuccess() throws RemotingException, MQBrokerException, InterruptedException {
        TopicRouteData topicRouteData = mock(TopicRouteData.class);
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        topicRouteInfoManager.updateTopicRouteInfoFromNameServer(defaultTopic, true, true);
        verify(brokerOuterAPI, times(1)).getTopicRouteInfoFromNameServer(defaultTopic, defaultTimeout);
        verify(topicRouteData, times(1)).getBrokerDatas();
    }
    
    @Test
    public void testUpdateTopicRouteInfoFromNameServerInterrupted() throws InterruptedException, IllegalAccessException {
        when(lockNamesrv.tryLock(anyLong(), any())).thenThrow(new InterruptedException());
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "lockNamesrv", lockNamesrv, true);
        topicRouteInfoManager.updateTopicRouteInfoFromNameServer(defaultTopic, true, true);
        verify(lockNamesrv, times(1)).tryLock(anyLong(), any());
    }
    
    @Test
    public void testTryToFindTopicPublishInfoTopicPublishInfoExistsAndOk() throws IllegalAccessException {
        when(topicPublishInfo.ok()).thenReturn(true);
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
        topicPublishInfoTable.put(defaultTopic, topicPublishInfo);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "topicPublishInfoTable", topicPublishInfoTable, true);
        TopicPublishInfo actual = topicRouteInfoManager.tryToFindTopicPublishInfo(defaultTopic);
        assertSame(topicPublishInfo, actual);
    }
    
    @Test
    public void testTryToFindTopicPublishInfoTopicPublishInfoExistsButNotOk() throws RemotingException, InterruptedException, MQBrokerException, IllegalAccessException {
        when(topicPublishInfo.ok()).thenReturn(false);
        ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
        topicPublishInfoTable.put(defaultTopic, topicPublishInfo);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "topicPublishInfoTable", topicPublishInfoTable, true);
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        TopicPublishInfo actual = topicRouteInfoManager.tryToFindTopicPublishInfo(defaultTopic);
        assertNotSame(topicPublishInfo, actual);
        assertNotNull(actual);
    }
    
    @Test
    public void testTryToFindTopicPublishInfoTopicPublishInfoDoesNotExist() throws RemotingException, InterruptedException, MQBrokerException {
        when(brokerOuterAPI.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        assertNotNull(topicRouteInfoManager.tryToFindTopicPublishInfo(defaultTopic));
    }
    
    @Test
    public void testFindBrokerAddressInPublishBrokerNameIsNull() {
        assertNull(topicRouteInfoManager.findBrokerAddressInPublish(null));
    }
    
    @Test
    public void testFindBrokerAddressInPublishBrokerNameExistsWithMasterId() throws IllegalAccessException {
        HashMap<Long, String> brokerMap = new HashMap<>();
        brokerMap.put(MixAll.MASTER_ID, masterAddress);
        brokerMap.put(MixAll.FIRST_SLAVE_ID, slaveAddress);
        ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
        brokerAddrTable.put(defaultBroker, brokerMap);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "brokerAddrTable", brokerAddrTable, true);
        String actual = topicRouteInfoManager.findBrokerAddressInPublish(defaultBroker);
        assertEquals(masterAddress, actual);
    }
    
    @Test
    public void testFindBrokerAddressInPublishBrokerNameExistsButNoMasterId() throws IllegalAccessException {
        HashMap<Long, String> brokerMap = new HashMap<>();
        brokerMap.put(MixAll.FIRST_SLAVE_ID, slaveAddress);
        ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
        brokerAddrTable.put(defaultBroker, brokerMap);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "brokerAddrTable", brokerAddrTable, true);
        assertNull(topicRouteInfoManager.findBrokerAddressInPublish(defaultBroker));
    }
    
    @Test
    public void testFindBrokerAddressInPublishBrokerNameNotExist() {
        assertNull(topicRouteInfoManager.findBrokerAddressInPublish(defaultBroker));
    }
    
    @Test
    public void testFindBrokerAddressInSubscribeBrokerNameIsNull() {
        assertNull(topicRouteInfoManager.findBrokerAddressInSubscribe(null, defaultBrokerId, defaultOnlyThisBroker));
    }
    
    @Test
    public void testFindBrokerAddressInSubscribeBrokerNameExistsWithMasterId() throws IllegalAccessException {
        HashMap<Long, String> brokerMap = new HashMap<>();
        brokerMap.put(MixAll.MASTER_ID, masterAddress);
        brokerMap.put(MixAll.FIRST_SLAVE_ID, slaveAddress);
        ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
        brokerAddrTable.put(defaultBroker, brokerMap);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "brokerAddrTable", brokerAddrTable, true);
        String actual = topicRouteInfoManager.findBrokerAddressInSubscribe(defaultBroker, defaultBrokerId, defaultOnlyThisBroker);
        assertEquals(masterAddress, actual);
    }
    
    @Test
    public void testFindBrokerAddressInSubscribeBrokerNameExistsWithSlaveId() throws IllegalAccessException {
        HashMap<Long, String> brokerMap = new HashMap<>();
        brokerMap.put(MixAll.FIRST_SLAVE_ID, slaveAddress);
        ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
        brokerAddrTable.put(defaultBroker, brokerMap);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "brokerAddrTable", brokerAddrTable, true);
        String actual = topicRouteInfoManager.findBrokerAddressInSubscribe(defaultBroker, MixAll.FIRST_SLAVE_ID, false);
        assertEquals(slaveAddress, actual);
    }
    
    @Test
    public void testFindBrokerAddressInSubscribeBrokerNameDoesNotExist() throws IllegalAccessException {
        ConcurrentMap<String, Map<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "brokerAddrTable", brokerAddrTable, true);
        assertNull(topicRouteInfoManager.findBrokerAddressInSubscribe(defaultBroker, defaultBrokerId, defaultOnlyThisBroker));
    }
    
    @Test
    public void testGetTopicSubscribeInfoWhenQueuesIsEmpty() throws IllegalAccessException {
        ConcurrentMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
        topicSubscribeInfoTable.put(defaultTopic, new HashSet<>());
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "topicSubscribeInfoTable", topicSubscribeInfoTable, true);
        assertNotNull(topicRouteInfoManager.getTopicSubscribeInfo(defaultTopic));
    }
    
    @Test
    public void testGetTopicSubscribeInfoWhenQueuesIsNotEmpty() throws IllegalAccessException {
        Set<MessageQueue> expected = new HashSet<>();
        expected.add(new MessageQueue(defaultTopic, defaultBroker, 0));
        ConcurrentMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
        topicSubscribeInfoTable.put(defaultTopic, expected);
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "topicSubscribeInfoTable", topicSubscribeInfoTable, true);
        assertEquals(expected, topicRouteInfoManager.getTopicSubscribeInfo(defaultTopic));
    }
    
    @Test
    public void testGetTopicSubscribeInfoWhenTopicDoesNotExist() throws IllegalAccessException {
        ConcurrentMap<String, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
        FieldUtils.writeDeclaredField(topicRouteInfoManager, "topicSubscribeInfoTable", topicSubscribeInfoTable, true);
        assertNull(topicRouteInfoManager.getTopicSubscribeInfo(defaultTopic));
    }
}
