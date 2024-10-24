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
package org.apache.rocketmq.remoting.rpc;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ClientMetadataTest {

    private ClientMetadata clientMetadata;

    private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

    private final String defaultTopic = "defaultTopic";

    private final String defaultBroker = "defaultBroker";

    @Before
    public void init() throws IllegalAccessException {
        clientMetadata = new ClientMetadata();

        FieldUtils.writeDeclaredField(clientMetadata, "topicRouteTable", topicRouteTable, true);
        FieldUtils.writeDeclaredField(clientMetadata, "topicEndPointsTable", topicEndPointsTable, true);
        FieldUtils.writeDeclaredField(clientMetadata, "brokerAddrTable", brokerAddrTable, true);
    }

    @Test
    public void testGetBrokerNameFromMessageQueue() {
        MessageQueue mq1 = new MessageQueue(defaultTopic, "broker0", 0);
        MessageQueue mq2 = new MessageQueue(defaultTopic, "broker1", 0);
        ConcurrentMap<MessageQueue, String> messageQueueMap = new ConcurrentHashMap<>();
        messageQueueMap.put(mq1, "broker0");
        messageQueueMap.put(mq2, "broker1");
        topicEndPointsTable.put(defaultTopic, messageQueueMap);

        String actual = clientMetadata.getBrokerNameFromMessageQueue(mq1);
        assertEquals("broker0", actual);
    }

    @Test
    public void testGetBrokerNameFromMessageQueueNotFound() {
        MessageQueue mq = new MessageQueue("topic1", "broker0", 0);
        topicEndPointsTable.put(defaultTopic, new ConcurrentHashMap<>());

        String actual = clientMetadata.getBrokerNameFromMessageQueue(mq);
        assertEquals("broker0", actual);
    }

    @Test
    public void testFindMasterBrokerAddrNotFound() {
        assertNull(clientMetadata.findMasterBrokerAddr(defaultBroker));
    }

    @Test
    public void testFindMasterBrokerAddr() {
        String defaultBrokerAddr = "127.0.0.1:10911";
        brokerAddrTable.put(defaultBroker, new HashMap<>());
        brokerAddrTable.get(defaultBroker).put(0L, defaultBrokerAddr);

        String actual = clientMetadata.findMasterBrokerAddr(defaultBroker);
        assertEquals(defaultBrokerAddr, actual);
    }

    @Test
    public void testTopicRouteData2EndpointsForStaticTopicNotFound() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setTopicQueueMappingByBroker(null);

        ConcurrentMap<MessageQueue, String> actual = ClientMetadata.topicRouteData2EndpointsForStaticTopic(defaultTopic, topicRouteData);
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testTopicRouteData2EndpointsForStaticTopic() {
        TopicRouteData topicRouteData = new TopicRouteData();
        Map<String, TopicQueueMappingInfo> mappingInfos = new HashMap<>();
        TopicQueueMappingInfo info = new TopicQueueMappingInfo();
        info.setScope("scope");
        info.setCurrIdMap(new ConcurrentHashMap<>());
        info.getCurrIdMap().put(0, 0);
        info.setTotalQueues(1);
        info.setBname("bname");
        mappingInfos.put(defaultBroker, info);
        topicRouteData.setTopicQueueMappingByBroker(mappingInfos);

        ConcurrentMap<MessageQueue, String> actual = ClientMetadata.topicRouteData2EndpointsForStaticTopic(defaultTopic, topicRouteData);
        assertEquals(1, actual.size());
    }
}
