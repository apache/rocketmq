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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import com.alibaba.fastjson.JSON;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AdminModelConverterTest {

    @Mock
    private AdminService adminService;
    @Mock
    private MessageQueueView messageQueueView;
    @Mock
    private MessageQueueSelector messageQueueSelector;

    private static final String TOPIC = "topicA";
    private static final String GROUP = "groupA";
    private static final String BROKER_ADDR = "127.0.0.1:10911";

    @Before
    public void setUp() {
    }

    @Test
    public void toGroupAccumulationSumsDiff() throws Exception {
        ConsumeStats consumeStats = new ConsumeStats();
        Map<org.apache.rocketmq.common.message.MessageQueue, OffsetWrapper> table = new HashMap<>();
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(100);
        wrapper.setConsumerOffset(60);
        table.put(new org.apache.rocketmq.common.message.MessageQueue(TOPIC, "broker-a", 0), wrapper);
        consumeStats.setOffsetTable(table);

        when(adminService.fetchConsumeStats(eq(BROKER_ADDR), eq(GROUP), eq(TOPIC), anyLong()))
            .thenReturn(consumeStats);

        DescribeGroupAccumulationResponse.GroupAccumulation accumulation =
            AdminModelConverter.toGroupAccumulation(adminService, BROKER_ADDR, GROUP, TOPIC, 3000L);

        assertNotNull(accumulation);
        assertEquals(40L, accumulation.getAccumulation());
        assertEquals(40L, accumulation.getReadyMessages());
    }

    @Test
    public void toGroupAccumulationNullConsumeStats() throws Exception {
        when(adminService.fetchConsumeStats(eq(BROKER_ADDR), eq(GROUP), eq(TOPIC), anyLong()))
            .thenReturn(null);

        DescribeGroupAccumulationResponse.GroupAccumulation accumulation =
            AdminModelConverter.toGroupAccumulation(adminService, BROKER_ADDR, GROUP, TOPIC, 3000L);

        assertNotNull(accumulation);
        assertEquals(0L, accumulation.getAccumulation());
        assertEquals(0L, accumulation.getReadyMessages());
    }

    @Test
    public void toTopicRouteReturnsJson() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setOrderTopicConf("orderConf");
        when(adminService.getTopicRouteData(eq(TOPIC))).thenReturn(topicRouteData);

        GetTopicRouteResponse response = AdminModelConverter.toTopicRoute(adminService, TOPIC);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(JSON.toJSONString(topicRouteData), response.getTopicRouteData());
    }

    @Test
    public void toTopicStatusReturnsConfig() throws Exception {
        TopicConfigAndQueueMapping topicConfig = new TopicConfigAndQueueMapping();
        topicConfig.setTopicName(TOPIC);
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(16);
        topicConfig.setPerm(6);
        when(adminService.getTopicConfig(eq(BROKER_ADDR), eq(TOPIC), anyLong())).thenReturn(topicConfig);

        DescribeTopicStatusResponse response =
            AdminModelConverter.toTopicStatus(adminService, BROKER_ADDR, TOPIC, 3000L);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertTrue(response.getDescription().contains(TOPIC));
        assertTrue(response.getDescription().contains("readQueues=8"));
        assertTrue(response.getDescription().contains("writeQueues=16"));
    }

    @Test
    public void toTopicStatusNullConfig() throws Exception {
        when(adminService.getTopicConfig(eq(BROKER_ADDR), eq(TOPIC), anyLong())).thenReturn(null);

        DescribeTopicStatusResponse response =
            AdminModelConverter.toTopicStatus(adminService, BROKER_ADDR, TOPIC, 3000L);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertTrue(response.getDescription().isEmpty());
    }

    @Test
    public void toQueryTimeSpanReturnsPerQueueSpan() throws Exception {
        AddressableMessageQueue amq = new AddressableMessageQueue(
            new org.apache.rocketmq.common.message.MessageQueue(TOPIC, "broker-a", 0), BROKER_ADDR);
        when(messageQueueView.getReadSelector()).thenReturn(messageQueueSelector);
        when(messageQueueSelector.getQueues()).thenReturn(Arrays.asList(amq));
        when(adminService.getEarliestMsgStoretime(eq(BROKER_ADDR), any(), anyLong())).thenReturn(12345L);

        QueryTimeSpanResponse response = AdminModelConverter.toQueryTimeSpan(
            adminService, BROKER_ADDR, GROUP, TOPIC, messageQueueView, 3000L);

        assertEquals(Code.OK, response.getStatus().getCode());
        assertEquals(1, response.getQueueTimeSpanListCount());
        QueryTimeSpanResponse.QueueTimeSpan span = response.getQueueTimeSpanList(0);
        assertEquals(12345L, span.getMinTimestamp());
        assertEquals("broker-a", span.getMessageQueue().getBroker().getName());
    }

    @Test
    public void toMessageConvertsFields() {
        MessageExt ext = new MessageExt();
        ext.setTopic(TOPIC);
        ext.setMsgId("msg-1");
        ext.setTags("tagA");
        ext.setKeys("k1 k2");
        ext.setBody("hello".getBytes());

        Message message = AdminModelConverter.toMessage(ext);

        assertNotNull(message);
        assertEquals(TOPIC, message.getTopic().getName());
        assertEquals("msg-1", message.getSystemProperties().getMessageId());
        assertEquals("tagA", message.getSystemProperties().getTag());
        assertEquals(Arrays.asList("k1", "k2"), message.getSystemProperties().getKeysList());
        assertEquals("hello", new String(message.getBody().toByteArray()));
    }

    @Test
    public void toMessageNullReturnsNull() {
        assertNull(AdminModelConverter.toMessage(null));
    }

    @Test
    public void toMessageQueueSetsBrokerAndId() {
        org.apache.rocketmq.common.message.MessageQueue mq =
            new org.apache.rocketmq.common.message.MessageQueue(TOPIC, "broker-a", 3);

        MessageQueue v2 = AdminModelConverter.toMessageQueue(mq);

        assertEquals("broker-a", v2.getBroker().getName());
        assertEquals(3, v2.getId());
        assertEquals(TOPIC, v2.getTopic().getName());
    }

    @Test
    public void toMessageQueueEmptyBrokerName() {
        org.apache.rocketmq.common.message.MessageQueue mq =
            new org.apache.rocketmq.common.message.MessageQueue(TOPIC, "", 0);

        MessageQueue v2 = AdminModelConverter.toMessageQueue(mq);

        assertEquals("", v2.getBroker().getName());
        assertEquals(0, v2.getId());
    }
}
