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
package org.apache.rocketmq.proxy.service.admin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultAdminServiceTest {

    @Mock
    private MQClientAPIFactory mqClientAPIFactory;
    @Mock
    private MQClientAPIExt mqClientAPIExt;

    private DefaultAdminService adminService;

    private static final String ADDR = "127.0.0.1:10911";
    private static final String TOPIC = "topicA";
    private static final String GROUP = "groupA";
    private static final long TIMEOUT = 3000L;

    @Before
    public void setUp() {
        when(mqClientAPIFactory.getClient()).thenReturn(mqClientAPIExt);
        adminService = new DefaultAdminService(mqClientAPIFactory);
    }

    @Test
    public void getMaxOffsetDelegatesToClient() throws Exception {
        MessageQueue mq = new MessageQueue(TOPIC, "broker-a", 0);
        when(mqClientAPIExt.getMaxOffset(anyString(), any(MessageQueue.class), anyLong())).thenReturn(100L);

        assertEquals(100L, adminService.getMaxOffset(ADDR, mq, TIMEOUT));
    }

    @Test
    public void getMinOffsetDelegatesToClient() throws Exception {
        MessageQueue mq = new MessageQueue(TOPIC, "broker-a", 0);
        when(mqClientAPIExt.getMinOffset(anyString(), any(MessageQueue.class), anyLong())).thenReturn(5L);

        assertEquals(5L, adminService.getMinOffset(ADDR, mq, TIMEOUT));
    }

    @Test
    public void getEarliestMsgStoretimeDelegatesToClient() throws Exception {
        MessageQueue mq = new MessageQueue(TOPIC, "broker-a", 0);
        when(mqClientAPIExt.getEarliestMsgStoretime(anyString(), any(MessageQueue.class), anyLong())).thenReturn(12345L);

        assertEquals(12345L, adminService.getEarliestMsgStoretime(ADDR, mq, TIMEOUT));
    }

    @Test
    public void fetchConsumeStatsDelegatesToClient() throws Exception {
        ConsumeStats consumeStats = new ConsumeStats();
        Map<MessageQueue, OffsetWrapper> table = new HashMap<>();
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(200);
        wrapper.setConsumerOffset(150);
        table.put(new MessageQueue(TOPIC, "broker-a", 0), wrapper);
        consumeStats.setOffsetTable(table);
        when(mqClientAPIExt.getConsumeStats(anyString(), anyString(), anyString(), anyLong())).thenReturn(consumeStats);

        ConsumeStats result = adminService.fetchConsumeStats(ADDR, GROUP, TOPIC, TIMEOUT);
        assertNotNull(result);
        assertEquals(200L, result.getOffsetTable().get(new MessageQueue(TOPIC, "broker-a", 0)).getBrokerOffset());
    }

    @Test
    public void resetOffsetDelegatesToClient() throws Exception {
        Map<MessageQueue, Long> map = new HashMap<>();
        map.put(new MessageQueue(TOPIC, "broker-a", 0), 42L);
        when(mqClientAPIExt.invokeBrokerToResetOffset(anyString(), anyString(), anyString(), anyLong(), anyBoolean(), anyLong()))
            .thenReturn(map);

        Map<MessageQueue, Long> result = adminService.resetOffset(ADDR, TOPIC, GROUP, System.currentTimeMillis(), true, TIMEOUT);
        assertEquals(42L, result.get(new MessageQueue(TOPIC, "broker-a", 0)).longValue());
    }

    @Test
    public void viewMessageDelegatesToClient() throws Exception {
        MessageExt ext = new MessageExt();
        ext.setTopic(TOPIC);
        ext.setMsgId("m1");
        when(mqClientAPIExt.viewMessage(anyString(), anyString(), anyLong(), anyLong())).thenReturn(ext);

        MessageExt result = adminService.viewMessage(ADDR, TOPIC, 12345L, TIMEOUT);
        assertNotNull(result);
        assertEquals("m1", result.getMsgId());
    }

    @Test
    public void getTopicConfigDelegatesToClient() throws Exception {
        TopicConfigAndQueueMapping topicConfig = new TopicConfigAndQueueMapping();
        topicConfig.setTopicName(TOPIC);
        topicConfig.setReadQueueNums(8);
        when(mqClientAPIExt.getTopicConfig(anyString(), anyString(), anyLong())).thenReturn(topicConfig);

        TopicConfigAndQueueMapping result = adminService.getTopicConfig(ADDR, TOPIC, TIMEOUT);
        assertNotNull(result);
        assertEquals(TOPIC, result.getTopicName());
    }

    @Test
    public void getTopicRouteDataDelegatesToClient() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setOrderTopicConf("orderConf");
        when(mqClientAPIExt.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        TopicRouteData result = adminService.getTopicRouteData(TOPIC);
        assertNotNull(result);
        assertEquals("orderConf", result.getOrderTopicConf());
    }

    @Test
    public void queryMessageReturnsDecodedMessages() throws Exception {
        MessageExt ext = new MessageExt();
        ext.setTopic(TOPIC);
        ext.setMsgId("m1");
        ext.setBody("payload".getBytes());
        ext.setBornHost(new java.net.InetSocketAddress("127.0.0.1", 10909));
        ext.setStoreHost(new java.net.InetSocketAddress("127.0.0.1", 10911));
        ext.setBornTimestamp(System.currentTimeMillis());
        ext.setStoreTimestamp(System.currentTimeMillis());
        final byte[] body = MessageDecoder.encode(ext, false);

        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            ResponseFuture responseFuture = mock(ResponseFuture.class);
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "ok");
            response.setBody(body);
            when(responseFuture.getResponseCommand()).thenReturn(response);
            callback.operationComplete(responseFuture);
            return null;
        }).when(mqClientAPIExt).queryMessage(anyString(), any(QueryMessageRequestHeader.class), anyLong(), any(InvokeCallback.class), anyBoolean());

        List<MessageExt> result = adminService.queryMessage(ADDR, TOPIC, "key", 10, 0L, System.currentTimeMillis(), TIMEOUT);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(TOPIC, result.get(0).getTopic());
        assertEquals("payload", new String(result.get(0).getBody()));
    }

    @Test
    public void queryMessageFailsWhenCallbackFails() throws Exception {
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            callback.operationFail(new RuntimeException("boom"));
            return null;
        }).when(mqClientAPIExt).queryMessage(anyString(), any(QueryMessageRequestHeader.class), anyLong(), any(InvokeCallback.class), anyBoolean());

        try {
            adminService.queryMessage(ADDR, TOPIC, "key", 10, 0L, System.currentTimeMillis(), TIMEOUT);
            fail("expected queryMessage to throw");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("boom", e.getCause().getMessage());
        }
    }

    @Test
    public void queryMessageReturnsEmptyWhenResponseNotSuccess() throws Exception {
        doAnswer(invocation -> {
            InvokeCallback callback = invocation.getArgument(3);
            ResponseFuture responseFuture = mock(ResponseFuture.class);
            RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SYSTEM_ERROR, "err");
            when(responseFuture.getResponseCommand()).thenReturn(response);
            callback.operationComplete(responseFuture);
            return null;
        }).when(mqClientAPIExt).queryMessage(anyString(), any(QueryMessageRequestHeader.class), anyLong(), any(InvokeCallback.class), anyBoolean());

        List<MessageExt> result = adminService.queryMessage(ADDR, TOPIC, "key", 10, 0L, System.currentTimeMillis(), TIMEOUT);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }
}
