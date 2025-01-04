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
package org.apache.rocketmq.broker.longpolling;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PopLongPollingServiceTest {
    
    @Mock
    private BrokerController brokerController;
    
    @Mock
    private NettyRequestProcessor processor;
    
    @Mock
    private ChannelHandlerContext ctx;
    
    @Mock
    private ExecutorService pullMessageExecutor;
    
    private PopLongPollingService popLongPollingService;
    
    private final String defaultTopic = "defaultTopic";
    
    @Before
    public void init() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setPopPollingMapSize(100);
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        popLongPollingService = spy(new PopLongPollingService(brokerController, processor, true));
    }
    
    @Test
    public void testNotifyMessageArrivingWithRetryTopic() {
        int queueId = 0;
        doNothing().when(popLongPollingService).notifyMessageArrivingWithRetryTopic(defaultTopic, queueId, -1L, null, 0L, null, null);
        popLongPollingService.notifyMessageArrivingWithRetryTopic(defaultTopic, queueId);
        verify(popLongPollingService, times(1)).notifyMessageArrivingWithRetryTopic(defaultTopic, queueId, -1L, null, 0L, null, null);
    }
    
    @Test
    public void testNotifyMessageArriving() {
        int queueId = 0;
        Long tagsCode = 123L;
        long offset = 123L;
        long msgStoreTime = System.currentTimeMillis();
        byte[] filterBitMap = new byte[]{0x01};
        Map<String, String> properties = new ConcurrentHashMap<>();
        doNothing().when(popLongPollingService).notifyMessageArriving(defaultTopic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
        popLongPollingService.notifyMessageArrivingWithRetryTopic(defaultTopic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
        verify(popLongPollingService).notifyMessageArriving(defaultTopic, queueId, offset, tagsCode, msgStoreTime, filterBitMap, properties);
    }
    
    @Test
    public void testNotifyMessageArrivingValidRequest() throws Exception {
        String cid = "CID_1";
        int queueId = 0;
        ConcurrentLinkedHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentHashMap<String, Byte>>()
            .maximumWeightedCapacity(10).build();
        ConcurrentHashMap<String, Byte> cids = new ConcurrentHashMap<>();
        cids.put(cid, (byte) 1);
        topicCidMap.put(defaultTopic, cids);
        popLongPollingService = new PopLongPollingService(brokerController, processor, true);
        ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap =
                new ConcurrentLinkedHashMap.Builder<String, ConcurrentSkipListSet<PopRequest>>().maximumWeightedCapacity(this.brokerController.getBrokerConfig().getPopPollingMapSize()).build();
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        PopRequest popRequest = mock(PopRequest.class);
        MessageFilter messageFilter = mock(MessageFilter.class);
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        when(popRequest.getMessageFilter()).thenReturn(messageFilter);
        when(popRequest.getSubscriptionData()).thenReturn(subscriptionData);
        when(popRequest.getChannel()).thenReturn(channel);
        String pollingKey = KeyBuilder.buildPollingKey(defaultTopic, cid, queueId);
        ConcurrentSkipListSet popRequests = mock(ConcurrentSkipListSet.class);
        when(popRequests.pollLast()).thenReturn(popRequest);
        pollingMap.put(pollingKey, popRequests);
        FieldUtils.writeDeclaredField(popLongPollingService, "topicCidMap", topicCidMap, true);
        FieldUtils.writeDeclaredField(popLongPollingService, "pollingMap", pollingMap, true);
        boolean actual = popLongPollingService.notifyMessageArriving(defaultTopic, queueId, cid, null, 0, null, null);
        assertFalse(actual);
    }
    
    @Test
    public void testWakeUpNullRequest() {
        assertFalse(popLongPollingService.wakeUp(null));
    }
    
    @Test
    public void testWakeUpIncompleteRequest() {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(false);
        assertFalse(popLongPollingService.wakeUp(request));
    }
    
    @Test
    public void testWakeUpInactiveChannel() {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(true);
        when(request.getCtx()).thenReturn(ctx);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(brokerController.getPullMessageExecutor()).thenReturn(pullMessageExecutor);
        assertTrue(popLongPollingService.wakeUp(request));
    }
    
    @Test
    public void testWakeUpValidRequestWithException() throws Exception {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(true);
        when(request.getCtx()).thenReturn(ctx);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(request.getChannel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(brokerController.getPullMessageExecutor()).thenReturn(pullMessageExecutor);
        when(processor.processRequest(any(), any())).thenThrow(new RuntimeException("Test Exception"));
        assertTrue(popLongPollingService.wakeUp(request));
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(pullMessageExecutor).submit(captor.capture());
        captor.getValue().run();
        verify(processor).processRequest(any(), any());
    }
    
    @Test
    public void testPollingNotPolling() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        PollingHeader requestHeader = mock(PollingHeader.class);
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        MessageFilter messageFilter = mock(MessageFilter.class);
        when(requestHeader.getPollTime()).thenReturn(0L);
        PollingResult result = popLongPollingService.polling(ctx, remotingCommand, requestHeader, subscriptionData, messageFilter);
        assertEquals(PollingResult.NOT_POLLING, result);
    }
    
    @Test
    public void testPollingServicePollingTimeout() throws IllegalAccessException {
        String cid = "CID_1";
        popLongPollingService = new PopLongPollingService(brokerController, processor, true);
        popLongPollingService.shutdown();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        PollingHeader requestHeader = mock(PollingHeader.class);
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        MessageFilter messageFilter = mock(MessageFilter.class);
        when(requestHeader.getPollTime()).thenReturn(1000L);
        when(requestHeader.getTopic()).thenReturn(defaultTopic);
        when(requestHeader.getConsumerGroup()).thenReturn("defaultGroup");
        ConcurrentLinkedHashMap<String, ConcurrentHashMap<String, Byte>> topicCidMap = new ConcurrentLinkedHashMap.Builder<String, ConcurrentHashMap<String, Byte>>()
            .maximumWeightedCapacity(10).build();
        ConcurrentHashMap<String, Byte> cids = new ConcurrentHashMap<>();
        cids.put(cid, (byte) 1);
        topicCidMap.put(defaultTopic, cids);
        FieldUtils.writeDeclaredField(popLongPollingService, "topicCidMap", topicCidMap, true);
        PollingResult result = popLongPollingService.polling(ctx, remotingCommand, requestHeader, subscriptionData, messageFilter);
        assertEquals(PollingResult.POLLING_TIMEOUT, result);
    }
    
    @Test
    public void testPollingPollingSuc() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        PollingHeader requestHeader = mock(PollingHeader.class);
        SubscriptionData subscriptionData = mock(SubscriptionData.class);
        MessageFilter messageFilter = mock(MessageFilter.class);
        when(requestHeader.getPollTime()).thenReturn(1000L);
        when(requestHeader.getBornTime()).thenReturn(System.currentTimeMillis());
        when(requestHeader.getTopic()).thenReturn("topic");
        when(requestHeader.getConsumerGroup()).thenReturn("cid");
        when(requestHeader.getQueueId()).thenReturn(0);
        PollingResult result = popLongPollingService.polling(ctx, remotingCommand, requestHeader, subscriptionData, messageFilter);
        assertEquals(PollingResult.POLLING_SUC, result);
    }
}
