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
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PopLiteLongPollingServiceTest {
    
    @Mock
    private BrokerController brokerController;
    @Mock
    private NettyRequestProcessor processor;
    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private ExecutorService pullMessageExecutor;

    private BrokerConfig brokerConfig;
    private PopLiteLongPollingService popLiteLongPollingService;
    private ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> pollingMap;
    private AtomicLong totalPollingNum;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IllegalAccessException {
        brokerConfig = new BrokerConfig();
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getPullMessageExecutor()).thenReturn(pullMessageExecutor);
        popLiteLongPollingService = new PopLiteLongPollingService(brokerController, processor, true);
        pollingMap = (ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>>)
            FieldUtils.readDeclaredField(popLiteLongPollingService, "pollingMap", true);
        totalPollingNum = (AtomicLong) FieldUtils.readDeclaredField(popLiteLongPollingService, "totalPollingNum", true);
    }

    @Test
    public void testNotifyMessageArriving_noRequest() {
        assertFalse(popLiteLongPollingService.notifyMessageArriving("clientId", true, 0, "group"));
    }

    @Test
    public void testNotifyMessageArriving_inactiveChannel() throws Exception {
        String clientId = "clientId";
        String group = "group";

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(false);
        when(ctx.channel()).thenReturn(channel);

        PollingResult result = popLiteLongPollingService.polling(
            ctx, remotingCommand, System.currentTimeMillis(), 10000, clientId, group);
        assertEquals(PollingResult.POLLING_SUC, result);
        assertEquals(1, totalPollingNum.get());

        assertFalse(popLiteLongPollingService.notifyMessageArriving(clientId, true, 0, group));
        assertEquals(0, totalPollingNum.get());
    }

    @Test
    public void testNotifyMessageArriving_success() throws Exception {
        String clientId = "clientId";
        String group = "group";

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand1 = mock(RemotingCommand.class);
        RemotingCommand remotingCommand2 = mock(RemotingCommand.class);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        when(ctx.channel()).thenReturn(channel);

        PollingResult result1 = popLiteLongPollingService.polling(
            ctx, remotingCommand1, System.currentTimeMillis(), 10000, clientId, group);
        PollingResult result2 = popLiteLongPollingService.polling(
            ctx, remotingCommand2, System.currentTimeMillis(), 15000, clientId, group);

        assertEquals(PollingResult.POLLING_SUC, result1);
        assertEquals(PollingResult.POLLING_SUC, result2);
        assertEquals(2, totalPollingNum.get());

        assertTrue(popLiteLongPollingService.notifyMessageArriving(clientId, true, 0, group));
        assertEquals(1, totalPollingNum.get());
        assertEquals(remotingCommand1, pollingMap.get(clientId).pollFirst().getRemotingCommand()); // notify last
    }

    @Test
    public void testWakeUp_nullRequest() {
        assertFalse(popLiteLongPollingService.wakeUp(null));
    }

    @Test
    public void testWakeUp_completeRequest() {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(false);

        assertFalse(popLiteLongPollingService.wakeUp(request));
    }

    @Test
    public void testWakeUp_inactiveChannel() {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(true);
        when(request.getCtx()).thenReturn(ctx);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(false);

        assertFalse(popLiteLongPollingService.wakeUp(request));
        verify(pullMessageExecutor, never()).submit(any(Runnable.class));
    }

    @Test
    public void testWakeUp_success() {
        PopRequest request = mock(PopRequest.class);
        when(request.complete()).thenReturn(true);
        when(request.getCtx()).thenReturn(ctx);
        Channel channel = mock(Channel.class);
        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);

        assertTrue(popLiteLongPollingService.wakeUp(request));
        verify(pullMessageExecutor).submit(any(Runnable.class));
    }

    @Test
    public void testPolling_notPolling() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);

        PollingResult result = popLiteLongPollingService.polling(ctx, remotingCommand, 0, 0, "clientId", "group");
        assertEquals(PollingResult.NOT_POLLING, result);
    }

    @Test
    public void testPolling_timeout() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);

        PollingResult result =
            popLiteLongPollingService.polling(ctx, remotingCommand, System.currentTimeMillis(), 40, "clientId", "group");
        assertEquals(PollingResult.POLLING_TIMEOUT, result);
    }

    @Test
    public void testPolling_success() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);

        PollingResult result = popLiteLongPollingService.polling(
            ctx, remotingCommand, System.currentTimeMillis(), 10000, "clientId", "group");
        assertEquals(PollingResult.POLLING_SUC, result);
    }

    @Test
    public void testPolling_totalPollingFull() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        totalPollingNum.set(brokerConfig.getMaxPopPollingSize() + 1);

        PollingResult result = popLiteLongPollingService.polling(
            ctx, remotingCommand, System.currentTimeMillis(), 10000, "clientId", "group");
        assertEquals(PollingResult.POLLING_FULL, result);
    }

    @Test
    public void testPolling_singlePollingFull() {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        brokerConfig.setPopPollingSize(-1);

        PollingResult result = popLiteLongPollingService.polling(
            ctx, remotingCommand, System.currentTimeMillis(), 10000, "clientId", "group");
        assertEquals(PollingResult.POLLING_SUC, result);

        result = popLiteLongPollingService.polling(
            ctx, remotingCommand, System.currentTimeMillis(), 10000, "clientId", "group");
        assertEquals(PollingResult.POLLING_FULL, result);
    }
}
