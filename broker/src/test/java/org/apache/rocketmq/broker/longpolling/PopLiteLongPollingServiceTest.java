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

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
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

    @Test
    public void testPolling_requestSurvivesQueueCleanupRace() throws Exception {
        String clientId = "clientId";
        String group = "group";

        // Routine post-sweep state: the client's last request was just woken, leaving an
        // empty queue for it in pollingMap.
        ConcurrentSkipListSet<PopRequest> queue = new ConcurrentSkipListSet<>(PopRequest.COMPARATOR);
        pollingMap.put(clientId, queue);

        // Gate polling() between its pollingMap.get() and its queue.add(): the per-key size
        // check consults the broker config on every call.
        CountDownLatch enteredSizeCheck = new CountDownLatch(1);
        CountDownLatch releaseSizeCheck = new CountDownLatch(1);
        BrokerConfig gatingConfig = new BrokerConfig() {
            @Override
            public int getPopPollingSize() {
                enteredSizeCheck.countDown();
                try {
                    releaseSizeCheck.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.getPopPollingSize();
            }
        };
        when(brokerController.getBrokerConfig()).thenReturn(gatingConfig);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        when(ctx.channel()).thenReturn(channel);

        AtomicReference<PollingResult> resultRef = new AtomicReference<>();
        CountDownLatch pollingDone = new CountDownLatch(1);
        Thread pollingThread = new Thread(() -> {
            resultRef.set(popLiteLongPollingService.polling(
                ctx, remotingCommand, System.currentTimeMillis(), 10000, clientId, group));
            pollingDone.countDown();
        });
        pollingThread.start();
        assertTrue(enteredSizeCheck.await(10, TimeUnit.SECONDS));

        // The service thread's periodic cleanup removes the empty queue from pollingMap
        // while the polling thread holds the stale reference.
        Method cleanUnusedResource = PopLiteLongPollingService.class.getDeclaredMethod("cleanUnusedResource");
        cleanUnusedResource.setAccessible(true);
        cleanUnusedResource.invoke(popLiteLongPollingService);
        assertFalse(pollingMap.containsKey(clientId));

        releaseSizeCheck.countDown();
        assertTrue(pollingDone.await(10, TimeUnit.SECONDS));
        assertEquals(PollingResult.POLLING_SUC, resultRef.get());

        // The suspended request must stay reachable through pollingMap: both
        // notifyMessageArriving() and the timeout sweep in run() look it up there.
        assertSame(queue, pollingMap.get(clientId));
        assertTrue(popLiteLongPollingService.notifyMessageArriving(clientId, true, 0, group));
        assertEquals(0, totalPollingNum.get());
    }

    @Test
    public void testPolling_evictedQueueWakesSuspendedRequests() throws Exception {
        // Capacity of one client: inserting the second client's queue evicts the first one.
        BrokerConfig smallConfig = new BrokerConfig();
        smallConfig.setPopPollingMapSize(1);
        when(brokerController.getBrokerConfig()).thenReturn(smallConfig);
        PopLiteLongPollingService service = new PopLiteLongPollingService(brokerController, processor, true);
        ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>> smallPollingMap =
            (ConcurrentLinkedHashMap<String, ConcurrentSkipListSet<PopRequest>>)
                FieldUtils.readDeclaredField(service, "pollingMap", true);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        RemotingCommand remotingCommand1 = mock(RemotingCommand.class);
        RemotingCommand remotingCommand2 = mock(RemotingCommand.class);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        when(ctx.channel()).thenReturn(channel);

        assertEquals(PollingResult.POLLING_SUC, service.polling(
            ctx, remotingCommand1, System.currentTimeMillis(), 10000, "clientA", "groupA"));
        assertEquals(PollingResult.POLLING_SUC, service.polling(
            ctx, remotingCommand2, System.currentTimeMillis(), 10000, "clientB", "groupB"));
        // Force any pending eviction bookkeeping to settle.
        assertFalse(smallPollingMap.containsKey("clientA"));

        // clientA's queue was evicted at capacity while still holding its suspended request;
        // the request must be woken up instead of left unreachable in the evicted queue.
        verify(pullMessageExecutor).submit(any(Runnable.class));
    }
}
