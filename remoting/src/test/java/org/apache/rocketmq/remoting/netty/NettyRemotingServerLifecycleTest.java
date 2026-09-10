/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NettyRemotingServerLifecycleTest {
    private NettyRemotingServer server;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() throws Exception {
        NettyServerConfig config = new NettyServerConfig();
        config.setBindAddress("127.0.0.1");
        config.setListenPort(0);
        config.setServerSelectorThreads(1);
        config.setServerWorkerThreads(1);
        config.setServerCallbackExecutorThreads(1);
        config.setServerAsyncSemaphoreValue(2);
        server = new NettyRemotingServer(config);
        scheduler = (ScheduledExecutorService) FieldUtils.readDeclaredField(server, "scheduledExecutorService", true);
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        // Also clean up the scheduler when the shutdown regression test fails on unfixed code.
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (server != null) {
            server.shutdown();
            server.eventLoopGroupBoss.terminationFuture().await(5, TimeUnit.SECONDS);
            server.eventLoopGroupSelector.terminationFuture().await(5, TimeUnit.SECONDS);
            if (server.getDefaultEventExecutorGroup() != null) {
                server.getDefaultEventExecutorGroup().terminationFuture().await(5, TimeUnit.SECONDS);
            }
            server.getCallbackExecutor().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testScanTimeoutsForParentAndSubServers() throws Exception {
        // No extra listeners are needed: server-initiated requests use an existing channel.
        RemotingServer firstSubServer = server.newRemotingServer(1234);
        RemotingServer secondSubServer = server.newRemotingServer(1235);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 4321));
        when(channel.writeAndFlush(any())).thenAnswer(invocation ->
            new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess());

        for (RemotingServer target : Arrays.asList(server, firstSubServer, secondSubServer)) {
            NettyRemotingAbstract remoting = (NettyRemotingAbstract) target;
            CompletableFuture<ResponseFuture> expired = new CompletableFuture<>();
            RemotingCommand request = RemotingCommand.createRequestCommand(0, null);
            target.invokeAsync(channel, request, 100, expired::complete);
            CompletableFuture<ResponseFuture> pending = remoting.invokeImpl(channel,
                RemotingCommand.createRequestCommand(0, null), TimeUnit.MINUTES.toMillis(1));
            assertThat(remoting.responseTable).hasSize(2);
            assertThat(remoting.semaphoreAsync.availablePermits()).isZero();

            ResponseFuture response = expired.get(10, TimeUnit.SECONDS);
            assertThat(response.getCause()).isInstanceOf(RemotingTimeoutException.class);
            assertThat(remoting.responseTable).hasSize(1).doesNotContainKey(request.getOpaque());
            assertThat(remoting.semaphoreAsync.availablePermits()).isEqualTo(1);
            assertThat(pending.isDone()).isFalse();
        }
    }

    @Test
    public void testShutdownStopsScheduler() throws Exception {
        // Ensure its worker has started before checking termination.
        scheduler.submit(() -> { }).get(5, TimeUnit.SECONDS);
        server.shutdown();
        assertThat(scheduler.isShutdown()).isTrue();
        assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
}
