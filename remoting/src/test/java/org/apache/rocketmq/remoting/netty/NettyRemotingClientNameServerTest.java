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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class NettyRemotingClientNameServerTest {
    private static final String FIRST = "127.0.0.1:9876";
    private static final String SECOND = "127.0.0.2:9876";
    private final List<ChannelPromise> promises = new ArrayList<>();
    private NettyClientConfig config;
    private ProbeClient client;

    @Before
    public void setUp() {
        config = new NettyClientConfig();
        config.setConnectTimeoutMillis(100);
        client = new ProbeClient(config);
    }

    @After
    public void tearDown() throws Exception {
        client.shutdown();
        for (ChannelPromise promise : promises) {
            promise.cancel(false);
            ((EmbeddedChannel) promise.channel()).finishAndReleaseAll();
        }
        EventLoopGroup group = (EventLoopGroup) FieldUtils.readField(client, "eventLoopGroupWorker", true);
        assertThat(group.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testScanStartsAllConnectionsWithoutWaiting() throws Exception {
        ChannelPromise first = connection(FIRST);
        ChannelPromise second = connection(SECOND);
        addresses(FIRST, SECOND);

        scan();

        assertThat(client.connectThreads).containsEntry(FIRST, Thread.currentThread().getName())
            .containsEntry(SECOND, Thread.currentThread().getName());
        assertThat(first.isDone()).isFalse();
        assertThat(second.isDone()).isFalse();
        assertThat(client.getAvailableNameSrvList()).isEmpty();
        first.setSuccess();
        second.setFailure(new ConnectTimeoutException("test connection timeout"));
        assertThat(client.getAvailableNameSrvList()).containsExactly(FIRST);
    }

    @Test
    public void testFailedProbeRemovesPreviouslyAvailableAddress() throws Exception {
        ChannelPromise first = connection(FIRST);
        addresses(FIRST);
        scan();
        first.setSuccess();
        assertThat(client.getAvailableNameSrvList()).containsExactly(FIRST);

        first.channel().close();
        ChannelPromise retry = connection(FIRST);
        scan();
        retry.setFailure(new ConnectTimeoutException("test connection timeout"));
        assertThat(client.getAvailableNameSrvList()).isEmpty();
    }

    @Test
    public void testRemovedAddressIsNotRestoredByLateCompletion() throws Exception {
        ChannelPromise first = connection(FIRST);
        ChannelPromise second = connection(SECOND);
        addresses(FIRST);
        scan();
        addresses(SECOND);
        scan();

        first.setSuccess();
        assertThat(client.getAvailableNameSrvList()).isEmpty();
        second.setSuccess();
        assertThat(client.getAvailableNameSrvList()).containsExactly(SECOND);
    }

    @Test
    public void testOldProbeCannotOverwriteReplacementConnection() throws Exception {
        ChannelPromise first = connection(FIRST);
        addresses(FIRST);
        scan();
        ChannelPromise replacement = connection(FIRST);
        Map<String, NettyRemotingClient.ChannelWrapper> channels =
            (Map<String, NettyRemotingClient.ChannelWrapper>) FieldUtils.readField(client, "channelTables", true);
        channels.put(FIRST, client.new ChannelWrapper(FIRST, replacement));
        scan();

        replacement.setSuccess();
        first.setFailure(new ConnectTimeoutException("old connection failed"));
        assertThat(client.getAvailableNameSrvList()).containsExactly(FIRST);
    }

    @Test
    public void testBusyChannelLockDefersProbeWithoutWaiting() throws Exception {
        connection(FIRST);
        addresses(FIRST);
        Lock channelLock = (Lock) FieldUtils.readField(client, "lockChannelTables", true);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        channelLock.lock();
        try {
            caller.submit(() -> {
                scan();
                return null;
            }).get(1, TimeUnit.SECONDS);
            assertThat(client.connectThreads).isEmpty();
        } finally {
            channelLock.unlock();
            caller.shutdownNow();
            assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        scan();
        assertThat(client.connectThreads).containsKey(FIRST);
    }

    @Test
    public void testHousekeepingTimerStartsProbesAndContinuesWhileConnectionsArePending() throws Exception {
        connection(FIRST);
        connection(SECOND);
        addresses(FIRST, SECOND);
        client.start();
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            assertThat(client.connectThreads).containsEntry(FIRST, "ClientHouseKeepingService")
                .containsEntry(SECOND, "ClientHouseKeepingService"));
        awaitTimer();
        assertThat(client.getAvailableNameSrvList()).isEmpty();
        assertThat(client.attempts.get(FIRST).get()).isEqualTo(1);
        assertThat(client.attempts.get(SECOND).get()).isEqualTo(1);
    }

    @Test
    public void testDisabledScanDoesNotProbe() throws Exception {
        config.setScanAvailableNameSrv(false);
        connection(FIRST);
        addresses(FIRST);
        client.start();
        awaitTimer();
        assertThat(client.connectThreads).isEmpty();
    }

    @Test
    public void testInactiveChannelIsNotAvailableEvenIfConnectSucceeded() throws Exception {
        ChannelPromise first = connection(FIRST);
        addresses(FIRST);
        scan();
        first.channel().close();
        first.setSuccess();
        assertThat(client.getAvailableNameSrvList()).isEmpty();
    }

    @Test
    public void testActualConnectionAndDisconnectionUpdateAvailability() throws Exception {
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setBindAddress("127.0.0.1");
        serverConfig.setListenPort(0);
        serverConfig.setServerSelectorThreads(1);
        serverConfig.setServerWorkerThreads(1);
        serverConfig.setServerCallbackExecutorThreads(1);
        NettyRemotingServer server = new NettyRemotingServer(serverConfig);
        NettyRemotingClient actualClient = new NettyRemotingClient(new NettyClientConfig());
        try {
            server.start();
            String address = "127.0.0.1:" + server.localListenPort();
            actualClient.updateNameServerAddressList(new ArrayList<>(Arrays.asList(address)));
            actualClient.start();
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(actualClient.getAvailableNameSrvList()).containsExactly(address));

            server.shutdown();
            await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(actualClient.getAvailableNameSrvList()).isEmpty());
        } finally {
            actualClient.shutdown();
            server.shutdown();
            server.eventLoopGroupBoss.terminationFuture().await(5, TimeUnit.SECONDS);
            server.eventLoopGroupSelector.terminationFuture().await(5, TimeUnit.SECONDS);
            server.getDefaultEventExecutorGroup().terminationFuture().await(5, TimeUnit.SECONDS);
            EventLoopGroup group = (EventLoopGroup) FieldUtils.readField(actualClient, "eventLoopGroupWorker", true);
            group.terminationFuture().await(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testShutdownDoesNotPublishLateSuccess() throws Exception {
        ChannelPromise first = connection(FIRST);
        addresses(FIRST);
        scan();
        client.shutdown();
        first.setSuccess();
        assertThat(client.getAvailableNameSrvList()).isEmpty();
    }

    private void awaitTimer() throws Exception {
        HashedWheelTimer timer = (HashedWheelTimer) FieldUtils.readField(client, "timer", true);
        CountDownLatch tick = new CountDownLatch(1);
        timer.newTimeout(timeout -> tick.countDown(), 0, TimeUnit.MILLISECONDS);
        assertThat(tick.await(5, TimeUnit.SECONDS)).isTrue();
    }

    private ChannelPromise connection(String address) {
        ChannelPromise promise = new DefaultChannelPromise(new EmbeddedChannel(), ImmediateEventExecutor.INSTANCE);
        promises.add(promise);
        client.connections.put(address, promise);
        return promise;
    }

    private void addresses(String... addresses) {
        client.updateNameServerAddressList(new ArrayList<>(Arrays.asList(addresses)));
    }

    private void scan() throws Exception {
        Method scan = NettyRemotingClient.class.getDeclaredMethod("scanAvailableNameSrv");
        scan.setAccessible(true);
        scan.invoke(client);
    }

    private static class ProbeClient extends NettyRemotingClient {
        private final Map<String, ChannelPromise> connections = new ConcurrentHashMap<>();
        private final Map<String, String> connectThreads = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

        ProbeClient(NettyClientConfig config) {
            super(config);
        }

        @Override
        protected ChannelFuture doConnect(String address) {
            connectThreads.put(address, Thread.currentThread().getName());
            attempts.computeIfAbsent(address, key -> new AtomicInteger()).incrementAndGet();
            return connections.get(address);
        }
    }
}
