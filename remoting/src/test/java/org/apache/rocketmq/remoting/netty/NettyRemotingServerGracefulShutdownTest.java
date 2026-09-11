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

import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class NettyRemotingServerGracefulShutdownTest {
    private static final int REQUEST_CODE = 32001;
    private static final int GRACE_SECONDS = 5;
    private final List<Socket> sockets = new ArrayList<>();
    private final ExecutorService shutdownExecutor = Executors.newFixedThreadPool(6);
    private NettyServerConfig config;
    private NettyRemotingServer server;

    @Before
    public void setUp() throws Exception {
        config = new NettyServerConfig();
        config.setBindAddress("127.0.0.1");
        config.setListenPort(0);
        config.setServerSelectorThreads(1);
        config.setServerWorkerThreads(1);
        config.setServerCallbackExecutorThreads(1);
        config.setEnableShutdownGracefully(true);
        config.setShutdownWaitTimeSeconds(GRACE_SECONDS);
        server = new NettyRemotingServer(config);
        registerProcessor(server);
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        for (Socket socket : sockets) {
            socket.close();
        }
        if (server != null) {
            config.setEnableShutdownGracefully(false);
            server.shutdown();
            server.eventLoopGroupBoss.terminationFuture().await(10, TimeUnit.SECONDS);
            server.eventLoopGroupSelector.terminationFuture().await(10, TimeUnit.SECONDS);
            server.getDefaultEventExecutorGroup().terminationFuture().await(10, TimeUnit.SECONDS);
            server.getCallbackExecutor().awaitTermination(10, TimeUnit.SECONDS);
        }
        shutdownExecutor.shutdownNow();
        assertThat(shutdownExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testSubServerShutdownPreservesGracePeriodAndOtherPorts() throws Exception {
        RemotingServer child = startSubServer();
        RemotingServer sibling = startSubServer();
        Socket existing = connect(child);
        assertThat(request(existing)).isEqualTo(ResponseCode.SUCCESS);

        Future<?> shutdown = shutdownExecutor.submit(child::shutdown);
        awaitDraining(child);
        Future<?> duplicate = shutdownExecutor.submit(child::shutdown);
        assertWaiting(duplicate);
        assertThat(request(existing)).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(request(connect(child))).isEqualTo(ResponseCode.GO_AWAY);
        // Retain the existing protocol-version condition for older clients.
        assertThat(request(existing, MQVersion.Version.V5_3_1.ordinal())).isEqualTo(ResponseCode.SUCCESS);
        assertThat(request(connect(server))).isEqualTo(ResponseCode.SUCCESS);
        assertThat(request(connect(sibling))).isEqualTo(ResponseCode.SUCCESS);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isFalse();

        shutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        duplicate.get(3, TimeUnit.SECONDS);
        assertListenerClosed(child);
        assertThat(request(connect(server))).isEqualTo(ResponseCode.SUCCESS);
        assertThat(request(connect(sibling))).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testParentShutdownDrainsAllPortsConcurrently() throws Exception {
        RemotingServer first = startSubServer();
        RemotingServer second = startSubServer();
        RemotingServer third = startSubServer();
        Socket existing = connect(first);
        assertThat(request(existing)).isEqualTo(ResponseCode.SUCCESS);

        Future<?> shutdown = shutdownExecutor.submit(server::shutdown);
        awaitDraining(third);
        Future<?> duplicate = shutdownExecutor.submit(server::shutdown);
        Future<?> childShutdown = shutdownExecutor.submit(first::shutdown);
        assertWaiting(duplicate);
        assertWaiting(childShutdown);
        assertThat(request(existing)).isEqualTo(ResponseCode.GO_AWAY);
        for (RemotingServer target : new RemotingServer[] {server, first, second, third}) {
            assertThat(request(connect(target))).isEqualTo(ResponseCode.GO_AWAY);
        }
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isFalse();
        // Three children must not add three separate grace periods to the parent shutdown.
        shutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        duplicate.get(3, TimeUnit.SECONDS);
        childShutdown.get(3, TimeUnit.SECONDS);
        assertListenerClosed(first);
        assertListenerClosed(second);
        assertListenerClosed(third);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isTrue();
    }

    @Test
    public void testChildShutdownBeforeParentShutdown() throws Exception {
        RemotingServer child = startSubServer();
        Socket existing = connect(child);
        assertThat(request(existing)).isEqualTo(ResponseCode.SUCCESS);
        Future<?> childShutdown = shutdownExecutor.submit(child::shutdown);
        awaitDraining(child);
        // The child keeps its original deadline even if a later parent shutdown has
        // a shorter configured wait. Shared event loops must outlive both deadlines.
        config.setShutdownWaitTimeSeconds(1);
        Future<?> parentShutdown = shutdownExecutor.submit(server::shutdown);
        awaitDraining(server);
        assertThatThrownBy(() -> parentShutdown.get(2, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isFalse();

        assertThat(request(existing)).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(request(connect(child))).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(request(connect(server))).isEqualTo(ResponseCode.GO_AWAY);
        childShutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        parentShutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        assertListenerClosed(child);
    }

    @Test
    public void testConcurrentParentAndChildShutdown() throws Exception {
        RemotingServer child = startSubServer();
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> shutdowns = new ArrayList<>();
        for (RemotingServer target : new RemotingServer[] {server, child, server, child}) {
            shutdowns.add(shutdownExecutor.submit(() -> {
                start.await();
                target.shutdown();
                return null;
            }));
        }
        start.countDown();
        awaitDraining(server);
        awaitDraining(child);
        assertThat(request(connect(server))).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(request(connect(child))).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isFalse();
        for (Future<?> shutdown : shutdowns) {
            shutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        }
        assertListenerClosed(child);
    }

    @Test
    public void testDisabledGracefulShutdownClosesImmediately() throws Exception {
        config.setEnableShutdownGracefully(false);
        config.setShutdownWaitTimeSeconds(30);
        RemotingServer child = startSubServer();
        shutdownExecutor.submit(child::shutdown).get(3, TimeUnit.SECONDS);
        assertListenerClosed(child);
        assertThat(request(connect(server))).isEqualTo(ResponseCode.SUCCESS);
        shutdownExecutor.submit(server::shutdown).get(3, TimeUnit.SECONDS);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isTrue();
    }

    @Test
    public void testInterruptedShutdownKeepsSharedGracePeriodAndCleansUp() throws Exception {
        RemotingServer child = startSubServer();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        Future<?> shutdown = shutdownExecutor.submit(() -> {
            Thread.currentThread().interrupt();
            server.shutdown();
            interruptRestored.set(Thread.interrupted());
        });
        awaitDraining(child);
        assertThat(request(connect(child))).isEqualTo(ResponseCode.GO_AWAY);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isFalse();
        shutdown.get(GRACE_SECONDS + 3, TimeUnit.SECONDS);
        assertThat(interruptRestored.get()).isTrue();
        assertListenerClosed(child);
        assertThat(server.eventLoopGroupSelector.isShuttingDown()).isTrue();
    }

    private RemotingServer startSubServer() throws Exception {
        int port;
        try (ServerSocket availablePort = new ServerSocket(0)) {
            port = availablePort.getLocalPort();
        }
        RemotingServer child = server.newRemotingServer(port);
        registerProcessor(child);
        child.start();
        return child;
    }

    private void registerProcessor(RemotingServer target) {
        target.registerProcessor(REQUEST_CODE, (ctx, command) ->
            RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "ok"), null);
    }

    private Socket connect(RemotingServer target) throws Exception {
        Socket socket = new Socket();
        sockets.add(socket);
        socket.connect(new InetSocketAddress("127.0.0.1", target.localListenPort()), 1000);
        socket.setSoTimeout(1000);
        return socket;
    }

    private int request(Socket socket) throws Exception {
        return request(socket, MQVersion.CURRENT_VERSION);
    }

    private int request(Socket socket, int version) throws Exception {
        RemotingCommand command = RemotingCommand.createRequestCommand(REQUEST_CODE, null);
        command.setVersion(version);
        ByteBuffer encoded = command.encode();
        byte[] data = new byte[encoded.remaining()];
        encoded.get(data);
        socket.getOutputStream().write(data);
        socket.getOutputStream().flush();
        DataInputStream input = new DataInputStream(socket.getInputStream());
        byte[] response = new byte[input.readInt()];
        input.readFully(response);
        return RemotingCommand.decode(response).getCode();
    }

    private void awaitDraining(RemotingServer target) {
        await().atMost(3, TimeUnit.SECONDS).untilTrue(((NettyRemotingAbstract) target).isShuttingDown);
    }

    private void assertWaiting(Future<?> shutdown) {
        assertThatThrownBy(() -> shutdown.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private void assertListenerClosed(RemotingServer target) {
        assertThatThrownBy(() -> connect(target)).isInstanceOf(java.net.ConnectException.class);
    }
}
