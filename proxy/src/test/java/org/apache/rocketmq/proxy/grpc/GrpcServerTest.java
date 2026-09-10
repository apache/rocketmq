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

package org.apache.rocketmq.proxy.grpc;

import com.google.protobuf.StringValue;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultithreadEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcServerTest extends InitConfigTest {
    private final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    private final TlsCertificateManager certificates = mock(TlsCertificateManager.class);
    private static final MethodDescriptor<StringValue, StringValue> METHOD =
        MethodDescriptor.<StringValue, StringValue>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test.Echo/Ping")
            .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
            .build();

    @Before
    public void configure() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setEnableGrpcEpoll(false);
        config.setTlsTestModeEnable(true);
        config.setGrpcBossLoopNum(2);
        config.setGrpcWorkerLoopNum(3);
    }

    @After
    public void cleanup() {
        executor.shutdownNow();
    }

    @Test
    public void testDefaultOwnedGroupsTerminate() throws Exception {
        checkOwnedGroups(GrpcServerBuilder.newBuilder(executor, 0, certificates), 2, 3);
    }

    @Test
    public void testExplicitOwnedGroupCounts() throws Exception {
        checkOwnedGroups(GrpcServerBuilder.newBuilder(executor, 0, certificates, 1, 1), 1, 1);
    }

    @Test
    public void testEpollOwnedGroupsTerminate() throws Exception {
        Assume.assumeTrue(Epoll.isAvailable());
        ConfigurationManager.getProxyConfig().setEnableGrpcEpoll(true);
        checkOwnedGroups(GrpcServerBuilder.newBuilder(executor, 0, certificates, 1, 1), 1, 1);
    }

    private void checkOwnedGroups(GrpcServerBuilder builder, int bossThreads, int workerThreads) throws Exception {
        GrpcServer server = builder.addService(service()).build();
        MultithreadEventLoopGroup boss = (MultithreadEventLoopGroup) field(server, "ownedBossGroup");
        MultithreadEventLoopGroup worker = (MultithreadEventLoopGroup) field(server, "ownedWorkerGroup");
        try {
            assertThat(boss.executorCount()).isEqualTo(bossThreads);
            assertThat(worker.executorCount()).isEqualTo(workerThreads);
            server.start();
            ping(server);
        } finally {
            server.shutdown();
        }
        assertTerminated(boss, worker);
        verify(certificates).unregisterReloadListener(server.tlsReloadHandler);
    }

    @Test
    public void testBorrowedGroupsSurviveShutdownAndBindFailure() throws Exception {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup(1);
        GrpcServer first = GrpcServerBuilder.newBuilder(executor, 0, certificates, boss, worker)
            .addService(service()).build();
        GrpcServer second = GrpcServerBuilder.newBuilder(executor, 0, certificates, boss, worker)
            .addService(service()).build();
        GrpcServer failed = null;
        try {
            first.start();
            second.start();
            ping(first);
            ping(second);
            first.shutdown();
            ping(second);
            assertThat(boss.isShuttingDown()).isFalse();
            assertThat(worker.isShuttingDown()).isFalse();

            failed = GrpcServerBuilder.newBuilder(executor, port(second), certificates, boss, worker).build();
            assertThatThrownBy(failed::start).isInstanceOf(IOException.class);
            ping(second);
            assertThat(boss.isShuttingDown()).isFalse();
            assertThat(worker.isShuttingDown()).isFalse();
        } finally {
            first.shutdown();
            second.shutdown();
            if (failed != null) {
                failed.shutdown();
            }
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
        assertTerminated(boss, worker);
    }

    @Test
    public void testOwnedGroupsTerminateAfterBindFailure() throws Exception {
        GrpcServer running = GrpcServerBuilder.newBuilder(executor, 0, certificates, 1, 1).build();
        GrpcServer failed = null;
        try {
            running.start();
            failed = GrpcServerBuilder.newBuilder(executor, port(running), certificates, 1, 1).build();
            assertThatThrownBy(failed::start).isInstanceOf(IOException.class);
            assertTerminated((EventLoopGroup) field(failed, "ownedBossGroup"),
                (EventLoopGroup) field(failed, "ownedWorkerGroup"));
        } finally {
            running.shutdown();
            if (failed != null) {
                failed.shutdown();
            }
        }
    }

    @Test
    public void testShutdownTimeoutReleasesOwnedGroups() throws Exception {
        Server delegate = mock(Server.class);
        EventLoopGroup boss = mock(EventLoopGroup.class);
        EventLoopGroup worker = mock(EventLoopGroup.class);
        when(delegate.shutdown()).thenReturn(delegate);
        when(delegate.shutdownNow()).thenReturn(delegate);
        when(delegate.awaitTermination(1, TimeUnit.SECONDS)).thenReturn(false, true);
        new GrpcServer(delegate, 1, TimeUnit.SECONDS, certificates, boss, worker).shutdown();
        verify(delegate).shutdownNow();
        verify(boss).shutdownGracefully();
        verify(worker).shutdownGracefully();
    }

    @Test
    public void testInterruptedShutdownReleasesOwnedGroups() throws Exception {
        Server delegate = mock(Server.class);
        EventLoopGroup boss = mock(EventLoopGroup.class);
        EventLoopGroup worker = mock(EventLoopGroup.class);
        when(delegate.shutdown()).thenReturn(delegate);
        when(delegate.awaitTermination(1, TimeUnit.SECONDS)).thenThrow(new InterruptedException());
        try {
            new GrpcServer(delegate, 1, TimeUnit.SECONDS, certificates, boss, worker).shutdown();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(delegate).shutdownNow();
            verify(boss).shutdownGracefully();
            verify(worker).shutdownGracefully();
        } finally {
            Thread.interrupted();
        }
    }

    private ServerServiceDefinition service() {
        return ServerServiceDefinition.builder("test.Echo")
            .addMethod(METHOD, ServerCalls.asyncUnaryCall((request, response) -> {
                response.onNext(request);
                response.onCompleted();
            })).build();
    }

    private int port(GrpcServer server) throws Exception {
        return ((Server) field(server, "server")).getPort();
    }

    private void ping(GrpcServer server) throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", port(server)).usePlaintext().build();
        try {
            assertThat(ClientCalls.blockingUnaryCall(channel, METHOD,
                CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), StringValue.of("ping")))
                .isEqualTo(StringValue.of("ping"));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private void assertTerminated(EventLoopGroup boss, EventLoopGroup worker) {
        assertThat(boss.terminationFuture().awaitUninterruptibly(10, TimeUnit.SECONDS)).isTrue();
        assertThat(worker.terminationFuture().awaitUninterruptibly(10, TimeUnit.SECONDS)).isTrue();
    }

    private Object field(GrpcServer server, String name) throws Exception {
        Field field = GrpcServer.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(server);
    }
}
