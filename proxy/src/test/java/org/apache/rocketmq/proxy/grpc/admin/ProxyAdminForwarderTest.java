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

import apache.rocketmq.v2.AdminGrpc;
import apache.rocketmq.v2.GetConsumerRunningInfoRequest;
import apache.rocketmq.v2.GetConsumerRunningInfoResponse;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyAdminForwarderTest extends InitConfigTest {
    private static final String GROUP = "group-a";
    private static final String CLIENT_ID = "remote-client";
    private static final long REQUEST_TIMEOUT_MILLIS = 1000;

    private final AtomicInteger requests = new AtomicInteger();
    private final CompletableFuture<Deadline> peerDeadline = new CompletableFuture<>();
    private final CompletableFuture<Void> peerCancelled = new CompletableFuture<>();
    private Server peer;
    private ProxyAdminForwarder forwarder;
    private TlsMode previousTlsMode;
    private ScheduledExecutorService scheduler;

    @Before
    public void setUp() throws Exception {
        previousTlsMode = TlsSystemConfig.tlsMode;
        TlsSystemConfig.tlsMode = TlsMode.PERMISSIVE;
        peer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .addService(new AdminGrpc.AdminImplBase() {
                @Override
                public void getConsumerRunningInfo(GetConsumerRunningInfoRequest request,
                    StreamObserver<GetConsumerRunningInfoResponse> observer) {
                    if (requests.incrementAndGet() == 1) {
                        Context context = Context.current();
                        peerDeadline.complete(context.getDeadline());
                        context.addListener(ignored -> peerCancelled.complete(null), Runnable::run);
                        // The owning proxy accepts the request but never returns a response.
                        return;
                    }
                    observer.onNext(GetConsumerRunningInfoResponse.getDefaultInstance());
                    observer.onCompleted();
                }
            }).build().start();

        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setLocalServeAddr("127.0.0.2");
        config.setTlsTestModeEnable(true);
        config.setGrpcAdminServerEnable(true);
        config.setGrpcAdminServerPort(peer.getPort());
        config.setGrpcAdminServerForwardTimeoutMillis(REQUEST_TIMEOUT_MILLIS);
        ConsumerManager consumers = mock(ConsumerManager.class);
        RemoteChannel remote = new RemoteChannel("127.0.0.1", "127.0.0.1:10000", "127.0.0.1:8081",
            ChannelProtocolType.GRPC_V2, null);
        when(consumers.findChannel(GROUP, CLIENT_ID)).thenReturn(new ClientChannelInfo(remote));
        ServiceManager services = mock(ServiceManager.class);
        when(services.getConsumerManager()).thenReturn(consumers);
        forwarder = new ProxyAdminForwarder(services);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (forwarder != null) {
                forwarder.shutdown();
            }
        } finally {
            if (peer != null) {
                peer.shutdownNow();
                assertTrue(peer.awaitTermination(10, TimeUnit.SECONDS));
            }
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            TlsSystemConfig.tlsMode = previousTlsMode;
        }
    }

    @Test
    public void testForwardWithoutCallerDeadlineTimesOutAndCachedPeerRemainsUsable() throws Exception {
        CompletableFuture<GetConsumerRunningInfoResponse> first = forward();
        assertNotNull("the peer call must carry the configured deadline", peerDeadline.get(10, TimeUnit.SECONDS));
        assertDeadlineExceeded(first);
        peerCancelled.get(10, TimeUnit.SECONDS);

        // A timed-out call must not leave an expired deadline on the cached peer stub.
        assertEquals(GetConsumerRunningInfoResponse.getDefaultInstance(), forward().get(10, TimeUnit.SECONDS));
        assertEquals(2, requests.get());
    }

    @Test
    public void testForwardPreservesShorterCallerDeadline() throws Exception {
        ConfigurationManager.getProxyConfig().setGrpcAdminServerForwardTimeoutMillis(30000);
        try (Context.CancellableContext caller = Context.current()
            .withDeadlineAfter(REQUEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, scheduler)) {
            CompletableFuture<GetConsumerRunningInfoResponse> result = caller.call(this::forward);
            Deadline deadline = peerDeadline.get(10, TimeUnit.SECONDS);
            assertNotNull(deadline);
            assertTrue("the peer must not get the longer configured timeout",
                deadline.timeRemaining(TimeUnit.MILLISECONDS) <= REQUEST_TIMEOUT_MILLIS);
            assertDeadlineExceeded(result);
            peerCancelled.get(10, TimeUnit.SECONDS);
        }
    }

    private CompletableFuture<GetConsumerRunningInfoResponse> forward() {
        CompletableFuture<GetConsumerRunningInfoResponse> result = new CompletableFuture<>();
        boolean forwarded = forwarder.forwardIfRemote(GROUP, CLIENT_ID,
            new StreamObserver<GetConsumerRunningInfoResponse>() {
                @Override
                public void onNext(GetConsumerRunningInfoResponse value) {
                    result.complete(value);
                }

                @Override
                public void onError(Throwable throwable) {
                    result.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {
                }
            }, (stub, observer) -> stub.getConsumerRunningInfo(
                GetConsumerRunningInfoRequest.getDefaultInstance(), observer));
        assertTrue("the remote client should be forwarded to its owning proxy", forwarded);
        return result;
    }

    private void assertDeadlineExceeded(CompletableFuture<?> result) throws Exception {
        try {
            result.get(10, TimeUnit.SECONDS);
            fail("the unresponsive peer should time out");
        } catch (ExecutionException expected) {
            assertEquals(Status.Code.DEADLINE_EXCEEDED, Status.fromThrowable(expected.getCause()).getCode());
        }
    }
}
