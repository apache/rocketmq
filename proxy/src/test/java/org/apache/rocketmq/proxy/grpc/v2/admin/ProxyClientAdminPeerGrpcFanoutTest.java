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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyClientAdminPeerGrpcFanoutTest {

    @Test
    public void coordinatorListsAllProxyClientsThroughRealGrpcPeerTransport() throws Exception {
        PeerServer proxyB = null;
        PeerServer proxyA = null;
        try {
            proxyB = PeerServer.start("proxy-b", client("client-b"));
            proxyA = PeerServer.start("proxy-a", client("client-a"));
            Map<String, Channel> channels = new LinkedHashMap<>();
            channels.put(proxyB.proxyId, proxyB.channel);
            channels.put(proxyA.proxyId, proxyA.channel);
            ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(
                new ProxyClientAdminPeerGrpcTransport(channels)
            );
            ProxyClientAdminCoordinatorService coordinatorService =
                new ProxyClientAdminCoordinatorService(peerClient);

            ProxyClientAdminResult<ProxyClientPage> result = coordinatorService.listClients(
                ProxyContext.create()
                    .setSubject(User.of("admin"))
                    .setRemoteAddress("192.168.0.1:8080")
                    .setLocalAddress("127.0.0.1:8081"),
                ProxyClientQuery.newBuilder()
                    .setScope(ProxyClientScope.ALL_PROXIES)
                    .setPageSize(10)
                    .build()
            );

            assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(result.getBody().getClients())
                .extracting(ProxyClientInfo::getClientId)
                .containsExactly("client-a", "client-b");
            assertThat(result.getBody().getClients())
                .extracting(ProxyClientInfo::getProxyId)
                .containsExactly("proxy-a", "proxy-b");
            assertThat(result.getBody().getNextPageToken()).isEmpty();
        } finally {
            close(proxyB);
            close(proxyA);
        }
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.singleton("group-a"),
            Collections.singleton("topic-a"),
            "JAVA",
            "192.168.0.1:8080",
            "127.0.0.1:8081",
            "V5_0_0",
            1000L,
            2000L
        );
    }

    private static void close(PeerServer peerServer) throws InterruptedException {
        if (peerServer != null) {
            peerServer.close();
        }
    }

    private static class PeerServer {
        private final String proxyId;
        private final Server server;
        private final ManagedChannel channel;

        private PeerServer(String proxyId, Server server, ManagedChannel channel) {
            this.proxyId = proxyId;
            this.server = server;
            this.channel = channel;
        }

        private static PeerServer start(String proxyId, ProxyClientInfo clientInfo) throws Exception {
            ProxyClientReadService readService = new ProxyClientReadService();
            readService.upsertClient(clientInfo);
            DefaultClientAdminService adminService = new DefaultClientAdminService(readService);
            ProxyClientAdminContextFactory contextFactory = new ProxyClientAdminContextFactory(
                (context, headers, request) -> {
                }
            );
            ProxyClientAdminPeerGrpcService service = new ProxyClientAdminPeerGrpcService(
                contextFactory,
                new ProxyClientAdminPeerMessageHandler(
                    new ProxyClientAdminPeerLocalExecutor(proxyId, adminService)
                )
            );
            Server server = NettyServerBuilder.forPort(0)
                .directExecutor()
                .addService(ServerInterceptors.intercept(service, new ContextInterceptor()))
                .build()
                .start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            return new PeerServer(proxyId, server, channel);
        }

        private void close() throws InterruptedException {
            this.channel.shutdownNow();
            this.server.shutdownNow();
            this.channel.awaitTermination(5, TimeUnit.SECONDS);
            this.server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
