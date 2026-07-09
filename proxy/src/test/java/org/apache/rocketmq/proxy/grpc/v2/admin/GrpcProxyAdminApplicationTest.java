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
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyClient;
import apache.rocketmq.v2.ProxyScope;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GrpcProxyAdminApplicationTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
    }

    @Test
    public void listAndDescribeClientsThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PUSH_CONSUMER, "group-b", "topic-b"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse listResponse = stub.listClients(ListClientsRequest.newBuilder()
                .setPageNum(1)
                .setPageSize(100)
                .build());

            assertThat(listResponse.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(listResponse.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a", "client-b");
            assertThat(listResponse.getHasMore()).isFalse();

            DescribeClientResponse describeResponse = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("client-a")
                .build());

            assertThat(describeResponse.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(describeResponse.getClient().getClientId()).isEqualTo("client-a");
            assertThat(describeResponse.getClient().getClientType()).isEqualTo(ClientType.PRODUCER);
            assertThat(describeResponse.getClient().getGroupsList()).containsExactly("group-a");
            assertThat(describeResponse.getClient().getTopicsList()).containsExactly("topic-a");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void describeClientReturnsDashboardClientViewFieldsThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-dashboard", ClientType.PUSH_CONSUMER,
                "dashboard-group", "dashboard-topic"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            DescribeClientResponse response = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("client-dashboard")
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
            ProxyClient client = response.getClient();
            assertThat(client.getClientId()).isEqualTo("client-dashboard");
            assertThat(client.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
            assertThat(client.getGroupsList()).containsExactly("dashboard-group");
            assertThat(client.getTopicsList()).containsExactly("dashboard-topic");
            assertThat(client.getLanguage()).isEqualTo("JAVA");
            assertThat(client.getRemoteAddress()).isEqualTo("127.0.0.1:8080");
            assertThat(client.getLocalAddress()).isEqualTo("192.168.0.1:8080");
            assertThat(client.getVersion()).isEqualTo("V5_0_0");
            assertThat(client.getConnectTimeMillis()).isEqualTo(100L);
            assertThat(client.getLastActiveTimeMillis()).isEqualTo(200L);
            assertThat(client.getProxyId()).isEqualTo("proxy-a");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void publicServiceRejectsNonLocalM1ScopeForEveryRpc() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse response = stub.listClients(ListClientsRequest.newBuilder()
                .setScope(ProxyScope.PROXY_SCOPE_ALL_PROXIES)
                .setPageNum(1)
                .setPageSize(100)
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(response.getStatus().getMessage()).contains("only supports LOCAL_PROXY");

            DescribeClientResponse describeResponse = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("client-a")
                .setScope(ProxyScope.PROXY_SCOPE_ALL_PROXIES)
                .build());
            assertThat(describeResponse.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(describeResponse.getStatus().getMessage()).contains("only supports LOCAL_PROXY");

            apache.rocketmq.v2.ListClientsByGroupResponse groupResponse = stub.listClientsByGroup(
                apache.rocketmq.v2.ListClientsByGroupRequest.newBuilder()
                    .setGroup("group-a")
                    .setScope(ProxyScope.PROXY_SCOPE_ALL_PROXIES)
                    .setPageNum(1)
                    .setPageSize(100)
                    .build());
            assertThat(groupResponse.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(groupResponse.getStatus().getMessage()).contains("only supports LOCAL_PROXY");

            apache.rocketmq.v2.ListClientsByTopicResponse topicResponse = stub.listClientsByTopic(
                apache.rocketmq.v2.ListClientsByTopicRequest.newBuilder()
                    .setTopic("topic-a")
                    .setScope(ProxyScope.PROXY_SCOPE_ALL_PROXIES)
                    .setPageNum(1)
                    .setPageSize(100)
                    .build());
            assertThat(topicResponse.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(topicResponse.getStatus().getMessage()).contains("only supports LOCAL_PROXY");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void publicServiceMapsInvalidContestParametersToBadRequest() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse negativePageNum = stub.listClients(ListClientsRequest.newBuilder()
                .setPageNum(-1)
                .setPageSize(100)
                .build());
            assertThat(negativePageNum.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(negativePageNum.getStatus().getMessage()).contains("pageNum");

            ListClientsResponse reversedConnectTime = stub.listClients(ListClientsRequest.newBuilder()
                .setConnectTimeStartMillis(200L)
                .setConnectTimeEndMillis(100L)
                .setPageSize(100)
                .build());
            assertThat(reversedConnectTime.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(reversedConnectTime.getStatus().getMessage()).contains("connectTimeStartMillis");

            ListClientsResponse unrecognizedScope = stub.listClients(ListClientsRequest.newBuilder()
                .setScopeValue(99)
                .setPageSize(100)
                .build());
            assertThat(unrecognizedScope.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(unrecognizedScope.getStatus().getMessage()).contains("Unsupported proxy scope");

            apache.rocketmq.v2.ListClientsByGroupResponse missingGroup = stub.listClientsByGroup(
                apache.rocketmq.v2.ListClientsByGroupRequest.newBuilder()
                    .setPageSize(100)
                    .build());
            assertThat(missingGroup.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(missingGroup.getStatus().getMessage()).contains("group is required");

            apache.rocketmq.v2.ListClientsByTopicResponse missingTopic = stub.listClientsByTopic(
                apache.rocketmq.v2.ListClientsByTopicRequest.newBuilder()
                    .setPageSize(100)
                    .build());
            assertThat(missingTopic.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
            assertThat(missingTopic.getStatus().getMessage()).contains("topic is required");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void listClientsByGroupAndTopicThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PUSH_CONSUMER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PRODUCER, "group-b", "topic-b"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            assertThat(stub.listClientsByGroup(apache.rocketmq.v2.ListClientsByGroupRequest.newBuilder()
                .setGroup("group-a")
                .setPageSize(100)
                .build()).getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a");

            assertThat(stub.listClientsByTopic(apache.rocketmq.v2.ListClientsByTopicRequest.newBuilder()
                .setTopic("topic-b")
                .setPageSize(100)
                .build()).getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-b");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void listClientsDefaultsPageNumAndHonorsOptionalConnectTime() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-a", ClientType.PRODUCER, "group-a", "topic-a"));
            readService(activity).upsertClient(client("client-b", ClientType.PRODUCER, "group-a", "topic-a"));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse response = stub.listClients(ListClientsRequest.newBuilder()
                .setConnectTimeStartMillis(100)
                .setConnectTimeEndMillis(100)
                .setPageSize(100)
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(response.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-a", "client-b");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void listClientsHonorsContestFiltersAndPaginationThroughGeneratedGrpcService() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            readService(activity).upsertClient(client("client-alpha-1", ClientType.PRODUCER, "group-a",
                "topic-a", "JAVA", 100L));
            readService(activity).upsertClient(client("client-alpha-2", ClientType.PRODUCER, "group-a",
                "topic-a", "JAVA", 200L));
            readService(activity).upsertClient(client("client-beta-1", ClientType.PRODUCER, "group-a",
                "topic-a", "CPP", 100L));
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            ListClientsResponse firstPage = stub.listClients(ListClientsRequest.newBuilder()
                .setClientIdPrefix("client-alpha")
                .setClientLanguage("JAVA")
                .setConnectTimeStartMillis(50L)
                .setConnectTimeEndMillis(250L)
                .setPageNum(1)
                .setPageSize(1)
                .build());

            assertThat(firstPage.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(firstPage.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-alpha-1");
            assertThat(firstPage.getHasMore()).isTrue();

            ListClientsResponse secondPage = stub.listClients(ListClientsRequest.newBuilder()
                .setClientIdPrefix("client-alpha")
                .setClientLanguage("JAVA")
                .setConnectTimeStartMillis(50L)
                .setConnectTimeEndMillis(250L)
                .setPageNum(2)
                .setPageSize(1)
                .build());

            assertThat(secondPage.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(secondPage.getClientsList())
                .extracting(ProxyClient::getClientId)
                .containsExactly("client-alpha-2");
            assertThat(secondPage.getHasMore()).isFalse();
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    @Test
    public void describeMissingClientReturnsNotFoundStatus() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);
        Server server = null;
        ManagedChannel channel = null;
        try {
            server = ServerBuilder.forPort(0)
                .directExecutor()
                .addService(new GrpcProxyAdminApplication(activity.getProxyClientAdminEndpointExecutor()))
                .build()
                .start();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .directExecutor()
                .build();
            ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub =
                ProxyAdminServiceGrpc.newBlockingStub(channel);

            DescribeClientResponse response = stub.describeClient(DescribeClientRequest.newBuilder()
                .setClientId("missing-client")
                .build());

            assertThat(response.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
            assertThat(response.hasClient()).isFalse();
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (server != null) {
                server.shutdownNow();
            }
            activity.shutdown();
        }
    }

    private static ProxyClientReadService readService(DefaultGrpcMessagingActivity activity) throws Exception {
        Field field = DefaultGrpcMessagingActivity.class.getDeclaredField("proxyClientReadService");
        field.setAccessible(true);
        return (ProxyClientReadService) field.get(activity);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String group, String topic) {
        return client(clientId, clientType, group, topic, "JAVA", 100L);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String group, String topic,
        String language, long connectTimeMillis) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            Collections.singleton(group),
            Collections.singleton(topic),
            language,
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            "proxy-a",
            connectTimeMillis,
            connectTimeMillis + 100L
        );
    }
}
