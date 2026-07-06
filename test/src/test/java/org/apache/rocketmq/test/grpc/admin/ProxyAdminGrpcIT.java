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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.grpc.admin;

import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.ClientLanguage;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.UA;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.net.URL;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminBindableService;
import org.apache.rocketmq.proxy.grpc.admin.ProxyAdminGrpcService;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.DefaultProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.apache.rocketmq.proxy.config.ConfigurationManager.RMQ_PROXY_HOME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration test for Proxy Admin gRPC service (RIP-2 M1).
 * <p>
 * Tests the complete flow: gRPC client → admin server → service layer → response,
 * including both empty-result scenarios and connected-client scenarios.
 * <p>
 * Test environment:
 * - Namesrv + 3 Brokers (from BaseConf)
 * - Data plane gRPC server (MessagingServiceGrpc)
 * - Admin plane gRPC server (ProxyAdminBindableService)
 */
@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class ProxyAdminGrpcIT extends BaseConf {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // Data plane components
    private MessagingProcessor messagingProcessor;
    private GrpcMessagingApplication grpcMessagingApplication;

    // Admin plane components
    private ProxyAdminGrpcService adminGrpcService;

    // gRPC channels
    private Channel dataPlaneChannel;
    private Channel adminChannel;

    // Data plane stubs
    private MessagingServiceGrpc.MessagingServiceStub dataPlaneStub;

    // Metadata for data plane client identification
    private final Metadata header = new Metadata();

    // Telemetry stream observer (kept open for connected-client tests)
    private StreamObserver<TelemetryCommand> telemetryStream;

    @Before
    public void setUp() throws Exception {
        // Configure broker transaction check interval
        brokerController1.getBrokerConfig().setTransactionCheckInterval(1 * 1000);
        brokerController2.getBrokerConfig().setTransactionCheckInterval(1 * 1000);
        brokerController3.getBrokerConfig().setTransactionCheckInterval(1 * 1000);

        // Set up client metadata headers
        header.put(GrpcConstants.CLIENT_ID, "admin-test-client-" + UUID.randomUUID());
        header.put(GrpcConstants.LANGUAGE, "JAVA");

        // Ensure namesrv address is available globally for all internal clients
        System.setProperty("rocketmq.namesrv.addr", NAMESRV_ADDR);

        // Initialize proxy configuration
        String mockProxyHome = "/mock/rmq/proxy/home";
        URL mockProxyHomeURL = getClass().getClassLoader().getResource("rmq-proxy-home");
        if (mockProxyHomeURL != null) {
            mockProxyHome = mockProxyHomeURL.toURI().getPath();
        }
        if (null != mockProxyHome) {
            System.setProperty(RMQ_PROXY_HOME, mockProxyHome);
        }
        ConfigurationManager.initEnv();
        ConfigurationManager.initConfig();
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setNamesrvAddr(NAMESRV_ADDR);
        config.setLongPollingReserveTimeInMillis(500);
        config.setRocketMQClusterName(brokerController1.getBrokerConfig().getBrokerClusterName());
        config.setHeartbeatSyncerTopicClusterName(brokerController1.getBrokerConfig().getBrokerClusterName());
        config.setMinInvisibleTimeMillsForRecv(3);
        config.setGrpcClientConsumerMinLongPollingTimeoutMillis(0);

        // Create and start data plane
        messagingProcessor = DefaultMessagingProcessor.createForClusterMode();
        messagingProcessor.start();
        grpcMessagingApplication = GrpcMessagingApplication.create(messagingProcessor);
        grpcMessagingApplication.start();

        // Start data plane gRPC server
        startDataPlaneServer();

        // Wait for brokers to register
        await().atMost(Duration.ofSeconds(40)).until(() -> {
            Map<String, BrokerData> brokerDataMap = MQAdminTestUtils.getCluster(NAMESRV_ADDR).getBrokerAddrTable();
            return brokerDataMap.size() == BROKER_NUM;
        });

        // Create admin service layer
        GrpcChannelManager channelManager = grpcMessagingApplication.getGrpcChannelManager();
        ProxyAdminClientService adminClientService = new DefaultProxyAdminClientService(
            channelManager,
            grpcMessagingApplication.getGrpcClientSettingsManager());
        grpcMessagingApplication.setProxyAdminClientService(adminClientService);

        // Create admin gRPC service
        adminGrpcService = new ProxyAdminGrpcService(
            adminClientService, config.getProxyAdminThreadPoolNums());

        // Start admin gRPC server
        startAdminServer();
    }

    private void startDataPlaneServer() throws IOException, CertificateException {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        ServerServiceDefinition serviceDef = ServerInterceptors.intercept(
            grpcMessagingApplication,
            new ContextInterceptor(), new HeaderInterceptor());
        Server server = NettyServerBuilder.forPort(0)
            .directExecutor()
            .addService(serviceDef)
            .useTransportSecurity(ssc.certificate(), ssc.privateKey())
            .build()
            .start();
        grpcCleanup.register(server);
        int dataPlanePort = server.getPort();
        ConfigurationManager.getProxyConfig().setGrpcServerPort(dataPlanePort);

        // Create data plane client channel and stubs
        dataPlaneChannel = createChannel(dataPlanePort);
        dataPlaneStub = MessagingServiceGrpc.newStub(dataPlaneChannel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(header));
    }

    private void startAdminServer() throws IOException, CertificateException {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        ProxyAdminBindableService adminBindableService = new ProxyAdminBindableService(adminGrpcService);
        Server adminServer = NettyServerBuilder.forPort(0)
            .directExecutor()
            .addService(adminBindableService)
            .useTransportSecurity(ssc.certificate(), ssc.privateKey())
            .build()
            .start();
        grpcCleanup.register(adminServer);
        int adminPort = adminServer.getPort();

        // Create admin client channel
        adminChannel = createChannel(adminPort);
    }

    private Channel createChannel(int port) throws SSLException {
        return grpcCleanup.register(
            NettyChannelBuilder.forAddress("127.0.0.1", port)
                .directExecutor()
                .sslContext(SslContextBuilder.forClient()
                    .sslProvider(SslProvider.OPENSSL)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocolConfig(new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2))
                    .build())
                .build());
    }

    @After
    public void tearDown() throws Exception {
        if (telemetryStream != null) {
            telemetryStream.onCompleted();
            telemetryStream = null;
        }
        if (adminGrpcService != null) {
            adminGrpcService.shutdown();
        }
        if (grpcMessagingApplication != null) {
            grpcMessagingApplication.shutdown();
        }
        if (messagingProcessor != null) {
            messagingProcessor.shutdown();
        }
    }

    // ==================== Admin RPC Helper Methods ====================

    private ListClientsResponse listClients(ListClientsRequest request) {
        return ClientCalls.blockingUnaryCall(
            adminChannel, ProxyAdminBindableService.LIST_CLIENTS_METHOD, CallOptions.DEFAULT, request);
    }

    private DescribeClientResponse describeClient(DescribeClientRequest request) {
        return ClientCalls.blockingUnaryCall(
            adminChannel, ProxyAdminBindableService.DESCRIBE_CLIENT_METHOD, CallOptions.DEFAULT, request);
    }

    private ListClientsByGroupResponse listClientsByGroup(ListClientsByGroupRequest request) {
        return ClientCalls.blockingUnaryCall(
            adminChannel, ProxyAdminBindableService.LIST_CLIENTS_BY_GROUP_METHOD, CallOptions.DEFAULT, request);
    }

    private ListClientsByTopicResponse listClientsByTopic(ListClientsByTopicRequest request) {
        return ClientCalls.blockingUnaryCall(
            adminChannel, ProxyAdminBindableService.LIST_CLIENTS_BY_TOPIC_METHOD, CallOptions.DEFAULT, request);
    }

    // ==================== Data Plane Client Helper Methods ====================

    private Settings buildProducerClientSettings(String... topics) {
        java.util.List<Resource> topicResources = Arrays.stream(topics)
            .map(topic -> Resource.newBuilder().setName(topic).build())
            .collect(Collectors.toList());
        return Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder()
                .setLanguage(Language.JAVA)
                .setVersion("5.0.0")
                .build())
            .setPublishing(Publishing.newBuilder()
                .addAllTopics(topicResources)
                .build())
            .build();
    }

    private Settings buildPushConsumerClientSettings(String group) {
        return Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setUserAgent(UA.newBuilder()
                .setLanguage(Language.JAVA)
                .setVersion("5.0.0")
                .build())
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group).build())
                .build())
            .build();
    }

    /**
     * Connect a gRPC client to the data plane by opening a telemetry stream
     * and sending Settings. This registers the client in GrpcChannelManager.
     * The telemetry stream is kept open until tearDown.
     *
     * @param clientId the client ID to use
     * @param settings the client settings to send
     * @return a future that completes when the server responds with settings
     */
    private CompletableFuture<Settings> connectClient(String clientId, Settings settings) {
        Metadata clientHeader = new Metadata();
        clientHeader.put(GrpcConstants.CLIENT_ID, clientId);
        clientHeader.put(GrpcConstants.LANGUAGE, "JAVA");

        MessagingServiceGrpc.MessagingServiceStub stub = MessagingServiceGrpc
            .newStub(dataPlaneChannel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(clientHeader));

        CompletableFuture<Settings> future = new CompletableFuture<>();
        telemetryStream = stub.telemetry(new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                if (value.getCommandCase() == TelemetryCommand.CommandCase.SETTINGS) {
                    future.complete(value.getSettings());
                }
            }
            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }
            @Override
            public void onCompleted() {
                if (!future.isDone()) {
                    future.completeExceptionally(new RuntimeException("Stream completed before settings received"));
                }
            }
        });

        telemetryStream.onNext(TelemetryCommand.newBuilder()
            .setSettings(settings)
            .build());

        return future;
    }

    // ==================== Test Methods: Empty Result Scenarios ====================

    @Test
    public void test01_ListClients_NoClients_ReturnsOkWithEmptyList() {
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(10)
            .build();

        ListClientsResponse response = listClients(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isEqualTo(0);
        assertThat(response.getListCount()).isEqualTo(0);
    }

    @Test
    public void test02_DescribeClient_NonExistentClient_ReturnsNotFound() {
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("non-existent-client-id-" + UUID.randomUUID())
            .build();

        DescribeClientResponse response = describeClient(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_NOT_FOUND);
    }

    @Test
    public void test03_ListClientsByGroup_NonExistentGroup_ReturnsOkWithEmptyList() {
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup("non-existent-group-" + UUID.randomUUID())
            .setPageNum(1)
            .setPageSize(10)
            .build();

        ListClientsByGroupResponse response = listClientsByGroup(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isEqualTo(0);
        assertThat(response.getListCount()).isEqualTo(0);
    }

    @Test
    public void test04_ListClientsByTopic_NonExistentTopic_ReturnsOkWithEmptyList() {
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic("non-existent-topic-" + UUID.randomUUID())
            .setPageNum(1)
            .setPageSize(10)
            .build();

        ListClientsByTopicResponse response = listClientsByTopic(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isEqualTo(0);
        assertThat(response.getListCount()).isEqualTo(0);
    }

    // ==================== Test Methods: Pagination ====================

    @Test
    public void test05_ListClients_Pagination() {
        // Test with page 1, size 5
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(5)
            .build();

        ListClientsResponse response = listClients(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getPageSize()).isEqualTo(5);
        assertThat(response.getPagination().getPageNum()).isEqualTo(1);
    }

    // ==================== Test Methods: With Connected Clients ====================

    @Test
    public void test06_ListClients_WithConnectedProducer() throws Exception {
        String topic = initTopic();
        String clientId = "test-producer-" + UUID.randomUUID();

        // Connect a producer client to the data plane
        Settings producerSettings = buildProducerClientSettings(topic);
        CompletableFuture<Settings> future = connectClient(clientId, producerSettings);
        Settings serverSettings = future.get(10, TimeUnit.SECONDS);
        assertThat(serverSettings).isNotNull();

        // Wait for the client to be registered in GrpcChannelManager
        await().atMost(Duration.ofSeconds(5)).until(() ->
            grpcMessagingApplication.getGrpcChannelManager().getClientIdChannelMap().containsKey(clientId));

        // Query admin service for connected clients
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(100)
            .build();

        ListClientsResponse response = listClients(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isGreaterThan(0);
        assertThat(response.getListList().stream()
            .anyMatch(client -> client.getClientId().equals(clientId)))
            .isTrue();
    }

    @Test
    public void test07_DescribeClient_WithConnectedProducer() throws Exception {
        String topic = initTopic();
        String clientId = "test-describe-producer-" + UUID.randomUUID();

        // Connect a producer client to the data plane
        Settings producerSettings = buildProducerClientSettings(topic);
        CompletableFuture<Settings> future = connectClient(clientId, producerSettings);
        Settings serverSettings = future.get(10, TimeUnit.SECONDS);
        assertThat(serverSettings).isNotNull();

        // Wait for the client to be registered
        await().atMost(Duration.ofSeconds(5)).until(() ->
            grpcMessagingApplication.getGrpcChannelManager().getClientIdChannelMap().containsKey(clientId));

        // Query admin service for the specific client
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId(clientId)
            .build();

        DescribeClientResponse response = describeClient(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.hasClientDetail()).isTrue();
        assertThat(response.getClientDetail().hasClientInstance()).isTrue();
        assertThat(response.getClientDetail().getClientInstance().getClientId()).isEqualTo(clientId);
    }

    @Test
    public void test08_ListClientsByGroup_WithConnectedConsumer() throws Exception {
        String topic = initTopic();
        String group = "test-consumer-group-" + UUID.randomUUID();
        String clientId = "test-consumer-" + UUID.randomUUID();
        initConsumerGroup(group);

        // Connect a consumer client to the data plane
        Settings consumerSettings = buildPushConsumerClientSettings(group);
        CompletableFuture<Settings> future = connectClient(clientId, consumerSettings);
        Settings serverSettings = future.get(10, TimeUnit.SECONDS);
        assertThat(serverSettings).isNotNull();

        // Wait for the client to be registered
        await().atMost(Duration.ofSeconds(5)).until(() ->
            grpcMessagingApplication.getGrpcChannelManager().getClientIdChannelMap().containsKey(clientId));

        // Query admin service for clients by group
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup(group)
            .setPageNum(1)
            .setPageSize(100)
            .build();

        ListClientsByGroupResponse response = listClientsByGroup(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isGreaterThan(0);
        assertThat(response.getListList().stream()
            .anyMatch(client -> client.getClientId().equals(clientId)))
            .isTrue();
    }

    @Test
    public void test09_ListClientsByTopic_WithConnectedProducer() throws Exception {
        String topic = initTopic();
        String clientId = "test-topic-producer-" + UUID.randomUUID();

        // Connect a producer client to the data plane
        Settings producerSettings = buildProducerClientSettings(topic);
        CompletableFuture<Settings> future = connectClient(clientId, producerSettings);
        Settings serverSettings = future.get(10, TimeUnit.SECONDS);
        assertThat(serverSettings).isNotNull();

        // Wait for the client to be registered
        await().atMost(Duration.ofSeconds(5)).until(() ->
            grpcMessagingApplication.getGrpcChannelManager().getClientIdChannelMap().containsKey(clientId));

        // Query admin service for clients by topic
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic(topic)
            .setPageNum(1)
            .setPageSize(100)
            .build();

        ListClientsByTopicResponse response = listClientsByTopic(request);

        assertThat(response.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(response.getPagination().getTotal()).isGreaterThan(0);
        assertThat(response.getListList().stream()
            .anyMatch(client -> client.getClientId().equals(clientId)))
            .isTrue();
    }

    @Test
    public void test10_ListClients_WithLanguageFilter() throws Exception {
        String topic = initTopic();
        String clientId = "test-filter-producer-" + UUID.randomUUID();

        // Connect a producer client to the data plane
        Settings producerSettings = buildProducerClientSettings(topic);
        CompletableFuture<Settings> future = connectClient(clientId, producerSettings);
        Settings serverSettings = future.get(10, TimeUnit.SECONDS);
        assertThat(serverSettings).isNotNull();

        // Wait for the client to be registered
        await().atMost(Duration.ofSeconds(5)).until(() ->
            grpcMessagingApplication.getGrpcChannelManager().getClientIdChannelMap().containsKey(clientId));

        // Query with Java language filter - should include our client
        ListClientsRequest javaRequest = ListClientsRequest.newBuilder()
            .setLanguage(ClientLanguage.CLIENT_LANGUAGE_JAVA)
            .setPageNum(1)
            .setPageSize(100)
            .build();

        ListClientsResponse javaResponse = listClients(javaRequest);
        assertThat(javaResponse.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(javaResponse.getListList().stream()
            .anyMatch(client -> client.getClientId().equals(clientId)))
            .isTrue();

        // Query with Go language filter - should NOT include our Java client
        ListClientsRequest goRequest = ListClientsRequest.newBuilder()
            .setLanguage(ClientLanguage.CLIENT_LANGUAGE_GOLANG)
            .setPageNum(1)
            .setPageSize(100)
            .build();

        ListClientsResponse goResponse = listClients(goRequest);
        assertThat(goResponse.getCode()).isEqualTo(AdminCode.ADMIN_CODE_OK);
        assertThat(goResponse.getListList().stream()
            .anyMatch(client -> client.getClientId().equals(clientId)))
            .isFalse();
    }
}