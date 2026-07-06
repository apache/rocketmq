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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminActivity;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminClientView;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminContextFactory;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminInProcessPeerMessageTransport;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminListClientsRequest;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPageView;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerClient;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerGrpcService;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerGrpcTarget;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerGrpcTransport;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerLocalExecutor;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerMessageClient;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerMessageHandler;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerRequest;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPeerResponse;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminResult;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminScopeRouter;
import org.apache.rocketmq.proxy.grpc.v2.admin.TimedProxyClientAdminPeerClient;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.MeteredAuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.MeteredClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceCleaner;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultGrpcMessagingActivityTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;
    @Mock
    private MetadataService metadataService;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
        when(this.messagingProcessor.getMetadataService()).thenReturn(this.metadataService);
    }

    @Test
    public void initCreatesClientAdminServiceWithSharedReadModel() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        activity.proxyClientReadService.upsertClient(clientInfo);

        ClientAdminService clientAdminService = activity.getClientAdminService();
        assertThat(clientAdminService).isNotNull();
        assertThat(clientAdminService.describeClient("client-a")).isSameAs(clientInfo);
    }

    @Test
    public void initCreatesAuthorizingClientAdminServiceWithSharedReadModel() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        activity.proxyClientReadService.upsertClient(clientInfo);

        AuthorizingClientAdminService clientAdminService = activity.getAuthorizingClientAdminService();
        assertThat(clientAdminService).isNotNull();
        assertThat(clientAdminService.describeClient(
            ClientAdminRequestContext.of(User.of("admin"), "127.0.0.1"),
            "client-a"
        )).isSameAs(clientInfo);
    }

    @Test
    public void initCreatesProxyClientAdminActivityWithSharedReadModel() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        activity.proxyClientReadService.upsertClient(clientInfo);

        ProxyClientAdminActivity proxyClientAdminActivity = activity.getProxyClientAdminActivity();
        ProxyClientAdminResult<ProxyClientInfo> result = proxyClientAdminActivity.describeClient(
            ProxyContext.create()
                .setSubject(User.of("admin"))
                .setRemoteAddress("127.0.0.1"),
            "client-a"
        );

        assertThat(proxyClientAdminActivity).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(clientInfo);
    }

    @Test
    public void initCreatesProxyClientAdminScopeRouterWithCrossProxyScopeDisabledByDefault() {
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.getProxyClientAdminScopeRouter()
            .listClientViews(
                ProxyContext.create()
                    .setSubject(User.of("admin"))
                    .setRemoteAddress("127.0.0.1"),
                ProxyClientAdminListClientsRequest.newBuilder()
                    .setScope(ProxyClientScope.ALL_PROXIES)
                    .setPageSize(10)
                    .build()
            );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void initSkipsProxyClientAdminPeerClientWhenCrossProxyScopeDisabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(false);
        ConfigurationManager.getProxyConfig().setProxyClientAdminPeerRequestTimeoutMillis(0L);
        AtomicReference<DefaultGrpcMessagingActivity> activityRef = new AtomicReference<>();

        assertThatCode(() -> activityRef.set(new DefaultGrpcMessagingActivity(this.messagingProcessor)))
            .doesNotThrowAnyException();

        DefaultGrpcMessagingActivity activity = activityRef.get();
        assertThat(activity.proxyClientAdminPeerClient).isNull();
        assertThat(activity.getProxyClientAdminPeerGrpcService()).isNull();
        assertThat(activity.proxyClientAdminPeerExecutor).isNull();
        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.getProxyClientAdminScopeRouter()
            .listClientViews(
                ProxyContext.create()
                    .setSubject(User.of("admin"))
                    .setRemoteAddress("127.0.0.1"),
                ProxyClientAdminListClientsRequest.newBuilder()
                    .setScope(ProxyClientScope.ALL_PROXIES)
                    .setPageSize(10)
                    .build()
            );
        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void initCreatesTimedProxyClientAdminPeerClientWhenCrossProxyScopeEnabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        assertThat(activity.proxyClientAdminPeerClient).isInstanceOf(TimedProxyClientAdminPeerClient.class);
    }

    @Test
    public void initCreatesProxyClientAdminPeerGrpcServiceWhenCrossProxyScopeEnabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        ProxyClientAdminPeerGrpcService peerGrpcService = activity.getProxyClientAdminPeerGrpcService();

        assertThat(peerGrpcService).isNotNull();
        assertThat(peerGrpcService.bindService().getServiceDescriptor().getName())
            .isEqualTo(ProxyClientAdminPeerGrpcService.SERVICE_NAME);
    }

    @Test
    public void initCreatesCrossProxyPeerClientThroughRawMessageTransport() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        Object delegate = fieldValue(activity.proxyClientAdminPeerClient, TimedProxyClientAdminPeerClient.class,
            "delegate");
        Object transport = fieldValue(delegate, ProxyClientAdminPeerMessageClient.class, "transport");
        @SuppressWarnings("unchecked")
        Map<String, ProxyClientAdminPeerMessageHandler> handlers =
            (Map<String, ProxyClientAdminPeerMessageHandler>) fieldValue(
                transport,
                ProxyClientAdminInProcessPeerMessageTransport.class,
                "handlers"
            );

        assertThat(delegate).isInstanceOf(ProxyClientAdminPeerMessageClient.class);
        assertThat(transport).isInstanceOf(ProxyClientAdminInProcessPeerMessageTransport.class);
        assertThat(handlers).containsOnlyKeys("proxy-a");
        assertThat(handlers.get("proxy-a")).isInstanceOf(ProxyClientAdminPeerMessageHandler.class);
    }

    @Test
    public void initCreatesGrpcPeerTransportFromStaticTargetsWhenConfigured() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        ConfigurationManager.getProxyConfig().setProxyClientAdminPeerGrpcTargets(
            "proxy-b=127.0.0.2:8081, proxy-a=127.0.0.1:8080"
        );

        CapturingPeerGrpcChannelsDefaultGrpcMessagingActivity activity =
            new CapturingPeerGrpcChannelsDefaultGrpcMessagingActivity(this.messagingProcessor);

        Object delegate = fieldValue(activity.proxyClientAdminPeerClient, TimedProxyClientAdminPeerClient.class,
            "delegate");
        Object transport = fieldValue(delegate, ProxyClientAdminPeerMessageClient.class, "transport");

        assertThat(transport).isInstanceOf(ProxyClientAdminPeerGrpcTransport.class);
        assertThat(activity.capturedTargets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getProxyId)
            .containsExactly("proxy-a", "proxy-b");
        assertThat(activity.capturedTargets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getHost)
            .containsExactly("127.0.0.1", "127.0.0.2");
        assertThat(activity.capturedTargets)
            .extracting(ProxyClientAdminPeerGrpcTarget::getPort)
            .containsExactly(8080, 8081);
    }

    @Test
    public void initRoutesAllProxyScopeThroughConfiguredStaticGrpcPeers() throws Exception {
        PeerServer proxyA = null;
        PeerServer proxyB = null;
        DefaultGrpcMessagingActivity activity = null;
        try {
            proxyA = PeerServer.start("proxy-a", clientInfo("client-a", 200L));
            proxyB = PeerServer.start("proxy-b", clientInfo("client-b", 300L));
            ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
            ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
            ConfigurationManager.getProxyConfig().setProxyClientAdminPeerGrpcTargets(
                "proxy-b=127.0.0.1:" + proxyB.port() + ",proxy-a=127.0.0.1:" + proxyA.port()
            );
            activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

            ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.getProxyClientAdminScopeRouter()
                .listClientViews(
                    ProxyContext.create()
                        .setSubject(User.of("admin"))
                        .setRemoteAddress("127.0.0.1"),
                    ProxyClientAdminListClientsRequest.newBuilder()
                        .setScope(ProxyClientScope.ALL_PROXIES)
                        .setPageSize(10)
                        .build()
                );

            assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(result.getBody().getClients())
                .extracting(ProxyClientAdminClientView::getClientId)
                .containsExactly("client-a", "client-b");
            assertThat(result.getBody().getClients())
                .extracting(ProxyClientAdminClientView::getProxyId)
                .containsExactly("proxy-a", "proxy-b");
        } finally {
            if (activity != null) {
                activity.shutdown();
            }
            close(proxyB);
            close(proxyA);
        }
    }

    @Test
    public void initRequiresStaticGrpcPeerTargetsToIncludeLocalProxyId() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        ConfigurationManager.getProxyConfig().setProxyClientAdminPeerGrpcTargets("proxy-b=127.0.0.2:8081");

        assertThatThrownBy(() -> new DefaultGrpcMessagingActivity(this.messagingProcessor))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyClientAdminPeerGrpcTargets")
            .hasMessageContaining("proxy-a");
    }

    @Test
    public void initCreatesCrossProxyPeerClientThroughFactorySeam() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName(" proxy-a ");
        ConfigurationManager.getProxyConfig().setProxyClientAdminPeerRequestTimeoutMillis(1234L);

        TestableDefaultGrpcMessagingActivity activity =
            new TestableDefaultGrpcMessagingActivity(this.messagingProcessor);

        assertThat(activity.capturedLocalProxyId).isEqualTo("proxy-a");
        assertThat(activity.capturedAdminActivity).isSameAs(activity.getProxyClientAdminActivity());
        assertThat(activity.capturedExecutorService).isSameAs(activity.proxyClientAdminPeerExecutor);
        assertThat(activity.capturedTimeoutMillis).isEqualTo(1234L);
        assertThat(activity.proxyClientAdminPeerClient).isSameAs(activity.createdPeerClient);
    }

    @Test
    public void initRequiresProxyNameWhenCrossProxyScopeEnabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName(" ");

        assertThatThrownBy(() -> new DefaultGrpcMessagingActivity(this.messagingProcessor))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyName is required");
    }

    @Test
    public void initRequiresPositivePeerRequestTimeoutWhenCrossProxyScopeEnabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        ConfigurationManager.getProxyConfig().setProxyClientAdminPeerRequestTimeoutMillis(0L);

        assertThatThrownBy(() -> new DefaultGrpcMessagingActivity(this.messagingProcessor))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyClientAdminPeerRequestTimeoutMillis must be positive");
    }

    @Test
    public void initTrimsConfiguredProxyNameForLocalProxyId() {
        String originalProxyName = ConfigurationManager.getProxyConfig().getProxyName();
        try {
            ConfigurationManager.getProxyConfig().setProxyName(" proxy-a ");
            DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

            assertThat(activity.localProxyId()).isEqualTo("proxy-a");
        } finally {
            ConfigurationManager.getProxyConfig().setProxyName(originalProxyName);
        }
    }

    @Test
    public void initCreatesProxyClientAdminScopeRouterWithSharedLocalPeerWhenCrossProxyScopeEnabled() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        activity.proxyClientReadService.upsertClient(clientInfo("client-a", 200L));

        ProxyClientAdminScopeRouter scopeRouter = activity.getProxyClientAdminScopeRouter();
        ProxyClientAdminResult<ProxyClientAdminPageView> result = scopeRouter.listClientViews(
            ProxyContext.create()
                .setSubject(User.of("admin"))
                .setRemoteAddress("127.0.0.1"),
            ProxyClientAdminListClientsRequest.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPageSize(10)
                .build()
        );

        assertThat(scopeRouter).isNotNull();
        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(client -> client.getClientId())
            .containsExactly("client-a");
    }

    @Test
    public void initCreatesPeerLocalExecutorWithSharedClientAdminService() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        CapturingPeerLocalExecutorDefaultGrpcMessagingActivity activity =
            new CapturingPeerLocalExecutorDefaultGrpcMessagingActivity(this.messagingProcessor);

        assertThat(activity.capturedClientAdminService).isSameAs(activity.getClientAdminService());
    }

    @Test
    public void initAuthorizesCrossProxyScopeBeforePeerDiscovery() {
        ConfigurationManager.getProxyConfig().setEnableProxyClientAdminCrossProxyQuery(true);
        ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
        ConfigurationManager.getAuthConfig().setAuthorizationEnabled(true);
        DefaultGrpcMessagingActivity activity =
            new FailingPeerDefaultGrpcMessagingActivity(this.messagingProcessor);

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.getProxyClientAdminScopeRouter()
            .listClientViews(
                ProxyContext.create()
                    .setRemoteAddress("127.0.0.1"),
                ProxyClientAdminListClientsRequest.newBuilder()
                    .setScope(ProxyClientScope.ALL_PROXIES)
                    .setPageSize(10)
                    .build()
            );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void telemetryWriteIsVisibleThroughProxyClientAdminActivity() {
        when(this.metadataService.getTopicMessageType(any(), eq("topic-a"))).thenReturn(TopicMessageType.NORMAL);
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyContext telemetryContext = ProxyContext.create()
            .setClientID("client-a")
            .setLanguage("JAVA")
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("192.168.0.1:8080")
            .setClientVersion("V5_0_0");
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("topic-a").build())
                .build())
            .build();

        ContextStreamObserver<TelemetryCommand> requestObserver = activity.telemetry(noopTelemetryObserver());
        requestObserver.onNext(telemetryContext, TelemetryCommand.newBuilder()
            .setSettings(settings)
            .build());

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.getProxyClientAdminActivity()
            .listClientViews(
                ProxyContext.create()
                    .setSubject(User.of("admin"))
                    .setRemoteAddress("127.0.0.1"),
                ProxyClientQuery.newBuilder()
                    .setTopic("topic-a")
                    .build()
            );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(client -> client.getClientId())
            .containsExactly("client-a");
    }

    @Test
    public void initMetersClientAdminRequestsOutsideAuthorizationLayer() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        assertThat(activity.getClientAdminService()).isNotInstanceOf(MeteredClientAdminService.class);
        assertThat(activity.getAuthorizingClientAdminService())
            .isInstanceOf(MeteredAuthorizingClientAdminService.class);
    }

    @Test
    public void initLeavesProxyClientReadServiceCleanerDisabledByDefault() throws Exception {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);

        assertThat(activity.proxyClientReadServiceCleaner).isNull();
        assertThat(lifecycleComponents(activity))
            .noneMatch(component -> component instanceof ProxyClientReadServiceCleaner);
    }

    @Test
    public void initRegistersProxyClientReadServiceCleanerWhenEnabled() throws Exception {
        ConfigurationManager.getProxyConfig().setEnableProxyClientReadServiceCleaner(true);
        ConfigurationManager.getProxyConfig().setProxyClientReadServiceCleanerInactiveTimeoutMillis(500L);
        ConfigurationManager.getProxyConfig().setProxyClientReadServiceCleanerIntervalMillis(1000L);

        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        activity.proxyClientReadService.upsertClient(clientInfo("client-old", 1L));
        activity.proxyClientReadService.upsertClient(
            clientInfo("client-active", System.currentTimeMillis() + 100000L)
        );

        int removed = activity.proxyClientReadServiceCleaner.cleanup();

        assertThat(activity.proxyClientReadServiceCleaner).isNotNull();
        assertThat(lifecycleComponents(activity)).contains(activity.proxyClientReadServiceCleaner);
        assertThat(removed).isEqualTo(1);
        assertThat(activity.proxyClientReadService.getClient("client-old")).isNull();
        assertThat(activity.proxyClientReadService.getClient("client-active")).isNotNull();
    }

    private static StreamObserver<TelemetryCommand> noopTelemetryObserver() {
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static ProxyClientInfo clientInfo(String clientId, long lastActiveTimeMillis) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            lastActiveTimeMillis
        );
    }

    @SuppressWarnings("unchecked")
    private static List<StartAndShutdown> lifecycleComponents(DefaultGrpcMessagingActivity activity) throws Exception {
        Field field = AbstractStartAndShutdown.class.getDeclaredField("startAndShutdownList");
        field.setAccessible(true);
        return (List<StartAndShutdown>) field.get(activity);
    }

    private static Object fieldValue(Object target, Class<?> declaringClass, String fieldName) throws Exception {
        Field field = declaringClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void close(PeerServer peerServer) throws Exception {
        if (peerServer != null) {
            peerServer.close();
        }
    }

    private static class PeerServer {
        private final Server server;
        private final ManagedChannel channel;

        private PeerServer(Server server, ManagedChannel channel) {
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
            return new PeerServer(server, channel);
        }

        private int port() {
            return this.server.getPort();
        }

        private void close() throws Exception {
            this.channel.shutdownNow();
            this.server.shutdownNow();
            this.channel.awaitTermination(5, TimeUnit.SECONDS);
            this.server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static class TestableDefaultGrpcMessagingActivity extends DefaultGrpcMessagingActivity {
        private ProxyClientAdminPeerClient createdPeerClient;
        private String capturedLocalProxyId;
        private ProxyClientAdminActivity capturedAdminActivity;
        private ExecutorService capturedExecutorService;
        private long capturedTimeoutMillis;

        private TestableDefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
            super(messagingProcessor);
        }

        @Override
        protected ProxyClientAdminPeerClient createProxyClientAdminPeerClient(String localProxyId,
            ProxyClientAdminActivity proxyClientAdminActivity, ExecutorService executorService, long timeoutMillis) {
            this.capturedLocalProxyId = localProxyId;
            this.capturedAdminActivity = proxyClientAdminActivity;
            this.capturedExecutorService = executorService;
            this.capturedTimeoutMillis = timeoutMillis;
            this.createdPeerClient = new RecordingProxyClientAdminPeerClient();
            return this.createdPeerClient;
        }
    }

    private static class RecordingProxyClientAdminPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            return Collections.emptyList();
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            throw new UnsupportedOperationException("not used");
        }
    }

    private static class CapturingPeerLocalExecutorDefaultGrpcMessagingActivity extends DefaultGrpcMessagingActivity {
        private ClientAdminService capturedClientAdminService;

        private CapturingPeerLocalExecutorDefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
            super(messagingProcessor);
        }

        @Override
        protected ProxyClientAdminPeerLocalExecutor createProxyClientAdminPeerLocalExecutor(String localProxyId,
            ClientAdminService clientAdminService) {
            this.capturedClientAdminService = clientAdminService;
            return super.createProxyClientAdminPeerLocalExecutor(localProxyId, clientAdminService);
        }
    }

    private static class CapturingPeerGrpcChannelsDefaultGrpcMessagingActivity extends DefaultGrpcMessagingActivity {
        private List<ProxyClientAdminPeerGrpcTarget> capturedTargets;

        private CapturingPeerGrpcChannelsDefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
            super(messagingProcessor);
        }

        @Override
        protected Map<String, Channel> createProxyClientAdminPeerGrpcChannels(
            List<ProxyClientAdminPeerGrpcTarget> targets) {
            this.capturedTargets = targets;
            Map<String, Channel> channels = new LinkedHashMap<>();
            for (ProxyClientAdminPeerGrpcTarget target : targets) {
                channels.put(target.getProxyId(), mock(Channel.class));
            }
            return channels;
        }
    }

    private static class FailingPeerDefaultGrpcMessagingActivity extends DefaultGrpcMessagingActivity {
        private FailingPeerDefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
            super(messagingProcessor);
        }

        @Override
        protected ProxyClientAdminPeerClient createProxyClientAdminPeerClient(String localProxyId,
            ProxyClientAdminActivity proxyClientAdminActivity, ExecutorService executorService, long timeoutMillis) {
            return new FailingProxyClientAdminPeerClient();
        }
    }

    private static class FailingProxyClientAdminPeerClient implements ProxyClientAdminPeerClient {
        @Override
        public List<String> listProxyIds() {
            throw new AssertionError("peer discovery should not run before authorization");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            throw new AssertionError("peer request should not run before authorization");
        }
    }
}
