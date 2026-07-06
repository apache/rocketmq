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
import io.grpc.stub.StreamObserver;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminActivity;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminListClientsRequest;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminPageView;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminResult;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminScopeRouter;
import org.apache.rocketmq.proxy.grpc.v2.admin.TimedProxyClientAdminPeerClient;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.MeteredAuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.MeteredClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
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
}
