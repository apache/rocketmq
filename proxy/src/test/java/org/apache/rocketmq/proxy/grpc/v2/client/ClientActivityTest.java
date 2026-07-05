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

package org.apache.rocketmq.proxy.grpc.v2.client;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.LiteSubscriptionAction;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.SyncLiteSubscriptionRequest;
import apache.rocketmq.v2.SyncLiteSubscriptionResponse;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.ContextStreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";

    private ClientActivity clientActivity;
    @Mock
    private GrpcChannelManager grpcChannelManagerMock;
    @Mock
    private CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> runningInfoFutureMock;
    @Captor
    ArgumentCaptor<ProxyRelayResult<ConsumerRunningInfo>> runningInfoArgumentCaptor;
    @Mock
    private CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> resultFutureMock;
    @Captor
    ArgumentCaptor<ProxyRelayResult<ConsumeMessageDirectlyResult>> resultArgumentCaptor;

    @Before
    public void before() throws Throwable {
        super.before();
        this.clientActivity = new ClientActivity(this.messagingProcessor, this.grpcClientSettingsManager, grpcChannelManager);
    }

    @Test
    public void testConstructorRejectsNullProxyClientReadService() {
        assertThatThrownBy(() -> new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            null
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyClientReadService is required");
    }

    protected TelemetryCommand sendProducerTelemetry(ProxyContext context) throws Throwable {
        return this.sendClientTelemetry(
            context,
            Settings.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setPublishing(Publishing.newBuilder()
                    .addTopics(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build()).get();
    }

    protected HeartbeatResponse sendProducerHeartbeat(ProxyContext context) throws Throwable {
        return this.clientActivity.heartbeat(context, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build()).get();
    }

    @Test
    public void testProducerHeartbeat() throws Throwable {
        ProxyContext context = createContext();

        this.sendProducerTelemetry(context);

        ArgumentCaptor<String> registerProducerGroupArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).registerProducer(any(),
            registerProducerGroupArgumentCaptor.capture(),
            channelInfoArgumentCaptor.capture());

        ArgumentCaptor<String> txProducerGroupArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> txProducerTopicArgumentCaptor = ArgumentCaptor.forClass(String.class);
        doNothing().when(this.messagingProcessor).addTransactionSubscription(any(),
            txProducerGroupArgumentCaptor.capture(),
            txProducerTopicArgumentCaptor.capture()
        );

        when(this.metadataService.getTopicMessageType(any(), anyString())).thenReturn(TopicMessageType.TRANSACTION);

        HeartbeatResponse response = this.sendProducerHeartbeat(context);

        assertEquals(Code.OK, response.getStatus().getCode());

        assertEquals(Lists.newArrayList(TOPIC), registerProducerGroupArgumentCaptor.getAllValues());
        ClientChannelInfo clientChannelInfo = channelInfoArgumentCaptor.getValue();
        assertClientChannelInfo(clientChannelInfo, TOPIC);

        assertEquals(Lists.newArrayList(TOPIC), txProducerGroupArgumentCaptor.getAllValues());
        assertEquals(Lists.newArrayList(TOPIC), txProducerTopicArgumentCaptor.getAllValues());
    }

    protected TelemetryCommand sendConsumerTelemetry(ProxyContext context) throws Throwable {
        return this.sendClientTelemetry(
            context,
            Settings.newBuilder()
                .setClientType(ClientType.PUSH_CONSUMER)
                .setSubscription(Subscription.newBuilder()
                    .setGroup(Resource.newBuilder().setName("Group").build())
                    .addSubscriptions(SubscriptionEntry.newBuilder()
                        .setExpression(FilterExpression.newBuilder()
                            .setExpression("tag")
                            .setType(FilterType.TAG)
                            .build())
                        .setTopic(Resource.newBuilder().setName(TOPIC).build())
                        .build())
                    .build())
                .build()).get();
    }

    protected HeartbeatResponse sendConsumerHeartbeat(ProxyContext context) throws Throwable {
        return this.clientActivity.heartbeat(context, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .build()).get();
    }

    @Test
    public void testConsumerHeartbeat() throws Throwable {
        ProxyContext context = createContext();
        this.sendConsumerTelemetry(context);

        ArgumentCaptor<Set<SubscriptionData>> subscriptionDatasArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).registerConsumer(any(),
            anyString(),
            channelInfoArgumentCaptor.capture(),
            any(),
            any(),
            any(),
            subscriptionDatasArgumentCaptor.capture(),
            anyBoolean()
        );

        HeartbeatResponse response = this.sendConsumerHeartbeat(context);
        assertEquals(Code.OK, response.getStatus().getCode());

        ClientChannelInfo clientChannelInfo = channelInfoArgumentCaptor.getValue();
        assertClientChannelInfo(clientChannelInfo, CONSUMER_GROUP);

        SubscriptionData data = subscriptionDatasArgumentCaptor.getValue().stream().findAny().get();
        assertEquals("TAG", data.getExpressionType());
        assertEquals("tag", data.getSubString());
    }

    @Test
    public void testConsumerTelemetryUpdatesProxyClientReadService() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();

        this.sendConsumerTelemetry(context);

        ProxyClientInfo clientInfo = proxyClientReadService.getClient(CLIENT_ID);
        assertThat(clientInfo).isNotNull();
        assertThat(clientInfo.getClientId()).isEqualTo(CLIENT_ID);
        assertThat(clientInfo.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(clientInfo.getGroups()).containsExactly("Group");
        assertThat(clientInfo.getTopics()).containsExactly(TOPIC);
        assertThat(clientInfo.getLanguage()).isEqualTo(JAVA);
        assertThat(clientInfo.getRemoteAddress()).isEqualTo(REMOTE_ADDR);
        assertThat(clientInfo.getLocalAddress()).isEqualTo(LOCAL_ADDR);
        assertThat(clientInfo.getConnectTimeMillis()).isGreaterThan(0L);
        assertThat(clientInfo.getLastActiveTimeMillis()).isGreaterThanOrEqualTo(clientInfo.getConnectTimeMillis());
    }

    @Test
    public void testProducerTelemetryUpdatesProxyClientReadService() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();

        this.sendProducerTelemetry(context);

        ProxyClientInfo clientInfo = proxyClientReadService.getClient(CLIENT_ID);
        assertThat(clientInfo).isNotNull();
        assertThat(clientInfo.getClientId()).isEqualTo(CLIENT_ID);
        assertThat(clientInfo.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(clientInfo.getGroups()).isEmpty();
        assertThat(clientInfo.getTopics()).containsExactly(TOPIC);
        assertThat(clientInfo.getLanguage()).isEqualTo(JAVA);
        assertThat(clientInfo.getRemoteAddress()).isEqualTo(REMOTE_ADDR);
        assertThat(clientInfo.getLocalAddress()).isEqualTo(LOCAL_ADDR);
        assertThat(clientInfo.getConnectTimeMillis()).isGreaterThan(0L);
        assertThat(clientInfo.getLastActiveTimeMillis()).isGreaterThanOrEqualTo(clientInfo.getConnectTimeMillis());
    }

    @Test
    public void testTelemetryRecordsLocalProxyIdInProxyClientReadService() throws Throwable {
        String originalProxyName = ConfigurationManager.getProxyConfig().getProxyName();
        try {
            ConfigurationManager.getProxyConfig().setProxyName("proxy-a");
            ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
            this.clientActivity = new ClientActivity(
                this.messagingProcessor,
                this.grpcClientSettingsManager,
                this.grpcChannelManager,
                proxyClientReadService
            );
            ProxyContext context = createContext();

            this.sendProducerTelemetry(context);

            ProxyClientInfo clientInfo = proxyClientReadService.getClient(CLIENT_ID);
            assertThat(clientInfo).isNotNull();
            assertThat(clientInfo.getProxyId()).isEqualTo("proxy-a");
        } finally {
            ConfigurationManager.getProxyConfig().setProxyName(originalProxyName);
        }
    }

    @Test
    public void testProducerTelemetryRejectsNonProducerClientTypeBeforeReadModelUpdate() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();

        assertThatThrownBy(() -> this.sendClientTelemetry(context, settings).get())
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(StatusRuntimeException.class)
            .satisfies(throwable -> assertThat(((StatusRuntimeException) throwable.getCause()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        verify(this.messagingProcessor, never()).registerProducer(any(), anyString(), any());
    }

    @Test
    public void testConsumerTelemetryRejectsNonConsumerClientTypeBeforeReadModelUpdate() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("Group").build())
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setExpression(FilterExpression.newBuilder()
                        .setExpression("tag")
                        .setType(FilterType.TAG)
                        .build())
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build())
            .build();

        assertThatThrownBy(() -> this.sendClientTelemetry(context, settings).get())
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(StatusRuntimeException.class)
            .satisfies(throwable -> assertThat(((StatusRuntimeException) throwable.getCause()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        verify(this.messagingProcessor, never()).registerConsumer(
            any(),
            anyString(),
            any(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean()
        );
    }

    @Test
    public void testEmptySettingsRemovesStaleProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        assertThatThrownBy(() -> this.sendClientTelemetry(context, Settings.getDefaultInstance()).get())
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(StatusRuntimeException.class)
            .satisfies(throwable -> assertThat(((StatusRuntimeException) throwable.getCause()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT));

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testInvalidTelemetrySettingsRemovesStaleProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        Settings invalidSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        assertThatThrownBy(() -> this.sendClientTelemetry(context, invalidSettings).get())
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(StatusRuntimeException.class)
            .satisfies(throwable -> assertThat(((StatusRuntimeException) throwable.getCause()).getStatus().getCode())
                .isEqualTo(Status.Code.INVALID_ARGUMENT));

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testHeartbeatPreservesConnectTimeAndUpdatesLastActiveTime() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        ProxyClientInfo firstClientInfo = proxyClientReadService.getClient(CLIENT_ID);

        Thread.sleep(5L);
        HeartbeatResponse response = this.sendProducerHeartbeat(context);

        ProxyClientInfo secondClientInfo = proxyClientReadService.getClient(CLIENT_ID);
        assertEquals(Code.OK, response.getStatus().getCode());
        assertThat(secondClientInfo.getConnectTimeMillis()).isEqualTo(firstClientInfo.getConnectTimeMillis());
        assertThat(secondClientInfo.getLastActiveTimeMillis()).isGreaterThan(firstClientInfo.getLastActiveTimeMillis());
        assertThat(secondClientInfo.getTopics()).containsExactly(TOPIC);
    }

    @Test
    public void testHeartbeatWithoutClientSettingsRemovesProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(null);
        HeartbeatResponse response = this.sendProducerHeartbeat(context);

        assertEquals(Code.UNRECOGNIZED_CLIENT_TYPE, response.getStatus().getCode());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testHeartbeatWithUnrecognizedClientTypeRemovesProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        when(this.grpcClientSettingsManager.getClientSettings(any())).thenReturn(Settings.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build());
        HeartbeatResponse response = this.sendProducerHeartbeat(context);

        assertEquals(Code.UNRECOGNIZED_CLIENT_TYPE, response.getStatus().getCode());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testNotifyClientTerminationRemovesProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(this.grpcClientSettingsManager.removeAndGetClientSettings(any())).thenReturn(producerSettings);
        when(this.metadataService.getTopicMessageType(any(), anyString())).thenReturn(TopicMessageType.NORMAL);

        this.sendProducerTelemetry(context);
        this.sendProducerHeartbeat(context);
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        NotifyClientTerminationResponse response = this.clientActivity.notifyClientTermination(
            context,
            NotifyClientTerminationRequest.newBuilder().build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testNotifyClientTerminationRemovesReadModelForUnrecognizedClientType() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        when(this.grpcClientSettingsManager.removeAndGetClientSettings(any()))
            .thenReturn(Settings.newBuilder()
                .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
                .build());

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();

        NotifyClientTerminationResponse response = this.clientActivity.notifyClientTermination(
            context,
            NotifyClientTerminationRequest.newBuilder().build()
        ).get();

        assertEquals(Code.UNRECOGNIZED_CLIENT_TYPE, response.getStatus().getCode());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testNotifyClientTerminationRemovesReadModelWhenUnregisterFails() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendConsumerTelemetry(context);
        when(this.grpcClientSettingsManager.removeAndGetClientSettings(any()))
            .thenReturn(Settings.newBuilder()
                .setClientType(ClientType.PUSH_CONSUMER)
                .build());

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();

        assertThatThrownBy(() -> this.clientActivity.notifyClientTermination(
            context,
            NotifyClientTerminationRequest.newBuilder().build()
        ).get())
            .isInstanceOf(ExecutionException.class);

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setGroup("Group")
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testProducerUnregisterListenerRemovesProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        ProducerChangeListener listener = latestProducerChangeListener();
        GrpcClientChannel channel = this.grpcChannelManager.getChannel(CLIENT_ID);

        listener.handle(
            ProducerGroupEvent.CLIENT_UNREGISTER,
            TOPIC,
            new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0)
        );

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testProducerUnregisterListenerRemovesProxyClientReadServiceIndexesWhenSettingsCleanupFails() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        doThrow(new RuntimeException("settings cleanup failed"))
            .when(this.grpcClientSettingsManager)
            .removeAndGetRawClientSettings(CLIENT_ID);
        ProducerChangeListener listener = latestProducerChangeListener();
        GrpcClientChannel channel = this.grpcChannelManager.getChannel(CLIENT_ID);

        assertThatCode(() -> listener.handle(
            ProducerGroupEvent.CLIENT_UNREGISTER,
            TOPIC,
            new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0)
        )).doesNotThrowAnyException();

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
    }

    @Test
    public void testConsumerUnregisterListenerRemovesProxyClientReadServiceIndexes() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendConsumerTelemetry(context);
        ConsumerIdsChangeListener listener = latestConsumerIdsChangeListener();
        GrpcClientChannel channel = this.grpcChannelManager.getChannel(CLIENT_ID);

        listener.handle(
            ConsumerGroupEvent.CLIENT_UNREGISTER,
            "Group",
            new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0)
        );

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setGroup("Group")
            .build()).getClients()).isEmpty();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testConsumerUnregisterListenerRemovesCachedClientSettingsBeforeOfflineHooks() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings consumerSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("Group").build())
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setExpression(FilterExpression.newBuilder()
                        .setExpression("tag")
                        .setType(FilterType.TAG)
                        .build())
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build())
            .build();
        this.sendClientTelemetry(context, consumerSettings).get();
        when(this.grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID))
            .thenReturn(consumerSettings);
        ConsumerIdsChangeListener listener = latestConsumerIdsChangeListener();
        GrpcClientChannel channel = this.grpcChannelManager.getChannel(CLIENT_ID);

        listener.handle(
            ConsumerGroupEvent.CLIENT_UNREGISTER,
            "Group",
            new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0)
        );

        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
        verify(this.grpcClientSettingsManager).offlineClientLiteSubscription(
            any(ProxyContext.class),
            eq(CLIENT_ID),
            same(consumerSettings)
        );
    }

    @Test
    public void testConsumerUnregisterListenerRemovesProxyClientReadServiceIndexesWhenOfflineHookFails() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings consumerSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("Group").build())
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setExpression(FilterExpression.newBuilder()
                        .setExpression("tag")
                        .setType(FilterType.TAG)
                        .build())
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build())
            .build();
        this.sendClientTelemetry(context, consumerSettings).get();
        when(this.grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID))
            .thenReturn(consumerSettings);
        doThrow(new RuntimeException("offline hook failed"))
            .when(this.grpcClientSettingsManager)
            .offlineClientLiteSubscription(any(ProxyContext.class), eq(CLIENT_ID), same(consumerSettings));
        ConsumerIdsChangeListener listener = latestConsumerIdsChangeListener();
        GrpcClientChannel channel = this.grpcChannelManager.getChannel(CLIENT_ID);

        assertThatCode(() -> listener.handle(
            ConsumerGroupEvent.CLIENT_UNREGISTER,
            "Group",
            new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0)
        )).doesNotThrowAnyException();

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setGroup("Group")
            .build()).getClients()).isEmpty();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
        verify(this.grpcClientSettingsManager).offlineClientLiteSubscription(
            any(ProxyContext.class),
            eq(CLIENT_ID),
            same(consumerSettings)
        );
    }

    @Test
    public void testProducerUnregisterListenerIgnoresRemoteChannel() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        this.sendProducerTelemetry(context);
        ProducerChangeListener listener = latestProducerChangeListener();

        listener.handle(
            ProducerGroupEvent.CLIENT_UNREGISTER,
            TOPIC,
            new ClientChannelInfo(remoteGrpcChannel(), CLIENT_ID, LanguageCode.JAVA, 0)
        );

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);
    }

    @Test
    public void testRemoteConsumerRegisterSyncsSettingsWithoutUpdatingProxyClientReadService() throws Throwable {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        Settings remoteSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setTopic(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build())
            .build();
        ConsumerIdsChangeListener listener = latestConsumerIdsChangeListener();

        listener.handle(
            ConsumerGroupEvent.REGISTER,
            CONSUMER_GROUP,
            Lists.newArrayList(),
            new ClientChannelInfo(remoteGrpcChannel(remoteSettings), CLIENT_ID, LanguageCode.JAVA, 0)
        );

        verify(this.grpcClientSettingsManager).updateClientSettings(
            any(ProxyContext.class),
            eq(CLIENT_ID),
            eq(remoteSettings)
        );
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setGroup(CONSUMER_GROUP)
            .build()).getClients()).isEmpty();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testTelemetryCancelRemovesProxyClientReadServiceIndexes() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(
            new StreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(TelemetryCommand value) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            }
        );
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        requestObserver.onError(Status.CANCELLED.asRuntimeException());

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testTelemetryStreamErrorRemovesProxyClientReadServiceIndexes() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(
            new StreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(TelemetryCommand value) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            }
        );
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        requestObserver.onError(Status.INTERNAL.asRuntimeException());

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testTelemetryNonStatusStreamErrorRemovesProxyClientReadServiceIndexes() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(
            new StreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(TelemetryCommand value) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            }
        );
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        requestObserver.onError(new RuntimeException("stream closed"));

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void testTelemetryCancelRemovesCachedClientSettingsBeforeOfflineHooks() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        when(grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID)).thenReturn(producerSettings);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(
            new StreamObserver<TelemetryCommand>() {
                @Override
                public void onNext(TelemetryCommand value) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            }
        );
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());

        requestObserver.onError(Status.CANCELLED.asRuntimeException());

        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
        verify(this.grpcClientSettingsManager).offlineClientLiteSubscription(context, CLIENT_ID, producerSettings);
    }

    @Test
    public void testTelemetryCompletedRemovesProxyClientReadServiceIndexesAndSettingsBeforeOfflineHooks() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        when(grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID)).thenReturn(producerSettings);
        StreamObserver<TelemetryCommand> responseObserver = mock(StreamObserver.class);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(responseObserver);
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        requestObserver.onCompleted();

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
        verify(this.grpcClientSettingsManager).offlineClientLiteSubscription(context, CLIENT_ID, producerSettings);
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testTelemetryCompletedRemovesProxyClientReadServiceIndexesWhenOfflineHookFails() {
        ProxyClientReadService proxyClientReadService = new ProxyClientReadService();
        this.clientActivity = new ClientActivity(
            this.messagingProcessor,
            this.grpcClientSettingsManager,
            this.grpcChannelManager,
            proxyClientReadService
        );
        ProxyContext context = createContext();
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build();
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(producerSettings);
        when(grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID)).thenReturn(producerSettings);
        doThrow(new RuntimeException("offline hook failed"))
            .when(grpcClientSettingsManager)
            .offlineClientLiteSubscription(context, CLIENT_ID, producerSettings);
        StreamObserver<TelemetryCommand> responseObserver = mock(StreamObserver.class);
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(responseObserver);
        requestObserver.onNext(context, TelemetryCommand.newBuilder()
            .setSettings(producerSettings)
            .build());
        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNotNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).hasSize(1);

        assertThatCode(requestObserver::onCompleted).doesNotThrowAnyException();

        assertThat(proxyClientReadService.getClient(CLIENT_ID)).isNull();
        assertThat(proxyClientReadService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(TOPIC)
            .build()).getClients()).isEmpty();
        verify(this.grpcClientSettingsManager).removeAndGetRawClientSettings(CLIENT_ID);
        verify(this.grpcClientSettingsManager).offlineClientLiteSubscription(context, CLIENT_ID, producerSettings);
        verify(responseObserver).onCompleted();
    }

    protected void assertClientChannelInfo(ClientChannelInfo clientChannelInfo, String group) {
        assertEquals(LanguageCode.JAVA, clientChannelInfo.getLanguage());
        assertEquals(CLIENT_ID, clientChannelInfo.getClientId());
        assertTrue(clientChannelInfo.getChannel() instanceof GrpcClientChannel);
        GrpcClientChannel channel = (GrpcClientChannel) clientChannelInfo.getChannel();
        assertEquals(REMOTE_ADDR, channel.getRemoteAddress());
        assertEquals(LOCAL_ADDR, channel.getLocalAddress());
    }

    @Test
    public void testProducerNotifyClientTermination() throws Throwable {
        ProxyContext context = createContext();

        when(this.grpcClientSettingsManager.removeAndGetClientSettings(any())).thenReturn(Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build());
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).unRegisterProducer(any(), anyString(), channelInfoArgumentCaptor.capture());
        when(this.metadataService.getTopicMessageType(any(), anyString())).thenReturn(TopicMessageType.NORMAL);

        this.sendProducerTelemetry(context);
        this.sendProducerHeartbeat(context);

        NotifyClientTerminationResponse response = this.clientActivity.notifyClientTermination(
            context,
            NotifyClientTerminationRequest.newBuilder()
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        ClientChannelInfo clientChannelInfo = channelInfoArgumentCaptor.getValue();
        assertClientChannelInfo(clientChannelInfo, TOPIC);
    }

    @Test
    public void testConsumerNotifyClientTermination() throws Throwable {
        ProxyContext context = createContext();

        when(this.grpcClientSettingsManager.removeAndGetClientSettings(any())).thenReturn(Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .build());
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).unRegisterConsumer(any(), anyString(), channelInfoArgumentCaptor.capture());

        this.sendConsumerTelemetry(context);
        this.sendConsumerHeartbeat(context);

        NotifyClientTerminationResponse response = this.clientActivity.notifyClientTermination(
            context,
            NotifyClientTerminationRequest.newBuilder()
                .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
                .build()
        ).get();

        assertEquals(Code.OK, response.getStatus().getCode());
        ClientChannelInfo clientChannelInfo = channelInfoArgumentCaptor.getValue();
        assertClientChannelInfo(clientChannelInfo, CONSUMER_GROUP);
    }

    @Test
    public void testErrorConsumerGroupName() throws Throwable {
        ProxyContext context = createContext();
        try {
            this.sendClientTelemetry(
                context,
                Settings.newBuilder()
                    .setClientType(ClientType.PUSH_CONSUMER)
                    .setSubscription(Subscription.newBuilder()
                        .addSubscriptions(SubscriptionEntry.newBuilder()
                            .setExpression(FilterExpression.newBuilder()
                                .setExpression("tag")
                                .setType(FilterType.TAG)
                                .build())
                            .setTopic(Resource.newBuilder().setName(TOPIC).build())
                            .build())
                        .build())
                    .build()).get();
            fail();
        } catch (ExecutionException e) {
            StatusRuntimeException exception = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        }
    }

    @Test
    public void testErrorProducerConfig() throws Throwable {
        ProxyContext context = createContext();
        try {
            this.sendClientTelemetry(
                context,
                Settings.newBuilder()
                    .setClientType(ClientType.PRODUCER)
                    .setPublishing(Publishing.newBuilder()
                        .addTopics(Resource.newBuilder().setName("()").build())
                        .build())
                    .build()).get();
            fail();
        } catch (ExecutionException e) {
            StatusRuntimeException exception = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        }
    }

    @Test
    public void testEmptySettings() throws Throwable {
        ProxyContext context = createContext();
        try {
            this.sendClientTelemetry(
                context,
                Settings.getDefaultInstance()).get();
            fail();
        } catch (ExecutionException e) {
            StatusRuntimeException exception = (StatusRuntimeException) e.getCause();
            assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
        }
    }

    @Test
    public void testEmptyProducerSettings() throws Throwable {
        ProxyContext context = createContext();
        TelemetryCommand command = this.sendClientTelemetry(
            context,
            Settings.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setPublishing(Publishing.getDefaultInstance())
                .build()).get();
        assertTrue(command.hasSettings());
        assertTrue(command.getSettings().hasPublishing());
    }

    @Test
    public void testReportThreadStackTrace() {
        this.clientActivity = new ClientActivity(this.messagingProcessor, this.grpcClientSettingsManager, grpcChannelManagerMock);
        String jstack = "jstack";
        String nonce = "123";
        when(grpcChannelManagerMock.getAndRemoveResponseFuture(anyString())).thenReturn((CompletableFuture) runningInfoFutureMock);
        ProxyContext context = createContext();
        ContextStreamObserver<TelemetryCommand> streamObserver = clientActivity.telemetry(new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });
        streamObserver.onNext(context, TelemetryCommand.newBuilder()
            .setThreadStackTrace(ThreadStackTrace.newBuilder()
                .setThreadStackTrace(jstack)
                .setNonce(nonce)
                .build())
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .build());
        verify(runningInfoFutureMock, times(1)).complete(runningInfoArgumentCaptor.capture());
        ProxyRelayResult<ConsumerRunningInfo> result = runningInfoArgumentCaptor.getValue();
        assertThat(result.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(result.getResult().getJstack()).isEqualTo(jstack);
    }

    @Test
    public void testReportVerifyMessageResult() {
        this.clientActivity = new ClientActivity(this.messagingProcessor, this.grpcClientSettingsManager, grpcChannelManagerMock);
        String nonce = "123";
        when(grpcChannelManagerMock.getAndRemoveResponseFuture(anyString())).thenReturn((CompletableFuture) resultFutureMock);
        ProxyContext context = createContext();
        ContextStreamObserver<TelemetryCommand> streamObserver = clientActivity.telemetry(new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });
        streamObserver.onNext(context, TelemetryCommand.newBuilder()
            .setVerifyMessageResult(VerifyMessageResult.newBuilder()
                .setNonce(nonce)
                .build())
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .build());
        verify(resultFutureMock, times(1)).complete(resultArgumentCaptor.capture());
        ProxyRelayResult<ConsumeMessageDirectlyResult> result = resultArgumentCaptor.getValue();
        assertThat(result.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(result.getResult().getConsumeResult()).isEqualTo(CMResult.CR_SUCCESS);
    }

    protected CompletableFuture<TelemetryCommand> sendClientTelemetry(ProxyContext ctx, Settings settings) {
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(settings);

        CompletableFuture<TelemetryCommand> future = new CompletableFuture<>();
        StreamObserver<TelemetryCommand> responseObserver = new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {

            }
        };
        ContextStreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(responseObserver);
        requestObserver.onNext(ctx, TelemetryCommand.newBuilder()
            .setSettings(settings)
            .build());
        return future;
    }

    private ProducerChangeListener latestProducerChangeListener() {
        ArgumentCaptor<ProducerChangeListener> listenerCaptor = ArgumentCaptor.forClass(ProducerChangeListener.class);
        verify(this.messagingProcessor, times(2)).registerProducerListener(listenerCaptor.capture());
        return listenerCaptor.getAllValues().get(1);
    }

    private ConsumerIdsChangeListener latestConsumerIdsChangeListener() {
        ArgumentCaptor<ConsumerIdsChangeListener> listenerCaptor =
            ArgumentCaptor.forClass(ConsumerIdsChangeListener.class);
        verify(this.messagingProcessor, times(2)).registerConsumerListener(listenerCaptor.capture());
        return listenerCaptor.getAllValues().get(1);
    }

    private RemoteChannel remoteGrpcChannel() {
        return new RemoteChannel(
            "remote-proxy",
            REMOTE_ADDR,
            LOCAL_ADDR,
            ChannelProtocolType.GRPC_V2,
            null
        );
    }

    private RemoteChannel remoteGrpcChannel(Settings settings) throws Exception {
        return new RemoteChannel(
            "remote-proxy",
            REMOTE_ADDR,
            LOCAL_ADDR,
            ChannelProtocolType.GRPC_V2,
            JsonFormat.printer().print(settings)
        );
    }

    @Test
    public void testSyncLiteSubscription_Success() {
        ProxyContext proxyContext = createContext();
        proxyContext.setClientID("client-id");
        Resource topic = Resource.newBuilder().setName("test-topic").build();
        Resource group = Resource.newBuilder().setName("test-group").build();
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder()
            .setTopic(topic)
            .setGroup(group)
            .setAction(LiteSubscriptionAction.PARTIAL_ADD)
            .addAllLiteTopicSet(java.util.Collections.emptyList())
            .setVersion(1L)
            .build();

        when(messagingProcessor.syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<SyncLiteSubscriptionResponse> future = clientActivity.syncLiteSubscription(proxyContext, request);

        SyncLiteSubscriptionResponse response = future.join();
        assertEquals(Code.OK, response.getStatus().getCode());
    }

    @Test
    public void testSyncLiteSubscription_ValidationFailure() {
        ProxyContext proxyContext = createContext();
        Resource topic = Resource.newBuilder().setName("test-topic").build();
        Resource group = Resource.newBuilder().setName("test-group").build();
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder()
            .setTopic(topic)
            .setGroup(group)
            .build();

        // Mock the GrpcValidator singleton
        GrpcValidator mockValidator = mock(GrpcValidator.class);
        try (MockedStatic<GrpcValidator> mocked = mockStatic(GrpcValidator.class)) {
            mocked.when(GrpcValidator::getInstance).thenReturn(mockValidator);

            doThrow(new IllegalArgumentException("Invalid topic"))
                .when(mockValidator).validateTopicAndConsumerGroup(topic, group);

            CompletableFuture<SyncLiteSubscriptionResponse> future = clientActivity.syncLiteSubscription(proxyContext, request);

            assertTrue(future.isCompletedExceptionally());
        }
    }

    @Test
    public void testSyncLiteSubscription_ProcessingFailure() {
        ProxyContext proxyContext = createContext();
        proxyContext.setClientID("client-id");
        Resource topic = Resource.newBuilder().setName("test-topic").build();
        Resource group = Resource.newBuilder().setName("test-group").build();
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder()
            .setTopic(topic)
            .setGroup(group)
            .setAction(LiteSubscriptionAction.PARTIAL_ADD)
            .addAllLiteTopicSet(java.util.Collections.emptyList())
            .setVersion(1L)
            .build();

        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Processing failed"));
        when(messagingProcessor.syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(failedFuture);

        CompletableFuture<SyncLiteSubscriptionResponse> future = clientActivity.syncLiteSubscription(proxyContext, request);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testSyncLiteSubscription_NullContext() {
        Resource topic = Resource.newBuilder().setName("test-topic").build();
        Resource group = Resource.newBuilder().setName("test-group").build();
        SyncLiteSubscriptionRequest request = SyncLiteSubscriptionRequest.newBuilder()
            .setTopic(topic)
            .setGroup(group)
            .build();

        CompletableFuture<SyncLiteSubscriptionResponse> future = clientActivity.syncLiteSubscription(null, request);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testSyncLiteSubscription_NullRequest() {
        ProxyContext proxyContext = createContext();

        CompletableFuture<SyncLiteSubscriptionResponse> future = clientActivity.syncLiteSubscription(proxyContext, null);

        assertTrue(future.isCompletedExceptionally());
    }
}
