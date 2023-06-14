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
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
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
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
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
        StreamObserver<TelemetryCommand> streamObserver = clientActivity.telemetry(context, new StreamObserver<TelemetryCommand>() {
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
        streamObserver.onNext(TelemetryCommand.newBuilder()
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
        StreamObserver<TelemetryCommand> streamObserver = clientActivity.telemetry(context, new StreamObserver<TelemetryCommand>() {
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
        streamObserver.onNext(TelemetryCommand.newBuilder()
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
        StreamObserver<TelemetryCommand> requestObserver = this.clientActivity.telemetry(
            ctx,
            responseObserver
        );
        requestObserver.onNext(TelemetryCommand.newBuilder()
            .setSettings(settings)
            .build());
        return future;
    }
}
