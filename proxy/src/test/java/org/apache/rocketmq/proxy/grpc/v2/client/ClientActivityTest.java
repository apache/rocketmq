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
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class ClientActivityTest extends BaseActivityTest {

    private static final String TOPIC = "topic";
    private static final String CONSUMER_GROUP = "consumerGroup";

    private ClientActivity clientActivity;

    @Before
    public void before() throws Throwable {
        super.before();
        this.clientActivity = new ClientActivity(this.messagingProcessor, this.grpcClientSettingsManager);
    }

    protected TelemetryCommand sendProducerTelemetry(Context context) throws Throwable {
        return this.sendClientTelemetry(
            context,
            Settings.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setPublishing(Publishing.newBuilder()
                    .addTopics(Resource.newBuilder().setName(TOPIC).build())
                    .build())
                .build()).get();
    }

    protected HeartbeatResponse sendProducerHeartbeat(Context context) throws Throwable {
        return this.clientActivity.heartbeat(context, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build()).get();
    }

    @Test
    public void testProducerHeartbeat() throws Throwable {
        Context context = createContext();

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

        HeartbeatResponse response = this.sendProducerHeartbeat(context);

        assertEquals(Code.OK, response.getStatus().getCode());

        assertEquals(Lists.newArrayList(TOPIC), registerProducerGroupArgumentCaptor.getAllValues());
        ClientChannelInfo clientChannelInfo = channelInfoArgumentCaptor.getValue();
        assertClientChannelInfo(clientChannelInfo, TOPIC);

        assertEquals(Lists.newArrayList(TOPIC), txProducerGroupArgumentCaptor.getAllValues());
        assertEquals(Lists.newArrayList(TOPIC), txProducerTopicArgumentCaptor.getAllValues());
    }

    protected TelemetryCommand sendConsumerTelemetry(Context context) throws Throwable {
        return this.sendClientTelemetry(
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
    }

    protected HeartbeatResponse sendConsumerHeartbeat(Context context) throws Throwable {
        return this.clientActivity.heartbeat(context, HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setGroup(Resource.newBuilder().setName(CONSUMER_GROUP).build())
            .build()).get();
    }

    @Test
    public void testConsumerHeartbeat() throws Throwable {
        Context context = createContext();
        this.sendConsumerTelemetry(context);

        ArgumentCaptor<Set<SubscriptionData>> subscriptionDatasArgumentCaptor = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).registerConsumer(any(),
            anyString(),
            channelInfoArgumentCaptor.capture(),
            any(),
            any(),
            any(),
            subscriptionDatasArgumentCaptor.capture()
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
        assertEquals(group, channel.getGroup());
    }

    @Test
    public void testProducerNotifyClientTermination() throws Throwable {
        Context context = createContext();

        when(this.grpcClientSettingsManager.removeClientSettings(eq(CLIENT_ID))).thenReturn(Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(TOPIC).build())
                .build())
            .build());
        ArgumentCaptor<ClientChannelInfo> channelInfoArgumentCaptor = ArgumentCaptor.forClass(ClientChannelInfo.class);
        doNothing().when(this.messagingProcessor).unRegisterProducer(any(), anyString(), channelInfoArgumentCaptor.capture());

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
        Context context = createContext();

        when(this.grpcClientSettingsManager.removeClientSettings(eq(CLIENT_ID))).thenReturn(Settings.newBuilder()
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

    protected CompletableFuture<TelemetryCommand> sendClientTelemetry(Context ctx, Settings settings) {
        when(grpcClientSettingsManager.getClientSettings(any())).thenReturn(settings);

        CompletableFuture<TelemetryCommand> future = new CompletableFuture<>();
        StreamObserver<TelemetryCommand> responseObserver = new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override public void onCompleted() {

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