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

import apache.rocketmq.v2.AdminSendMessageRequest;
import apache.rocketmq.v2.AdminSendMessageResponse;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.ChangeLogLevelRequest;
import apache.rocketmq.v2.ChangeLogLevelResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeGroupAccumulationRequest;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeSubscriptionRequest;
import apache.rocketmq.v2.DescribeSubscriptionResponse;
import apache.rocketmq.v2.DescribeTopicStatusRequest;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.DeleteSubscriptionRequest;
import apache.rocketmq.v2.DeleteSubscriptionResponse;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.GetConsumerRunningInfoRequest;
import apache.rocketmq.v2.GetConsumerRunningInfoResponse;
import apache.rocketmq.v2.GetProxyRuntimeStatsRequest;
import apache.rocketmq.v2.GetProxyRuntimeStatsResponse;
import apache.rocketmq.v2.GetTopicRouteRequest;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.ListConsumerConnectionRequest;
import apache.rocketmq.v2.ListConsumerConnectionResponse;
import apache.rocketmq.v2.ListMessageRequest;
import apache.rocketmq.v2.ListMessageResponse;
import apache.rocketmq.v2.ListSubscriptionRequest;
import apache.rocketmq.v2.ListSubscriptionResponse;
import apache.rocketmq.v2.PrintThreadStackTraceRequest;
import apache.rocketmq.v2.PrintThreadStackTraceResponse;
import apache.rocketmq.v2.QueryTimeSpanRequest;
import apache.rocketmq.v2.QueryTimeSpanResponse;
import apache.rocketmq.v2.ResetGroupOffsetRequest;
import apache.rocketmq.v2.ResetGroupOffsetResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.UA;
import apache.rocketmq.v2.VerifyMessageRequest;
import apache.rocketmq.v2.VerifyMessageResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminGrpcServiceTest {

    @Mock
    private ServiceManager serviceManager;
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private GrpcChannelManager grpcChannelManager;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;
    @Mock
    private AdminService adminService;
    @Mock
    private TopicRouteService topicRouteService;
    @Mock
    private GrpcClientChannel channel;

    private ProxyAdminGrpcService service;

    @Before
    public void setUp() {
        when(serviceManager.getAdminService()).thenReturn(adminService);
        when(serviceManager.getTopicRouteService()).thenReturn(topicRouteService);
        service = new ProxyAdminGrpcService(serviceManager, messagingProcessor, grpcChannelManager,
            grpcClientSettingsManager);
    }

    // ------------------------------------------------------------------ helpers

    private static class SimpleObserver<T> implements StreamObserver<T> {
        T value;
        Throwable error;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onCompleted() {
        }
    }

    private Settings subscriptionSettings(ClientType clientType, String group, String topic, String expression) {
        return Settings.newBuilder()
            .setClientType(clientType)
            .setUserAgent(UA.newBuilder().setVersion("4.9.0").setLanguage(Language.JAVA).setHostname("host").build())
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName(group).build())
                .addSubscriptions(SubscriptionEntry.newBuilder()
                    .setTopic(Resource.newBuilder().setName(topic).build())
                    .setExpression(FilterExpression.newBuilder()
                        .setType(FilterType.TAG).setExpression(expression).build())
                    .build())
                .build())
            .build();
    }

    private void stubBrokerRoute() throws Exception {
        AddressableMessageQueue mq = new AddressableMessageQueue(new MessageQueue("t", "broker-a", 0),
            "127.0.0.1:10911");
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        when(selector.getQueues()).thenReturn(Collections.singletonList(mq));
        MessageQueueView mqv = mock(MessageQueueView.class);
        when(mqv.getReadSelector()).thenReturn(selector);
        when(topicRouteService.getAllMessageQueueView(any(), anyString())).thenReturn(mqv);
    }

    private void stubOnlineClient(String clientId, Settings settings) {
        when(channel.getClientId()).thenReturn(clientId);
        when(channel.getRemoteAddress()).thenReturn("1.2.3.4:8888");
        when(grpcChannelManager.getClientChannels()).thenReturn(Collections.singletonList(channel));
        when(grpcChannelManager.getChannel(clientId)).thenReturn(channel);
        when(grpcClientSettingsManager.getRawClientSettings(clientId)).thenReturn(settings);
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void changeLogLevelSucceeds() {
        SimpleObserver<ChangeLogLevelResponse> obs = new SimpleObserver<>();
        service.changeLogLevel(ChangeLogLevelRequest.newBuilder().setLevel(ChangeLogLevelRequest.Level.DEBUG).build(),
            obs);
        assertNotNull(obs.value);
        assertTrue(obs.value.getRemark().toLowerCase().contains("log level changed"));
    }

    @Test
    public void getProxyRuntimeStatsCountsClients() {
        GrpcClientChannel producer = mock(GrpcClientChannel.class);
        when(producer.getClientId()).thenReturn("p1");
        when(grpcChannelManager.getClientChannels()).thenReturn(java.util.Arrays.asList(channel, producer));
        when(channel.getClientId()).thenReturn("c1");
        when(grpcClientSettingsManager.getRawClientSettings("c1"))
            .thenReturn(subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        when(grpcClientSettingsManager.getRawClientSettings("p1"))
            .thenReturn(subscriptionSettings(ClientType.PRODUCER, "g", "t", "*"));

        SimpleObserver<GetProxyRuntimeStatsResponse> obs = new SimpleObserver<>();
        service.getProxyRuntimeStats(GetProxyRuntimeStatsRequest.newBuilder().build(), obs);
        assertNotNull(obs.value);
        assertEquals(1, obs.value.getConsumers());
        assertEquals(1, obs.value.getProducers());
        assertEquals(2, obs.value.getConnections());
    }

    @Test
    public void getTopicRouteReturnsJson() throws Exception {
        when(adminService.getTopicRouteData("t")).thenReturn(new TopicRouteData());
        SimpleObserver<GetTopicRouteResponse> obs = new SimpleObserver<>();
        service.getTopicRoute(GetTopicRouteRequest.newBuilder().setTopic(Resource.newBuilder().setName("t")).build(),
            obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertNotNull(obs.value.getTopicRouteData());
    }

    @Test
    public void describeTopicStatusReturnsConfig() throws Exception {
        stubBrokerRoute();
        TopicConfigAndQueueMapping cfg = new TopicConfigAndQueueMapping();
        cfg.setTopicName("t");
        cfg.setReadQueueNums(4);
        cfg.setWriteQueueNums(4);
        cfg.setPerm(6);
        when(adminService.getTopicConfig(anyString(), eq("t"), anyLong())).thenReturn(cfg);

        SimpleObserver<DescribeTopicStatusResponse> obs = new SimpleObserver<>();
        service.describeTopicStatus(
            DescribeTopicStatusRequest.newBuilder().setTopic(Resource.newBuilder().setName("t")).build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertTrue(obs.value.getDescription().contains("readQueues=4"));
    }

    @Test
    public void listSubscriptionReturnsSubscriptions() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "tagA"));
        SimpleObserver<ListSubscriptionResponse> obs = new SimpleObserver<>();
        service.listSubscription(ListSubscriptionRequest.newBuilder().build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertTrue(obs.value.getSubscriptionInfoCount() >= 1);
        assertEquals("t", obs.value.getSubscriptionInfo(0).getTopic().getName());
    }

    @Test
    public void describeSubscriptionReturnsClientSubscriptions() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "tagA"));
        SimpleObserver<DescribeSubscriptionResponse> obs = new SimpleObserver<>();
        service.describeSubscription(DescribeSubscriptionRequest.newBuilder().build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertTrue(obs.value.getClientSubscriptionInfoCount() >= 1);
    }

    @Test
    public void listSubscriptionFiltersByGroup() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "tagA"));
        SimpleObserver<ListSubscriptionResponse> obs = new SimpleObserver<>();
        service.listSubscription(
            ListSubscriptionRequest.newBuilder().setGroup(Resource.newBuilder().setName("other")).build(), obs);
        assertNotNull(obs.value);
        assertEquals(0, obs.value.getSubscriptionInfoCount());
    }

    @Test
    public void deleteSubscriptionSucceeds() {
        SimpleObserver<DeleteSubscriptionResponse> obs = new SimpleObserver<>();
        service.deleteSubscription(
            DeleteSubscriptionRequest.newBuilder().setTopic(Resource.newBuilder().setName("t")).build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
    }

    @Test
    public void listConsumerConnectionReturnsOnlineConsumers() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        SimpleObserver<ListConsumerConnectionResponse> obs = new SimpleObserver<>();
        service.listConsumerConnection(
            ListConsumerConnectionRequest.newBuilder().setGroup(Resource.newBuilder().setName("g")).build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(1, obs.value.getClientInfoCount());
        assertEquals("c1", obs.value.getClientInfo(0).getClientId());
        assertEquals("1.2.3.4:8888", obs.value.getClientInfo(0).getEgressIp());
    }

    @Test
    public void describeGroupAccumulationSumsDiff() throws Exception {
        stubBrokerRoute();
        MessageQueue mq = new MessageQueue("t", "broker-a", 0);
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(100);
        wrapper.setConsumerOffset(60);
        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(mq, wrapper);
        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable(offsetTable);
        when(adminService.fetchConsumeStats(anyString(), eq("g"), eq("t"), anyLong())).thenReturn(consumeStats);

        SimpleObserver<DescribeGroupAccumulationResponse> obs = new SimpleObserver<>();
        service.describeGroupAccumulation(DescribeGroupAccumulationRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("g"))
            .addTopics(Resource.newBuilder().setName("t"))
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(40, obs.value.getAccumulation().getAccumulation());
    }

    @Test
    public void resetGroupOffsetSucceeds() throws Exception {
        stubBrokerRoute();
        when(adminService.resetOffset(anyString(), eq("t"), eq("g"), anyLong(), eq(true), anyLong()))
            .thenReturn(Collections.emptyMap());
        SimpleObserver<ResetGroupOffsetResponse> obs = new SimpleObserver<>();
        service.resetGroupOffset(ResetGroupOffsetRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("g"))
            .setTopic(Resource.newBuilder().setName("t"))
            .setResetTimestamp(com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).build())
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
    }

    @Test
    public void queryMessageByMessageIdReturnsMessage() throws Exception {
        stubBrokerRoute();
        MessageExt ext = new MessageExt();
        ext.setMsgId("MSG-1");
        ext.setTopic("t");
        ext.setTags("tag");
        when(adminService.viewMessage(anyString(), eq("t"), anyLong(), anyLong())).thenReturn(ext);

        SimpleObserver<ListMessageResponse> obs = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t"))
            .setMessageId("010000000000000000000000000000000000000000000000")
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(1, obs.value.getMessagesCount());
        assertEquals("MSG-1", obs.value.getMessages(0).getSystemProperties().getMessageId());
    }

    @Test
    public void queryMessageByKeyReturnsMessages() throws Exception {
        stubBrokerRoute();
        MessageExt ext = new MessageExt();
        ext.setMsgId("MSG-2");
        ext.setTopic("t");
        when(adminService.queryMessage(anyString(), eq("t"), eq("key"), anyInt(), anyLong(), anyLong(), anyLong()))
            .thenReturn(Collections.singletonList(ext));

        SimpleObserver<ListMessageResponse> obs = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t"))
            .setMessageKey("key")
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(1, obs.value.getMessagesCount());
    }

    @Test
    public void queryMessageRequiresIdOrKey() {
        SimpleObserver<ListMessageResponse> obs = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t")).build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.BAD_REQUEST, obs.value.getStatus().getCode());
    }

    @Test
    public void printThreadStackTraceDispatchesWhenConnected() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        SimpleObserver<PrintThreadStackTraceResponse> obs = new SimpleObserver<>();
        service.printThreadStackTrace(
            PrintThreadStackTraceRequest.newBuilder().setClientId("c1").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
    }

    @Test
    public void printThreadStackTraceNotFoundWhenDisconnected() {
        when(grpcChannelManager.getChannel("missing")).thenReturn(null);
        SimpleObserver<PrintThreadStackTraceResponse> obs = new SimpleObserver<>();
        service.printThreadStackTrace(
            PrintThreadStackTraceRequest.newBuilder().setClientId("missing").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.NOT_FOUND, obs.value.getStatus().getCode());
    }

    @Test
    public void verifyMessageDispatchesWhenConnected() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        SimpleObserver<VerifyMessageResponse> obs = new SimpleObserver<>();
        service.verifyMessage(VerifyMessageRequest.newBuilder()
            .setClientId("c1")
            .setTopic(Resource.newBuilder().setName("t"))
            .setMessageId("MSG-1")
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
    }

    @Test
    public void adminSendMessageReturnsId() {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("SENT-1");
        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        SimpleObserver<AdminSendMessageResponse> obs = new SimpleObserver<>();
        service.adminSendMessage(AdminSendMessageRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t"))
            .setBody(com.google.protobuf.ByteString.copyFromUtf8("hello"))
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals("SENT-1", obs.value.getMessageId());
    }

    @Test
    public void getConsumerRunningInfoReturnsSubscriptions() {
        stubOnlineClient("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "tagA"));
        SimpleObserver<GetConsumerRunningInfoResponse> obs = new SimpleObserver<>();
        service.getConsumerRunningInfo(
            GetConsumerRunningInfoRequest.newBuilder().setClientId("c1").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertTrue(obs.value.getConsumerRunningInfo().getSubscriptionsMap().containsKey("t"));
    }

    @Test
    public void getConsumerRunningInfoNotFoundWhenDisconnected() {
        when(grpcChannelManager.getChannel("missing")).thenReturn(null);
        SimpleObserver<GetConsumerRunningInfoResponse> obs = new SimpleObserver<>();
        service.getConsumerRunningInfo(
            GetConsumerRunningInfoRequest.newBuilder().setClientId("missing").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.NOT_FOUND, obs.value.getStatus().getCode());
    }

    @Test
    public void queryTimeSpanReturnsPerQueueSpan() throws Exception {
        stubBrokerRoute();
        MessageQueue mq = new MessageQueue("t", "broker-a", 0);
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(100);
        wrapper.setConsumerOffset(60);
        Map<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();
        offsetTable.put(mq, wrapper);
        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setOffsetTable(offsetTable);
        when(adminService.fetchConsumeStats(anyString(), eq("g"), eq("t"), anyLong())).thenReturn(consumeStats);
        when(adminService.getEarliestMsgStoretime(anyString(), any(MessageQueue.class), anyLong()))
            .thenReturn(12345L);

        SimpleObserver<QueryTimeSpanResponse> obs = new SimpleObserver<>();
        service.queryTimeSpan(QueryTimeSpanRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("g"))
            .addTopics(Resource.newBuilder().setName("t"))
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertTrue(obs.value.getQueueTimeSpanListCount() >= 1);
        assertEquals(12345L, obs.value.getQueueTimeSpanList(0).getMinTimestamp());
        Broker broker = obs.value.getQueueTimeSpanList(0).getMessageQueue().getBroker();
        assertEquals("broker-a", broker.getName());
    }
}
