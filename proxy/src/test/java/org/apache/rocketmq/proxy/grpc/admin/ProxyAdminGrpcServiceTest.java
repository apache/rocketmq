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
import apache.rocketmq.v2.ChangeLogLevelRequest;
import apache.rocketmq.v2.ChangeLogLevelResponse;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DeleteSubscriptionRequest;
import apache.rocketmq.v2.DeleteSubscriptionResponse;
import apache.rocketmq.v2.DescribeGroupAccumulationRequest;
import apache.rocketmq.v2.DescribeGroupAccumulationResponse;
import apache.rocketmq.v2.DescribeSubscriptionRequest;
import apache.rocketmq.v2.DescribeSubscriptionResponse;
import apache.rocketmq.v2.DescribeTopicStatusRequest;
import apache.rocketmq.v2.DescribeTopicStatusResponse;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.GetConsumerRunningInfoRequest;
import apache.rocketmq.v2.GetConsumerRunningInfoResponse;
import apache.rocketmq.v2.GetProxyRuntimeStatsRequest;
import apache.rocketmq.v2.GetProxyRuntimeStatsResponse;
import apache.rocketmq.v2.GetTopicRouteRequest;
import apache.rocketmq.v2.GetTopicRouteResponse;
import apache.rocketmq.v2.ListConsumerConnectionRequest;
import apache.rocketmq.v2.ListConsumerConnectionResponse;
import apache.rocketmq.v2.ListMessageRequest;
import apache.rocketmq.v2.ListMessageResponse;
import apache.rocketmq.v2.ListSubscriptionRequest;
import apache.rocketmq.v2.ListSubscriptionResponse;
import apache.rocketmq.v2.MessageType;
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
import apache.rocketmq.v2.SubscriptionInfo;
import apache.rocketmq.v2.SystemProperties;
import apache.rocketmq.v2.VerifyMessageRequest;
import apache.rocketmq.v2.VerifyMessageResponse;
import com.alibaba.fastjson2.JSON;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayRequest;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SimpleSubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminGrpcServiceTest extends InitConfigTest {

    @Mock
    private ServiceManager serviceManager;
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private GrpcChannelManager grpcChannelManager;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;
    @Mock
    private ProxyAdminForwarder forwarder;
    @Mock
    private AdminService adminService;
    @Mock
    private TopicRouteService topicRouteService;
    @Mock
    private ConsumerManager consumerManager;
    @Mock
    private GrpcClientChannel channel;

    private ProxyAdminGrpcService service;

    private static final String TOPIC = "topicA";
    private static final String GROUP = "groupA";
    private static final String CLIENT_ID = "client-1";
    private static final String ADDRESS_A = "127.0.0.1:10911";
    private static final String ADDRESS_B = "127.0.0.2:10911";

    @Before
    public void setUp() {
        when(serviceManager.getAdminService()).thenReturn(adminService);
        when(serviceManager.getTopicRouteService()).thenReturn(topicRouteService);
        service = new ProxyAdminGrpcService(serviceManager, messagingProcessor, grpcChannelManager,
            grpcClientSettingsManager, forwarder);
    }

    @After
    public void tearDown() {
        service.shutdown();
    }

    // ------------------------------------------------------------------ helpers

    private static class SimpleObserver<T> implements StreamObserver<T> {
        T value;
        Throwable error;
        boolean completed;

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
            this.completed = true;
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }

    private void stubClusterInfo(String... addrs) {
        Map<String, BrokerData> table = new HashMap<>();
        for (int i = 0; i < addrs.length; i++) {
            HashMap<Long, String> brokerAddrs = new HashMap<>();
            brokerAddrs.put(MixAll.MASTER_ID, addrs[i]);
            table.put("broker-" + i, new BrokerData("DefaultCluster", "broker-" + i, brokerAddrs));
        }
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setBrokerAddrTable(table);
        when(adminService.getBrokerClusterInfo(anyLong()))
            .thenReturn(CompletableFuture.completedFuture(clusterInfo));
    }

    private void stubRoute(boolean forWrite, String... brokerAddrs) throws Exception {
        List<AddressableMessageQueue> queues = new ArrayList<>();
        for (int i = 0; i < brokerAddrs.length; i++) {
            queues.add(new AddressableMessageQueue(new MessageQueue(TOPIC, "broker-" + i, i), brokerAddrs[i]));
        }
        MessageQueueSelector selector = mock(MessageQueueSelector.class);
        when(selector.getQueues()).thenReturn(queues);
        MessageQueueView view = mock(MessageQueueView.class);
        if (forWrite) {
            when(view.getWriteSelector()).thenReturn(selector);
        } else {
            when(view.getReadSelector()).thenReturn(selector);
        }
        when(topicRouteService.getAllMessageQueueView(any(), anyString())).thenReturn(view);
    }

    private SubscriptionData subscriptionData(String topic, String expressionType, String expression) {
        SubscriptionData data = new SubscriptionData();
        data.setTopic(topic);
        data.setExpressionType(expressionType);
        data.setSubString(expression);
        return data;
    }

    private ConsumerConnection consumerConnection(String clientId, String clientAddr, String... topics) {
        ConsumerConnection connection = new ConsumerConnection();
        Connection conn = new Connection();
        conn.setClientId(clientId);
        conn.setClientAddr(clientAddr);
        conn.setLanguage(LanguageCode.JAVA);
        conn.setVersion(355);
        connection.getConnectionSet().add(conn);
        for (String topic : topics) {
            connection.getSubscriptionTable().put(topic, subscriptionData(topic, ExpressionType.TAG, "tagA"));
        }
        connection.setMessageModel(MessageModel.CLUSTERING);
        connection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        return connection;
    }

    private Settings settings(ClientType clientType, String group, String topic, String expression) {
        return Settings.newBuilder()
            .setClientType(clientType)
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

    private MessageExt messageExt(String msgId) {
        MessageExt ext = new MessageExt();
        ext.setTopic(TOPIC);
        ext.setMsgId(msgId);
        ext.setBody("payload".getBytes(StandardCharsets.UTF_8));
        ext.setQueueId(0);
        ext.setQueueOffset(100L);
        ext.setBrokerName("broker-0");
        long now = System.currentTimeMillis();
        ext.setBornTimestamp(now);
        ext.setStoreTimestamp(now);
        ext.setBornHost(new InetSocketAddress("127.0.0.1", 10909));
        ext.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        return ext;
    }

    private ConsumeStats consumeStats(String topic, long brokerOffset, long consumerOffset, long pullOffset) {
        ConsumeStats stats = new ConsumeStats();
        OffsetWrapper wrapper = new OffsetWrapper();
        wrapper.setBrokerOffset(brokerOffset);
        wrapper.setConsumerOffset(consumerOffset);
        wrapper.setPullOffset(pullOffset);
        stats.getOffsetTable().put(new MessageQueue(topic, "broker-0", 0), wrapper);
        return stats;
    }

    private TopicConfig topicConfig(TopicMessageType type, int readQueueNums, int writeQueueNums) {
        TopicConfig config = new TopicConfig();
        config.setTopicName(TOPIC);
        config.setReadQueueNums(readQueueNums);
        config.setWriteQueueNums(writeQueueNums);
        config.setPerm(6);
        if (type != null) {
            config.setTopicMessageType(type);
        }
        return config;
    }

    /** Registers the client as a local channel reachable through the consumer manager. */
    private void stubLocalClient() {
        when(serviceManager.getConsumerManager()).thenReturn(consumerManager);
        when(consumerManager.findChannel(GROUP, CLIENT_ID))
            .thenReturn(new ClientChannelInfo(channel, CLIENT_ID, LanguageCode.JAVA, 0));
        when(channel.isActive()).thenReturn(true);
    }

    /**
     * Simulates the relay round trip: when the service writes a {@link ProxyRelayRequest} into the
     * client channel, the captured caller future is completed with the given result, exactly like
     * {@code ClusterProxyRelayService} + the channel implementation would do in production.
     */
    private <T> void answerRelayWith(int expectedCode, ProxyRelayResult<T> result) {
        when(channel.writeAndFlush(any())).thenAnswer(invocation -> {
            ProxyRelayRequest request = invocation.getArgument(0);
            assertEquals(expectedCode, request.getCode());
            CompletableFuture<ProxyRelayResult<T>> future = request.typedResponseFuture();
            future.complete(result);
            return null;
        });
    }

    private static Resource resource(String name) {
        return Resource.newBuilder().setName(name).build();
    }

    // ------------------------------------------------------------------ 1. ChangeLogLevel

    @Test
    public void changeLogLevelChangesRootLevelTest() {
        SimpleObserver<ChangeLogLevelResponse> observer = new SimpleObserver<>();
        service.changeLogLevel(ChangeLogLevelRequest.newBuilder()
            .setLevel(ChangeLogLevelRequest.Level.DEBUG).build(), observer);

        assertNotNull(observer.value);
        assertTrue(observer.value.getRemark(), observer.value.getRemark().contains("log level changed"));
        assertTrue(observer.completed);
    }

    // ------------------------------------------------------------------ 2. DescribeTopicStatus

    @Test
    public void describeTopicStatusReadsMessageTypeFromAttributesTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.getTopicConfig(anyString(), eq(TOPIC), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(topicConfig(TopicMessageType.FIFO, 4, 4)));

        SimpleObserver<DescribeTopicStatusResponse> observer = new SimpleObserver<>();
        service.describeTopicStatus(DescribeTopicStatusRequest.newBuilder()
            .setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(MessageType.FIFO, observer.value.getTopicMessageType());
        assertTrue(observer.value.getDescription().contains("readQueueNums=4"));
    }

    @Test
    public void describeTopicStatusMergesQueueNumsAcrossBrokersTest() throws Exception {
        stubRoute(false, ADDRESS_A, ADDRESS_B);
        when(adminService.getTopicConfig(anyString(), eq(TOPIC), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(topicConfig(TopicMessageType.FIFO, 4, 4)));

        SimpleObserver<DescribeTopicStatusResponse> observer = new SimpleObserver<>();
        service.describeTopicStatus(DescribeTopicStatusRequest.newBuilder()
            .setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(MessageType.FIFO, observer.value.getTopicMessageType());
        assertTrue(observer.value.getDescription().contains("readQueueNums=8"));
        assertTrue(observer.value.getDescription().contains("writeQueueNums=8"));
    }

    @Test
    public void describeTopicStatusReportsMixedWhenBrokersDisagreeTest() throws Exception {
        stubRoute(false, ADDRESS_A, ADDRESS_B);
        when(adminService.getTopicConfig(eq(ADDRESS_A), eq(TOPIC), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(topicConfig(TopicMessageType.FIFO, 4, 4)));
        when(adminService.getTopicConfig(eq(ADDRESS_B), eq(TOPIC), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(topicConfig(TopicMessageType.NORMAL, 4, 4)));

        SimpleObserver<DescribeTopicStatusResponse> observer = new SimpleObserver<>();
        service.describeTopicStatus(DescribeTopicStatusRequest.newBuilder()
            .setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        // MIXED degrades to UNSPECIFIED rather than pretending one broker is right
        assertEquals(MessageType.MESSAGE_TYPE_UNSPECIFIED, observer.value.getTopicMessageType());
    }

    @Test
    public void describeTopicStatusFailsWhenRouteMissingTest() {
        SimpleObserver<DescribeTopicStatusResponse> observer = new SimpleObserver<>();
        service.describeTopicStatus(DescribeTopicStatusRequest.newBuilder()
            .setTopic(resource("missingTopic")).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("topic route not found"));
    }

    // ------------------------------------------------------------------ 3/4. List & DescribeSubscription

    @Test
    public void listSubscriptionRejectsEmptyFiltersTest() {
        SimpleObserver<ListSubscriptionResponse> observer = new SimpleObserver<>();
        service.listSubscription(ListSubscriptionRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("at least one of topic or group"));
    }

    @Test
    public void describeSubscriptionRejectsEmptyFiltersTest() {
        SimpleObserver<DescribeSubscriptionResponse> observer = new SimpleObserver<>();
        service.describeSubscription(DescribeSubscriptionRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("at least one of topic or group"));
    }

    @Test
    public void listSubscriptionReturnsBrokerSideSubscriptionsTest() {
        stubClusterInfo(ADDRESS_A);
        ConsumerConnection connection = consumerConnection(CLIENT_ID, "192.168.1.5:43210", TOPIC);
        // retry topics are bookkeeping, not user-visible subscriptions
        connection.getSubscriptionTable().put(MixAll.getRetryTopic(GROUP),
            subscriptionData(MixAll.getRetryTopic(GROUP), ExpressionType.TAG, "*"));
        connection.getSubscriptionTable().put(KeyBuilder.buildPopRetryTopicV2(TOPIC, GROUP),
            subscriptionData(KeyBuilder.buildPopRetryTopicV2(TOPIC, GROUP), ExpressionType.TAG, "*"));
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(connection));

        SimpleObserver<ListSubscriptionResponse> observer = new SimpleObserver<>();
        service.listSubscription(ListSubscriptionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getSubscriptionInfoCount());
        SubscriptionInfo info = observer.value.getSubscriptionInfo(0);
        assertEquals(GROUP, info.getGroup().getName());
        assertEquals(TOPIC, info.getTopic().getName());
        assertEquals(FilterType.TAG, info.getExpression().getType());
        assertEquals("tagA", info.getExpression().getExpression());
        assertTrue(info.getOnline());
        assertEquals(apache.rocketmq.v2.MessageModel.CLUSTERING, info.getMessageModel());
    }

    @Test
    public void listSubscriptionByTopicResolvesGroupsFromBrokersTest() {
        stubClusterInfo(ADDRESS_A);
        GroupList groupList = new GroupList();
        groupList.setGroupList(new HashSet<>(Collections.singletonList(GROUP)));
        when(adminService.queryTopicConsumeByWho(eq(ADDRESS_A), eq(TOPIC), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(groupList));
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumerConnection(CLIENT_ID, "192.168.1.5:43210", TOPIC)));

        SimpleObserver<ListSubscriptionResponse> observer = new SimpleObserver<>();
        service.listSubscription(ListSubscriptionRequest.newBuilder()
            .setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getSubscriptionInfoCount());
        assertEquals(GROUP, observer.value.getSubscriptionInfo(0).getGroup().getName());
        verify(adminService).queryTopicConsumeByWho(eq(ADDRESS_A), eq(TOPIC), anyLong());
    }

    @Test
    public void describeSubscriptionReportsOneEntryPerClientTest() {
        stubClusterInfo(ADDRESS_A);
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumerConnection(CLIENT_ID, "192.168.1.5:43210", TOPIC)));

        SimpleObserver<DescribeSubscriptionResponse> observer = new SimpleObserver<>();
        service.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getClientSubscriptionInfoCount());
        assertEquals(CLIENT_ID, observer.value.getClientSubscriptionInfo(0).getClientInfo().getClientId());
        SubscriptionInfo info = observer.value.getClientSubscriptionInfo(0).getSubscriptionInfo();
        assertEquals(GROUP, info.getGroup().getName());
        assertEquals(TOPIC, info.getTopic().getName());
        assertEquals("tagA", info.getExpression().getExpression());
        assertTrue(info.getOnline());
    }

    @Test
    public void describeSubscriptionPrefersClientSettingsTest() {
        stubClusterInfo(ADDRESS_A);
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumerConnection(CLIENT_ID, "192.168.1.5:43210", TOPIC)));
        when(grpcClientSettingsManager.getRawClientSettings(CLIENT_ID))
            .thenReturn(settings(ClientType.SIMPLE_CONSUMER, GROUP, TOPIC, "tagFromSettings"));

        SimpleObserver<DescribeSubscriptionResponse> observer = new SimpleObserver<>();
        service.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getClientSubscriptionInfoCount());
        assertEquals("tagFromSettings",
            observer.value.getClientSubscriptionInfo(0).getSubscriptionInfo().getExpression().getExpression());
    }

    // ------------------------------------------------------------------ 5. DeleteSubscription

    @Test
    public void deleteSubscriptionRejectsMissingTopicOrGroupTest() {
        SimpleObserver<DeleteSubscriptionResponse> observer = new SimpleObserver<>();
        service.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void deleteSubscriptionRemovesOnlyMatchingEntryTest() throws Exception {
        stubRoute(true, ADDRESS_A);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(GROUP);
        Set<SimpleSubscriptionData> dataSet = new HashSet<>();
        dataSet.add(new SimpleSubscriptionData(TOPIC, ExpressionType.TAG, "tagA", 0L));
        dataSet.add(new SimpleSubscriptionData(TOPIC, ExpressionType.TAG, "tagB", 0L));
        dataSet.add(new SimpleSubscriptionData("topicOther", ExpressionType.TAG, "*", 0L));
        config.setSubscriptionDataSet(dataSet);
        when(adminService.getSubscriptionGroupConfig(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(config));
        when(adminService.updateSubscriptionGroupConfig(eq(ADDRESS_A), any(SubscriptionGroupConfig.class), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        SimpleObserver<DeleteSubscriptionResponse> observer = new SimpleObserver<>();
        service.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setGroup(resource(GROUP))
            .setExpression(FilterExpression.newBuilder().setType(FilterType.TAG).setExpression("tagA").build())
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());

        ArgumentCaptor<SubscriptionGroupConfig> captor = ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(adminService).updateSubscriptionGroupConfig(eq(ADDRESS_A), captor.capture(), anyLong());
        Set<SimpleSubscriptionData> remaining = captor.getValue().getSubscriptionDataSet();
        assertEquals(2, remaining.size());
        assertTrue(remaining.contains(new SimpleSubscriptionData(TOPIC, ExpressionType.TAG, "tagB", 0L)));
        assertTrue(remaining.contains(new SimpleSubscriptionData("topicOther", ExpressionType.TAG, "*", 0L)));
        assertFalse(remaining.contains(new SimpleSubscriptionData(TOPIC, ExpressionType.TAG, "tagA", 0L)));
        // deleting one subscription must never delete the whole group
        verify(adminService, never()).deleteSubscriptionGroup(anyString(), anyString(), anyBoolean(), anyLong());
    }

    @Test
    public void deleteSubscriptionReturnsNotFoundWhenNothingMatchedTest() throws Exception {
        stubRoute(true, ADDRESS_A);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(GROUP);
        config.setSubscriptionDataSet(new HashSet<>(Collections.singletonList(
            new SimpleSubscriptionData("topicOther", ExpressionType.TAG, "*", 0L))));
        when(adminService.getSubscriptionGroupConfig(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(config));

        SimpleObserver<DeleteSubscriptionResponse> observer = new SimpleObserver<>();
        service.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setGroup(resource(GROUP))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.NOT_FOUND, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("no subscription of group"));
        verify(adminService, never()).updateSubscriptionGroupConfig(anyString(),
            any(SubscriptionGroupConfig.class), anyLong());
    }

    @Test
    public void deleteSubscriptionReturnsNotFoundWhenGroupUnknownTest() throws Exception {
        stubRoute(true, ADDRESS_A);
        when(adminService.getSubscriptionGroupConfig(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        SimpleObserver<DeleteSubscriptionResponse> observer = new SimpleObserver<>();
        service.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setGroup(resource(GROUP))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.NOT_FOUND, observer.value.getStatus().getCode());
    }

    @Test
    public void deleteSubscriptionPropagatesBrokerFailureTest() throws Exception {
        stubRoute(true, ADDRESS_A);
        when(adminService.getSubscriptionGroupConfig(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("broker down")));

        SimpleObserver<DeleteSubscriptionResponse> observer = new SimpleObserver<>();
        service.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setGroup(resource(GROUP))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("broker down"));
    }

    // ------------------------------------------------------------------ 6. DescribeGroupAccumulation

    @Test
    public void describeGroupAccumulationRejectsMissingGroupTest() {
        SimpleObserver<DescribeGroupAccumulationResponse> observer = new SimpleObserver<>();
        service.describeGroupAccumulation(DescribeGroupAccumulationRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void describeGroupAccumulationSplitsInflightReadyTest() {
        stubClusterInfo(ADDRESS_A);
        // the service asks the broker for the whole group (blank topic) and filters locally
        when(adminService.getConsumeStats(eq(ADDRESS_A), eq(GROUP), eq(""), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumeStats(TOPIC, 100L, 60L, 75L)));

        SimpleObserver<DescribeGroupAccumulationResponse> observer = new SimpleObserver<>();
        service.describeGroupAccumulation(DescribeGroupAccumulationRequest.newBuilder()
            .setGroup(resource(GROUP))
            .addTopics(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(40L, observer.value.getAccumulation().getAccumulation());
        assertEquals(15L, observer.value.getAccumulation().getInflightMessages());
        assertEquals(25L, observer.value.getAccumulation().getReadyMessages());
        assertTrue(observer.value.getTopicAccumulationMap().containsKey(TOPIC));
        assertEquals(40L, observer.value.getTopicAccumulationMap().get(TOPIC).getAccumulation());
    }

    @Test
    public void describeGroupAccumulationWholeGroupResolvesTopicsTest() {
        stubClusterInfo(ADDRESS_A);
        TopicList topicList = new TopicList();
        topicList.setTopicList(new HashSet<>(Collections.singletonList(TOPIC)));
        when(adminService.queryTopicsByConsumer(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(topicList));
        when(adminService.getConsumeStats(eq(ADDRESS_A), eq(GROUP), eq(""), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumeStats(TOPIC, 100L, 60L, 60L)));

        SimpleObserver<DescribeGroupAccumulationResponse> observer = new SimpleObserver<>();
        service.describeGroupAccumulation(DescribeGroupAccumulationRequest.newBuilder()
            .setGroup(resource(GROUP))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(40L, observer.value.getAccumulation().getAccumulation());
        verify(adminService).queryTopicsByConsumer(eq(ADDRESS_A), eq(GROUP), anyLong());
    }

    // ------------------------------------------------------------------ 7. ListConsumerConnection

    @Test
    public void listConsumerConnectionRejectsMissingGroupTest() {
        SimpleObserver<ListConsumerConnectionResponse> observer = new SimpleObserver<>();
        service.listConsumerConnection(ListConsumerConnectionRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void listConsumerConnectionReturnsBrokerSideClientsTest() {
        stubClusterInfo(ADDRESS_A);
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(
                consumerConnection(CLIENT_ID, "consumer-host@192.168.1.5:43210", TOPIC)));

        SimpleObserver<ListConsumerConnectionResponse> observer = new SimpleObserver<>();
        service.listConsumerConnection(ListConsumerConnectionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getClientInfoCount());
        assertEquals(CLIENT_ID, observer.value.getClientInfo(0).getClientId());
        // egress_ip is the bare IP, hostname the client-reported name from "hostname@ip:port"
        assertEquals("192.168.1.5", observer.value.getClientInfo(0).getEgressIp());
        assertEquals("consumer-host", observer.value.getClientInfo(0).getHostname());
        assertEquals("355", observer.value.getClientInfo(0).getVersion());
        assertEquals(apache.rocketmq.v2.MessageModel.CLUSTERING, observer.value.getClientInfo(0).getMessageModel());
    }

    @Test
    public void listConsumerConnectionOfflineGroupIsOkTest() {
        stubClusterInfo(ADDRESS_A);
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("consumer group not online")));

        SimpleObserver<ListConsumerConnectionResponse> observer = new SimpleObserver<>();
        service.listConsumerConnection(ListConsumerConnectionRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        // an offline group is a normal answer for a listing RPC, not a server error
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(0, observer.value.getClientInfoCount());
    }

    @Test
    public void listConsumerConnectionTopicFilterHidesNonSubscribersTest() {
        stubClusterInfo(ADDRESS_A);
        when(adminService.getConsumerConnectionList(eq(ADDRESS_A), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(consumerConnection(CLIENT_ID, "192.168.1.5:43210", TOPIC)));

        SimpleObserver<ListConsumerConnectionResponse> observer = new SimpleObserver<>();
        service.listConsumerConnection(ListConsumerConnectionRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setTopic(resource("topicOther"))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(0, observer.value.getClientInfoCount());
    }

    // ------------------------------------------------------------------ 8. ResetGroupOffset

    @Test
    public void resetGroupOffsetRejectsMissingTimestampTest() {
        SimpleObserver<ResetGroupOffsetResponse> observer = new SimpleObserver<>();
        service.resetGroupOffset(ResetGroupOffsetRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setTopic(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        // an unset Timestamp used to mean "replay everything since 1970"
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("reset_timestamp"));
        verify(adminService, never()).resetOffset(anyString(), anyString(), anyString(), anyLong(), anyBoolean(),
            anyLong());
    }

    @Test
    public void resetGroupOffsetRejectsZeroTimestampTest() {
        SimpleObserver<ResetGroupOffsetResponse> observer = new SimpleObserver<>();
        service.resetGroupOffset(ResetGroupOffsetRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setTopic(resource(TOPIC))
            .setResetTimestamp(Timestamp.newBuilder().setSeconds(0L).build())
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
        verify(adminService, never()).resetOffset(anyString(), anyString(), anyString(), anyLong(), anyBoolean(),
            anyLong());
    }

    @Test
    public void resetGroupOffsetReachesEveryBrokerTest() throws Exception {
        stubRoute(true, ADDRESS_A, ADDRESS_B);
        when(adminService.resetOffset(anyString(), eq(TOPIC), eq(GROUP), anyLong(), eq(true), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));

        SimpleObserver<ResetGroupOffsetResponse> observer = new SimpleObserver<>();
        service.resetGroupOffset(ResetGroupOffsetRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setTopic(resource(TOPIC))
            .setResetTimestamp(Timestamp.newBuilder().setSeconds(1000L).setNanos(500_000_000).build())
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        // seconds + nanos must be converted to one millisecond timestamp
        verify(adminService).resetOffset(eq(ADDRESS_A), eq(TOPIC), eq(GROUP), eq(1000500L), eq(true), anyLong());
        verify(adminService).resetOffset(eq(ADDRESS_B), eq(TOPIC), eq(GROUP), eq(1000500L), eq(true), anyLong());
    }

    @Test
    public void resetGroupOffsetPropagatesBrokerFailureTest() throws Exception {
        stubRoute(true, ADDRESS_A);
        when(adminService.resetOffset(anyString(), eq(TOPIC), eq(GROUP), anyLong(), eq(true), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("reset rejected")));

        SimpleObserver<ResetGroupOffsetResponse> observer = new SimpleObserver<>();
        service.resetGroupOffset(ResetGroupOffsetRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setTopic(resource(TOPIC))
            .setResetTimestamp(Timestamp.newBuilder().setSeconds(1000L).build())
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("reset rejected"));
    }

    // ------------------------------------------------------------------ 9. QueryMessage

    @Test
    public void queryMessageRejectsMissingTopicTest() {
        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder().setMessageKey("key").build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void queryMessageRejectsUnsupportedSearchKeyTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        SimpleObserver<ListMessageResponse> noKeyObserver = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder().setTopic(resource(TOPIC)).build(), noKeyObserver);
        assertNotNull(noKeyObserver.value);
        assertEquals(Code.BAD_REQUEST, noKeyObserver.value.getStatus().getCode());

        SimpleObserver<ListMessageResponse> subscriptionObserver = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setSubscription("tagA")
            .build(), subscriptionObserver);
        assertNotNull(subscriptionObserver.value);
        assertEquals(Code.BAD_REQUEST, subscriptionObserver.value.getStatus().getCode());
        assertTrue(subscriptionObserver.value.getStatus().getMessage().contains("not supported"));
    }

    @Test
    public void queryMessageByMessageIdUsesUniqueKeyTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(messageExt("UNIQ-1"))));

        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setMessageId("UNIQ-1")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getMessagesCount());
        assertEquals("UNIQ-1", observer.value.getMessages(0).getSystemProperties().getMessageId());
        assertEquals("payload", observer.value.getMessages(0).getBody().toString(StandardCharsets.UTF_8));

        // a v2 message_id is the client-generated unique key, so the lookup must use the
        // unique-key index (uniqueKey=true), never the offset-decoding path
        ArgumentCaptor<Boolean> uniqueKeyCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(adminService).queryMessage(eq(ADDRESS_A), eq(TOPIC), eq("UNIQ-1"), anyInt(), anyLong(), anyLong(),
            uniqueKeyCaptor.capture(), anyBoolean(), anyLong());
        assertTrue(uniqueKeyCaptor.getValue());
    }

    @Test
    public void queryMessageByKeyUsesKeyIndexTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(messageExt("MSG-1"))));

        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setMessageKey("key-1")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getMessagesCount());

        ArgumentCaptor<Boolean> uniqueKeyCaptor = ArgumentCaptor.forClass(Boolean.class);
        verify(adminService).queryMessage(eq(ADDRESS_A), eq(TOPIC), eq("key-1"), anyInt(), anyLong(), anyLong(),
            uniqueKeyCaptor.capture(), anyBoolean(), anyLong());
        assertFalse(uniqueKeyCaptor.getValue());
    }

    @Test
    public void queryMessageReturnsNotFoundWhenNoBrokerHasItTest() throws Exception {
        stubRoute(false, ADDRESS_A, ADDRESS_B);
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setMessageId("UNIQ-MISSING")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.MESSAGE_NOT_FOUND, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("no message found"));
        assertEquals(0, observer.value.getMessagesCount());
    }

    @Test
    public void queryMessageSurvivesPartialBrokerFailureTest() throws Exception {
        stubRoute(false, ADDRESS_A, ADDRESS_B);
        when(adminService.queryMessage(eq(ADDRESS_A), eq(TOPIC), eq("key-1"), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("broker-a down")));
        when(adminService.queryMessage(eq(ADDRESS_B), eq(TOPIC), eq("key-1"), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(messageExt("MSG-2"))));

        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setMessageKey("key-1")
            .build(), observer);

        assertNotNull(observer.value);
        // a partially available cluster still returns the data it has
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getMessagesCount());
    }

    @Test
    public void queryMessageRespectsMaxNumsTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Arrays.asList(messageExt("MSG-1"), messageExt("MSG-2"))));

        SimpleObserver<ListMessageResponse> observer = new SimpleObserver<>();
        service.queryMessage(ListMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setMessageKey("key-1")
            .setMaxMessageNums(1)
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getMessagesCount());
        verify(adminService).queryMessage(eq(ADDRESS_A), eq(TOPIC), eq("key-1"), eq(1), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong());
    }

    // ------------------------------------------------------------------ 10/13. client relay RPCs

    @Test
    public void printThreadStackTraceReturnsJstackTest() {
        stubLocalClient();
        ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
        runningInfo.setJstack("STACK-TRACE");
        answerRelayWith(RequestCode.GET_CONSUMER_RUNNING_INFO,
            new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", runningInfo));

        SimpleObserver<PrintThreadStackTraceResponse> observer = new SimpleObserver<>();
        service.printThreadStackTrace(PrintThreadStackTraceRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals("STACK-TRACE", observer.value.getThreadStackTrace());

        ArgumentCaptor<ProxyRelayRequest> captor = ArgumentCaptor.forClass(ProxyRelayRequest.class);
        verify(channel).writeAndFlush(captor.capture());
        GetConsumerRunningInfoRequestHeader header =
            (GetConsumerRunningInfoRequestHeader) captor.getValue().readCustomHeader();
        assertTrue(header.isJstackEnable());
        assertEquals(GROUP, header.getConsumerGroup());
        assertEquals(CLIENT_ID, header.getClientId());
    }

    @Test
    public void printThreadStackTraceEmptyJstackIsNotFoundTest() {
        stubLocalClient();
        answerRelayWith(RequestCode.GET_CONSUMER_RUNNING_INFO,
            new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", new ConsumerRunningInfo()));

        SimpleObserver<PrintThreadStackTraceResponse> observer = new SimpleObserver<>();
        service.printThreadStackTrace(PrintThreadStackTraceRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.NOT_FOUND, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("did not return a thread stack"));
    }

    @Test
    public void printThreadStackTraceMissingClientIsNotFoundTest() {
        SimpleObserver<PrintThreadStackTraceResponse> observer = new SimpleObserver<>();
        service.printThreadStackTrace(PrintThreadStackTraceRequest.newBuilder()
            .setClientId("missing-client")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.NOT_FOUND, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("not connected"));
    }

    @Test
    public void printThreadStackTraceForwardsToOwningProxyTest() {
        when(forwarder.forwardIfRemote(eq(GROUP), eq(CLIENT_ID), any(), any())).thenReturn(true);

        SimpleObserver<PrintThreadStackTraceResponse> observer = new SimpleObserver<>();
        service.printThreadStackTrace(PrintThreadStackTraceRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .build(), observer);

        // ownership of the observer moved to the peer proxy: the local service must stay silent
        assertNull(observer.value);
        assertNull(observer.error);
        assertFalse(observer.completed);
    }

    @Test
    public void getConsumerRunningInfoRejectsMissingClientIdTest() {
        SimpleObserver<GetConsumerRunningInfoResponse> observer = new SimpleObserver<>();
        service.getConsumerRunningInfo(GetConsumerRunningInfoRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void getConsumerRunningInfoReturnsFullInfoTest() {
        stubLocalClient();
        ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
        Properties properties = runningInfo.getProperties();
        properties.setProperty(ConsumerRunningInfo.PROP_CLIENT_VERSION, "V5_0_0");
        runningInfo.getSubscriptionSet().add(subscriptionData(TOPIC, ExpressionType.TAG, "tagA"));
        answerRelayWith(RequestCode.GET_CONSUMER_RUNNING_INFO,
            new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", runningInfo));

        SimpleObserver<GetConsumerRunningInfoResponse> observer = new SimpleObserver<>();
        service.getConsumerRunningInfo(GetConsumerRunningInfoRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals("V5_0_0", observer.value.getConsumerRunningInfo()
            .getPropertiesMap().get(ConsumerRunningInfo.PROP_CLIENT_VERSION));
        assertTrue(observer.value.getConsumerRunningInfo().getSubscriptionsMap().containsKey(TOPIC));

        ArgumentCaptor<ProxyRelayRequest> captor = ArgumentCaptor.forClass(ProxyRelayRequest.class);
        verify(channel).writeAndFlush(captor.capture());
        GetConsumerRunningInfoRequestHeader header =
            (GetConsumerRunningInfoRequestHeader) captor.getValue().readCustomHeader();
        assertFalse(header.isJstackEnable());
    }

    @Test
    public void getConsumerRunningInfoFallsBackToSubscriptionsForGrpcClientTest() {
        stubLocalClient();
        // the GrpcClientChannel jstack guard completes the relay future with this result for a
        // gRPC v2 client that cannot answer a running-info request without a thread dump
        answerRelayWith(RequestCode.GET_CONSUMER_RUNNING_INFO,
            new ProxyRelayResult<>(ResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                "gRPC v2 protocol cannot report consumer running info without jstack, "
                    + "retry with jstackEnable=true", null));
        when(grpcClientSettingsManager.getRawClientSettings(CLIENT_ID))
            .thenReturn(settings(ClientType.SIMPLE_CONSUMER, GROUP, TOPIC, "tagA"));

        SimpleObserver<GetConsumerRunningInfoResponse> observer = new SimpleObserver<>();
        service.getConsumerRunningInfo(GetConsumerRunningInfoRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("only subscriptions are available"));
        assertEquals("tagA", observer.value.getConsumerRunningInfo()
            .getSubscriptionsMap().get(TOPIC).getExpression());
    }

    @Test
    public void getConsumerRunningInfoErrorsWithoutSettingsOrChannelTest() {
        SimpleObserver<GetConsumerRunningInfoResponse> observer = new SimpleObserver<>();
        service.getConsumerRunningInfo(GetConsumerRunningInfoRequest.newBuilder()
            .setClientId("missing-client")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("not connected"));
    }

    // ------------------------------------------------------------------ 11. VerifyMessage

    @Test
    public void verifyMessageRejectsMissingFieldsTest() {
        SimpleObserver<VerifyMessageResponse> observer = new SimpleObserver<>();
        service.verifyMessage(VerifyMessageRequest.newBuilder()
            .setClientId(CLIENT_ID)
            .setTopic(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void verifyMessageSuccessTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        stubLocalClient();
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(messageExt("UNIQ-9"))));
        ConsumeMessageDirectlyResult directlyResult = new ConsumeMessageDirectlyResult();
        directlyResult.setConsumeResult(CMResult.CR_SUCCESS);
        answerRelayWith(RequestCode.CONSUME_MESSAGE_DIRECTLY,
            new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", directlyResult));

        SimpleObserver<VerifyMessageResponse> observer = new SimpleObserver<>();
        service.verifyMessage(VerifyMessageRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .setTopic(resource(TOPIC))
            .setMessageId("UNIQ-9")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());

        // the real message is fetched through the unique-key index and relayed to the client
        verify(adminService).queryMessage(eq(ADDRESS_A), eq(TOPIC), eq("UNIQ-9"), eq(1), eq(0L), eq(Long.MAX_VALUE),
            eq(true), eq(false), anyLong());
        ArgumentCaptor<ProxyRelayRequest> captor = ArgumentCaptor.forClass(ProxyRelayRequest.class);
        verify(channel).writeAndFlush(captor.capture());
        ConsumeMessageDirectlyResultRequestHeader header =
            (ConsumeMessageDirectlyResultRequestHeader) captor.getValue().readCustomHeader();
        assertEquals(GROUP, header.getConsumerGroup());
        assertEquals(CLIENT_ID, header.getClientId());
        assertEquals(TOPIC, header.getTopic());
        assertEquals("UNIQ-9", header.getMsgId());
        assertNotNull(captor.getValue().getBody());
    }

    @Test
    public void verifyMessageReportsCorruptedOnFailureTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        stubLocalClient();
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(messageExt("UNIQ-9"))));
        ConsumeMessageDirectlyResult directlyResult = new ConsumeMessageDirectlyResult();
        directlyResult.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
        directlyResult.setRemark("consume crashed");
        answerRelayWith(RequestCode.CONSUME_MESSAGE_DIRECTLY,
            new ProxyRelayResult<>(ResponseCode.SUCCESS, "ok", directlyResult));

        SimpleObserver<VerifyMessageResponse> observer = new SimpleObserver<>();
        service.verifyMessage(VerifyMessageRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .setTopic(resource(TOPIC))
            .setMessageId("UNIQ-9")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.MESSAGE_CORRUPTED, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("failed to consume message UNIQ-9"));
        assertTrue(observer.value.getStatus().getMessage().contains("consume crashed"));
    }

    @Test
    public void verifyMessageFailsWhenMessageNotFoundTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.queryMessage(anyString(), anyString(), anyString(), anyInt(), anyLong(), anyLong(),
            anyBoolean(), anyBoolean(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        SimpleObserver<VerifyMessageResponse> observer = new SimpleObserver<>();
        service.verifyMessage(VerifyMessageRequest.newBuilder()
            .setGroup(resource(GROUP))
            .setClientId(CLIENT_ID)
            .setTopic(resource(TOPIC))
            .setMessageId("UNIQ-MISSING")
            .build(), observer);

        assertNotNull(observer.value);
        // a missing message is a normal answer, so it must not be reported as a proxy fault;
        // same grading queryMessage uses
        assertEquals(Code.MESSAGE_NOT_FOUND, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("not found"));
    }

    // ------------------------------------------------------------------ 12. AdminSendMessage

    @Test
    public void adminSendMessageReturnsIdTest() {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("SENT-1");
        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        SimpleObserver<AdminSendMessageResponse> observer = new SimpleObserver<>();
        service.adminSendMessage(AdminSendMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setBody(ByteString.copyFromUtf8("hello"))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals("SENT-1", observer.value.getMessageId());
    }

    @Test
    public void adminSendMessageCarriesTagsKeysAndPropertiesTest() {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("SENT-2");
        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList(), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(sendResult)));

        SimpleObserver<AdminSendMessageResponse> observer = new SimpleObserver<>();
        service.adminSendMessage(AdminSendMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setBody(ByteString.copyFromUtf8("hello"))
            .setTag("tagA")
            .setKey("key1")
            .putUserProperties("traceId", "abc")
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals("SENT-2", observer.value.getMessageId());

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<List> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(messagingProcessor).sendMessage(any(), any(), anyString(), anyInt(), messagesCaptor.capture(),
            anyLong());
        Message message = (Message) messagesCaptor.getValue().get(0);
        assertEquals(TOPIC, message.getTopic());
        assertEquals("tagA", message.getTags());
        assertEquals("key1", message.getKeys());
        assertEquals("abc", message.getUserProperty("traceId"));
    }

    @Test
    public void adminSendMessageRejectsPastDeliveryTimestampTest() {
        SimpleObserver<AdminSendMessageResponse> observer = new SimpleObserver<>();
        service.adminSendMessage(AdminSendMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setBody(ByteString.copyFromUtf8("hello"))
            .setSystemProperties(SystemProperties.newBuilder()
                .setDeliveryTimestamp(Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000L - 100L).build())
                .build())
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.ILLEGAL_DELIVERY_TIME, observer.value.getStatus().getCode());
    }

    @Test
    public void adminSendMessageFailsGracefullyWhenSendFailsTest() {
        when(messagingProcessor.sendMessage(any(), any(), anyString(), anyInt(), anyList(), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("send failed")));

        SimpleObserver<AdminSendMessageResponse> observer = new SimpleObserver<>();
        service.adminSendMessage(AdminSendMessageRequest.newBuilder()
            .setTopic(resource(TOPIC))
            .setBody(ByteString.copyFromUtf8("hello"))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("send failed"));
    }

    // ------------------------------------------------------------------ 14. GetTopicRoute

    @Test
    public void getTopicRouteReturnsJsonTest() {
        TopicRouteData routeData = new TopicRouteData();
        routeData.setOrderTopicConf("orderConf");
        when(adminService.getTopicRouteData(TOPIC)).thenReturn(CompletableFuture.completedFuture(routeData));

        SimpleObserver<GetTopicRouteResponse> observer = new SimpleObserver<>();
        service.getTopicRoute(GetTopicRouteRequest.newBuilder().setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(JSON.toJSONString(routeData), observer.value.getTopicRouteData());
    }

    @Test
    public void getTopicRouteFailsWhenAdminServiceFailsTest() {
        when(adminService.getTopicRouteData(TOPIC))
            .thenReturn(failedFuture(new RuntimeException("namesrv unreachable")));

        SimpleObserver<GetTopicRouteResponse> observer = new SimpleObserver<>();
        service.getTopicRoute(GetTopicRouteRequest.newBuilder().setTopic(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("namesrv unreachable"));
    }

    @Test
    public void getTopicRouteRejectsMissingTopicTest() {
        SimpleObserver<GetTopicRouteResponse> observer = new SimpleObserver<>();
        service.getTopicRoute(GetTopicRouteRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    // ------------------------------------------------------------------ 15. QueryTimeSpan

    @Test
    public void queryTimeSpanRejectsMissingGroupTest() {
        SimpleObserver<QueryTimeSpanResponse> observer = new SimpleObserver<>();
        service.queryTimeSpan(QueryTimeSpanRequest.newBuilder()
            .addTopics(resource(TOPIC)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void queryTimeSpanRejectsMissingTopicsTest() {
        SimpleObserver<QueryTimeSpanResponse> observer = new SimpleObserver<>();
        service.queryTimeSpan(QueryTimeSpanRequest.newBuilder()
            .setGroup(resource(GROUP)).build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.BAD_REQUEST, observer.value.getStatus().getCode());
    }

    @Test
    public void queryTimeSpanReturnsBrokerComputedSpansTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        QueueTimeSpan span = new QueueTimeSpan();
        span.setMessageQueue(new MessageQueue(TOPIC, "broker-0", 0));
        span.setMinTimeStamp(100L);
        span.setMaxTimeStamp(200L);
        span.setConsumeTimeStamp(150L);
        span.setDelayTime(42L);
        when(adminService.queryConsumeTimeSpan(eq(ADDRESS_A), eq(TOPIC), eq(GROUP), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(span)));

        SimpleObserver<QueryTimeSpanResponse> observer = new SimpleObserver<>();
        service.queryTimeSpan(QueryTimeSpanRequest.newBuilder()
            .setGroup(resource(GROUP))
            .addTopics(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1, observer.value.getQueueTimeSpanListCount());
        QueryTimeSpanResponse.QueueTimeSpan v2Span = observer.value.getQueueTimeSpanList(0);
        assertEquals("broker-0", v2Span.getMessageQueue().getBroker().getName());
        assertEquals(100L, v2Span.getMinTimestamp());
        assertEquals(200L, v2Span.getMaxTimestamp());
        assertEquals(150L, v2Span.getConsumeTimestamp());
        assertEquals(42L, v2Span.getDelayTimeMs());
    }

    @Test
    public void queryTimeSpanPropagatesFailureWhenAllBrokersFailTest() throws Exception {
        stubRoute(false, ADDRESS_A);
        when(adminService.queryConsumeTimeSpan(eq(ADDRESS_A), eq(TOPIC), eq(GROUP), anyLong()))
            .thenReturn(failedFuture(new RuntimeException("span query failed")));

        SimpleObserver<QueryTimeSpanResponse> observer = new SimpleObserver<>();
        service.queryTimeSpan(QueryTimeSpanRequest.newBuilder()
            .setGroup(resource(GROUP))
            .addTopics(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.INTERNAL_SERVER_ERROR, observer.value.getStatus().getCode());
        assertTrue(observer.value.getStatus().getMessage().contains("span query failed"));
    }

    // ------------------------------------------------------------------ 16. GetProxyRuntimeStats

    @Test
    public void getProxyRuntimeStatsCountsLitePushConsumerAsConsumerTest() {
        GrpcClientChannel consumerChannel = mock(GrpcClientChannel.class);
        when(consumerChannel.getClientId()).thenReturn(CLIENT_ID);
        GrpcClientChannel producerChannel = mock(GrpcClientChannel.class);
        when(producerChannel.getClientId()).thenReturn("producer-1");
        GrpcClientChannel litePushChannel = mock(GrpcClientChannel.class);
        when(litePushChannel.getClientId()).thenReturn("lite-push-1");
        when(grpcChannelManager.getClientChannels())
            .thenReturn(Arrays.asList(consumerChannel, producerChannel, litePushChannel));
        when(grpcClientSettingsManager.getRawClientSettings(CLIENT_ID))
            .thenReturn(settings(ClientType.SIMPLE_CONSUMER, GROUP, TOPIC, "*"));
        when(grpcClientSettingsManager.getRawClientSettings("producer-1"))
            .thenReturn(settings(ClientType.PRODUCER, GROUP, TOPIC, "*"));
        when(grpcClientSettingsManager.getRawClientSettings("lite-push-1"))
            .thenReturn(settings(ClientType.LITE_PUSH_CONSUMER, GROUP, TOPIC, "*"));

        SimpleObserver<GetProxyRuntimeStatsResponse> observer = new SimpleObserver<>();
        service.getProxyRuntimeStats(GetProxyRuntimeStatsRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        // LITE_PUSH_CONSUMER is a consumer, not a producer
        assertEquals(2L, observer.value.getConsumers());
        assertEquals(1L, observer.value.getProducers());
        assertEquals(3L, observer.value.getConnections());
        // the version comes from MQVersion, not from a hardcoded string
        assertEquals(MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), observer.value.getVersion());
        assertEquals(ConfigurationManager.getProxyConfig().getProxyName(), observer.value.getProxyName());
    }

    @Test
    public void getProxyRuntimeStatsSkipsChannelsWithoutSettingsTest() {
        GrpcClientChannel unknownChannel = mock(GrpcClientChannel.class);
        when(unknownChannel.getClientId()).thenReturn("unknown");
        when(grpcChannelManager.getClientChannels()).thenReturn(Collections.singletonList(unknownChannel));

        SimpleObserver<GetProxyRuntimeStatsResponse> observer = new SimpleObserver<>();
        service.getProxyRuntimeStats(GetProxyRuntimeStatsRequest.newBuilder().build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        assertEquals(1L, observer.value.getConnections());
        assertEquals(0L, observer.value.getProducers());
        assertEquals(0L, observer.value.getConsumers());
    }

    // ------------------------------------------------------------------ cross-RPC

    @Test
    public void describeGroupAccumulationKeepsRetryBacklogInTotalTest() {
        stubClusterInfo(ADDRESS_A);
        ConsumeStats stats = consumeStats(TOPIC, 100L, 60L, 60L);
        OffsetWrapper retryWrapper = new OffsetWrapper();
        retryWrapper.setBrokerOffset(30L);
        retryWrapper.setConsumerOffset(10L);
        retryWrapper.setPullOffset(10L);
        stats.getOffsetTable().put(new MessageQueue(MixAll.getRetryTopic(GROUP), "broker-0", 0), retryWrapper);
        when(adminService.getConsumeStats(eq(ADDRESS_A), eq(GROUP), eq(""), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(stats));

        SimpleObserver<DescribeGroupAccumulationResponse> observer = new SimpleObserver<>();
        service.describeGroupAccumulation(DescribeGroupAccumulationRequest.newBuilder()
            .setGroup(resource(GROUP))
            .addTopics(resource(TOPIC))
            .build(), observer);

        assertNotNull(observer.value);
        assertEquals(Code.OK, observer.value.getStatus().getCode());
        // 40 from the topic + 20 from the group's retry topic
        assertEquals(60L, observer.value.getAccumulation().getAccumulation());
        assertEquals(40L, observer.value.getTopicAccumulationMap().get(TOPIC).getAccumulation());
        assertFalse(observer.value.getTopicAccumulationMap().containsKey(MixAll.getRetryTopic(GROUP)));
    }
}
