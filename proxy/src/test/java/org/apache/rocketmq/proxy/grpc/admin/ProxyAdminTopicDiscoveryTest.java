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

import apache.rocketmq.v2.AdminGrpc;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeSubscriptionRequest;
import apache.rocketmq.v2.DescribeSubscriptionResponse;
import apache.rocketmq.v2.ListSubscriptionRequest;
import apache.rocketmq.v2.ListSubscriptionResponse;
import apache.rocketmq.v2.Resource;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.body.GroupList;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyAdminTopicDiscoveryTest extends InitConfigTest {
    private ProxyAdminGrpcService service;
    private Server server;
    private ManagedChannel channel;
    private AdminGrpc.AdminBlockingStub stub;
    private ConsumerManager proxyConsumers;
    private ServiceManager services;
    private AdminService admin;

    @Before
    public void setUp() throws Exception {
        proxyConsumers = new ConsumerManager(mock(ConsumerIdsChangeListener.class), 120000L);
        GrpcClientChannel client = mock(GrpcClientChannel.class);
        when(client.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 40000));
        SubscriptionData subscription = new SubscriptionData();
        subscription.setTopic("new-topic");
        subscription.setSubString("*");
        subscription.setExpressionType("TAG");
        proxyConsumers.registerConsumer("new-group",
            new ClientChannelInfo(client, "new-grpc-client", LanguageCode.JAVA, MQVersion.CURRENT_VERSION),
            ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, Collections.singleton(subscription), false);
        assertEquals(Collections.singleton("new-group"), proxyConsumers.queryTopicConsumeByWho("new-topic"));
        services = mock(ServiceManager.class);
        admin = mock(AdminService.class);
        when(services.getAdminService()).thenReturn(admin);
        when(services.getConsumerManager()).thenReturn(proxyConsumers);
        ClusterInfo cluster = new ClusterInfo();
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(0L, "127.0.0.1:10911");
        HashMap<String, BrokerData> brokers = new HashMap<>();
        brokers.put("broker-a", new BrokerData("DefaultCluster", "broker-a", addresses));
        cluster.setBrokerAddrTable(brokers);
        when(admin.getBrokerClusterInfo(anyLong())).thenReturn(CompletableFuture.completedFuture(cluster));
        // A newly connected gRPC client has neither a broker registration nor committed offsets.
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("new-topic"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new GroupList()));
        when(admin.getConsumerConnectionList(eq("127.0.0.1:10911"), eq("new-group"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new ConsumerConnection()));
        service = new ProxyAdminGrpcService(services, mock(MessagingProcessor.class), mock(GrpcChannelManager.class),
            mock(GrpcClientSettingsManager.class), mock(ProxyAdminForwarder.class));
        server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).addService(service).build().start();
        channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        stub = AdminGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    public void listByTopicIncludesProxyOnlyConsumer() {
        ListSubscriptionResponse byGroup = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("new-group")).build());
        ListSubscriptionResponse byTopic = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        assertEquals(Code.OK, byGroup.getStatus().getCode());
        assertEquals(1, byGroup.getSubscriptionInfoCount());
        assertEquals(Code.OK, byTopic.getStatus().getCode());
        assertEquals(1, byTopic.getSubscriptionInfoCount());
        assertEquals("new-group", byTopic.getSubscriptionInfo(0).getGroup().getName());
        assertTrue(byTopic.getSubscriptionInfo(0).getOnline());
    }

    @Test
    public void describeByTopicIncludesProxyOnlyConsumer() {
        DescribeSubscriptionResponse byGroup = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("new-group")).build());
        DescribeSubscriptionResponse byTopic = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        assertEquals(Code.OK, byGroup.getStatus().getCode());
        assertEquals(1, byGroup.getClientSubscriptionInfoCount());
        assertEquals(Code.OK, byTopic.getStatus().getCode());
        assertEquals(1, byTopic.getClientSubscriptionInfoCount());
        assertEquals("new-grpc-client", byTopic.getClientSubscriptionInfo(0).getClientInfo().getClientId());
        assertTrue(byTopic.getClientSubscriptionInfo(0).getSubscriptionInfo().getOnline());
    }

    @Test
    public void topicQueriesIncludePeerSyncedConsumer() {
        ConsumerManager peerView = new ConsumerManager(mock(ConsumerIdsChangeListener.class), 120000L);
        RemoteChannel remote = new RemoteChannel("192.168.1.2", "192.168.1.3:40000", "192.168.1.2:8081",
            ChannelProtocolType.GRPC_V2, null);
        SubscriptionData subscription = proxyConsumers.findSubscriptionData("new-group", "new-topic");
        peerView.registerConsumer("new-group",
            new ClientChannelInfo(remote, "peer-client", LanguageCode.JAVA, MQVersion.CURRENT_VERSION),
            ConsumeType.CONSUME_ACTIVELY, MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, Collections.singleton(subscription), false);
        when(services.getConsumerManager()).thenReturn(peerView);

        assertTopicCounts(1, 1);
        DescribeSubscriptionResponse response = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        assertEquals("peer-client", response.getClientSubscriptionInfo(0).getClientInfo().getClientId());
    }

    @Test
    public void topicQueriesDeduplicateGroupsFromProxyAndBroker() {
        GroupList groups = new GroupList();
        groups.getGroupList().add("new-group");
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("new-topic"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(groups));
        when(admin.getConsumerConnectionList(eq("127.0.0.1:10911"), eq("new-group"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(brokerConnection("new-grpc-client")));

        assertTopicCounts(1, 1);
        verify(admin, times(2)).getConsumerConnectionList(eq("127.0.0.1:10911"), eq("new-group"), anyLong());
    }

    @Test
    public void topicQueriesMergeDistinctBrokerOnlyGroup() {
        GroupList groups = new GroupList();
        groups.getGroupList().add("broker-group");
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("new-topic"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(groups));
        when(admin.getConsumerConnectionList(eq("127.0.0.1:10911"), eq("broker-group"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(brokerConnection("broker-client")));

        assertTopicCounts(2, 2);
    }

    @Test
    public void topicQueriesDoNotIncludeUnrelatedProxySubscriptions() {
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("other-topic"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(new GroupList()));
        ListSubscriptionResponse list = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("other-topic")).build());
        DescribeSubscriptionResponse describe = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("other-topic")).build());
        assertEquals(Code.OK, list.getStatus().getCode());
        assertEquals(Code.OK, describe.getStatus().getCode());
        assertEquals(0, list.getSubscriptionInfoCount());
        assertEquals(0, describe.getClientSubscriptionInfoCount());
        verify(admin, never()).getConsumerConnectionList(anyString(), anyString(), anyLong());
    }

    @Test
    public void groupAndTopicQueriesKeepTopicFilteringWithoutDiscovery() {
        Resource group = Resource.newBuilder().setName("new-group").build();
        Resource topic = Resource.newBuilder().setName("other-topic").build();
        ListSubscriptionResponse list = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setGroup(group).setTopic(topic).build());
        DescribeSubscriptionResponse describe = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setGroup(group).setTopic(topic).build());
        assertEquals(Code.OK, list.getStatus().getCode());
        assertEquals(Code.OK, describe.getStatus().getCode());
        assertEquals(0, list.getSubscriptionInfoCount());
        assertEquals(0, describe.getClientSubscriptionInfoCount());
        verify(admin, never()).queryTopicConsumeByWho(anyString(), anyString(), anyLong());
    }

    @Test
    public void topicQueriesKeepBrokerOnlyBehaviorWithoutConsumerManager() {
        when(services.getConsumerManager()).thenReturn(null);
        GroupList groups = new GroupList();
        groups.getGroupList().add("new-group");
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("new-topic"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(groups));
        when(admin.getConsumerConnectionList(eq("127.0.0.1:10911"), eq("new-group"), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(brokerConnection("broker-client")));

        assertTopicCounts(1, 1);
    }

    @Test
    public void topicQueriesPreserveBrokerDiscoveryErrors() {
        CompletableFuture<GroupList> failure = new CompletableFuture<>();
        failure.completeExceptionally(new IllegalStateException("broker discovery failed"));
        when(admin.queryTopicConsumeByWho(eq("127.0.0.1:10911"), eq("new-topic"), anyLong()))
            .thenReturn(failure);
        ListSubscriptionResponse list = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        DescribeSubscriptionResponse describe = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        assertNotEquals(Code.OK, list.getStatus().getCode());
        assertNotEquals(Code.OK, describe.getStatus().getCode());
        assertTrue(list.getStatus().getMessage().contains("broker discovery failed"));
        assertTrue(describe.getStatus().getMessage().contains("broker discovery failed"));
        verify(admin, never()).getConsumerConnectionList(anyString(), anyString(), anyLong());
    }

    private ConsumerConnection brokerConnection(String clientId) {
        ConsumerConnection connection = new ConsumerConnection();
        connection.setConsumeType(ConsumeType.CONSUME_ACTIVELY);
        connection.setMessageModel(MessageModel.CLUSTERING);
        Connection client = new Connection();
        client.setClientId(clientId);
        client.setClientAddr("127.0.0.1:50000");
        client.setLanguage(LanguageCode.JAVA);
        client.setVersion(MQVersion.CURRENT_VERSION);
        connection.getConnectionSet().add(client);
        connection.getSubscriptionTable().put("new-topic", proxyConsumers.findSubscriptionData("new-group", "new-topic"));
        return connection;
    }

    private void assertTopicCounts(int subscriptions, int clients) {
        ListSubscriptionResponse list = stub.listSubscription(ListSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        DescribeSubscriptionResponse describe = stub.describeSubscription(DescribeSubscriptionRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("new-topic")).build());
        assertEquals(Code.OK, list.getStatus().getCode());
        assertEquals(Code.OK, describe.getStatus().getCode());
        assertEquals(subscriptions, list.getSubscriptionInfoCount());
        assertEquals(clients, describe.getClientSubscriptionInfoCount());
    }
}
