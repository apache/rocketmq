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

import apache.rocketmq.v2.ClientFilter;
import apache.rocketmq.v2.ClientInstance;
import apache.rocketmq.v2.ClientProtocol;
import apache.rocketmq.v2.ClientRole;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.UA;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminServiceGrpcServiceTest {

    @Mock
    private ServiceManager serviceManager;
    @Mock
    private DefaultMessagingProcessor messagingProcessor;
    @Mock
    private GrpcChannelManager grpcChannelManager;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;

    private ProxyAdminServiceGrpcService service;

    @Before
    public void setUp() {
        service = new ProxyAdminServiceGrpcService(serviceManager, messagingProcessor, grpcChannelManager,
            grpcClientSettingsManager, new ProxyAdminPeerClient(), new RouteChangeNotifier());
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

    private static Settings subscriptionSettings(ClientType clientType, String group, String topic, String expression) {
        return Settings.newBuilder()
            .setClientType(clientType)
            .setUserAgent(UA.newBuilder().setVersion("5.0.0").setLanguage(Language.JAVA).setHostname("host").build())
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

    private static Settings publishingSettings(ClientType clientType, String topic) {
        return Settings.newBuilder()
            .setClientType(clientType)
            .setUserAgent(UA.newBuilder().setVersion("5.0.0").setLanguage(Language.GOLANG).setHostname("host").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(topic).build())
                .build())
            .build();
    }

    private GrpcClientChannel mockChannel(String clientId) {
        GrpcClientChannel channel = mock(GrpcClientChannel.class);
        when(channel.getClientId()).thenReturn(clientId);
        when(channel.getRemoteAddress()).thenReturn("1.2.3.4:8888");
        when(channel.getLocalAddress()).thenReturn("9.9.9.9:8081");
        when(channel.getConnectTimeMillis()).thenReturn(1000L);
        when(channel.getLastActiveTimeMillis()).thenReturn(2000L);
        when(channel.getRecentHeartbeats()).thenReturn(new ArrayList<>());
        return channel;
    }

    private void stubClients(Map<String, Settings> clients) {
        List<GrpcClientChannel> channels = new ArrayList<>();
        for (Map.Entry<String, Settings> entry : clients.entrySet()) {
            GrpcClientChannel channel = mockChannel(entry.getKey());
            channels.add(channel);
            when(grpcChannelManager.getChannel(entry.getKey())).thenReturn(channel);
            when(grpcClientSettingsManager.getRawClientSettings(entry.getKey())).thenReturn(entry.getValue());
        }
        when(grpcChannelManager.getClientChannels()).thenReturn(channels);
    }

    private static ClientInstance find(List<ClientInstance> list, String clientId) {
        for (ClientInstance ci : list) {
            if (ci.getClientId().equals(clientId)) {
                return ci;
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ tests

    @Test
    public void listClientsReturnsConnectedClientsWithRoles() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("consumer-1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g1", "t1", "*"));
        clients.put("producer-1", publishingSettings(ClientType.PRODUCER, "t2"));
        clients.put("push-1", subscriptionSettings(ClientType.PUSH_CONSUMER, "g2", "t3", "*"));
        stubClients(clients);

        SimpleObserver<ListClientsResponse> obs = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder().build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(3, obs.value.getClientsCount());
        assertFalse(obs.value.getProxyEndpoint().isEmpty());
        assertTrue(obs.value.getEpoch() > 0);

        ClientInstance consumer = find(obs.value.getClientsList(), "consumer-1");
        assertNotNull(consumer);
        assertEquals(ClientRole.CLIENT_ROLE_SIMPLE_CONSUMER, consumer.getRole());
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_GRPC, consumer.getProtocol());
        assertEquals(Language.JAVA, consumer.getLanguage());
        assertEquals("5.0.0", consumer.getClientVersion());
        assertTrue(consumer.getGroupsList().contains("g1"));
        assertTrue(consumer.getTopicsList().contains("t1"));

        ClientInstance producer = find(obs.value.getClientsList(), "producer-1");
        assertNotNull(producer);
        assertEquals(ClientRole.CLIENT_ROLE_PRODUCER, producer.getRole());
        assertTrue(producer.getGroupsList().isEmpty());
        assertTrue(producer.getTopicsList().contains("t2"));

        ClientInstance push = find(obs.value.getClientsList(), "push-1");
        assertNotNull(push);
        assertEquals(ClientRole.CLIENT_ROLE_PUSH_CONSUMER, push.getRole());
    }

    @Test
    public void listClientsHonoursGroupFilter() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("consumer-1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g1", "t1", "*"));
        clients.put("producer-1", publishingSettings(ClientType.PRODUCER, "t2"));
        stubClients(clients);

        SimpleObserver<ListClientsResponse> obs = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder()
            .setFilter(ClientFilter.newBuilder().setGroup(Resource.newBuilder().setName("g1")))
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(1, obs.value.getClientsCount());
        assertEquals("consumer-1", obs.value.getClientsList().get(0).getClientId());
    }

    @Test
    public void listClientsByGroupReturnsMatchingClients() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("consumer-1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g1", "t1", "*"));
        clients.put("consumer-2", subscriptionSettings(ClientType.PUSH_CONSUMER, "g2", "t3", "*"));
        stubClients(clients);

        SimpleObserver<ListClientsByGroupResponse> obs = new SimpleObserver<>();
        service.listClientsByGroup(ListClientsByGroupRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("g1"))
            .build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertEquals(1, obs.value.getClientsCount());
        assertEquals("consumer-1", obs.value.getClientsList().get(0).getClientId());

        SimpleObserver<ListClientsByGroupResponse> none = new SimpleObserver<>();
        service.listClientsByGroup(ListClientsByGroupRequest.newBuilder()
            .setGroup(Resource.newBuilder().setName("absent"))
            .build(), none);
        assertEquals(0, none.value.getClientsCount());
    }

    @Test
    public void listClientsByTopicReturnsMatchingClients() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("consumer-1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g1", "t1", "*"));
        clients.put("producer-1", publishingSettings(ClientType.PRODUCER, "t2"));
        stubClients(clients);

        SimpleObserver<ListClientsByTopicResponse> bySub = new SimpleObserver<>();
        service.listClientsByTopic(ListClientsByTopicRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t1"))
            .build(), bySub);
        assertEquals(1, bySub.value.getClientsCount());
        assertEquals("consumer-1", bySub.value.getClientsList().get(0).getClientId());

        SimpleObserver<ListClientsByTopicResponse> byPub = new SimpleObserver<>();
        service.listClientsByTopic(ListClientsByTopicRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("t2"))
            .build(), byPub);
        assertEquals(1, byPub.value.getClientsCount());
        assertEquals("producer-1", byPub.value.getClientsList().get(0).getClientId());

        SimpleObserver<ListClientsByTopicResponse> missing = new SimpleObserver<>();
        service.listClientsByTopic(ListClientsByTopicRequest.newBuilder()
            .setTopic(Resource.newBuilder().setName("absent"))
            .build(), missing);
        assertEquals(0, missing.value.getClientsCount());
    }

    @Test
    public void describeClientReturnsDetailForConnectedClient() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("consumer-1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g1", "t1", "*"));
        stubClients(clients);

        SimpleObserver<DescribeClientResponse> obs = new SimpleObserver<>();
        service.describeClient(DescribeClientRequest.newBuilder().setClientId("consumer-1").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        assertNotNull(obs.value.getClientDetail());
        assertEquals("consumer-1", obs.value.getClientDetail().getInstance().getClientId());
        assertNotNull(obs.value.getClientDetail().getSettings());
        assertEquals(1, obs.value.getClientDetail().getSubscriptionsCount());
        assertEquals("t1", obs.value.getClientDetail().getSubscriptions(0).getTopic().getName());
        assertEquals("1.2.3.4:8888", obs.value.getClientDetail().getNetworkInfo().getRemoteAddress());
    }

    @Test
    public void describeClientReturnsNotFoundForUnknownClient() {
        stubClients(Collections.emptyMap());

        SimpleObserver<DescribeClientResponse> obs = new SimpleObserver<>();
        service.describeClient(DescribeClientRequest.newBuilder().setClientId("ghost").build(), obs);
        assertNotNull(obs.value);
        assertEquals(Code.NOT_FOUND, obs.value.getStatus().getCode());
    }

    @Test
    public void listClientsPaginatesWithCursor() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        clients.put("c2", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        clients.put("c3", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        stubClients(clients);

        SimpleObserver<ListClientsResponse> first = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder().setPageSize(2).build(), first);
        assertEquals(2, first.value.getClientsCount());
        assertFalse(first.value.getNextToken().isEmpty());

        SimpleObserver<ListClientsResponse> second = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder()
            .setPageSize(2)
            .setNextToken(first.value.getNextToken())
            .build(), second);
        assertEquals(1, second.value.getClientsCount());
        assertTrue(second.value.getNextToken().isEmpty());
    }

    @Test
    public void listClientsCursorIsStableUnderMembershipChurn() {
        // sorted order: c1 < c2 < c3
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        clients.put("c2", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        clients.put("c3", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        stubClients(clients);

        SimpleObserver<ListClientsResponse> first = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder().setPageSize(2).build(), first);
        assertEquals(2, first.value.getClientsCount());
        String token = first.value.getNextToken();
        assertFalse(token.isEmpty());

        // a new client connects between the two page fetches; it sorts before the cursor
        // position, so the continuation page must neither duplicate nor shift entries.
        Map<String, Settings> grown = new LinkedHashMap<>(clients);
        grown.put("c10", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        stubClients(grown);

        SimpleObserver<ListClientsResponse> second = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder()
            .setPageSize(2)
            .setNextToken(token)
            .build(), second);
        assertEquals(1, second.value.getClientsCount());
        assertEquals("c3", second.value.getClientsList().get(0).getClientId());
        assertTrue(second.value.getNextToken().isEmpty());
    }

    @Test
    public void listClientsRejectsTamperedCursorGracefully() {
        Map<String, Settings> clients = new LinkedHashMap<>();
        clients.put("c1", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        clients.put("c2", subscriptionSettings(ClientType.SIMPLE_CONSUMER, "g", "t", "*"));
        stubClients(clients);

        SimpleObserver<ListClientsResponse> obs = new SimpleObserver<>();
        service.listClients(ListClientsRequest.newBuilder()
            .setPageSize(10)
            .setNextToken("c1:not-base64-!!!")
            .build(), obs);
        assertEquals(Code.OK, obs.value.getStatus().getCode());
        // invalid cursor falls back to the first page
        assertEquals(2, obs.value.getClientsCount());
    }
}
