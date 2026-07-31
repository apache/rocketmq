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

package org.apache.rocketmq.proxy.service.admin;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.UA;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DefaultProxyAdminClientService using Mockito mocks.
 * <p>
 * Uses Mockito to mock GrpcChannelManager, GrpcClientSettingsManager, and GrpcClientChannel.
 * Real Settings proto objects are built via {@code Settings.newBuilder()}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultProxyAdminClientServiceUnitTest {

    @Mock
    private GrpcChannelManager grpcChannelManager;

    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;

    private DefaultProxyAdminClientService adminService;
    private Map<String, GrpcClientChannel> channelMap;

    @Before
    public void before() {
        channelMap = new HashMap<>();
        when(grpcChannelManager.getClientIdChannelMap()).thenReturn(channelMap);
        adminService = new DefaultProxyAdminClientService(grpcChannelManager, grpcClientSettingsManager);
    }

    // ==================== Helper Methods ====================

    /**
     * Create a mock GrpcClientChannel with specified attributes.
     * All unmocked methods will return Mockito defaults (null / 0 / false).
     */
    private GrpcClientChannel mockChannel(long createTime, long lastAccessTime, String remoteAddress) {
        GrpcClientChannel channel = mock(GrpcClientChannel.class);
        when(channel.getCreateTime()).thenReturn(createTime);
        when(channel.getLastAccessTime()).thenReturn(lastAccessTime);
        when(channel.getRemoteAddress()).thenReturn(remoteAddress);
        return channel;
    }

    /**
     * Add a client to the channel map and stub the settings lookup.
     * Also stubs {@code grpcChannelManager.getChannel(clientId)} for describeClient tests.
     */
    private void addClient(String clientId, GrpcClientChannel channel, Settings settings) {
        channelMap.put(clientId, channel);
        when(grpcClientSettingsManager.getRawClientSettings(clientId)).thenReturn(settings);
        lenient().when(grpcChannelManager.getChannel(clientId)).thenReturn(channel);
    }

    // ==================== Test 1: listClients empty channels ====================

    @Test
    public void testListClients_EmptyChannels_ReturnsEmptyResultWithTotalZero() {
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 10);

        assertEquals(0, result.getTotal());
        assertTrue(result.getList().isEmpty());
    }

    // ==================== Test 2: listClients pageNum=0 normalized to 1 ====================

    @Test
    public void testListClients_PageNumZero_NormalizedToOne() {
        // Add 3 clients
        for (int i = 0; i < 3; i++) {
            String clientId = "client-" + i;
            GrpcClientChannel channel = mockChannel(i * 1000L, i * 1000L + 500, "10.0.0." + i + ":1234");
            Settings settings = createProducerSettings(Language.JAVA, "5.0.0", "topic-" + i);
            addClient(clientId, channel, settings);
        }

        // pageNum=0 should be normalized to 1
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 0, 10);
        assertEquals(1, result.getPageNum());
    }

    // ==================== Test 3: listClients pageSize=0 normalized to 1 ====================

    @Test
    public void testListClients_PageSizeZero_NormalizedToOne() {
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = createProducerSettings(Language.JAVA, "5.0.0", "topic-A");
        addClient("client-1", channel, settings);

        // pageSize=0 should be normalized to 1
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 0);
        assertEquals(1, result.getPageSize());
    }

    // ==================== Test 4: listClients pageSize > 100 capped at 100 ====================

    @Test
    public void testListClients_PageSizeExceedsMax_CappedAt100() {
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 200);
        assertEquals(100, result.getPageSize());
    }

    // ==================== Test 5: listClients null filter no NPE ====================

    @Test
    public void testListClients_NullFilter_NoNpe() {
        // Add one client so the filter path is exercised
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = createProducerSettings(Language.JAVA, "5.0.0", "topic-A");
        addClient("client-1", channel, settings);

        // Passing null filter should not cause NullPointerException
        // The FilterContext constructor handles null by using a default empty filter
        ListClientsResult result = adminService.listClients(null, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("client-1", result.getList().get(0).getClientId());
    }

    // ==================== Test 6: listClients pagination first page ====================

    @Test
    public void testListClients_Pagination_FirstPageReturnsCorrectSubset() {
        // Add 5 clients
        for (int i = 0; i < 5; i++) {
            String clientId = "client-" + i;
            GrpcClientChannel channel = mockChannel(i * 1000L, i * 1000L + 500, "10.0.0." + i + ":1234");
            Settings settings = createProducerSettings(Language.JAVA, "5.0.0", "topic-" + i);
            addClient(clientId, channel, settings);
        }

        // Page 1 with size 2
        ListClientsResult page1 = adminService.listClients(new ListClientsFilter(), 1, 2);
        assertEquals(5, page1.getTotal());
        assertEquals(2, page1.getList().size());
        assertEquals(1, page1.getPageNum());
        assertEquals(2, page1.getPageSize());

        // Page 2 with size 2
        ListClientsResult page2 = adminService.listClients(new ListClientsFilter(), 2, 2);
        assertEquals(5, page2.getTotal());
        assertEquals(2, page2.getList().size());

        // Page 3 with size 2 (only 1 remaining)
        ListClientsResult page3 = adminService.listClients(new ListClientsFilter(), 3, 2);
        assertEquals(5, page3.getTotal());
        assertEquals(1, page3.getList().size());
    }

    // ==================== Test 7: listClients pageNum beyond range ====================

    @Test
    public void testListClients_PageNumBeyondRange_ReturnsEmptyList() {
        // Add 2 clients
        for (int i = 0; i < 2; i++) {
            String clientId = "client-" + i;
            GrpcClientChannel channel = mockChannel(i * 1000L, i * 1000L + 500, "10.0.0." + i + ":1234");
            Settings settings = createProducerSettings(Language.JAVA, "5.0.0", "topic-" + i);
            addClient(clientId, channel, settings);
        }

        // Request page 5 when only 2 clients exist (page 5 starts at index 8, beyond size 2)
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 5, 2);
        assertEquals(2, result.getTotal());
        assertTrue(result.getList().isEmpty());
    }

    // ==================== Test 8: listClients filtering by group ====================

    @Test
    public void testListClients_FilterByGroup_ReturnsOnlyMatchingClients() {
        // Producer client (no group)
        GrpcClientChannel producerChannel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder().build())
            .build();
        addClient("producer-1", producerChannel, producerSettings);

        // Consumer client with group "test-group"
        GrpcClientChannel consumerChannel = mockChannel(3000L, 4000L, "10.0.0.2:1234");
        Settings consumerSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("test-group").build())
                .build())
            .build();
        addClient("consumer-1", consumerChannel, consumerSettings);

        // Filter by group
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup("test-group");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("test-group", result.getList().get(0).getGroup());
    }

    // ==================== Test 9: listClients filtering by clientIdPrefix ====================

    @Test
    public void testListClients_FilterByClientIdPrefix_ReturnsOnlyMatchingClients() {
        GrpcClientChannel channel1 = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel channel2 = mockChannel(3000L, 4000L, "10.0.0.2:1234");

        Settings settings1 = createProducerSettings(Language.JAVA, "5.0.0", "topic-A");
        Settings settings2 = createProducerSettings(Language.GOLANG, "1.2.3", "topic-B");

        addClient("producer-app1-001", channel1, settings1);
        addClient("consumer-app1-001", channel2, settings2);

        ListClientsFilter filter = new ListClientsFilter();
        filter.setClientIdPrefix("producer");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("producer-app1-001", result.getList().get(0).getClientId());
    }

    // ==================== Test 10: describeClient null/blank clientId ====================

    @Test
    public void testDescribeClient_NullClientId_ReturnsNull() {
        ClientDetailInfo detail = adminService.describeClient(null);
        assertNull(detail);
    }

    @Test
    public void testDescribeClient_BlankClientId_ReturnsNull() {
        ClientDetailInfo detail = adminService.describeClient("");
        assertNull(detail);

        detail = adminService.describeClient("   ");
        assertNull(detail);
    }

    // ==================== Test 11: describeClient nonexistent clientId ====================

    @Test
    public void testDescribeClient_NonexistentClientId_ReturnsNull() {
        // channelMap does not contain "nonexistent", so getChannel returns null
        ClientDetailInfo detail = adminService.describeClient("nonexistent");
        assertNull(detail);
    }

    // ==================== Test 12: describeClient client found but no settings ====================

    @Test
    public void testDescribeClient_ClientFoundButNoSettings_ReturnsNull() {
        String clientId = "client-no-settings";
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");

        // Add channel to map but do NOT stub settings (returns null by default)
        channelMap.put(clientId, channel);
        when(grpcChannelManager.getChannel(clientId)).thenReturn(channel);
        // grpcClientSettingsManager.getRawClientSettings(clientId) returns null (mock default)

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNull(detail);
    }

    // ==================== Test 13: listClientsByGroup delegates correctly ====================

    @Test
    public void testListClientsByGroup_DelegatesWithGroupFilter() {
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("target-group").build())
                .build())
            .build();
        addClient("consumer-1", channel, settings);

        // Also add a producer that should be filtered out
        GrpcClientChannel producerChannel = mockChannel(5000L, 6000L, "10.0.0.2:1234");
        Settings producerSettings = createProducerSettings(Language.JAVA, "5.0.0", "topic-A");
        addClient("producer-1", producerChannel, producerSettings);

        ListClientsResult result = adminService.listClientsByGroup("target-group", 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("target-group", result.getList().get(0).getGroup());
        assertEquals("consumer-1", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClientsByGroup_NoMatchingGroup_ReturnsEmpty() {
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("other-group").build())
                .build())
            .build();
        addClient("consumer-1", channel, settings);

        ListClientsResult result = adminService.listClientsByGroup("nonexistent-group", 1, 10);
        assertEquals(0, result.getTotal());
        assertTrue(result.getList().isEmpty());
    }

    // ==================== Test 14: listClientsByTopic delegates correctly ====================

    @Test
    public void testListClientsByTopic_DelegatesWithTopicFilter() {
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("target-topic").build())
                .build())
            .build();
        addClient("producer-1", channel, settings);

        // Also add a producer on a different topic that should be filtered out
        GrpcClientChannel otherChannel = mockChannel(5000L, 6000L, "10.0.0.2:1234");
        Settings otherSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("other-topic").build())
                .build())
            .build();
        addClient("producer-2", otherChannel, otherSettings);

        ListClientsResult result = adminService.listClientsByTopic("target-topic", 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("producer-1", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClientsByTopic_NoMatchingTopic_ReturnsEmpty() {
        GrpcClientChannel channel = mockChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("some-topic").build())
                .build())
            .build();
        addClient("producer-1", channel, settings);

        ListClientsResult result = adminService.listClientsByTopic("nonexistent-topic", 1, 10);
        assertEquals(0, result.getTotal());
        assertTrue(result.getList().isEmpty());
    }

    // ==================== Private Helpers ====================

    /**
     * Create a simple producer Settings object with a user agent and publishing topic.
     */
    private static Settings createProducerSettings(Language language, String version, String topicName) {
        return Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder()
                .setLanguage(language)
                .setVersion(version)
                .build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName(topicName).build())
                .build())
            .build();
    }
}
