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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.BatchConsumeDiagnosticResult;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.PopReceiptHandleDiagnosticResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for DefaultProxyAdminClientService.
 * <p>
 * Uses sun.misc.Unsafe.allocateInstance() to create GrpcChannelManager,
 * GrpcClientSettingsManager, and GrpcClientChannel instances without calling
 * their constructors, because Mockito 3.10 cannot mock classes implementing
 * StartAndShutdown or extending Netty AbstractChannel on Java 21
 * (class file major version 65 incompatibility with Byte Buddy).
 */
public class DefaultProxyAdminClientServiceTest {

    private GrpcChannelManager grpcChannelManager;
    private GrpcClientSettingsManager grpcClientSettingsManager;
    private DefaultProxyAdminClientService adminService;
    private ConcurrentMap<String, GrpcClientChannel> channelMap;

    @Before
    public void before() throws Exception {
        // Create GrpcChannelManager without constructor (avoids scheduled executor startup)
        grpcChannelManager = createInstanceWithoutConstructor(GrpcChannelManager.class);
        channelMap = new ConcurrentHashMap<>();
        setFieldUnsafe(grpcChannelManager, GrpcChannelManager.class, "clientIdChannelMap", channelMap);

        // Create GrpcClientSettingsManager without constructor
        grpcClientSettingsManager = createInstanceWithoutConstructor(GrpcClientSettingsManager.class);

        adminService = new DefaultProxyAdminClientService(grpcChannelManager, grpcClientSettingsManager);
    }

    @After
    public void after() throws Exception {
        clearClientSettingsMap();
        if (channelMap != null) {
            channelMap.clear();
        }
    }

    // ==================== convertLanguage Tests ====================

    @Test
    public void testConvertLanguageFromUserAgent_JAVA() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.JAVA);
        assertEquals("JAVA", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_GOLANG() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.GOLANG);
        assertEquals("GOLANG", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_CPP() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.CPP);
        assertEquals("CPP", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_RUST() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.RUST);
        assertEquals("RUST", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_PYTHON() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.PYTHON);
        assertEquals("PYTHON", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_DOT_NET() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.DOT_NET);
        assertEquals("DOTNET", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_PHP() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.PHP);
        assertEquals("PHP", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_NODE_JS() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.NODE_JS);
        assertEquals("NODE_JS", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_RUBY() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.RUBY);
        assertEquals("RUBY", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_KOTLIN() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.KOTLIN);
        assertEquals("KOTLIN", info.getLanguage());
    }

    @Test
    public void testConvertLanguageFromUserAgent_UNSPECIFIED() {
        ClientInstanceInfo info = buildClientWithLanguage(Language.LANGUAGE_UNSPECIFIED);
        assertEquals("UNSPECIFIED", info.getLanguage());
    }

    @Test
    public void testConvertLanguage_NoUserAgent() {
        String clientId = "test-client-no-ua";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 10);
        assertEquals(1, result.getList().size());
        assertEquals("UNSPECIFIED", result.getList().get(0).getLanguage());
    }

    // ==================== clientVersion Tests ====================

    @Test
    public void testClientVersionFromUserAgent() {
        String clientId = "test-client-version";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        UA userAgent = UA.newBuilder()
            .setLanguage(Language.JAVA)
            .setVersion("5.0.0")
            .build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 10);
        assertEquals(1, result.getList().size());
        assertEquals("5.0.0", result.getList().get(0).getClientVersion());
    }

    // ==================== listClients Tests ====================

    @Test
    public void testListClients_Pagination() {
        for (int i = 0; i < 5; i++) {
            String clientId = "client-" + i;
            GrpcClientChannel channel = createTestChannel((long) i * 1000, (long) i * 1000 + 500, "10.0.0." + i + ":1234");

            UA userAgent = UA.newBuilder()
                .setLanguage(Language.JAVA)
                .setVersion("5.0.0")
                .build();
            Settings settings = Settings.newBuilder()
                .setClientType(ClientType.PRODUCER)
                .setUserAgent(userAgent)
                .setPublishing(Publishing.newBuilder()
                    .addTopics(Resource.newBuilder().setName("topic-" + i).build())
                    .build())
                .build();

            channelMap.put(clientId, channel);
            putClientSettings(clientId, settings);
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

    @Test
    public void testListClients_PageSizeUpperBound() {
        // Request pageSize=200, should be capped at 100
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 200);
        assertEquals(100, result.getPageSize());
    }

    @Test
    public void testListClients_FilterByGroup() {
        // Producer client (no group)
        GrpcClientChannel producerChannel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder().build())
            .build();
        channelMap.put("producer-1", producerChannel);
        putClientSettings("producer-1", producerSettings);

        // Consumer client with group
        GrpcClientChannel consumerChannel = createTestChannel(3000L, 4000L, "10.0.0.2:1234");
        Settings consumerSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("test-group").build())
                .build())
            .build();
        channelMap.put("consumer-1", consumerChannel);
        putClientSettings("consumer-1", consumerSettings);

        // Filter by group
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup("test-group");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("test-group", result.getList().get(0).getGroup());
    }

    @Test
    public void testListClients_EmptyResult() {
        // channelMap is already empty (no channels added)
        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 10);
        assertEquals(0, result.getTotal());
        assertTrue(result.getList().isEmpty());
    }

    // ==================== describeClient Tests ====================

    @Test
    public void testDescribeClient_Success() {
        String clientId = "test-describe-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        UA userAgent = UA.newBuilder()
            .setLanguage(Language.GOLANG)
            .setVersion("1.2.3")
            .build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setUserAgent(userAgent)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("my-group").build())
                .build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getClientInstance());
        assertEquals("GOLANG", detail.getClientInstance().getLanguage());
        assertEquals("1.2.3", detail.getClientInstance().getClientVersion());
        assertEquals("PUSH_CONSUMER", detail.getClientInstance().getRole());
    }

    @Test
    public void testDescribeClient_NotFound() {
        // channelMap doesn't contain "nonexistent", so getChannel returns null
        ClientDetailInfo detail = adminService.describeClient("nonexistent");
        assertNull(detail);
    }

    @Test
    public void testDescribeClient_BlankClientId() {
        ClientDetailInfo detail = adminService.describeClient("");
        assertNull(detail);
    }

    // ==================== Filter Pushdown Tests (RIP-2 §8.1) ====================

    @Test
    public void testListClients_FilterByClientIdPrefix() {
        GrpcClientChannel channel1 = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel channel2 = createTestChannel(3000L, 4000L, "10.0.0.2:1234");

        UA userAgent = UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put("producer-app1-001", channel1);
        putClientSettings("producer-app1-001", settings);
        channelMap.put("consumer-app2-001", channel2);
        putClientSettings("consumer-app2-001", settings);

        ListClientsFilter filter = new ListClientsFilter();
        filter.setClientIdPrefix("producer");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("producer-app1-001", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClients_FilterByLanguage() {
        GrpcClientChannel javaChannel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel goChannel = createTestChannel(3000L, 4000L, "10.0.0.2:1234");

        UA javaUA = UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build();
        Settings javaSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(javaUA)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        UA goUA = UA.newBuilder().setLanguage(Language.GOLANG).setVersion("1.2.3").build();
        Settings goSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(goUA)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put("java-client", javaChannel);
        putClientSettings("java-client", javaSettings);
        channelMap.put("go-client", goChannel);
        putClientSettings("go-client", goSettings);

        ListClientsFilter filter = new ListClientsFilter();
        filter.setLanguage("JAVA");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("java-client", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClients_FilterByConnectTimeRange() {
        GrpcClientChannel oldChannel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel newChannel = createTestChannel(5000L, 6000L, "10.0.0.2:1234");

        UA userAgent = UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put("old-client", oldChannel);
        putClientSettings("old-client", settings);
        channelMap.put("new-client", newChannel);
        putClientSettings("new-client", settings);

        // Filter: connect time after 3000
        ListClientsFilter filter = new ListClientsFilter();
        filter.setConnectTimeStart(3000L);
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("new-client", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClients_FilterByTopic() {
        GrpcClientChannel channel1 = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel channel2 = createTestChannel(3000L, 4000L, "10.0.0.2:1234");

        Settings settings1 = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("topic-A").build())
                .build())
            .build();

        Settings settings2 = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("topic-B").build())
                .build())
            .build();

        channelMap.put("producer-A", channel1);
        putClientSettings("producer-A", settings1);
        channelMap.put("producer-B", channel2);
        putClientSettings("producer-B", settings2);

        ListClientsFilter filter = new ListClientsFilter();
        filter.setTopic("topic-A");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("producer-A", result.getList().get(0).getClientId());
    }

    @Test
    public void testListClients_CombinedFilter() {
        GrpcClientChannel channel1 = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        GrpcClientChannel channel2 = createTestChannel(5000L, 6000L, "10.0.0.2:1234");

        // Java producer with topic-A
        Settings settings1 = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("topic-A").build())
                .build())
            .build();

        // Go producer with topic-A
        Settings settings2 = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.GOLANG).setVersion("1.2.3").build())
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("topic-A").build())
                .build())
            .build();

        channelMap.put("java-producer-A", channel1);
        putClientSettings("java-producer-A", settings1);
        channelMap.put("go-producer-A", channel2);
        putClientSettings("go-producer-A", settings2);

        // Combined filter: JAVA language + topic-A
        ListClientsFilter filter = new ListClientsFilter();
        filter.setLanguage("JAVA");
        filter.setTopic("topic-A");
        ListClientsResult result = adminService.listClients(filter, 1, 10);
        assertEquals(1, result.getTotal());
        assertEquals("java-producer-A", result.getList().get(0).getClientId());
    }

    // ==================== DescribeClient Field Completion Tests ====================

    @Test
    public void testDescribeClient_AuthStatusWithUsername() {
        String clientId = "test-auth-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        UA userAgent = UA.newBuilder()
            .setLanguage(Language.JAVA)
            .setVersion("5.0.0")
            .setPlatform("test-user")
            .build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getAuthStatus());
        assertTrue(detail.getAuthStatus().isAuthenticated());
        assertEquals("test-user", detail.getAuthStatus().getUsername());
        assertEquals(1000L, detail.getAuthStatus().getLastAuthTime());
    }

    @Test
    public void testDescribeClient_NetworkInfoWithLocalAddress() throws Exception {
        String clientId = "test-network-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        // Also set localAddress
        setFieldUnsafe(channel, SimpleChannel.class, "localAddress", "192.168.1.1:8080");

        UA userAgent = UA.newBuilder()
            .setLanguage(Language.JAVA)
            .setVersion("5.0.0")
            .build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getNetworkInfo());
        assertEquals("10.0.0.1:1234", detail.getNetworkInfo().getRemoteAddress());
        assertEquals("192.168.1.1:8080", detail.getNetworkInfo().getLocalAddress());
    }

    @Test
    public void testDescribeClient_HeartbeatHistoryFallback() {
        String clientId = "test-heartbeat-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getHeartbeatHistory());
        // Without tracked heartbeat records, should have synthetic fallback
        assertTrue(detail.getHeartbeatHistory().size() >= 1);
        assertEquals(true, detail.getHeartbeatHistory().get(0).isSuccess());
    }

    @Test
    public void testDescribeClient_ConsumerWithConsumeProgress() {
        String clientId = "test-consumer-progress";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("test-group").build())
                .addSubscriptions(apache.rocketmq.v2.SubscriptionEntry.newBuilder()
                    .setTopic(Resource.newBuilder().setName("topic-1").build())
                    .build())
                .build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getConsumeProgress());
        assertNotNull(detail.getConsumeProgress().getTopicProgress());
        assertEquals(1, detail.getConsumeProgress().getTopicProgress().size());
        assertEquals("topic-1", detail.getConsumeProgress().getTopicProgress().get(0).getTopic());
        // Lag and latency are -1 in M1 (requires broker-side data, M2 scope)
        assertEquals(-1, detail.getConsumeProgress().getTopicProgress().get(0).getLag());
    }

    @Test
    public void testDescribeClient_ProducerNoConsumeProgress() {
        String clientId = "test-producer-no-progress";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        // Producer should not have consume progress
        assertNull(detail.getConsumeProgress());
    }

    @Test
    public void testDescribeClient_ClientSettingsInfo() {
        String clientId = "test-settings-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("test-group").build())
                .setFifo(true)
                .setReceiveBatchSize(32)
                .setLongPollingTimeout(com.google.protobuf.Duration.newBuilder()
                    .setSeconds(30)
                    .setNanos(0)
                    .build())
                .addSubscriptions(apache.rocketmq.v2.SubscriptionEntry.newBuilder()
                    .setTopic(Resource.newBuilder().setName("topic-1").build())
                    .build())
                .build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getSettings());
        assertEquals("FIFO", detail.getSettings().getSubscriptionMode());
        assertEquals(32, detail.getSettings().getReceiveBatchSize());
        assertEquals(30000L, detail.getSettings().getLongPollingTimeoutMs());
        assertTrue(detail.getSettings().isFifo());
    }

    // ==================== Sampling and Degradation Tests ====================

    @Test
    public void testShouldSample_BelowThreshold() {
        // With empty channelMap, client count is 0, which is below SAMPLING_THRESHOLD
        // shouldSample() should return false
        assertFalse(adminService.shouldSample());
    }

    @Test
    public void testRecordHeartbeat() {
        String clientId = "heartbeat-test-client";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(UA.newBuilder().setLanguage(Language.JAVA).setVersion("5.0.0").build())
            .setPublishing(Publishing.newBuilder().build())
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        // Record heartbeat
        adminService.recordHeartbeat(clientId);

        // Verify heartbeat history is populated
        ClientDetailInfo detail = adminService.describeClient(clientId);
        assertNotNull(detail);
        assertNotNull(detail.getHeartbeatHistory());
        // Should have tracked heartbeat records (not just synthetic fallback)
        assertTrue(detail.getHeartbeatHistory().size() >= 1);
        assertTrue(detail.getHeartbeatHistory().get(0).isSuccess());
        assertEquals("Heartbeat OK", detail.getHeartbeatHistory().get(0).getRemark());
    }

    // ==================== listClientsByGroup / listClientsByTopic Tests ====================

    @Test
    public void testListClientsByGroup() {
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("target-group").build())
                .build())
            .build();

        channelMap.put("consumer-1", channel);
        putClientSettings("consumer-1", settings);

        ListClientsResult result = adminService.listClientsByGroup("target-group", 1, 10);
        assertEquals(1, result.getTotal());
    }

    @Test
    public void testListClientsByTopic() {
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPublishing(Publishing.newBuilder()
                .addTopics(Resource.newBuilder().setName("target-topic").build())
                .build())
            .build();

        channelMap.put("producer-1", channel);
        putClientSettings("producer-1", settings);

        ListClientsResult result = adminService.listClientsByTopic("target-topic", 1, 10);
        assertEquals(1, result.getTotal());
    }

    // ==================== Helper Methods ====================

    private ClientInstanceInfo buildClientWithLanguage(Language language) {
        String clientId = "client-lang-" + language.name();
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");

        UA userAgent = UA.newBuilder()
            .setLanguage(language)
            .setVersion("1.0.0")
            .build();
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setUserAgent(userAgent)
            .build();

        channelMap.put(clientId, channel);
        putClientSettings(clientId, settings);

        ListClientsResult result = adminService.listClients(new ListClientsFilter(), 1, 10);
        assertEquals(1, result.getList().size());
        return result.getList().get(0);
    }

    // ==================== forceDisconnectClient Tests ====================

    @Test
    public void testForceDisconnectClient_BlankClientId() {
        boolean result = adminService.forceDisconnectClient("", "test reason");
        assertFalse("Should return false for blank clientId", result);
    }

    @Test
    public void testForceDisconnectClient_NullClientId() {
        boolean result = adminService.forceDisconnectClient(null, "test reason");
        assertFalse("Should return false for null clientId", result);
    }

    @Test
    public void testForceDisconnectClient_ClientNotFound() {
        boolean result = adminService.forceDisconnectClient("nonexistent-client", "test reason");
        assertFalse("Should return false when client not found in channel manager", result);
    }

    @Test
    public void testForceDisconnectClient_Success() {
        String clientId = "disconnect-client-1";
        GrpcClientChannel channel = createTestChannel(1000L, 2000L, "10.0.0.1:1234");
        channelMap.put(clientId, channel);

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .build();
        putClientSettings(clientId, settings);

        boolean result = adminService.forceDisconnectClient(clientId, "admin disconnect");

        // forceClose returns false (no active stream observer), but forceDisconnectClient
        // still returns true because channel exists and cleanup is performed.
        assertTrue("Should return true on successful disconnect", result);
        // Verify channel was removed from channelMap regardless of forceClose result
        assertNull("Channel should be removed from channel map", channelMap.get(clientId));
        // Verify settings were removed
        assertNull("Settings should be removed", grpcClientSettingsManager.getRawClientSettings(clientId));
    }

    // ==================== describePopReceiptHandles Tests ====================

    @Test
    public void testDescribePopReceiptHandles_ReceiptHandleManagerNotInitialized() {
        // Default adminService has no ReceiptHandleManager (null)
        PopReceiptHandleDiagnosticResult result = adminService.describePopReceiptHandles("test-group", "test-topic", 1, 10);

        assertNotNull("Should return non-null result", result);
        assertEquals(0, result.getTotal());
        assertEquals(1, result.getPageNum());
        assertEquals(1, result.getPageSize());
        assertTrue("Handles should be empty", result.getHandles().isEmpty());
    }

    // ==================== describeBatchConsumeDiagnostics Tests ====================

    @Test
    public void testDescribeBatchConsumeDiagnostics_ReceiptHandleManagerNotInitialized() {
        // Default adminService has no ReceiptHandleManager (null)
        BatchConsumeDiagnosticResult result = adminService.describeBatchConsumeDiagnostics(
            "test-group", "test-topic", "client-1", 1, 10);

        assertNotNull("Should return non-null result", result);
        assertEquals(0, result.getTotal());
        assertEquals(1, result.getPageNum());
        assertEquals(1, result.getPageSize());
        assertTrue("Diagnostics should be empty", result.getDiagnostics().isEmpty());
    }

    @Test
    public void testDescribeBatchConsumeDiagnostics_BlankGroup() {
        BatchConsumeDiagnosticResult result = adminService.describeBatchConsumeDiagnostics(
            "", "test-topic", "client-1", 1, 10);

        assertNotNull("Should return non-null result for blank group", result);
        assertEquals(0, result.getTotal());
        assertTrue("Diagnostics should be empty for blank group", result.getDiagnostics().isEmpty());
    }

    /**
     * Create a GrpcClientChannel instance without calling the constructor.
     * Uses Unsafe.allocateInstance() and sets createTime, lastAccessTime, remoteAddress fields
     * via Unsafe to bypass final field restrictions.
     * <p>
     * Field locations in the class hierarchy:
     * - createTime: declared in GrpcClientChannel (private final long)
     * - lastAccessTime: declared in SimpleChannel (protected long)
     * - remoteAddress: declared in SimpleChannel (protected final String)
     */
    private static GrpcClientChannel createTestChannel(long createTime, long lastAccessTime,
        String remoteAddress) {
        try {
            GrpcClientChannel channel = createInstanceWithoutConstructor(GrpcClientChannel.class);
            // createTime is declared in GrpcClientChannel
            setLongFieldUnsafe(channel, GrpcClientChannel.class, "createTime", createTime);
            // lastAccessTime is declared in SimpleChannel (grandparent class)
            setLongFieldUnsafe(channel, SimpleChannel.class, "lastAccessTime", lastAccessTime);
            // remoteAddress is declared in SimpleChannel
            setFieldUnsafe(channel, SimpleChannel.class, "remoteAddress", remoteAddress);
            // telemetryCommandRef is needed for forceClose() to work without NPE.
            // With AtomicReference(null), forceClose() returns false (no active stream observer).
            setFieldUnsafe(channel, GrpcClientChannel.class, "telemetryCommandRef",
                new java.util.concurrent.atomic.AtomicReference<>());
            return channel;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test GrpcClientChannel", e);
        }
    }

    /**
     * Put client settings into the static CLIENT_SETTINGS_MAP via reflection.
     * GrpcClientSettingsManager.getRawClientSettings() reads from this static map.
     */
    @SuppressWarnings("unchecked")
    private void putClientSettings(String clientId, Settings settings) {
        try {
            Field field = GrpcClientSettingsManager.class.getDeclaredField("CLIENT_SETTINGS_MAP");
            field.setAccessible(true);
            Map<String, Settings> map = (Map<String, Settings>) field.get(null);
            map.put(clientId, settings);
        } catch (Exception e) {
            throw new RuntimeException("Failed to put client settings", e);
        }
    }

    /**
     * Clear the static CLIENT_SETTINGS_MAP after each test to avoid interference.
     */
    @SuppressWarnings("unchecked")
    private void clearClientSettingsMap() {
        try {
            Field field = GrpcClientSettingsManager.class.getDeclaredField("CLIENT_SETTINGS_MAP");
            field.setAccessible(true);
            Map<String, Settings> map = (Map<String, Settings>) field.get(null);
            map.clear();
        } catch (Exception e) {
            throw new RuntimeException("Failed to clear client settings map", e);
        }
    }

    /**
     * Create an instance without calling the constructor using sun.misc.Unsafe.
     * This bypasses constructor logic (e.g., scheduled executor startup in GrpcChannelManager)
     * and avoids Mockito 3.10 / Java 21 incompatibility with classes implementing StartAndShutdown
     * or extending Netty AbstractChannel.
     */
    @SuppressWarnings("unchecked")
    private static <T> T createInstanceWithoutConstructor(Class<T> clazz) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (T) allocateInstance.invoke(unsafe, clazz);
    }

    /**
     * Set an Object field value using sun.misc.Unsafe, bypassing final field restrictions.
     * Accepts the declaring class to support fields in parent classes.
     */
    private static void setFieldUnsafe(Object obj, Class<?> declaringClass, String fieldName,
        Object value) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);

        Field field = declaringClass.getDeclaredField(fieldName);
        Method objectFieldOffsetMethod = unsafeClass.getMethod("objectFieldOffset", Field.class);
        long offset = (Long) objectFieldOffsetMethod.invoke(unsafe, field);

        Method putObjectMethod = unsafeClass.getMethod("putObject", Object.class, long.class, Object.class);
        putObjectMethod.invoke(unsafe, obj, offset, value);
    }

    /**
     * Set a long field value using sun.misc.Unsafe, bypassing final field restrictions.
     * Accepts the declaring class to support fields in parent classes.
     */
    private static void setLongFieldUnsafe(Object obj, Class<?> declaringClass, String fieldName,
        long value) throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);

        Field field = declaringClass.getDeclaredField(fieldName);
        Method objectFieldOffsetMethod = unsafeClass.getMethod("objectFieldOffset", Field.class);
        long offset = (Long) objectFieldOffsetMethod.invoke(unsafe, field);

        Method putLongMethod = unsafeClass.getMethod("putLong", Object.class, long.class, long.class);
        putLongMethod.invoke(unsafe, obj, offset, value);
    }
}