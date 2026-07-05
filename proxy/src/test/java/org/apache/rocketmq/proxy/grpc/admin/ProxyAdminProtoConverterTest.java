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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.junit.Test;

import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.AuthStatus;
import apache.rocketmq.proxy.admin.v1.ClientDetail;
import apache.rocketmq.proxy.admin.v1.ClientInstance;
import apache.rocketmq.proxy.admin.v1.ClientLanguage;
import apache.rocketmq.proxy.admin.v1.ClientProtocol;
import apache.rocketmq.proxy.admin.v1.ClientRole;
import apache.rocketmq.proxy.admin.v1.ClientSettings;
import apache.rocketmq.proxy.admin.v1.ConsumeProgress;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.HeartbeatRecord;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.NetworkInfo;
import apache.rocketmq.proxy.admin.v1.TopicConsumeProgress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for ProxyAdminProtoConverter covering proto-to-model conversion, enum mapping, and null/edge-case handling for all admin RPC message types. */
public class ProxyAdminProtoConverterTest {

    // ==================== toFilter Tests ====================

    @Test
    public void testToFilter_EmptyRequest() {
        ListClientsRequest request = ListClientsRequest.newBuilder().build();
        ListClientsFilter filter = ProxyAdminProtoConverter.toFilter(request);
        assertNotNull(filter);
        assertEquals("", filter.getGroup());
        assertEquals("", filter.getTopic());
        assertEquals("", filter.getClientIdPrefix());
        assertNull(filter.getLanguage());
        assertEquals(0, filter.getConnectTimeStart());
        assertEquals(0, filter.getConnectTimeEnd());
        // Proto defaults set empty strings, so group/topic/clientIdPrefix are ""
        // which are not null, so hasFilter() returns true
        assertTrue(filter.hasFilter());
    }

    @Test
    public void testToFilter_FullRequest() {
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setGroup("testGroup")
            .setTopic("testTopic")
            .setClientIdPrefix("testPrefix")
            .setLanguage(ClientLanguage.CLIENT_LANGUAGE_JAVA)
            .setConnectTimeStart(1000L)
            .setConnectTimeEnd(2000L)
            .build();
        ListClientsFilter filter = ProxyAdminProtoConverter.toFilter(request);
        assertEquals("testGroup", filter.getGroup());
        assertEquals("testTopic", filter.getTopic());
        assertEquals("testPrefix", filter.getClientIdPrefix());
        assertEquals("JAVA", filter.getLanguage());
        assertEquals(1000L, filter.getConnectTimeStart());
        assertEquals(2000L, filter.getConnectTimeEnd());
        assertTrue(filter.hasFilter());
    }

    // ==================== toClientLanguage Tests ====================

    @Test
    public void testToClientLanguage_JAVA() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_JAVA,
            ProxyAdminProtoConverter.toClientLanguage("JAVA"));
    }

    @Test
    public void testToClientLanguage_GOLANG() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_GOLANG,
            ProxyAdminProtoConverter.toClientLanguage("GOLANG"));
    }

    @Test
    public void testToClientLanguage_GO() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_GOLANG,
            ProxyAdminProtoConverter.toClientLanguage("GO"));
    }

    @Test
    public void testToClientLanguage_CPP() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_CPP,
            ProxyAdminProtoConverter.toClientLanguage("CPP"));
    }

    @Test
    public void testToClientLanguage_RUST() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_RUST,
            ProxyAdminProtoConverter.toClientLanguage("RUST"));
    }

    @Test
    public void testToClientLanguage_PYTHON() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_PYTHON,
            ProxyAdminProtoConverter.toClientLanguage("PYTHON"));
    }

    @Test
    public void testToClientLanguage_NODEJS() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_NODEJS,
            ProxyAdminProtoConverter.toClientLanguage("NODEJS"));
    }

    @Test
    public void testToClientLanguage_CSHARP() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_CSHARP,
            ProxyAdminProtoConverter.toClientLanguage("CSHARP"));
    }

    @Test
    public void testToClientLanguage_PHP() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_PHP,
            ProxyAdminProtoConverter.toClientLanguage("PHP"));
    }

    @Test
    public void testToClientLanguage_Null() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED,
            ProxyAdminProtoConverter.toClientLanguage(null));
    }

    @Test
    public void testToClientLanguage_Unknown() {
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED,
            ProxyAdminProtoConverter.toClientLanguage("RUBY"));
    }

    // ==================== fromClientLanguage Tests ====================

    @Test
    public void testFromClientLanguage_JAVA() {
        assertEquals("JAVA",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_JAVA));
    }

    @Test
    public void testFromClientLanguage_GOLANG() {
        assertEquals("GOLANG",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_GOLANG));
    }

    @Test
    public void testFromClientLanguage_CPP() {
        assertEquals("CPP",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_CPP));
    }

    @Test
    public void testFromClientLanguage_RUST() {
        assertEquals("RUST",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_RUST));
    }

    @Test
    public void testFromClientLanguage_PYTHON() {
        assertEquals("PYTHON",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_PYTHON));
    }

    @Test
    public void testFromClientLanguage_NODEJS() {
        assertEquals("NODE_JS",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_NODEJS));
    }

    @Test
    public void testFromClientLanguage_CSHARP() {
        assertEquals("DOTNET",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_CSHARP));
    }

    @Test
    public void testFromClientLanguage_PHP() {
        assertEquals("PHP",
            ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_PHP));
    }

    @Test
    public void testFromClientLanguage_Null() {
        assertNull(ProxyAdminProtoConverter.fromClientLanguage(null));
    }

    @Test
    public void testFromClientLanguage_Unspecified() {
        assertNull(ProxyAdminProtoConverter.fromClientLanguage(ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED));
    }

    // ==================== toClientProtocol Tests ====================

    @Test
    public void testToClientProtocol_GRPC() {
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_GRPC,
            ProxyAdminProtoConverter.toClientProtocol("GRPC"));
    }

    @Test
    public void testToClientProtocol_REMOTING() {
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_REMOTING,
            ProxyAdminProtoConverter.toClientProtocol("REMOTING"));
    }

    @Test
    public void testToClientProtocol_Null() {
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_UNSPECIFIED,
            ProxyAdminProtoConverter.toClientProtocol(null));
    }

    // ==================== toClientRole Tests ====================

    @Test
    public void testToClientRole_PRODUCER() {
        assertEquals(ClientRole.CLIENT_ROLE_PRODUCER,
            ProxyAdminProtoConverter.toClientRole("PRODUCER"));
    }

    @Test
    public void testToClientRole_PUSH_CONSUMER() {
        assertEquals(ClientRole.CLIENT_ROLE_PUSH_CONSUMER,
            ProxyAdminProtoConverter.toClientRole("PUSH_CONSUMER"));
    }

    @Test
    public void testToClientRole_SIMPLE_CONSUMER() {
        assertEquals(ClientRole.CLIENT_ROLE_SIMPLE_CONSUMER,
            ProxyAdminProtoConverter.toClientRole("SIMPLE_CONSUMER"));
    }

    @Test
    public void testToClientRole_Null() {
        assertEquals(ClientRole.CLIENT_ROLE_UNSPECIFIED,
            ProxyAdminProtoConverter.toClientRole(null));
    }

    // ==================== toProtoAdminCode Tests ====================

    @Test
    public void testToProtoAdminCode_OK() {
        assertEquals(AdminCode.ADMIN_CODE_OK,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.OK));
    }

    @Test
    public void testToProtoAdminCode_INTERNAL_ERROR() {
        assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.INTERNAL_ERROR));
    }

    @Test
    public void testToProtoAdminCode_BAD_REQUEST() {
        assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.BAD_REQUEST));
    }

    @Test
    public void testToProtoAdminCode_UNAUTHORIZED() {
        assertEquals(AdminCode.ADMIN_CODE_UNAUTHORIZED,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.UNAUTHORIZED));
    }

    @Test
    public void testToProtoAdminCode_FORBIDDEN() {
        assertEquals(AdminCode.ADMIN_CODE_FORBIDDEN,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.FORBIDDEN));
    }

    @Test
    public void testToProtoAdminCode_NOT_FOUND() {
        assertEquals(AdminCode.ADMIN_CODE_NOT_FOUND,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.NOT_FOUND));
    }

    @Test
    public void testToProtoAdminCode_TOO_MANY_REQUESTS() {
        assertEquals(AdminCode.ADMIN_CODE_TOO_MANY_REQUESTS,
            ProxyAdminProtoConverter.toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode.TOO_MANY_REQUESTS));
    }

    @Test
    public void testToProtoAdminCode_Null() {
        assertEquals(AdminCode.ADMIN_CODE_UNSPECIFIED,
            ProxyAdminProtoConverter.toProtoAdminCode(null));
    }

    // ==================== toClientInstance Tests ====================

    @Test
    public void testToClientInstance_Full() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("testClientId");
        info.setLanguage("JAVA");
        info.setClientVersion("5.0.0");
        info.setProtocol("GRPC");
        info.setAccessPoint("127.0.0.1:10911");
        info.setConnectAt(1000L);
        info.setLastActiveAt(2000L);
        info.setRole("PRODUCER");
        info.setGroup("testGroup");
        info.setTopics(Arrays.asList("topicA", "topicB"));

        ClientInstance instance = ProxyAdminProtoConverter.toClientInstance(info);

        assertEquals("testClientId", instance.getClientId());
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_JAVA, instance.getLanguage());
        assertEquals("5.0.0", instance.getClientVersion());
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_GRPC, instance.getProtocol());
        assertEquals("127.0.0.1:10911", instance.getAccessPoint());
        assertEquals(1000L, instance.getConnectAt());
        assertEquals(2000L, instance.getLastActiveAt());
        assertEquals(ClientRole.CLIENT_ROLE_PRODUCER, instance.getRole());
        assertEquals("testGroup", instance.getGroup());
        assertEquals(2, instance.getTopicsCount());
        assertTrue(instance.getTopicsList().contains("topicA"));
        assertTrue(instance.getTopicsList().contains("topicB"));
    }

    @Test
    public void testToClientInstance_NullFields() {
        ClientInstanceInfo info = new ClientInstanceInfo();

        ClientInstance instance = ProxyAdminProtoConverter.toClientInstance(info);

        assertEquals("", instance.getClientId());
        assertEquals("", instance.getClientVersion());
        assertEquals("", instance.getAccessPoint());
        assertEquals(0L, instance.getConnectAt());
        assertEquals(0L, instance.getLastActiveAt());
    }

    @Test
    public void testToClientInstance_NullLanguageProtocolRole() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("testClient");
        info.setLanguage(null);
        info.setProtocol(null);
        info.setRole(null);

        ClientInstance instance = ProxyAdminProtoConverter.toClientInstance(info);

        assertEquals("testClient", instance.getClientId());
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED, instance.getLanguage());
        assertEquals(ClientProtocol.CLIENT_PROTOCOL_UNSPECIFIED, instance.getProtocol());
        assertEquals(ClientRole.CLIENT_ROLE_UNSPECIFIED, instance.getRole());
    }

    // ==================== Error Response Tests ====================

    @Test
    public void testToListClientsError() {
        ListClientsResponse response = ProxyAdminProtoConverter.toListClientsError(
            AdminCode.ADMIN_CODE_BAD_REQUEST, "Invalid filter");

        assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, response.getCode());
        assertEquals("Invalid filter", response.getMessage());
    }

    @Test
    public void testToDescribeClientError() {
        DescribeClientResponse response = ProxyAdminProtoConverter.toDescribeClientError(
            AdminCode.ADMIN_CODE_NOT_FOUND, "Client not found");

        assertEquals(AdminCode.ADMIN_CODE_NOT_FOUND, response.getCode());
        assertEquals("Client not found", response.getMessage());
    }

    @Test
    public void testToListClientsByGroupError() {
        ListClientsByGroupResponse response = ProxyAdminProtoConverter.toListClientsByGroupError(
            AdminCode.ADMIN_CODE_FORBIDDEN, "Access denied");

        assertEquals(AdminCode.ADMIN_CODE_FORBIDDEN, response.getCode());
        assertEquals("Access denied", response.getMessage());
    }

    @Test
    public void testToListClientsByTopicError() {
        ListClientsByTopicResponse response = ProxyAdminProtoConverter.toListClientsByTopicError(
            AdminCode.ADMIN_CODE_TOO_MANY_REQUESTS, "Rate limited");

        assertEquals(AdminCode.ADMIN_CODE_TOO_MANY_REQUESTS, response.getCode());
        assertEquals("Rate limited", response.getMessage());
    }

    // ==================== toHeartbeatRecord Tests ====================

    @Test
    public void testToHeartbeatRecord_NullRemark() {
        ClientDetailInfo.HeartbeatRecordInfo info = new ClientDetailInfo.HeartbeatRecordInfo();
        info.setTimestamp(3000L);
        info.setSuccess(true);
        info.setRemark(null);

        HeartbeatRecord record = ProxyAdminProtoConverter.toHeartbeatRecord(info);

        assertEquals(3000L, record.getTimestamp());
        assertTrue(record.getSuccess());
        assertEquals("", record.getRemark());
    }

    // ==================== toAuthStatus Tests ====================

    @Test
    public void testToAuthStatus_NullFields() {
        ClientDetailInfo.AuthStatusInfo info = new ClientDetailInfo.AuthStatusInfo();
        info.setAuthenticated(false);
        info.setUsername(null);
        info.setLastAuthTime(0L);
        info.setFailureReason(null);

        AuthStatus status = ProxyAdminProtoConverter.toAuthStatus(info);

        assertFalse(status.getAuthenticated());
        assertEquals("", status.getUsername());
        assertEquals(0L, status.getLastAuthTime());
        assertEquals("", status.getFailureReason());
    }

    // ==================== toNetworkInfo Tests ====================

    @Test
    public void testToNetworkInfo_SSLEnabled() {
        ClientDetailInfo.NetworkInfoInfo info = new ClientDetailInfo.NetworkInfoInfo();
        info.setLocalAddress("192.168.1.1:8080");
        info.setRemoteAddress("10.0.0.1:9090");
        info.setRttMs(50L);
        info.setSslEnabled(true);

        NetworkInfo networkInfo = ProxyAdminProtoConverter.toNetworkInfo(info);

        assertEquals("192.168.1.1:8080", networkInfo.getLocalAddress());
        assertEquals("10.0.0.1:9090", networkInfo.getRemoteAddress());
        assertEquals(50L, networkInfo.getRttMs());
        assertEquals("true", networkInfo.getSslEnabled());
    }

    @Test
    public void testToNetworkInfo_SSLDisabled() {
        ClientDetailInfo.NetworkInfoInfo info = new ClientDetailInfo.NetworkInfoInfo();
        info.setLocalAddress("192.168.1.1:8080");
        info.setRemoteAddress("10.0.0.1:9090");
        info.setRttMs(50L);
        info.setSslEnabled(false);

        NetworkInfo networkInfo = ProxyAdminProtoConverter.toNetworkInfo(info);

        assertEquals("false", networkInfo.getSslEnabled());
    }

    // ==================== toConsumeProgress Tests ====================

    @Test
    public void testToConsumeProgress_NullTopicProgress() {
        ClientDetailInfo.ConsumeProgressInfo info = new ClientDetailInfo.ConsumeProgressInfo();
        info.setLag(100L);
        info.setLatencyMs(10L);
        info.setTopicProgress(null);

        ConsumeProgress progress = ProxyAdminProtoConverter.toConsumeProgress(info);

        assertEquals(100L, progress.getLag());
        assertEquals(10L, progress.getLatencyMs());
        assertEquals(0, progress.getTopicProgressCount());
    }

    // ==================== toListClientsResponse / toListClientsByGroupResponse / toListClientsByTopicResponse Tests ====================

    @Test
    public void testToListClientsResponse() {
        ClientInstanceInfo info1 = new ClientInstanceInfo();
        info1.setClientId("client1");
        ClientInstanceInfo info2 = new ClientInstanceInfo();
        info2.setClientId("client2");

        List<ClientInstanceInfo> list = Arrays.asList(info1, info2);
        ListClientsResult result = new ListClientsResult(2L, 1, 10, list);

        ListClientsResponse response = ProxyAdminProtoConverter.toListClientsResponse(result);

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals(AdminCode.ADMIN_CODE_OK.name(), response.getMessage());
        assertEquals(2L, response.getPagination().getTotal());
        assertEquals(1, response.getPagination().getPageNum());
        assertEquals(10, response.getPagination().getPageSize());
        assertEquals(2, response.getListCount());
        assertEquals("client1", response.getList(0).getClientId());
        assertEquals("client2", response.getList(1).getClientId());
    }

    @Test
    public void testToDescribeClientResponse() {
        ClientDetailInfo detail = new ClientDetailInfo();
        ClientInstanceInfo instanceInfo = new ClientInstanceInfo();
        instanceInfo.setClientId("detailClient");
        detail.setClientInstance(instanceInfo);

        DescribeClientResponse response = ProxyAdminProtoConverter.toDescribeClientResponse(detail);

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals(AdminCode.ADMIN_CODE_OK.name(), response.getMessage());
        assertNotNull(response.getClientDetail());
        assertEquals("detailClient", response.getClientDetail().getClientInstance().getClientId());
    }

    @Test
    public void testToListClientsByGroupResponse() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("groupClient");
        info.setGroup("myGroup");
        List<ClientInstanceInfo> list = Collections.singletonList(info);
        ListClientsResult result = new ListClientsResult(1L, 1, 10, list);

        ListClientsByGroupResponse response = ProxyAdminProtoConverter.toListClientsByGroupResponse(result);

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals(1, response.getListCount());
        assertEquals("groupClient", response.getList(0).getClientId());
    }

    @Test
    public void testToListClientsByTopicResponse() {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("topicClient");
        info.setTopics(Arrays.asList("myTopic"));
        List<ClientInstanceInfo> list = Collections.singletonList(info);
        ListClientsResult result = new ListClientsResult(1L, 1, 10, list);

        ListClientsByTopicResponse response = ProxyAdminProtoConverter.toListClientsByTopicResponse(result);

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals(1, response.getListCount());
        assertEquals("topicClient", response.getList(0).getClientId());
    }

    // ==================== toClientDetail Tests ====================

    @Test
    public void testToClientDetail_Full() {
        ClientDetailInfo detail = new ClientDetailInfo();

        // ClientInstance
        ClientInstanceInfo instanceInfo = new ClientInstanceInfo();
        instanceInfo.setClientId("fullClient");
        instanceInfo.setLanguage("JAVA");
        instanceInfo.setClientVersion("5.0.0");
        instanceInfo.setProtocol("GRPC");
        instanceInfo.setAccessPoint("127.0.0.1:10911");
        instanceInfo.setConnectAt(1000L);
        instanceInfo.setLastActiveAt(2000L);
        instanceInfo.setRole("PRODUCER");
        instanceInfo.setGroup("testGroup");
        instanceInfo.setTopics(Arrays.asList("topic1"));
        detail.setClientInstance(instanceInfo);

        // Settings
        ClientDetailInfo.ClientSettingsInfo settings = new ClientDetailInfo.ClientSettingsInfo();
        settings.setSubscriptionMode("POP");
        settings.setReceiveBatchSize(32);
        settings.setLongPollingTimeoutMs(30000L);
        settings.setFifo(true);
        settings.setSubscriptionTopics(Arrays.asList("subTopic"));
        settings.setPublishingTopics(Arrays.asList("pubTopic"));
        detail.setSettings(settings);

        // HeartbeatHistory
        ClientDetailInfo.HeartbeatRecordInfo heartbeat = new ClientDetailInfo.HeartbeatRecordInfo();
        heartbeat.setTimestamp(5000L);
        heartbeat.setSuccess(true);
        heartbeat.setRemark("ok");
        detail.setHeartbeatHistory(Collections.singletonList(heartbeat));

        // AuthStatus
        ClientDetailInfo.AuthStatusInfo authStatus = new ClientDetailInfo.AuthStatusInfo();
        authStatus.setAuthenticated(true);
        authStatus.setUsername("admin");
        authStatus.setLastAuthTime(4000L);
        authStatus.setFailureReason(null);
        detail.setAuthStatus(authStatus);

        // ConsumeProgress
        ClientDetailInfo.ConsumeProgressInfo consumeProgress = new ClientDetailInfo.ConsumeProgressInfo();
        consumeProgress.setLag(50L);
        consumeProgress.setLatencyMs(5L);
        ClientDetailInfo.TopicConsumeProgressInfo topicProgress = new ClientDetailInfo.TopicConsumeProgressInfo();
        topicProgress.setTopic("topic1");
        topicProgress.setLag(25L);
        topicProgress.setLatencyMs(3L);
        consumeProgress.setTopicProgress(Collections.singletonList(topicProgress));
        detail.setConsumeProgress(consumeProgress);

        // NetworkInfo
        ClientDetailInfo.NetworkInfoInfo networkInfo = new ClientDetailInfo.NetworkInfoInfo();
        networkInfo.setLocalAddress("192.168.1.1");
        networkInfo.setRemoteAddress("10.0.0.1");
        networkInfo.setRttMs(10L);
        networkInfo.setSslEnabled(true);
        detail.setNetworkInfo(networkInfo);

        ClientDetail clientDetail = ProxyAdminProtoConverter.toClientDetail(detail);

        // Verify ClientInstance
        assertNotNull(clientDetail.getClientInstance());
        assertEquals("fullClient", clientDetail.getClientInstance().getClientId());

        // Verify Settings
        assertNotNull(clientDetail.getSettings());
        assertEquals("POP", clientDetail.getSettings().getSubscriptionMode());
        assertEquals(32, clientDetail.getSettings().getReceiveBatchSize());
        assertEquals(30000L, clientDetail.getSettings().getLongPollingTimeoutMs());
        assertTrue(clientDetail.getSettings().getFifo());
        assertEquals(1, clientDetail.getSettings().getSubscriptionTopicsCount());
        assertEquals("subTopic", clientDetail.getSettings().getSubscriptionTopics(0));
        assertEquals(1, clientDetail.getSettings().getPublishingTopicsCount());
        assertEquals("pubTopic", clientDetail.getSettings().getPublishingTopics(0));

        // Verify HeartbeatHistory
        assertEquals(1, clientDetail.getHeartbeatHistoryCount());
        assertEquals(5000L, clientDetail.getHeartbeatHistory(0).getTimestamp());
        assertTrue(clientDetail.getHeartbeatHistory(0).getSuccess());
        assertEquals("ok", clientDetail.getHeartbeatHistory(0).getRemark());

        // Verify AuthStatus
        assertNotNull(clientDetail.getAuthStatus());
        assertTrue(clientDetail.getAuthStatus().getAuthenticated());
        assertEquals("admin", clientDetail.getAuthStatus().getUsername());

        // Verify ConsumeProgress
        assertNotNull(clientDetail.getConsumeProgress());
        assertEquals(50L, clientDetail.getConsumeProgress().getLag());
        assertEquals(5L, clientDetail.getConsumeProgress().getLatencyMs());
        assertEquals(1, clientDetail.getConsumeProgress().getTopicProgressCount());
        assertEquals("topic1", clientDetail.getConsumeProgress().getTopicProgress(0).getTopic());
        assertEquals(25L, clientDetail.getConsumeProgress().getTopicProgress(0).getLag());

        // Verify NetworkInfo
        assertNotNull(clientDetail.getNetworkInfo());
        assertEquals("192.168.1.1", clientDetail.getNetworkInfo().getLocalAddress());
        assertEquals("true", clientDetail.getNetworkInfo().getSslEnabled());
    }

    // ==================== toClientSettings Tests ====================

    @Test
    public void testToClientSettings_Full() {
        ClientDetailInfo.ClientSettingsInfo info = new ClientDetailInfo.ClientSettingsInfo();
        info.setSubscriptionMode("SUBSCRIBE");
        info.setReceiveBatchSize(64);
        info.setLongPollingTimeoutMs(15000L);
        info.setFifo(false);
        info.setSubscriptionTopics(Arrays.asList("topicX", "topicY"));
        info.setPublishingTopics(Collections.singletonList("pubX"));

        ClientSettings settings = ProxyAdminProtoConverter.toClientSettings(info);

        assertEquals("SUBSCRIBE", settings.getSubscriptionMode());
        assertEquals(64, settings.getReceiveBatchSize());
        assertEquals(15000L, settings.getLongPollingTimeoutMs());
        assertFalse(settings.getFifo());
        assertEquals(2, settings.getSubscriptionTopicsCount());
        assertEquals("topicX", settings.getSubscriptionTopics(0));
        assertEquals("topicY", settings.getSubscriptionTopics(1));
        assertEquals(1, settings.getPublishingTopicsCount());
        assertEquals("pubX", settings.getPublishingTopics(0));
    }

    // ==================== toTopicConsumeProgress Tests ====================

    @Test
    public void testToTopicConsumeProgress_NullTopic() {
        ClientDetailInfo.TopicConsumeProgressInfo info = new ClientDetailInfo.TopicConsumeProgressInfo();
        info.setTopic(null);
        info.setLag(100L);
        info.setLatencyMs(5L);

        TopicConsumeProgress progress = ProxyAdminProtoConverter.toTopicConsumeProgress(info);

        assertEquals("", progress.getTopic());
        assertEquals(100L, progress.getLag());
        assertEquals(5L, progress.getLatencyMs());
    }
}
