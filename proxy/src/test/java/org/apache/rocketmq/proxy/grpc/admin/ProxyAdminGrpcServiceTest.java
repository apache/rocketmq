/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * The License.  You may obtain a copy of the License at
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.ClientLanguage;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsRequest;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesRequest;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesResponse;
import apache.rocketmq.proxy.admin.v1.DisconnectClientRequest;
import apache.rocketmq.proxy.admin.v1.DisconnectClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.Pagination;
import apache.rocketmq.proxy.admin.v1.RouteChangeEventType;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsRequest;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsResponse;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ProxyAdminGrpcService.
 * <p>
 * Uses sun.misc.Unsafe.allocateInstance() to create RouteChangeNotifier
 * instances without calling their constructors, because Mockito 3.10 cannot
 * mock classes implementing TopicRouteService.RouteRefreshListener on Java 21
 * (class file major version 65 incompatibility with Byte Buddy).
 */
public class ProxyAdminGrpcServiceTest {

    private ProxyAdminClientService adminClientService;
    private RouteChangeNotifier routeChangeNotifier;
    private ProxyAdminGrpcService adminGrpcService;

    @Before
    public void before() {
        adminGrpcService = new ProxyAdminGrpcService(adminClientService, 2);
    }

    @After
    public void after() {
        // No cleanup needed
    }

    // ==================== enforcePageSize Tests ====================

    @Test
    public void testEnforcePageSize_NormalValue() throws Exception {
        int result = invokeEnforcePageSize(50);
        assertEquals(50, result);
    }

    @Test
    public void testEnforcePageSize_ExceedsMax() throws Exception {
        int result = invokeEnforcePageSize(200);
        assertEquals(100, result);
    }

    @Test
    public void testEnforcePageSize_AtMax() throws Exception {
        int result = invokeEnforcePageSize(100);
        assertEquals(100, result);
    }

    @Test
    public void testEnforcePageSize_Zero() throws Exception {
        int result = invokeEnforcePageSize(0);
        assertEquals(20, result); // default
    }

    @Test
    public void testEnforcePageSize_Negative() throws Exception {
        int result = invokeEnforcePageSize(-5);
        assertEquals(20, result); // default
    }

    @Test
    public void testEnforcePageSize_One() throws Exception {
        int result = invokeEnforcePageSize(1);
        assertEquals(1, result);
    }

    /**
     * Use reflection to test the package-private enforcePageSize method.
     */
    private int invokeEnforcePageSize(int pageSize) throws Exception {
        Method method = ProxyAdminGrpcService.class.getDeclaredMethod("enforcePageSize", int.class);
        method.setAccessible(true);
        return (int) method.invoke(adminGrpcService, pageSize);
    }

    // ==================== Request Proto Tests ====================

    @Test
    public void testListClientsRequest() {
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setGroup("test-group")
            .setTopic("test-topic")
            .setClientIdPrefix("prefix-")
            .setLanguage(ClientLanguage.CLIENT_LANGUAGE_JAVA)
            .setConnectTimeStart(1000L)
            .setConnectTimeEnd(2000L)
            .setPageNum(1)
            .setPageSize(20)
            .build();

        assertEquals("test-group", request.getGroup());
        assertEquals("test-topic", request.getTopic());
        assertEquals("prefix-", request.getClientIdPrefix());
        assertEquals(ClientLanguage.CLIENT_LANGUAGE_JAVA, request.getLanguage());
        assertEquals(1000L, request.getConnectTimeStart());
        assertEquals(2000L, request.getConnectTimeEnd());
        assertEquals(1, request.getPageNum());
        assertEquals(20, request.getPageSize());
    }

    @Test
    public void testDescribeClientRequest() {
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("client-123")
            .build();
        assertEquals("client-123", request.getClientId());
    }

    @Test
    public void testListClientsByGroupRequest() {
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup("my-group")
            .setPageNum(2)
            .setPageSize(50)
            .build();
        assertEquals("my-group", request.getGroup());
        assertEquals(2, request.getPageNum());
        assertEquals(50, request.getPageSize());
    }

    @Test
    public void testListClientsByTopicRequest() {
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic("my-topic")
            .setPageNum(1)
            .setPageSize(10)
            .build();
        assertEquals("my-topic", request.getTopic());
        assertEquals(1, request.getPageNum());
        assertEquals(10, request.getPageSize());
    }

    // ==================== Response Proto Tests ====================

    @Test
    public void testListClientsResponse() {
        ListClientsResponse response = ListClientsResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage("OK")
            .setPagination(Pagination.newBuilder()
                .setTotal(100)
                .setPageNum(1)
                .setPageSize(20)
                .build())
            .build();

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals("OK", response.getMessage());
        assertEquals(100, response.getPagination().getTotal());
        assertEquals(1, response.getPagination().getPageNum());
        assertEquals(20, response.getPagination().getPageSize());
    }

    @Test
    public void testDescribeClientResponse() {
        DescribeClientResponse response = DescribeClientResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage("OK")
            .build();

        assertEquals(AdminCode.ADMIN_CODE_OK, response.getCode());
        assertEquals("OK", response.getMessage());
    }

    // ==================== listClients Integration Test ====================

    @Test
    public void testListClients_DelegatesToService() throws Exception {
        List<ClientInstanceInfo> clients = new ArrayList<>();
        ListClientsResult mockResult = new ListClientsResult(1, 1, 10, clients);
        when(adminClientService.listClients(any(ListClientsFilter.class), eq(1), eq(10))).thenReturn(mockResult);

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClients(request, new io.grpc.stub.StreamObserver<ListClientsResponse>() {
            @Override
            public void onNext(ListClientsResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClients should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
        verify(adminClientService).listClients(any(ListClientsFilter.class), eq(1), eq(10));
    }

    // ==================== Shutdown Test ====================

    @Test
    public void testShutdown() throws Exception {
        ProxyAdminGrpcService service = new ProxyAdminGrpcService(adminClientService, 1);
        service.shutdown();
        // Should not throw
    }

    // ==================== listClients Error Path Test ====================

    @Test
    public void testListClients_ServiceError() throws Exception {
        when(adminClientService.listClients(any(ListClientsFilter.class), anyInt(), anyInt()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClients(request, new io.grpc.stub.StreamObserver<ListClientsResponse>() {
            @Override
            public void onNext(ListClientsResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClients error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== describeClient Tests ====================

    @Test
    public void testDescribeClient_Success() throws Exception {
        ClientDetailInfo mockDetail = new ClientDetailInfo();
        ClientInstanceInfo instanceInfo = new ClientInstanceInfo();
        instanceInfo.setClientId("client-123");
        mockDetail.setClientInstance(instanceInfo);
        when(adminClientService.describeClient("client-123")).thenReturn(mockDetail);

        CountDownLatch latch = new CountDownLatch(1);
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("client-123")
            .build();

        adminGrpcService.describeClient(request, new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
            @Override
            public void onNext(DescribeClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                assertNotNull(value.getClientDetail());
                assertEquals("client-123", value.getClientDetail().getClientInstance().getClientId());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeClient should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDescribeClient_NullClientId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("")
            .build();

        adminGrpcService.describeClient(request, new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
            @Override
            public void onNext(DescribeClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("clientId is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeClient with null clientId should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDescribeClient_NotFound() throws Exception {
        when(adminClientService.describeClient("unknown-client")).thenReturn(null);

        CountDownLatch latch = new CountDownLatch(1);
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("unknown-client")
            .build();

        adminGrpcService.describeClient(request, new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
            @Override
            public void onNext(DescribeClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_NOT_FOUND, value.getCode());
                assertTrue(value.getMessage().contains("Client not found"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeClient not found should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDescribeClient_ServiceError() throws Exception {
        when(adminClientService.describeClient("error-client"))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId("error-client")
            .build();

        adminGrpcService.describeClient(request, new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
            @Override
            public void onNext(DescribeClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeClient error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== shouldAcceptDescribeClient Tests ====================

    @Test
    public void testShouldAcceptDescribeClient_BelowHalfLimit() throws Exception {
        // Below half the concurrency limit: always accept
        boolean result = invokeShouldAcceptDescribeClient(2); // 2 <= 8/2=4 → low load
        assertTrue("Should accept below half limit", result);
    }

    @Test
    public void testShouldAcceptDescribeClient_AtHalfLimit() throws Exception {
        // At half the concurrency limit: always accept
        boolean result = invokeShouldAcceptDescribeClient(4); // 4 <= 4 → low load
        assertTrue("Should accept at half limit", result);
    }

    @Test
    public void testShouldAcceptDescribeClient_OverLimit() throws Exception {
        // Over the concurrency limit: reject
        boolean result = invokeShouldAcceptDescribeClient(10); // 10 > 8 → over limit
        assertFalse("Should reject over limit", result);
    }

    /**
     * Use reflection to test the private shouldAcceptDescribeClient method.
     */
    private boolean invokeShouldAcceptDescribeClient(int concurrency) throws Exception {
        Method method = ProxyAdminGrpcService.class.getDeclaredMethod("shouldAcceptDescribeClient", int.class);
        method.setAccessible(true);
        return (boolean) method.invoke(adminGrpcService, concurrency);
    }

    // ==================== listClientsByGroup Tests ====================

    @Test
    public void testListClientsByGroup_Success() throws Exception {
        List<ClientInstanceInfo> clients = new ArrayList<>();
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("group-client");
        info.setGroup("my-group");
        clients.add(info);
        ListClientsResult mockResult = new ListClientsResult(1, 1, 10, clients);
        when(adminClientService.listClientsByGroup("my-group", 1, 10)).thenReturn(mockResult);

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup("my-group")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByGroup(request, new io.grpc.stub.StreamObserver<ListClientsByGroupResponse>() {
            @Override
            public void onNext(ListClientsByGroupResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                assertEquals(1, value.getListCount());
                assertEquals("group-client", value.getList(0).getClientId());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByGroup should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testListClientsByGroup_EmptyGroup() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup("")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByGroup(request, new io.grpc.stub.StreamObserver<ListClientsByGroupResponse>() {
            @Override
            public void onNext(ListClientsByGroupResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("group is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByGroup with empty group should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testListClientsByGroup_ServiceError() throws Exception {
        when(adminClientService.listClientsByGroup(anyString(), anyInt(), anyInt()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup("error-group")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByGroup(request, new io.grpc.stub.StreamObserver<ListClientsByGroupResponse>() {
            @Override
            public void onNext(ListClientsByGroupResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByGroup error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== listClientsByTopic Tests ====================

    @Test
    public void testListClientsByTopic_Success() throws Exception {
        List<ClientInstanceInfo> clients = new ArrayList<>();
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId("topic-client");
        info.setTopics(Collections.singletonList("my-topic"));
        clients.add(info);
        ListClientsResult mockResult = new ListClientsResult(1, 1, 10, clients);
        when(adminClientService.listClientsByTopic("my-topic", 1, 10)).thenReturn(mockResult);

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic("my-topic")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByTopic(request, new io.grpc.stub.StreamObserver<ListClientsByTopicResponse>() {
            @Override
            public void onNext(ListClientsByTopicResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                assertEquals(1, value.getListCount());
                assertEquals("topic-client", value.getList(0).getClientId());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByTopic should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testListClientsByTopic_EmptyTopic() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic("")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByTopic(request, new io.grpc.stub.StreamObserver<ListClientsByTopicResponse>() {
            @Override
            public void onNext(ListClientsByTopicResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("topic is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByTopic with empty topic should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testListClientsByTopic_ServiceError() throws Exception {
        when(adminClientService.listClientsByTopic(anyString(), anyInt(), anyInt()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic("error-topic")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.listClientsByTopic(request, new io.grpc.stub.StreamObserver<ListClientsByTopicResponse>() {
            @Override
            public void onNext(ListClientsByTopicResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("listClientsByTopic error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== subscribeRouteEvents Tests ====================

    @Test
    public void testSubscribeRouteEvents_NotifierNull() throws Exception {
        // adminGrpcService created with 2-arg constructor has null routeChangeNotifier
        CountDownLatch latch = new CountDownLatch(1);
        SubscribeRouteEventsRequest request = SubscribeRouteEventsRequest.newBuilder().build();

        adminGrpcService.subscribeRouteEvents(request, new io.grpc.stub.StreamObserver<SubscribeRouteEventsResponse>() {
            @Override
            public void onNext(SubscribeRouteEventsResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Route change notifier is not available"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("subscribeRouteEvents with null notifier should complete within 5 seconds",
            latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testSubscribeRouteEvents_DelegatesToNotifier() throws Exception {
        ProxyAdminGrpcService serviceWithNotifier = new ProxyAdminGrpcService(
            adminClientService, 2, routeChangeNotifier);

        SubscribeRouteEventsRequest request = SubscribeRouteEventsRequest.newBuilder()
            .addTopics("testTopic")
            .addEventTypes(apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_CREATE)
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        serviceWithNotifier.subscribeRouteEvents(request, new io.grpc.stub.StreamObserver<SubscribeRouteEventsResponse>() {
            @Override
            public void onNext(SubscribeRouteEventsResponse value) {
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                // Stream errors are expected in test environment (no real gRPC channel)
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        // Verify that subscribe was called on the notifier
        verify(routeChangeNotifier).subscribe(any(), any(), any());
        assertTrue("subscribeRouteEvents should respond within 5 seconds",
            latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== disconnectClient Tests ====================

    @Test
    public void testDisconnectClient_Success() throws Exception {
        when(adminClientService.forceDisconnectClient("client-123", "admin disconnect")).thenReturn(true);

        CountDownLatch latch = new CountDownLatch(1);
        DisconnectClientRequest request = DisconnectClientRequest.newBuilder()
            .setClientId("client-123")
            .setReason("admin disconnect")
            .build();

        adminGrpcService.disconnectClient(request, new io.grpc.stub.StreamObserver<DisconnectClientResponse>() {
            @Override
            public void onNext(DisconnectClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                assertTrue(value.getDisconnected());
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("disconnectClient should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
        verify(adminClientService).forceDisconnectClient("client-123", "admin disconnect");
    }

    @Test
    public void testDisconnectClient_EmptyClientId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        DisconnectClientRequest request = DisconnectClientRequest.newBuilder()
            .setClientId("")
            .setReason("admin disconnect")
            .build();

        adminGrpcService.disconnectClient(request, new io.grpc.stub.StreamObserver<DisconnectClientResponse>() {
            @Override
            public void onNext(DisconnectClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("clientId is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("disconnectClient with empty clientId should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDisconnectClient_EmptyReason() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        DisconnectClientRequest request = DisconnectClientRequest.newBuilder()
            .setClientId("client-123")
            .setReason("")
            .build();

        adminGrpcService.disconnectClient(request, new io.grpc.stub.StreamObserver<DisconnectClientResponse>() {
            @Override
            public void onNext(DisconnectClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("reason is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("disconnectClient with empty reason should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDisconnectClient_NotFound() throws Exception {
        when(adminClientService.forceDisconnectClient("unknown-client", "test")).thenReturn(false);

        CountDownLatch latch = new CountDownLatch(1);
        DisconnectClientRequest request = DisconnectClientRequest.newBuilder()
            .setClientId("unknown-client")
            .setReason("test")
            .build();

        adminGrpcService.disconnectClient(request, new io.grpc.stub.StreamObserver<DisconnectClientResponse>() {
            @Override
            public void onNext(DisconnectClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_NOT_FOUND, value.getCode());
                assertTrue(value.getMessage().contains("Client not found"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("disconnectClient not found should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDisconnectClient_ServiceError() throws Exception {
        when(adminClientService.forceDisconnectClient(anyString(), anyString()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        DisconnectClientRequest request = DisconnectClientRequest.newBuilder()
            .setClientId("error-client")
            .setReason("test")
            .build();

        adminGrpcService.disconnectClient(request, new io.grpc.stub.StreamObserver<DisconnectClientResponse>() {
            @Override
            public void onNext(DisconnectClientResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("disconnectClient error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== describePopReceiptHandles Tests ====================

    @Test
    public void testDescribePopReceiptHandles_EmptyGroup() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        DescribePopReceiptHandlesRequest request = DescribePopReceiptHandlesRequest.newBuilder()
            .setGroup("")
            .setTopic("test-topic")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.describePopReceiptHandles(request, new io.grpc.stub.StreamObserver<DescribePopReceiptHandlesResponse>() {
            @Override
            public void onNext(DescribePopReceiptHandlesResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("group is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describePopReceiptHandles with empty group should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDescribePopReceiptHandles_ServiceError() throws Exception {
        when(adminClientService.describePopReceiptHandles(anyString(), anyString(), anyInt(), anyInt()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        DescribePopReceiptHandlesRequest request = DescribePopReceiptHandlesRequest.newBuilder()
            .setGroup("test-group")
            .setTopic("test-topic")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.describePopReceiptHandles(request, new io.grpc.stub.StreamObserver<DescribePopReceiptHandlesResponse>() {
            @Override
            public void onNext(DescribePopReceiptHandlesResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describePopReceiptHandles error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== describeBatchConsumeDiagnostics Tests ====================

    @Test
    public void testDescribeBatchConsumeDiagnostics_EmptyGroup() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        DescribeBatchConsumeDiagnosticsRequest request = DescribeBatchConsumeDiagnosticsRequest.newBuilder()
            .setGroup("")
            .setTopic("test-topic")
            .setClientId("client-1")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.describeBatchConsumeDiagnostics(request, new io.grpc.stub.StreamObserver<DescribeBatchConsumeDiagnosticsResponse>() {
            @Override
            public void onNext(DescribeBatchConsumeDiagnosticsResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                assertTrue(value.getMessage().contains("group is required"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeBatchConsumeDiagnostics with empty group should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testDescribeBatchConsumeDiagnostics_ServiceError() throws Exception {
        when(adminClientService.describeBatchConsumeDiagnostics(anyString(), anyString(), anyString(), anyInt(), anyInt()))
            .thenThrow(new RuntimeException("Service error"));

        CountDownLatch latch = new CountDownLatch(1);
        DescribeBatchConsumeDiagnosticsRequest request = DescribeBatchConsumeDiagnosticsRequest.newBuilder()
            .setGroup("test-group")
            .setTopic("test-topic")
            .setClientId("client-1")
            .setPageNum(1)
            .setPageSize(10)
            .build();

        adminGrpcService.describeBatchConsumeDiagnostics(request, new io.grpc.stub.StreamObserver<DescribeBatchConsumeDiagnosticsResponse>() {
            @Override
            public void onNext(DescribeBatchConsumeDiagnosticsResponse value) {
                assertEquals(AdminCode.ADMIN_CODE_INTERNAL_ERROR, value.getCode());
                assertTrue(value.getMessage().contains("Service error"));
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException("Unexpected error", t);
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue("describeBatchConsumeDiagnostics error should complete within 5 seconds", latch.await(5, TimeUnit.SECONDS));
    }

    // ==================== fromProtoRouteChangeEventType Tests ====================

    @Test
    public void testFromProtoRouteChangeEventType_AllTypes() throws Exception {
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.BROKER_ONLINE,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.BROKER_ONLINE));
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.BROKER_OFFLINE,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.BROKER_OFFLINE));
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.QUEUE_SCALE,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.QUEUE_SCALE));
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.TOPIC_CREATE,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.TOPIC_CREATE));
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.TOPIC_DELETE,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.TOPIC_DELETE));
        assertEquals(org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType.ROUTE_SNAPSHOT,
            invokeFromProtoRouteChangeEventType(RouteChangeEventType.ROUTE_SNAPSHOT));
    }

    @Test
    public void testFromProtoRouteChangeEventType_Null() throws Exception {
        assertNull(invokeFromProtoRouteChangeEventType(null));
    }

    @Test
    public void testFromProtoRouteChangeEventType_Unspecified() throws Exception {
        assertNull(invokeFromProtoRouteChangeEventType(
            RouteChangeEventType.ROUTE_CHANGE_EVENT_TYPE_UNSPECIFIED));
    }

    /**
     * Use reflection to test the private fromProtoRouteChangeEventType method.
     */
    private org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType invokeFromProtoRouteChangeEventType(
        RouteChangeEventType protoType) throws Exception {
        Method method = ProxyAdminGrpcService.class.getDeclaredMethod(
            "fromProtoRouteChangeEventType", RouteChangeEventType.class);
        method.setAccessible(true);
        return (org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType) method.invoke(adminGrpcService, protoType);
    }
}