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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminGrpcServiceTest {

    @Mock
    private ProxyAdminClientService adminClientService;

    private ProxyAdminGrpcService adminGrpcService;

    @Before
    public void before() {
        adminGrpcService = new ProxyAdminGrpcService(adminClientService, 2);
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
            .setLanguage(apache.rocketmq.proxy.admin.v1.ClientLanguage.CLIENT_LANGUAGE_JAVA)
            .setConnectTimeStart(1000L)
            .setConnectTimeEnd(2000L)
            .setPageNum(1)
            .setPageSize(20)
            .build();

        assertEquals("test-group", request.getGroup());
        assertEquals("test-topic", request.getTopic());
        assertEquals("prefix-", request.getClientIdPrefix());
        assertEquals(apache.rocketmq.proxy.admin.v1.ClientLanguage.CLIENT_LANGUAGE_JAVA, request.getLanguage());
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
            .setPagination(apache.rocketmq.proxy.admin.v1.Pagination.newBuilder()
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
}