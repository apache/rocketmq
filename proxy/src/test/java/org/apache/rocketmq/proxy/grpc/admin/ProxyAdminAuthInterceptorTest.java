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

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminAuthInterceptorTest extends InitConfigTest {

    @Mock
    private MessagingProcessor messagingProcessor;

    private static final MethodDescriptor.Marshaller<byte[]> BYTE_MARSHALLER =
        new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(byte[] value) {
                return new ByteArrayInputStream(value == null ? new byte[0] : value);
            }

            @Override
            public byte[] parse(InputStream stream) {
                return new byte[0];
            }
        };

    private static MethodDescriptor<byte[], byte[]> method(String name) {
        return MethodDescriptor.<byte[], byte[]>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("apache.rocketmq.v2.Admin/" + name)
            .setRequestMarshaller(BYTE_MARSHALLER)
            .setResponseMarshaller(BYTE_MARSHALLER)
            .build();
    }

    @SuppressWarnings("unchecked")
    private static ServerCall<byte[], byte[]> serverCall(String methodName) {
        ServerCall<byte[], byte[]> call = mock(ServerCall.class);
        when(call.getMethodDescriptor()).thenReturn(method(methodName));
        return call;
    }

    // ---------------------------------------------------------------------
    // per-method resource / action mapping (read-only vs high-privilege isolation)
    // ---------------------------------------------------------------------

    @Test
    public void everyAdminMethodMapsToExpectedResourceAndAction() {
        // the full per-method ACL table: resource module + action. Read-only RPCs use GET/LIST,
        // high-privilege mutations use UPDATE/DELETE/PUB, and each is scoped to its own resource.
        Map<String, ProxyAdminAuthInterceptor.ResourceAction> expected = new LinkedHashMap<>();
        expected.put("GetProxyRuntimeStats", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.GET));
        expected.put("GetTopicRoute", ra(ProxyAdminAuthInterceptor.RESOURCE_ROUTE, Action.GET));
        expected.put("DescribeTopicStatus", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.GET));
        expected.put("ListSubscription", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.LIST));
        expected.put("DescribeSubscription", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.GET));
        expected.put("ListConsumerConnection", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.LIST));
        expected.put("DescribeGroupAccumulation", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.GET));
        expected.put("GetConsumerRunningInfo", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.GET));
        expected.put("QueryTimeSpan", ra(ProxyAdminAuthInterceptor.RESOURCE_CLIENT, Action.GET));
        expected.put("QueryMessage", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.GET));
        expected.put("ChangeLogLevel", ra(ProxyAdminAuthInterceptor.RESOURCE_CONFIG, Action.UPDATE));
        expected.put("DeleteSubscription", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.DELETE));
        expected.put("ResetGroupOffset", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.UPDATE));
        expected.put("AdminSendMessage", ra(ProxyAdminAuthInterceptor.RESOURCE_OPS, Action.PUB));
        expected.put("PrintThreadStackTrace", ra(ProxyAdminAuthInterceptor.RESOURCE_CONNECTION, Action.UPDATE));
        expected.put("VerifyMessage", ra(ProxyAdminAuthInterceptor.RESOURCE_CONNECTION, Action.UPDATE));

        Set<String> resources = new HashSet<>();
        for (Map.Entry<String, ProxyAdminAuthInterceptor.ResourceAction> e : expected.entrySet()) {
            ProxyAdminAuthInterceptor.ResourceAction actual =
                ProxyAdminAuthInterceptor.resolveResourceAction(e.getKey());
            assertNotNull("missing mapping for " + e.getKey(), actual);
            assertEquals("resource for " + e.getKey(), e.getValue().resource, actual.resource);
            assertEquals("action for " + e.getKey(), e.getValue().action, actual.action);
            resources.add(actual.resource);
        }
        // five distinct modules, all under the proxy.admin.* namespace
        assertEquals(5, resources.size());
        for (String resource : resources) {
            assertTrue(resource.startsWith("proxy.admin."));
        }
    }

    private static ProxyAdminAuthInterceptor.ResourceAction ra(String resource, Action action) {
        return new ProxyAdminAuthInterceptor.ResourceAction(resource, action);
    }

    // ---------------------------------------------------------------------
    // behavior modes
    // ---------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void openModePassesThroughWhenClusterAuthDisabled() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(false);
        authConfig.setAuthorizationEnabled(false);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);

        ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
        ServerCall<byte[], byte[]> call = serverCall("ListConsumerConnection");
        ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, new Metadata(), next);
        verify(next).startCall(any(), any());
        verify(call, never()).close(any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failClosedRejectsWhenRequireAuthButClusterAuthDisabled() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(false);
        authConfig.setAuthorizationEnabled(false);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(true);
        try {
            ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
            ServerCall<byte[], byte[]> call = serverCall("ListClients");
            ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

            interceptor.interceptCall(call, new Metadata(), next);
            verify(next, never()).startCall(any(), any());
            org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
            verify(call).close(statusCaptor.capture(), any(Metadata.class));
            assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
            assertTrue(statusCaptor.getValue().getDescription().contains("grpcAdminServerAuthEnable"));
        } finally {
            ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failClosedRejectsWhenRequireAuthButAuthorizationDisabled() {
        // authentication on but authorization off: the ACL evaluator is globally gated off, so
        // enforcement would silently pass. Fail-closed must refuse instead of serving an
        // unauthorized (potentially destructive) admin RPC to any authenticated identity.
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthorizationEnabled(false);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(true);
        try {
            ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
            ServerCall<byte[], byte[]> call = serverCall("DeleteSubscription");
            ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

            interceptor.interceptCall(call, new Metadata(), next);
            verify(next, never()).startCall(any(), any());
            org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
            verify(call).close(statusCaptor.capture(), any(Metadata.class));
            assertEquals(Status.Code.FAILED_PRECONDITION, statusCaptor.getValue().getCode());
            assertTrue(statusCaptor.getValue().getDescription().contains("authorizationEnabled"));
        } finally {
            ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failClosedRejectsAnonymousWhenRequireAuth() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthorizationEnabled(true);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(true);
        try {
            ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
            ServerCall<byte[], byte[]> call = serverCall("PrintThreadStackTrace");
            ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

            // empty metadata: no credentials at all
            ServerCall.Listener<byte[]> listener = interceptor.interceptCall(call, new Metadata(), next);
            assertNotNull(listener);
            verify(next, never()).startCall(any(), any());
            org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
            verify(call).close(statusCaptor.capture(), any(Metadata.class));
            assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
        } finally {
            ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void authenticationFailureClosesUnauthenticated() {
        // cluster authentication on + no credentials -> evaluator throws AuthenticationException.
        // AuthenticationFactory caches evaluators per configName, so use a unique name to get
        // a fresh evaluator bound to THIS config (otherwise a stale one from another test wins).
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("proxy-admin-auth-failure-test-" + System.nanoTime());
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthorizationEnabled(false);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);

        ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
        ServerCall<byte[], byte[]> call = serverCall("ListConsumerConnection");
        ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, new Metadata(), next);
        verify(next, never()).startCall(any(), any());
        org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
        verify(call).close(statusCaptor.capture(), any(Metadata.class));
        assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
        assertNotNull(statusCaptor.getValue().getDescription());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void authorizationEnabledRejectsMissingCredentials() {
        // authorization on without authentication -> mapped method demands credentials first
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("proxy-admin-authz-only-test-" + System.nanoTime());
        authConfig.setAuthenticationEnabled(false);
        authConfig.setAuthorizationEnabled(true);
        ConfigurationManager.getProxyConfig().setGrpcAdminServerAuthEnable(false);

        ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
        ServerCall<byte[], byte[]> call = serverCall("GetTopicRoute");
        ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, new Metadata(), next);
        verify(next, never()).startCall(any(), any());
        org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
        verify(call).close(statusCaptor.capture(), any(Metadata.class));
        assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
        assertTrue(statusCaptor.getValue().getDescription().contains("missing credentials"));
    }
}
