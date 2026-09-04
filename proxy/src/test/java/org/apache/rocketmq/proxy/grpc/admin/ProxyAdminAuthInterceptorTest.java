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
import static org.junit.Assert.assertFalse;
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
    public void allAdminMethodsAreMapped() {
        String[] methods = {
            "GetProxyRuntimeStats", "GetTopicRoute", "DescribeTopicStatus", "ListSubscription",
            "DescribeSubscription", "ListConsumerConnection", "DescribeGroupAccumulation",
            "GetConsumerRunningInfo", "QueryTimeSpan", "QueryMessage", "ChangeLogLevel",
            "DeleteSubscription", "ResetGroupOffset", "AdminSendMessage", "PrintThreadStackTrace",
            "VerifyMessage"};
        for (String m : methods) {
            assertNotNull("missing permission mapping for " + m,
                ProxyAdminAuthInterceptor.resolveResourceAction(m));
        }
    }

    @Test
    public void highPrivilegeOperationsNeverMapToReadActions() {
        assertEquals(Action.UPDATE, ProxyAdminAuthInterceptor.resolveResourceAction("ResetGroupOffset").action);
        assertEquals(Action.UPDATE, ProxyAdminAuthInterceptor.resolveResourceAction("ChangeLogLevel").action);
        assertEquals(Action.UPDATE, ProxyAdminAuthInterceptor.resolveResourceAction("PrintThreadStackTrace").action);
        assertEquals(Action.UPDATE, ProxyAdminAuthInterceptor.resolveResourceAction("VerifyMessage").action);
        assertEquals(Action.DELETE, ProxyAdminAuthInterceptor.resolveResourceAction("DeleteSubscription").action);
        assertEquals(Action.PUB, ProxyAdminAuthInterceptor.resolveResourceAction("AdminSendMessage").action);
    }

    @Test
    public void readOnlyOperationsMapToReadActions() {
        assertEquals(Action.LIST, ProxyAdminAuthInterceptor.resolveResourceAction("ListSubscription").action);
        assertEquals(Action.LIST, ProxyAdminAuthInterceptor.resolveResourceAction("ListConsumerConnection").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("DescribeSubscription").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("DescribeGroupAccumulation").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("GetConsumerRunningInfo").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("QueryTimeSpan").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("QueryMessage").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("GetTopicRoute").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("GetProxyRuntimeStats").action);
        assertEquals(Action.GET, ProxyAdminAuthInterceptor.resolveResourceAction("DescribeTopicStatus").action);
    }

    @Test
    public void resourcesAreScopedPerModule() {
        assertEquals(ProxyAdminAuthInterceptor.RESOURCE_CLIENT,
            ProxyAdminAuthInterceptor.resolveResourceAction("ListConsumerConnection").resource);
        assertEquals(ProxyAdminAuthInterceptor.RESOURCE_CONFIG,
            ProxyAdminAuthInterceptor.resolveResourceAction("ChangeLogLevel").resource);
        assertEquals(ProxyAdminAuthInterceptor.RESOURCE_CONNECTION,
            ProxyAdminAuthInterceptor.resolveResourceAction("PrintThreadStackTrace").resource);
        assertEquals(ProxyAdminAuthInterceptor.RESOURCE_ROUTE,
            ProxyAdminAuthInterceptor.resolveResourceAction("GetTopicRoute").resource);
        assertEquals(ProxyAdminAuthInterceptor.RESOURCE_OPS,
            ProxyAdminAuthInterceptor.resolveResourceAction("ResetGroupOffset").resource);

        Set<String> distinct = new HashSet<>();
        distinct.add(ProxyAdminAuthInterceptor.RESOURCE_CLIENT);
        distinct.add(ProxyAdminAuthInterceptor.RESOURCE_CONFIG);
        distinct.add(ProxyAdminAuthInterceptor.RESOURCE_CONNECTION);
        distinct.add(ProxyAdminAuthInterceptor.RESOURCE_ROUTE);
        distinct.add(ProxyAdminAuthInterceptor.RESOURCE_OPS);
        assertEquals(5, distinct.size());
        for (String resource : distinct) {
            assertTrue(resource.startsWith("proxy.admin."));
        }
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
        ConfigurationManager.getProxyConfig().setProxyAdminRequireAuth(false);

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
        ConfigurationManager.getProxyConfig().setProxyAdminRequireAuth(true);
        try {
            ProxyAdminAuthInterceptor interceptor = new ProxyAdminAuthInterceptor(authConfig, messagingProcessor);
            ServerCall<byte[], byte[]> call = serverCall("ListClients");
            ServerCallHandler<byte[], byte[]> next = mock(ServerCallHandler.class);

            interceptor.interceptCall(call, new Metadata(), next);
            verify(next, never()).startCall(any(), any());
            org.mockito.ArgumentCaptor<Status> statusCaptor = org.mockito.ArgumentCaptor.forClass(Status.class);
            verify(call).close(statusCaptor.capture(), any(Metadata.class));
            assertEquals(Status.Code.UNAUTHENTICATED, statusCaptor.getValue().getCode());
            assertTrue(statusCaptor.getValue().getDescription().contains("proxyAdminRequireAuth"));
        } finally {
            ConfigurationManager.getProxyConfig().setProxyAdminRequireAuth(false);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failClosedRejectsAnonymousWhenRequireAuth() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthorizationEnabled(true);
        ConfigurationManager.getProxyConfig().setProxyAdminRequireAuth(true);
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
            ConfigurationManager.getProxyConfig().setProxyAdminRequireAuth(false);
        }
    }

    @Test
    public void resourceActionModelIsImmutablePerMethod() {
        // sanity: repeated resolution yields the same mapping (no stateful drift)
        ProxyAdminAuthInterceptor.ResourceAction first =
            ProxyAdminAuthInterceptor.resolveResourceAction("DescribeSubscription");
        ProxyAdminAuthInterceptor.ResourceAction second =
            ProxyAdminAuthInterceptor.resolveResourceAction("DescribeSubscription");
        assertEquals(first.resource, second.resource);
        assertEquals(first.action, second.action);
        assertFalse(first.resource.isEmpty());
    }
}
