/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.ClientType;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminInProcessPeerClientTest {

    @Test
    public void inProcessPeerClientListsStableProxyIdsAndDelegatesToTargetExecutor() {
        ClientAdminService proxyBDelegate = mock(ClientAdminService.class);
        ClientAdminService proxyADelegate = mock(ClientAdminService.class);
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), "client-a");
        when(proxyBDelegate.listClients(any())).thenReturn(page);
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put(" proxy-b ", newExecutor("proxy-b", proxyBDelegate));
        executors.put(" proxy-a ", newExecutor("proxy-a", proxyADelegate));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminInProcessPeerClient(executors);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setPageSize(10)
            .setPageToken("client-10")
            .build();

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-b ", request);

        assertThat(peerClient.listProxyIds()).containsExactly("proxy-a", "proxy-b");
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        ProxyClientPage responsePage = (ProxyClientPage) response.getBody();
        assertThat(responsePage.getNextPageToken()).isEqualTo("client-a");
        assertThat(responsePage.getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a");
        assertThat(responsePage.getClients())
            .extracting(ProxyClientInfo::getProxyId)
            .containsExactly("proxy-b");
    }

    @Test
    public void inProcessPeerClientReturnsPeerErrorForMissingTargetProxy() {
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", newExecutor("proxy-a", mock(ClientAdminService.class)));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminInProcessPeerClient(executors);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-missing ", request);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-missing");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(response.getErrorMessage()).contains("proxy-missing");
    }

    @Test
    public void inProcessPeerClientMapsExecutorFailureToPeerError() {
        ProxyClientAdminPeerLocalExecutor executor = mock(ProxyClientAdminPeerLocalExecutor.class);
        when(executor.getLocalProxyId()).thenReturn("proxy-a");
        when(executor.execute(any(), any())).thenThrow(new IllegalStateException("boom"));
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", executor);
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminInProcessPeerClient(executors);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-a ", request);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo("INTERNAL_SERVER_ERROR");
        assertThat(response.getErrorMessage()).contains("boom");
        verify(executor).execute(any(), any());
    }

    @Test
    public void inProcessPeerClientRestoresInterruptWhenExecutorIsInterrupted() {
        ProxyClientAdminPeerLocalExecutor executor = mock(ProxyClientAdminPeerLocalExecutor.class);
        when(executor.getLocalProxyId()).thenReturn("proxy-a");
        when(executor.execute(any(), any())).thenAnswer(invocation -> {
            throwUnchecked(new InterruptedException("in-process peer executor interrupted"));
            return null;
        });
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", executor);
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminInProcessPeerClient(executors);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();

        try {
            ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-a ", request);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getBody()).isNull();
            assertThat(response.getErrorCode()).isEqualTo("INTERNAL_SERVER_ERROR");
            assertThat(response.getErrorMessage()).contains("in-process peer executor interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void inProcessPeerClientRestoresInterruptWhenExecutorFailureWrapsInterruptedException() {
        ProxyClientAdminPeerLocalExecutor executor = mock(ProxyClientAdminPeerLocalExecutor.class);
        when(executor.getLocalProxyId()).thenReturn("proxy-a");
        when(executor.execute(any(), any())).thenThrow(
            new CompletionException(new InterruptedException("wrapped in-process peer executor interrupted"))
        );
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", executor);
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminInProcessPeerClient(executors);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();

        try {
            ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-a ", request);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getBody()).isNull();
            assertThat(response.getErrorCode()).isEqualTo("INTERNAL_SERVER_ERROR");
            assertThat(response.getErrorMessage()).contains("wrapped in-process peer executor interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void inProcessPeerClientRejectsDuplicateNormalizedProxyIds() {
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", newExecutor("proxy-a", mock(ClientAdminService.class)));
        executors.put(" proxy-a ", newExecutor("proxy-a", mock(ClientAdminService.class)));

        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerClient(executors))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate proxyId")
            .hasMessageContaining("proxy-a");
    }

    @Test
    public void inProcessPeerClientRejectsExecutorProxyIdMismatch() {
        Map<String, ProxyClientAdminPeerLocalExecutor> executors = new LinkedHashMap<>();
        executors.put("proxy-a", newExecutor("proxy-b", mock(ClientAdminService.class)));

        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerClient(executors))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("executor proxyId mismatch")
            .hasMessageContaining("proxy-a")
            .hasMessageContaining("proxy-b");
    }

    @Test
    public void inProcessPeerClientRejectsEmptyExecutorMap() {
        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerClient(Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one executor is required");
    }

    private static ProxyClientAdminPeerLocalExecutor newExecutor(String localProxyId, ClientAdminService delegate) {
        return new ProxyClientAdminPeerLocalExecutor(
            localProxyId,
            new ProxyClientAdminActivity(new AuthorizingClientAdminService(delegate, (subject, operation, sourceIp) -> {
            }))
        );
    }

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "1.0.0",
            1000L,
            2000L
        );
    }

    private static void throwUnchecked(InterruptedException interruptedException) {
        ProxyClientAdminInProcessPeerClientTest.<RuntimeException>throwAny(interruptedException);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAny(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
