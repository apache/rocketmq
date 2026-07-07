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
import apache.rocketmq.v2.Code;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminInProcessPeerMessageTransportTest {

    @Test
    public void inProcessMessageTransportListsStableProxyIdsAndDelegatesToTargetHandler() {
        ClientAdminService proxyBService = mock(ClientAdminService.class);
        ClientAdminService proxyAService = mock(ClientAdminService.class);
        when(proxyBService.listClients(any())).thenReturn(new ProxyClientPage(
            Collections.singletonList(client("client-b", "proxy-b")),
            "client-b"
        ));
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put(" proxy-b ", newHandler("proxy-b", proxyBService));
        handlers.put(" proxy-a ", newHandler("proxy-a", proxyAService));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(
            new ProxyClientAdminInProcessPeerMessageTransport(handlers)
        );

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(
            proxyContext(),
            " proxy-b ",
            ProxyClientAdminPeerRequest.newBuilder()
                .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
                .setPageSize(10)
                .build()
        );

        assertThat(peerClient.listProxyIds()).containsExactly("proxy-a", "proxy-b");
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        ProxyClientPage page = (ProxyClientPage) response.getBody();
        assertThat(page.getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-b");
        assertThat(page.getClients())
            .extracting(ProxyClientInfo::getProxyId)
            .containsExactly("proxy-b");
    }

    @Test
    public void inProcessMessageTransportReturnsEncodedPeerErrorForMissingTargetProxy() {
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", newHandler("proxy-a", mock(ClientAdminService.class)));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(
            new ProxyClientAdminInProcessPeerMessageTransport(handlers)
        );

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(
            proxyContext(),
            " proxy-missing ",
            ProxyClientAdminPeerRequest.newBuilder()
                .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
                .setClientId("client-a")
                .build()
        );

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-missing");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(response.getErrorMessage()).contains("proxy-missing");
    }

    @Test
    public void inProcessMessageTransportRejectsBlankRequestMessageBeforeCallingHandler() {
        ProxyClientAdminPeerMessageHandler handler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(handler.getLocalProxyId()).thenReturn("proxy-a");
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", handler);
        ProxyClientAdminInProcessPeerMessageTransport transport =
            new ProxyClientAdminInProcessPeerMessageTransport(handlers);

        String responseMessage = transport.execute(proxyContext(), " proxy-a ", " ");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("peer request message is required");
        verify(handler, never()).execute(any(), anyString());
    }

    @Test
    public void inProcessMessageTransportRejectsOverlongRequestMessageBeforeCallingHandler() {
        ProxyClientAdminPeerMessageHandler handler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(handler.getLocalProxyId()).thenReturn("proxy-a");
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", handler);
        ProxyClientAdminInProcessPeerMessageTransport transport =
            new ProxyClientAdminInProcessPeerMessageTransport(handlers);

        String responseMessage = transport.execute(
            proxyContext(),
            "proxy-a",
            StringUtils.repeat("a", 1024 * 1024 + 1)
        );
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("peer request message length exceeds");
        verify(handler, never()).execute(any(), anyString());
    }

    @Test
    public void inProcessMessageTransportSendsNormalizedRequestMessageToHandler() {
        ProxyClientAdminPeerMessageHandler handler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(handler.getLocalProxyId()).thenReturn("proxy-a");
        when(handler.execute(any(), anyString())).thenReturn(ProxyClientAdminPeerMessageCodec.getInstance()
            .encodePageResponse(ProxyClientAdminPeerResponse.success(
                "proxy-a",
                new ProxyClientPage(Collections.emptyList(), "")
            )));
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", handler);
        ProxyClientAdminInProcessPeerMessageTransport transport =
            new ProxyClientAdminInProcessPeerMessageTransport(handlers);

        transport.execute(proxyContext(), " proxy-a ", " {\"operation\":\"LIST_CLIENTS\"} ");

        verify(handler).execute(any(), eq("{\"operation\":\"LIST_CLIENTS\"}"));
    }

    @Test
    public void inProcessMessageTransportRestoresInterruptWhenHandlerIsInterrupted() {
        ProxyClientAdminPeerMessageHandler handler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(handler.getLocalProxyId()).thenReturn("proxy-a");
        when(handler.execute(any(), anyString())).thenAnswer(invocation -> {
            throwUnchecked(new InterruptedException("in-process peer handler interrupted"));
            return null;
        });
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", handler);
        ProxyClientAdminInProcessPeerMessageTransport transport =
            new ProxyClientAdminInProcessPeerMessageTransport(handlers);

        try {
            String responseMessage = transport.execute(proxyContext(), " proxy-a ", "{\"operation\":\"LIST_CLIENTS\"}");
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getBody()).isNull();
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("in-process peer handler interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void inProcessMessageTransportRestoresInterruptWhenHandlerFailureWrapsInterruptedException() {
        ProxyClientAdminPeerMessageHandler handler = mock(ProxyClientAdminPeerMessageHandler.class);
        when(handler.getLocalProxyId()).thenReturn("proxy-a");
        when(handler.execute(any(), anyString())).thenThrow(
            new CompletionException(new InterruptedException("wrapped in-process peer handler interrupted"))
        );
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", handler);
        ProxyClientAdminInProcessPeerMessageTransport transport =
            new ProxyClientAdminInProcessPeerMessageTransport(handlers);

        try {
            String responseMessage = transport.execute(proxyContext(), " proxy-a ", "{\"operation\":\"LIST_CLIENTS\"}");
            ProxyClientAdminPeerResponse<ProxyClientPage> response =
                ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

            assertThat(response.isSuccess()).isFalse();
            assertThat(response.getProxyId()).isEqualTo("proxy-a");
            assertThat(response.getBody()).isNull();
            assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
            assertThat(response.getErrorMessage()).contains("wrapped in-process peer handler interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void inProcessMessageTransportRejectsDuplicateNormalizedProxyIds() {
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", newHandler("proxy-a", mock(ClientAdminService.class)));
        handlers.put(" proxy-a ", newHandler("proxy-a", mock(ClientAdminService.class)));

        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerMessageTransport(handlers))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate proxyId")
            .hasMessageContaining("proxy-a");
    }

    @Test
    public void inProcessMessageTransportRejectsHandlerProxyIdMismatch() {
        Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        handlers.put("proxy-a", newHandler("proxy-b", mock(ClientAdminService.class)));

        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerMessageTransport(handlers))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("handler proxyId mismatch")
            .hasMessageContaining("proxy-a")
            .hasMessageContaining("proxy-b");
    }

    @Test
    public void inProcessMessageTransportRejectsEmptyHandlerMap() {
        assertThatThrownBy(() -> new ProxyClientAdminInProcessPeerMessageTransport(Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one handler is required");
    }

    private static ProxyClientAdminPeerMessageHandler newHandler(String localProxyId,
        ClientAdminService clientAdminService) {
        return new ProxyClientAdminPeerMessageHandler(
            new ProxyClientAdminPeerLocalExecutor(localProxyId, clientAdminService)
        );
    }

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }

    private static ProxyClientInfo client(String clientId, String proxyId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "1.0.0",
            proxyId,
            1000L,
            2000L
        );
    }

    private static void throwUnchecked(InterruptedException interruptedException) {
        ProxyClientAdminInProcessPeerMessageTransportTest.<RuntimeException>throwAny(interruptedException);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAny(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
