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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminPeerMessageClientTest {

    @Test
    public void messagePeerClientListsProxyIdsFromTransport() {
        RecordingMessageTransport transport = new RecordingMessageTransport();
        transport.addProxy("proxy-b", null);
        transport.addProxy("proxy-a", null);
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(transport);

        assertThat(peerClient.listProxyIds()).containsExactly("proxy-b", "proxy-a");
    }

    @Test
    public void messagePeerClientExecutesListRequestThroughRawTransport() {
        ClientAdminService adminService = mock(ClientAdminService.class);
        when(adminService.listClients(any())).thenReturn(new ProxyClientPage(
            Collections.singletonList(client("client-a", "proxy-a")),
            "client-a"
        ));
        RecordingMessageTransport transport = new RecordingMessageTransport();
        transport.addProxy("proxy-a", new ProxyClientAdminPeerMessageHandler(newExecutor("proxy-a", adminService)));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(transport);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setPageSize(20)
            .setPageToken("client-0")
            .build();

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-a ", request);

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        ProxyClientPage page = (ProxyClientPage) response.getBody();
        assertThat(page.getNextPageToken()).isEqualTo("client-a");
        assertThat(page.getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a");
        assertThat(page.getClients())
            .extracting(ProxyClientInfo::getProxyId)
            .containsExactly("proxy-a");
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(adminService).listClients(queryCaptor.capture());
        assertThat(queryCaptor.getValue().getBoundedPageSize()).isEqualTo(20);
        assertThat(queryCaptor.getValue().getPageToken()).isEqualTo("client-0");
        assertThat(transport.requestMessages("proxy-a")).hasSize(1);
    }

    @Test
    public void messagePeerClientExecutesDescribeRequestThroughRawTransport() {
        ClientAdminService adminService = mock(ClientAdminService.class);
        when(adminService.describeClient("client-b")).thenReturn(client("client-b", "proxy-b"));
        RecordingMessageTransport transport = new RecordingMessageTransport();
        transport.addProxy("proxy-b", new ProxyClientAdminPeerMessageHandler(newExecutor("proxy-b", adminService)));
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(transport);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId(" client-b ")
            .build();

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-b ", request);

        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        ProxyClientInfo clientInfo = (ProxyClientInfo) response.getBody();
        assertThat(clientInfo.getClientId()).isEqualTo("client-b");
        assertThat(clientInfo.getProxyId()).isEqualTo("proxy-b");
        verify(adminService).describeClient("client-b");
    }

    @Test
    public void messagePeerClientMapsTransportFailureToPeerError() {
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(
            new ThrowingMessageTransport()
        );

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(
            proxyContext(),
            " proxy-a ",
            ProxyClientAdminPeerRequest.newBuilder()
                .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
                .build()
        );

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("boom");
    }

    @Test
    public void messagePeerClientMapsMissingRequestToBadRequestPeerError() {
        RecordingMessageTransport transport = new RecordingMessageTransport();
        transport.addProxy("proxy-a", null);
        ProxyClientAdminPeerClient peerClient = new ProxyClientAdminPeerMessageClient(transport);

        ProxyClientAdminPeerResponse<?> response = peerClient.execute(proxyContext(), " proxy-a ", null);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("request is required");
        assertThat(transport.requestMessages("proxy-a")).isNull();
    }

    @Test
    public void messageHandlerEncodesPeerErrorForMalformedRequest() {
        ProxyClientAdminPeerMessageHandler handler = new ProxyClientAdminPeerMessageHandler(
            newExecutor("proxy-a", mock(ClientAdminService.class))
        );

        String responseMessage = handler.execute(proxyContext(), " ");
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(responseMessage);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-a");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.BAD_REQUEST.name());
        assertThat(response.getErrorMessage()).contains("peer request message is required");
    }

    private static ProxyClientAdminPeerLocalExecutor newExecutor(String localProxyId,
        ClientAdminService clientAdminService) {
        return new ProxyClientAdminPeerLocalExecutor(localProxyId, clientAdminService);
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
            Collections.singleton("group-a"),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "1.0.0",
            proxyId,
            1000L,
            2000L
        );
    }

    private static class RecordingMessageTransport implements ProxyClientAdminPeerMessageTransport {
        private final Map<String, ProxyClientAdminPeerMessageHandler> handlers = new LinkedHashMap<>();
        private final Map<String, List<String>> requestMessages = new LinkedHashMap<>();

        void addProxy(String proxyId, ProxyClientAdminPeerMessageHandler handler) {
            this.handlers.put(proxyId, handler);
        }

        List<String> requestMessages(String proxyId) {
            return this.requestMessages.get(proxyId);
        }

        @Override
        public List<String> listProxyIds() {
            return new ArrayList<>(this.handlers.keySet());
        }

        @Override
        public String execute(ProxyContext ctx, String proxyId, String requestMessage) {
            this.requestMessages.computeIfAbsent(proxyId, ignored -> new ArrayList<>()).add(requestMessage);
            ProxyClientAdminPeerMessageHandler handler = this.handlers.get(proxyId);
            if (handler == null) {
                return ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(
                    ProxyClientAdminPeerResponse.error(proxyId, Code.NOT_FOUND.name(), "Proxy not found: " + proxyId)
                );
            }
            return handler.execute(ctx, requestMessage);
        }
    }

    private static class ThrowingMessageTransport implements ProxyClientAdminPeerMessageTransport {
        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList("proxy-a");
        }

        @Override
        public String execute(ProxyContext ctx, String proxyId, String requestMessage) {
            throw new IllegalStateException("boom");
        }
    }
}
