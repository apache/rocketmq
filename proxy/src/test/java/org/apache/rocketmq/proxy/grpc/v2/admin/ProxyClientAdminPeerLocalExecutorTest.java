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
import java.util.NoSuchElementException;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminPeerLocalExecutorTest {

    @Test
    public void executeListByGroupUsesLocalQueryAndReturnsPeerPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientAdminPeerLocalExecutor executor = newExecutor(" proxy-b ", delegate);
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), "client-a");
        when(delegate.listClientsByGroup(eq("group-a"), any(ProxyClientQuery.class))).thenReturn(page);
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setProxyId("proxy-a")
            .setGroup(" group-a ")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(20)
            .setPageToken(" client-10 ")
            .build();

        ProxyClientAdminPeerResponse<?> response = executor.execute(proxyContext(), request);

        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(delegate).listClientsByGroup(eq("group-a"), queryCaptor.capture());
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        assertThat(response.getBody()).isSameAs(page);
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(queryCaptor.getValue().getProxyId()).isNull();
        assertThat(queryCaptor.getValue().getGroup()).isEqualTo("group-a");
        assertThat(queryCaptor.getValue().getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(queryCaptor.getValue().getPageSize()).isEqualTo(20);
        assertThat(queryCaptor.getValue().getPageToken()).isEqualTo("client-10");
    }

    @Test
    public void executeDescribeMapsActivityErrorToPeerError() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ProxyClientAdminPeerLocalExecutor executor = newExecutor("proxy-b", delegate);
        when(delegate.describeClient("missing-client"))
            .thenThrow(new NoSuchElementException("Client not found: missing-client"));
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-b")
            .setClientId(" missing-client ")
            .build();

        ProxyClientAdminPeerResponse<?> response = executor.execute(proxyContext(), request);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(response.getErrorMessage()).contains("missing-client");
    }

    @Test
    public void executeMapsOkActivityResultWithoutBodyToPeerError() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminPeerLocalExecutor executor = new ProxyClientAdminPeerLocalExecutor("proxy-b", activity);
        when(activity.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                null
            )
        );
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .build();

        ProxyClientAdminPeerResponse<?> response = executor.execute(proxyContext(), request);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getProxyId()).isEqualTo("proxy-b");
        assertThat(response.getBody()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR.name());
        assertThat(response.getErrorMessage()).contains("peer result body is required");
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
}
