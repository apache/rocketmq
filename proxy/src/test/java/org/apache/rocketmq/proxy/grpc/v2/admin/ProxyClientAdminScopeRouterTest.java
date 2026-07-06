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
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminScopeRouterTest {

    @Test
    public void listClientsLocalProxyDelegatesToActivity() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .build();
        ProxyClientPage page = page("client-a");
        when(activity.listClients(ctx, request)).thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        verify(activity).listClients(ctx, request);
        verify(coordinator, never()).listClients(any(), any());
    }

    @Test
    public void listClientsAllProxiesDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setClientType(ClientType.PRODUCER)
            .setPageSize(25)
            .build();
        ProxyClientPage page = page("client-b");
        when(coordinator.listClients(eq(ctx), any(ProxyClientQuery.class))).thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClients(eq(ctx), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(queryCaptor.getValue().getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(queryCaptor.getValue().getPageSize()).isEqualTo(25);
        verify(activity, never()).listClients(any(), any(ProxyClientAdminListClientsRequest.class));
    }

    @Test
    public void listClientsAllProxiesReturnsBadRequestWhenCoordinatorScopesDisabled() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator, false);
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        verify(activity, never()).listClients(any(), any(ProxyClientAdminListClientsRequest.class));
        verify(coordinator, never()).listClients(any(), any());
    }

    @Test
    public void listClientsProxyIdReturnsBadRequestUntilCoordinatorSupportsIt() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        verify(activity, never()).listClients(any(), any(ProxyClientAdminListClientsRequest.class));
        verify(coordinator, never()).listClients(any(), any());
    }

    @Test
    public void describeClientProxyIdReturnsBadRequestWhenCoordinatorScopesDisabled() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator, false);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = router.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        verify(activity, never()).describeClient(any(), any(ProxyClientAdminDescribeClientRequest.class));
        verify(coordinator, never()).describeClient(any(), any());
    }

    @Test
    public void describeClientProxyIdDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setClientId("client-a")
            .build();
        ProxyClientInfo clientInfo = client("client-a");
        when(coordinator.describeClient(ctx, request)).thenReturn(okResult(clientInfo));

        ProxyClientAdminResult<ProxyClientInfo> result = router.describeClient(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(clientInfo);
        verify(coordinator).describeClient(ctx, request);
        verify(activity, never()).describeClient(any(), any(ProxyClientAdminDescribeClientRequest.class));
    }

    @Test
    public void listClientsByGroupAllProxiesDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setGroup("group-a")
            .build();
        ProxyClientPage page = page("client-a");
        when(coordinator.listClientsByGroup(eq(ctx), eq("group-a"), any(ProxyClientQuery.class)))
            .thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClientsByGroup(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClientsByGroup(eq(ctx), eq("group-a"), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(queryCaptor.getValue().getGroup()).isEqualTo("group-a");
    }

    @Test
    public void listClientsByTopicAllProxiesDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setTopic("topic-a")
            .build();
        ProxyClientPage page = page("client-a");
        when(coordinator.listClientsByTopic(eq(ctx), eq("topic-a"), any(ProxyClientQuery.class)))
            .thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClientsByTopic(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClientsByTopic(eq(ctx), eq("topic-a"), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(queryCaptor.getValue().getTopic()).isEqualTo("topic-a");
    }

    private static <T> ProxyClientAdminResult<T> okResult(T body) {
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            body
        );
    }

    private static ProxyClientPage page(String clientId) {
        return new ProxyClientPage(Collections.singletonList(client(clientId)), "");
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

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }
}
