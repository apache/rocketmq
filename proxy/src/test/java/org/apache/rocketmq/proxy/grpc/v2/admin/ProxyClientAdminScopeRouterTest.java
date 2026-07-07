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
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminAuthorizationService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsRecorder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsResult;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    public void listClientsLocalProxyAllowsMissingCoordinatorWhenCoordinatorScopesDisabled() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, null, false);
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
    public void listClientsAllProxiesAuthorizesBeforeCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            authorizationService,
            metricsRecorder
        );
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(eq(ctx), any(ProxyClientQuery.class))).thenReturn(okResult(page("client-a")));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        InOrder inOrder = inOrder(authorizationService, coordinator);
        inOrder.verify(authorizationService)
            .authorize(ctx.getSubject(), ClientAdminOperation.LIST_CLIENTS, "127.0.0.1");
        inOrder.verify(coordinator).listClients(eq(ctx), any(ProxyClientQuery.class));
    }

    @Test
    public void listClientsAllProxiesAuthorizationFailureSkipsCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            authorizationService,
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        doThrow(new AuthorizationException("denied")).when(authorizationService)
            .authorize(any(), eq(ClientAdminOperation.LIST_CLIENTS), any());

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getBody()).isNull();
        verify(coordinator, never()).listClients(any(), any(ProxyClientQuery.class));
        verify(metricsRecorder).record(
            eq(ClientAdminOperation.LIST_CLIENTS),
            eq(ClientAdminMetricsResult.UNAUTHORIZED),
            anyLong()
        );
        verifyNoMoreInteractions(metricsRecorder);
    }

    @Test
    public void listClientsAllProxiesRecordsSingleMetricsResult() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        List<ClientAdminMetricsResult> results = new ArrayList<>();
        ClientAdminMetricsRecorder metricsRecorder = (operation, result, latencyMillis) -> {
            assertThat(operation).isEqualTo(ClientAdminOperation.LIST_CLIENTS);
            results.add(result);
        };
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            (subject, operation, sourceIp) -> {
            },
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client"),
                page("stale-client")
            )
        );

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(results).containsExactly(ClientAdminMetricsResult.NOT_FOUND);
    }

    @Test
    public void listClientsAllProxiesRecordsTimeoutMetricsResult() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            (subject, operation, sourceIp) -> {
            },
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.PROXY_TIMEOUT, "peer discovery timeout"),
                null
            )
        );

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.PROXY_TIMEOUT);
        assertThat(result.getBody()).isNull();
        verify(metricsRecorder).record(
            eq(ClientAdminOperation.LIST_CLIENTS),
            eq(ClientAdminMetricsResult.TIMEOUT),
            anyLong()
        );
        verifyNoMoreInteractions(metricsRecorder);
    }

    @Test
    public void listClientsAllProxiesRecordsTooManyRequestsMetricsResult() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            (subject, operation, sourceIp) -> {
            },
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.TOO_MANY_REQUESTS, "peer is throttled"),
                null
            )
        );

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.TOO_MANY_REQUESTS);
        assertThat(result.getBody()).isNull();
        verify(metricsRecorder).record(
            eq(ClientAdminOperation.LIST_CLIENTS),
            eq(ClientAdminMetricsResult.TOO_MANY_REQUESTS),
            anyLong()
        );
        verifyNoMoreInteractions(metricsRecorder);
    }

    @Test
    public void listClientsAllProxiesRecordsNotImplementedMetricsResult() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            (subject, operation, sourceIp) -> {
            },
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_IMPLEMENTED, "peer service is not implemented"),
                null
            )
        );

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_IMPLEMENTED);
        assertThat(result.getBody()).isNull();
        verify(metricsRecorder).record(
            eq(ClientAdminOperation.LIST_CLIENTS),
            eq(ClientAdminMetricsResult.NOT_IMPLEMENTED),
            anyLong()
        );
        verifyNoMoreInteractions(metricsRecorder);
    }

    @Test
    public void listClientsLocalProxyDoesNotRunRouterAuthorizationOrMetrics() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            authorizationService,
            metricsRecorder
        );
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
        verifyNoInteractions(authorizationService, metricsRecorder);
    }

    @Test
    public void listClientsLocalProxyRestoresInterruptWhenActivityIsInterrupted() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .build();
        when(activity.listClients(ctx, request)).thenAnswer(invocation -> {
            throwUnchecked(new InterruptedException("local activity interrupted"));
            return null;
        });

        try {
            ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

            assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
            assertThat(result.getStatus().getMessage()).contains("local activity interrupted");
            assertThat(result.getBody()).isNull();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void listClientsLocalProxyRestoresInterruptWhenActivityFailureWrapsInterruptedException() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .build();
        when(activity.listClients(ctx, request)).thenThrow(
            new CompletionException(new InterruptedException("wrapped local activity interrupted"))
        );

        try {
            ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

            assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
            assertThat(result.getStatus().getMessage()).contains("wrapped local activity interrupted");
            assertThat(result.getBody()).isNull();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void listClientsAllProxiesRestoresInterruptWhenCoordinatorIsInterrupted() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            (subject, operation, sourceIp) -> {
            },
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenAnswer(invocation -> {
            throwUnchecked(new InterruptedException("coordinator interrupted"));
            return null;
        });

        try {
            ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

            assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
            assertThat(result.getStatus().getMessage()).contains("coordinator interrupted");
            assertThat(result.getBody()).isNull();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(metricsRecorder).record(
                eq(ClientAdminOperation.LIST_CLIENTS),
                eq(ClientAdminMetricsResult.INTERNAL_ERROR),
                anyLong()
            );
        } finally {
            Thread.interrupted();
        }
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
    public void listClientsAllProxiesDisabledRecordsBadRequestMetricsWithoutAuthorization() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            false,
            authorizationService,
            metricsRecorder
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        verifyNoInteractions(authorizationService);
        verify(coordinator, never()).listClients(any(), any(ProxyClientQuery.class));
        verify(metricsRecorder).record(
            eq(ClientAdminOperation.LIST_CLIENTS),
            eq(ClientAdminMetricsResult.BAD_REQUEST),
            anyLong()
        );
        verifyNoMoreInteractions(metricsRecorder);
    }

    @Test
    public void listClientsDropsCoordinatorErrorBody() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(coordinator.listClients(any(), any(ProxyClientQuery.class))).thenReturn(
            new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client"),
                page("stale-client")
            )
        );

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(result.getStatus().getMessage()).contains("missing client");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsProxyIdDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setPageToken("client-b")
            .build();
        ProxyClientPage page = page("client-c");
        when(coordinator.listClients(eq(ctx), any(ProxyClientQuery.class))).thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClients(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClients(eq(ctx), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(queryCaptor.getValue().getProxyId()).isEqualTo("proxy-a");
        assertThat(queryCaptor.getValue().getPageToken()).isEqualTo("client-b");
        verify(activity, never()).listClients(any(), any(ProxyClientAdminListClientsRequest.class));
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
    public void describeClientProxyIdAuthorizesDescribeOperationBeforeCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ClientAdminMetricsRecorder metricsRecorder = mock(ClientAdminMetricsRecorder.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(
            activity,
            coordinator,
            true,
            authorizationService,
            metricsRecorder
        );
        ProxyContext ctx = proxyContext();
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setClientId("client-a")
            .build();
        when(coordinator.describeClient(ctx, request)).thenReturn(okResult(client("client-a")));

        ProxyClientAdminResult<ProxyClientInfo> result = router.describeClient(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        InOrder inOrder = inOrder(authorizationService, coordinator);
        inOrder.verify(authorizationService)
            .authorize(ctx.getSubject(), ClientAdminOperation.DESCRIBE_CLIENT, "127.0.0.1");
        inOrder.verify(coordinator).describeClient(ctx, request);
    }

    @Test
    public void describeClientAllProxiesDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
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
    public void listClientsByGroupProxyIdDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-b")
            .setGroup("group-a")
            .build();
        ProxyClientPage page = page("client-b");
        when(coordinator.listClientsByGroup(eq(ctx), eq("group-a"), any(ProxyClientQuery.class)))
            .thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClientsByGroup(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClientsByGroup(eq(ctx), eq("group-a"), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(queryCaptor.getValue().getProxyId()).isEqualTo("proxy-b");
        assertThat(queryCaptor.getValue().getGroup()).isEqualTo("group-a");
        verify(activity, never()).listClientsByGroup(any(), any(ProxyClientAdminListClientsByGroupRequest.class));
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

    @Test
    public void listClientsByTopicProxyIdDelegatesToCoordinator() {
        ProxyClientAdminActivity activity = mock(ProxyClientAdminActivity.class);
        ProxyClientAdminCoordinatorService coordinator = mock(ProxyClientAdminCoordinatorService.class);
        ProxyClientAdminScopeRouter router = new ProxyClientAdminScopeRouter(activity, coordinator);
        ProxyContext ctx = proxyContext();
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-c")
            .setTopic("topic-a")
            .build();
        ProxyClientPage page = page("client-c");
        when(coordinator.listClientsByTopic(eq(ctx), eq("topic-a"), any(ProxyClientQuery.class)))
            .thenReturn(okResult(page));

        ProxyClientAdminResult<ProxyClientPage> result = router.listClientsByTopic(ctx, request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(coordinator).listClientsByTopic(eq(ctx), eq("topic-a"), queryCaptor.capture());
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(queryCaptor.getValue().getProxyId()).isEqualTo("proxy-c");
        assertThat(queryCaptor.getValue().getTopic()).isEqualTo("topic-a");
        verify(activity, never()).listClientsByTopic(any(), any(ProxyClientAdminListClientsByTopicRequest.class));
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
            .setSubject(User.of("admin"))
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }

    private static void throwUnchecked(InterruptedException interruptedException) {
        ProxyClientAdminScopeRouterTest.<RuntimeException>throwAny(interruptedException);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwAny(Throwable throwable) throws T {
        throw (T) throwable;
    }
}
