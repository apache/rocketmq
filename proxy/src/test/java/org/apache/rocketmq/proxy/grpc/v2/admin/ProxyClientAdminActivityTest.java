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
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminAuthorizationService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyClientAdminActivityTest {

    @Test
    public void listClientsReturnsOkStatusAndPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClients(query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientPage> result = activity.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
    }

    @Test
    public void listClientsByGroupReturnsOkStatusAndPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClientsByGroup("group-a", query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByGroup(proxyContext(), "group-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
    }

    @Test
    public void listClientsByTopicReturnsOkStatusAndPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClientsByTopic("topic-a", query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByTopic(proxyContext(), "topic-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody()).isSameAs(page);
    }

    @Test
    public void listClientViewsReturnsConvertedPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), "client-a");
        when(delegate.listClients(query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getNextPageToken()).isEqualTo("client-a");
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientAdminClientView::getClientId)
            .containsExactly("client-a");
    }

    @Test
    public void describeClientViewReturnsConvertedClient() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        when(delegate.describeClient("client-a")).thenReturn(client("client-a"));

        ProxyClientAdminResult<ProxyClientAdminClientView> result =
            activity.describeClientView(proxyContext(), "client-a");

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClientId()).isEqualTo("client-a");
        assertThat(result.getBody().getTopics()).containsExactly("topic-a");
    }

    @Test
    public void listClientViewsByGroupReturnsConvertedPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), "");
        when(delegate.listClientsByGroup("group-a", query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByGroup(proxyContext(), "group-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientAdminClientView::getClientId)
            .containsExactly("client-a");
    }

    @Test
    public void listClientViewsByTopicReturnsConvertedPage() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), "");
        when(delegate.listClientsByTopic("topic-a", query)).thenReturn(page);

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByTopic(proxyContext(), "topic-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientAdminClientView::getClientId)
            .containsExactly("client-a");
    }

    @Test
    public void listClientViewsPropagatesErrorStatusWithoutBody() {
        ProxyClientReadService readService = new ProxyClientReadService();
        readService.upsertClient(client("client-a"));
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(readService));

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(
            proxyContext(),
            ProxyClientQuery.newBuilder().setPageToken("missing-client").build()
        );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Invalid page token");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientViewsMapsNullSuccessBodyToInternalServerError() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        when(delegate.listClients(query)).thenReturn(null);

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("result body is required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientViewsAcceptsRequestDto() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClients(any(ProxyClientQuery.class))).thenReturn(page);
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPageSize(10)
            .setPageToken("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(proxyContext(), request);

        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(delegate).listClients(queryCaptor.capture());
        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientAdminClientView::getClientId)
            .containsExactly("client-a");
        assertThat(queryCaptor.getValue().getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(queryCaptor.getValue().getPageSize()).isEqualTo(10);
        assertThat(queryCaptor.getValue().getPageToken()).isEqualTo("client-a");
        assertThat(queryCaptor.getValue().getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void describeClientViewAcceptsRequestDto() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        when(delegate.describeClient("client-a")).thenReturn(client("client-a"));
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientAdminClientView> result =
            activity.describeClientView(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClientId()).isEqualTo("client-a");
        verify(delegate).describeClient("client-a");
    }

    @Test
    public void listClientViewsByGroupAcceptsRequestDto() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClientsByGroup(eq("group-a"), any(ProxyClientQuery.class))).thenReturn(page);
        ProxyClientAdminListClientsByGroupRequest request =
            ProxyClientAdminListClientsByGroupRequest.newBuilder()
                .setGroup("group-a")
                .setClientType(ClientType.PUSH_CONSUMER)
                .setPageSize(10)
                .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByGroup(proxyContext(), request);

        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(delegate).listClientsByGroup(eq("group-a"), queryCaptor.capture());
        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(queryCaptor.getValue().getGroup()).isEqualTo("group-a");
        assertThat(queryCaptor.getValue().getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(queryCaptor.getValue().getPageSize()).isEqualTo(10);
    }

    @Test
    public void listClientViewsByTopicAcceptsRequestDto() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(client("client-a")), null);
        when(delegate.listClientsByTopic(eq("topic-a"), any(ProxyClientQuery.class))).thenReturn(page);
        ProxyClientAdminListClientsByTopicRequest request =
            ProxyClientAdminListClientsByTopicRequest.newBuilder()
                .setTopic("topic-a")
                .setClientType(ClientType.PRODUCER)
                .setPageToken("client-a")
                .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByTopic(proxyContext(), request);

        ArgumentCaptor<ProxyClientQuery> queryCaptor = ArgumentCaptor.forClass(ProxyClientQuery.class);
        verify(delegate).listClientsByTopic(eq("topic-a"), queryCaptor.capture());
        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(queryCaptor.getValue().getTopic()).isEqualTo("topic-a");
        assertThat(queryCaptor.getValue().getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(queryCaptor.getValue().getPageToken()).isEqualTo("client-a");
    }

    @Test
    public void listClientViewsMapsMissingRequestDtoToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViews(proxyContext(), (ProxyClientAdminListClientsRequest) null);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("request is required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientViewsMapsUnrecognizedClientTypeToBadRequest() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(ClientType.UNRECOGNIZED)
            .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported client type");
        assertThat(result.getStatus().getMessage()).contains("UNRECOGNIZED");
        assertThat(result.getBody()).isNull();
        verify(delegate, never()).listClients(any(ProxyClientQuery.class));
    }

    @Test
    public void listClientViewsRejectsUnsupportedScopeBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();
        when(delegate.listClients(any(ProxyClientQuery.class)))
            .thenReturn(new ProxyClientPage(Collections.emptyList(), ""));

        ProxyClientAdminResult<ProxyClientAdminPageView> result = activity.listClientViews(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported proxy scope");
        assertThat(result.getStatus().getMessage()).contains("ALL_PROXIES");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).listClients(any(ProxyClientQuery.class));
    }

    @Test
    public void listClientViewsByGroupRejectsUnsupportedScopeBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsByGroupRequest request =
            ProxyClientAdminListClientsByGroupRequest.newBuilder()
                .setGroup("group-a")
                .setScope(ProxyClientScope.PROXY_ID)
                .setProxyId("proxy-a")
                .build();
        when(delegate.listClientsByGroup(eq("group-a"), any(ProxyClientQuery.class)))
            .thenReturn(new ProxyClientPage(Collections.emptyList(), ""));

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByGroup(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported proxy scope");
        assertThat(result.getStatus().getMessage()).contains("PROXY_ID");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).listClientsByGroup(eq("group-a"), any(ProxyClientQuery.class));
    }

    @Test
    public void listClientViewsByTopicRejectsUnsupportedScopeBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsByTopicRequest request =
            ProxyClientAdminListClientsByTopicRequest.newBuilder()
                .setTopic("topic-a")
                .setScope(ProxyClientScope.ALL_PROXIES)
                .build();
        when(delegate.listClientsByTopic(eq("topic-a"), any(ProxyClientQuery.class)))
            .thenReturn(new ProxyClientPage(Collections.emptyList(), ""));

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByTopic(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported proxy scope");
        assertThat(result.getStatus().getMessage()).contains("ALL_PROXIES");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).listClientsByTopic(eq("topic-a"), any(ProxyClientQuery.class));
    }

    @Test
    public void describeClientViewRejectsMissingRequestClientIdBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId(" ")
            .build();

        ProxyClientAdminResult<ProxyClientAdminClientView> result =
            activity.describeClientView(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("clientId is required");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).describeClient(anyString());
    }

    @Test
    public void listClientViewsByGroupRejectsMissingRequestGroupBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsByGroupRequest request =
            ProxyClientAdminListClientsByGroupRequest.newBuilder()
                .setGroup(" ")
                .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByGroup(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("group is required");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).listClientsByGroup(anyString(), any(ProxyClientQuery.class));
    }

    @Test
    public void listClientViewsByTopicRejectsMissingRequestTopicBeforeAuthorization() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientAdminListClientsByTopicRequest request =
            ProxyClientAdminListClientsByTopicRequest.newBuilder()
                .setTopic(" ")
                .build();

        ProxyClientAdminResult<ProxyClientAdminPageView> result =
            activity.listClientViewsByTopic(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("topic is required");
        assertThat(result.getBody()).isNull();
        verify(authorizationService, never()).authorize(any(), any(), any());
        verify(delegate, never()).listClientsByTopic(anyString(), any(ProxyClientQuery.class));
    }

    @Test
    public void describeClientMapsMissingClientIdToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientInfo> result = activity.describeClient(proxyContext(), " ");

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("clientId is required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void describeClientMapsUnknownClientToNotFound() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientInfo> result = activity.describeClient(proxyContext(), "missing-client");

        assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(result.getStatus().getMessage()).contains("missing-client");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsMapsInvalidPageTokenToBadRequest() {
        ProxyClientReadService readService = new ProxyClientReadService();
        readService.upsertClient(client("client-a"));
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(readService));

        ProxyClientAdminResult<ProxyClientPage> result = activity.listClients(
            proxyContext(),
            ProxyClientQuery.newBuilder().setPageToken("missing-client").build()
        );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Invalid page token");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsMapsUnsupportedScopeToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientPage> result = activity.listClients(
            proxyContext(),
            ProxyClientQuery.newBuilder().setScope(ProxyClientScope.ALL_PROXIES).build()
        );

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported proxy scope");
        assertThat(result.getStatus().getMessage()).contains("ALL_PROXIES");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void describeClientMapsUnsupportedScopeToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientInfo> result =
            activity.describeClient(proxyContext(), "client-a", ProxyClientScope.PROXY_ID);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("Unsupported proxy scope");
        assertThat(result.getStatus().getMessage()).contains("PROXY_ID");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsByGroupMapsMissingGroupToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByGroup(proxyContext(), " ", ProxyClientQuery.newBuilder().build());

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("group is required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsByTopicMapsMissingTopicToBadRequest() {
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService(new ProxyClientReadService()));

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByTopic(proxyContext(), " ", ProxyClientQuery.newBuilder().build());

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("topic is required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsMapsAuthorizationFailureToUnauthorized() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        doThrow(new AuthorizationException("denied"))
            .when(authorizationService)
            .authorize(any(), eq(ClientAdminOperation.LIST_CLIENTS), any());
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();

        ProxyClientAdminResult<ProxyClientPage> result = activity.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getStatus().getMessage()).contains("denied");
        assertThat(result.getBody()).isNull();
        verify(delegate, never()).listClients(any(ProxyClientQuery.class));
    }

    @Test
    public void describeClientMapsAuthorizationFailureToUnauthorized() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        doThrow(new AuthorizationException("denied"))
            .when(authorizationService)
            .authorize(any(), eq(ClientAdminOperation.DESCRIBE_CLIENT), any());
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );

        ProxyClientAdminResult<ProxyClientInfo> result = activity.describeClient(proxyContext(), "client-a");

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getStatus().getMessage()).contains("denied");
        assertThat(result.getBody()).isNull();
        verify(delegate, never()).describeClient("client-a");
    }

    @Test
    public void listClientsByGroupMapsAuthorizationFailureToUnauthorized() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        doThrow(new AuthorizationException("denied"))
            .when(authorizationService)
            .authorize(any(), eq(ClientAdminOperation.LIST_CLIENTS_BY_GROUP), any());
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByGroup(proxyContext(), "group-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getStatus().getMessage()).contains("denied");
        assertThat(result.getBody()).isNull();
        verify(delegate, never()).listClientsByGroup("group-a", query);
    }

    @Test
    public void listClientsByTopicMapsAuthorizationFailureToUnauthorized() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        doThrow(new AuthorizationException("denied"))
            .when(authorizationService)
            .authorize(any(), eq(ClientAdminOperation.LIST_CLIENTS_BY_TOPIC), any());
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();

        ProxyClientAdminResult<ProxyClientPage> result =
            activity.listClientsByTopic(proxyContext(), "topic-a", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getStatus().getMessage()).contains("denied");
        assertThat(result.getBody()).isNull();
        verify(delegate, never()).listClientsByTopic("topic-a", query);
    }

    @Test
    public void listClientsMapsUnexpectedRuntimeExceptionToInternalServerError() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(
            new AuthorizingClientAdminService(delegate, authorizationService)
        );
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        when(delegate.listClients(query)).thenThrow(new RuntimeException("boom"));

        ProxyClientAdminResult<ProxyClientPage> result = activity.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("boom");
        assertThat(result.getBody()).isNull();
    }

    private static AuthorizingClientAdminService authorizingService(ProxyClientReadService readService) {
        return new AuthorizingClientAdminService(
            new DefaultClientAdminService(readService),
            mock(ClientAdminAuthorizationService.class)
        );
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setSubject(User.of("admin"))
            .setRemoteAddress("127.0.0.1");
    }
}
