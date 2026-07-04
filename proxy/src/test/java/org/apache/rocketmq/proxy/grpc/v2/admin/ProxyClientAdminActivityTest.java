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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
