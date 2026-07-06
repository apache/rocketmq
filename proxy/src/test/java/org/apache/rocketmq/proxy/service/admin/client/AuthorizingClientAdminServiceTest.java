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

package org.apache.rocketmq.proxy.service.admin.client;

import java.util.Collections;
import org.apache.rocketmq.auth.authentication.model.User;
import org.junit.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizingClientAdminServiceTest {

    @Test
    public void listClientsAuthorizesBeforeDelegating() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        AuthorizingClientAdminService adminService =
            new AuthorizingClientAdminService(delegate, authorizationService);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), null);
        when(delegate.listClients(query)).thenReturn(page);

        ProxyClientPage actual = adminService.listClients(requestContext, query);

        assertThat(actual).isSameAs(page);
        InOrder inOrder = inOrder(authorizationService, delegate);
        inOrder.verify(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.LIST_CLIENTS,
            requestContext.getSourceIp()
        );
        inOrder.verify(delegate).listClients(query);
    }

    @Test
    public void describeClientAuthorizesBeforeDelegating() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        AuthorizingClientAdminService adminService =
            new AuthorizingClientAdminService(delegate, authorizationService);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientInfo clientInfo = mock(ProxyClientInfo.class);
        when(delegate.describeClient("client-a")).thenReturn(clientInfo);

        ProxyClientInfo actual = adminService.describeClient(requestContext, "client-a");

        assertThat(actual).isSameAs(clientInfo);
        InOrder inOrder = inOrder(authorizationService, delegate);
        inOrder.verify(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.DESCRIBE_CLIENT,
            requestContext.getSourceIp()
        );
        inOrder.verify(delegate).describeClient("client-a");
    }

    @Test
    public void listClientsByGroupAuthorizesBeforeDelegating() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        AuthorizingClientAdminService adminService =
            new AuthorizingClientAdminService(delegate, authorizationService);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), null);
        when(delegate.listClientsByGroup("group-a", query)).thenReturn(page);

        ProxyClientPage actual = adminService.listClientsByGroup(requestContext, "group-a", query);

        assertThat(actual).isSameAs(page);
        InOrder inOrder = inOrder(authorizationService, delegate);
        inOrder.verify(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
            requestContext.getSourceIp()
        );
        inOrder.verify(delegate).listClientsByGroup("group-a", query);
    }

    @Test
    public void listClientsByTopicAuthorizesBeforeDelegating() {
        ClientAdminService delegate = mock(ClientAdminService.class);
        ClientAdminAuthorizationService authorizationService = mock(ClientAdminAuthorizationService.class);
        AuthorizingClientAdminService adminService =
            new AuthorizingClientAdminService(delegate, authorizationService);
        ClientAdminRequestContext requestContext = requestContext();
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();
        ProxyClientPage page = new ProxyClientPage(Collections.emptyList(), null);
        when(delegate.listClientsByTopic("topic-a", query)).thenReturn(page);

        ProxyClientPage actual = adminService.listClientsByTopic(requestContext, "topic-a", query);

        assertThat(actual).isSameAs(page);
        InOrder inOrder = inOrder(authorizationService, delegate);
        inOrder.verify(authorizationService).authorize(
            requestContext.getSubject(),
            ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
            requestContext.getSourceIp()
        );
        inOrder.verify(delegate).listClientsByTopic("topic-a", query);
    }

    private static ClientAdminRequestContext requestContext() {
        return ClientAdminRequestContext.of(User.of("admin"), "127.0.0.1");
    }
}
