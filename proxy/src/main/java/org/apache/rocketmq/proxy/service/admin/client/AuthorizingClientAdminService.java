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
package org.apache.rocketmq.proxy.service.admin.client;

public class AuthorizingClientAdminService {
    private final ClientAdminService clientAdminService;
    private final ClientAdminAuthorizationService authorizationService;

    public AuthorizingClientAdminService(ClientAdminService clientAdminService,
        ClientAdminAuthorizationService authorizationService) {
        if (clientAdminService == null) {
            throw new IllegalArgumentException("clientAdminService is required");
        }
        if (authorizationService == null) {
            throw new IllegalArgumentException("authorizationService is required");
        }
        this.clientAdminService = clientAdminService;
        this.authorizationService = authorizationService;
    }

    public ProxyClientPage listClients(ClientAdminRequestContext requestContext, ProxyClientQuery query) {
        this.authorize(requestContext, ClientAdminOperation.LIST_CLIENTS);
        return this.clientAdminService.listClients(query);
    }

    public ProxyClientInfo describeClient(ClientAdminRequestContext requestContext, String clientId) {
        this.authorize(requestContext, ClientAdminOperation.DESCRIBE_CLIENT);
        return this.clientAdminService.describeClient(clientId);
    }

    public ProxyClientPage listClientsByGroup(ClientAdminRequestContext requestContext, String group,
        ProxyClientQuery query) {
        this.authorize(requestContext, ClientAdminOperation.LIST_CLIENTS_BY_GROUP);
        return this.clientAdminService.listClientsByGroup(group, query);
    }

    public ProxyClientPage listClientsByTopic(ClientAdminRequestContext requestContext, String topic,
        ProxyClientQuery query) {
        this.authorize(requestContext, ClientAdminOperation.LIST_CLIENTS_BY_TOPIC);
        return this.clientAdminService.listClientsByTopic(topic, query);
    }

    private void authorize(ClientAdminRequestContext requestContext, ClientAdminOperation operation) {
        if (requestContext == null) {
            throw new IllegalArgumentException("requestContext is required");
        }
        this.authorizationService.authorize(
            requestContext.getSubject(),
            operation,
            requestContext.getSourceIp()
        );
    }
}
