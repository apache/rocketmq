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

import apache.rocketmq.v2.ClientType;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.StringUtils;

public class DefaultClientAdminService implements ClientAdminService {
    private final ProxyClientReadService proxyClientReadService;

    public DefaultClientAdminService(ProxyClientReadService proxyClientReadService) {
        if (proxyClientReadService == null) {
            throw new IllegalArgumentException("proxyClientReadService is required");
        }
        this.proxyClientReadService = proxyClientReadService;
    }

    @Override
    public ProxyClientPage listClients(ProxyClientQuery query) {
        ProxyClientQuery effectiveQuery = this.effectiveQuery(query);
        this.validateLocalProxyScope(effectiveQuery.getScope());
        this.validateClientType(effectiveQuery.getClientType());
        return this.proxyClientReadService.listClients(effectiveQuery);
    }

    @Override
    public ProxyClientInfo describeClient(String clientId) {
        String normalizedClientId = ProxyClientInfo.normalizeClientId(clientId);
        ProxyClientInfo clientInfo = this.proxyClientReadService.getClient(normalizedClientId);
        if (clientInfo == null) {
            throw new NoSuchElementException("Client not found: " + normalizedClientId);
        }
        return clientInfo;
    }

    @Override
    public ProxyClientPage listClientsByGroup(String group, ProxyClientQuery query) {
        if (StringUtils.isBlank(group)) {
            throw new IllegalArgumentException("group is required");
        }
        return this.listClients(this.mergeQuery(query, group, null));
    }

    @Override
    public ProxyClientPage listClientsByTopic(String topic, ProxyClientQuery query) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("topic is required");
        }
        return this.listClients(this.mergeQuery(query, null, topic));
    }

    static void validateLocalProxyScope(ProxyClientScope scope) {
        ProxyClientScope effectiveScope = scope == null ? ProxyClientScope.LOCAL_PROXY : scope;
        if (effectiveScope != ProxyClientScope.LOCAL_PROXY) {
            throw new IllegalArgumentException("Unsupported proxy scope: " + effectiveScope);
        }
    }

    private void validateClientType(ClientType clientType) {
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
    }

    private ProxyClientQuery mergeQuery(ProxyClientQuery query, String group, String topic) {
        ProxyClientQuery effectiveQuery = this.effectiveQuery(query);
        ProxyClientQuery.Builder builder = effectiveQuery.toBuilder();
        if (group != null) {
            builder.setGroup(group);
        }
        if (topic != null) {
            builder.setTopic(topic);
        }
        return builder.build();
    }

    private ProxyClientQuery effectiveQuery(ProxyClientQuery query) {
        ProxyClientQuery effectiveQuery = query == null ? ProxyClientQuery.newBuilder().build() : query;
        if (effectiveQuery.getScope() != ProxyClientScope.LOCAL_PROXY || effectiveQuery.getProxyId() == null) {
            return effectiveQuery;
        }
        return effectiveQuery.toBuilder().setProxyId(null).build();
    }
}
