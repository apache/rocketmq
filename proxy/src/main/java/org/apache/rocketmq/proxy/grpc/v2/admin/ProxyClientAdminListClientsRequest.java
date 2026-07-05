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
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminListClientsRequest {
    private final ClientType clientType;
    private final int pageSize;
    private final String pageToken;
    private final ProxyClientScope scope;
    private final String proxyId;

    protected ProxyClientAdminListClientsRequest(Builder<?> builder) {
        this.clientType = builder.clientType;
        this.pageSize = builder.pageSize;
        this.pageToken = builder.pageToken;
        this.scope = builder.scope == null ? ProxyClientScope.LOCAL_PROXY : builder.scope;
        this.proxyId = builder.proxyId;
    }

    public static Builder<?> newBuilder() {
        return new Builder<>();
    }

    public ClientType getClientType() {
        return clientType;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getPageToken() {
        return pageToken;
    }

    public ProxyClientScope getScope() {
        return scope;
    }

    public String getProxyId() {
        return proxyId;
    }

    public ProxyClientQuery toQuery() {
        return this.populateQueryBuilder(ProxyClientQuery.newBuilder()).build();
    }

    protected ProxyClientQuery.Builder populateQueryBuilder(ProxyClientQuery.Builder builder) {
        return builder
            .setClientType(this.normalizeClientType(clientType))
            .setPageSize(pageSize)
            .setPageToken(ProxyClientAdminPageTokenCodec.getInstance().decode(pageToken))
            .setScope(scope)
            .setProxyId(proxyId);
    }

    private ClientType normalizeClientType(ClientType clientType) {
        if (clientType == ClientType.CLIENT_TYPE_UNSPECIFIED) {
            return null;
        }
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
        return clientType;
    }

    public static class Builder<T extends Builder<T>> {
        private ClientType clientType;
        private int pageSize = ProxyClientQuery.DEFAULT_PAGE_SIZE;
        private String pageToken;
        private ProxyClientScope scope = ProxyClientScope.LOCAL_PROXY;
        private String proxyId;

        public T setClientType(ClientType clientType) {
            this.clientType = clientType;
            return this.self();
        }

        public T setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this.self();
        }

        public T setPageToken(String pageToken) {
            this.pageToken = pageToken;
            return this.self();
        }

        public T setScope(ProxyClientScope scope) {
            this.scope = scope;
            return this.self();
        }

        public T setScopeName(String scopeName) {
            this.scope = ProxyClientAdminScopeMapper.getInstance().decode(scopeName);
            return this.self();
        }

        public T setProxyId(String proxyId) {
            this.proxyId = proxyId;
            return this.self();
        }

        public ProxyClientAdminListClientsRequest build() {
            return new ProxyClientAdminListClientsRequest(this);
        }

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }
    }
}
