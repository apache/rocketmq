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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminPeerRequest {
    private final ProxyClientAdminPeerOperation operation;
    private final String clientId;
    private final String group;
    private final String topic;
    private final ClientType clientType;
    private final int pageSize;
    private final String pageToken;
    private final ProxyClientScope scope;
    private final String proxyId;

    private ProxyClientAdminPeerRequest(Builder builder) {
        if (builder.operation == null) {
            throw new IllegalArgumentException("operation is required");
        }
        this.operation = builder.operation;
        this.clientId = StringUtils.trimToNull(builder.clientId);
        this.group = StringUtils.trimToNull(builder.group);
        this.topic = StringUtils.trimToNull(builder.topic);
        this.clientType = normalizeClientType(builder.clientType);
        this.pageSize = ProxyClientQuery.boundPageSize(builder.pageSize);
        this.pageToken = StringUtils.trimToNull(builder.pageToken);
        this.scope = builder.scope == null ? ProxyClientScope.LOCAL_PROXY : builder.scope;
        if (this.scope != ProxyClientScope.LOCAL_PROXY) {
            throw new IllegalArgumentException("Unsupported peer request scope: " + this.scope);
        }
        this.proxyId = null;
        this.validateOperationFields();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ProxyClientQuery toLocalQuery() {
        if (this.operation == ProxyClientAdminPeerOperation.DESCRIBE_CLIENT) {
            throw new IllegalStateException("operation is not a list operation: " + this.operation);
        }
        return ProxyClientQuery.newBuilder()
            .setGroup(this.group)
            .setTopic(this.topic)
            .setClientType(this.clientType)
            .setPageSize(this.pageSize)
            .setPageToken(this.pageToken)
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .setProxyId(null)
            .build();
    }

    public ProxyClientAdminDescribeClientRequest toLocalDescribeClientRequest() {
        if (this.operation != ProxyClientAdminPeerOperation.DESCRIBE_CLIENT) {
            throw new IllegalStateException("operation is not DESCRIBE_CLIENT: " + this.operation);
        }
        return ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId(this.clientId)
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .setProxyId(null)
            .build();
    }

    public ProxyClientAdminPeerOperation getOperation() {
        return operation;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
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

    private void validateOperationFields() {
        if (this.operation == ProxyClientAdminPeerOperation.LIST_CLIENTS) {
            rejectUnexpectedField(this.operation, "group", this.group);
            rejectUnexpectedField(this.operation, "topic", this.topic);
        }
        if (this.operation == ProxyClientAdminPeerOperation.DESCRIBE_CLIENT && this.clientId == null) {
            throw new IllegalArgumentException("clientId is required");
        }
        if (this.operation == ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP) {
            rejectUnexpectedField(this.operation, "topic", this.topic);
        }
        if (this.operation == ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP && this.group == null) {
            throw new IllegalArgumentException("group is required");
        }
        if (this.operation == ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC) {
            rejectUnexpectedField(this.operation, "group", this.group);
        }
        if (this.operation == ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC && this.topic == null) {
            throw new IllegalArgumentException("topic is required");
        }
    }

    private static void rejectUnexpectedField(ProxyClientAdminPeerOperation operation, String fieldName,
        String value) {
        if (value != null) {
            throw new IllegalArgumentException(operation + " request must not set " + fieldName);
        }
    }

    private static ClientType normalizeClientType(ClientType clientType) {
        if (clientType == null || clientType == ClientType.CLIENT_TYPE_UNSPECIFIED) {
            return null;
        }
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
        return clientType;
    }

    public static class Builder {
        private ProxyClientAdminPeerOperation operation;
        private String clientId;
        private String group;
        private String topic;
        private ClientType clientType;
        private int pageSize = ProxyClientQuery.DEFAULT_PAGE_SIZE;
        private String pageToken;
        private ProxyClientScope scope = ProxyClientScope.LOCAL_PROXY;
        private String proxyId;

        public Builder setOperation(ProxyClientAdminPeerOperation operation) {
            this.operation = operation;
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setGroup(String group) {
            this.group = group;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setClientType(ClientType clientType) {
            this.clientType = clientType;
            return this;
        }

        public Builder setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder setPageToken(String pageToken) {
            this.pageToken = pageToken;
            return this;
        }

        public Builder setScope(ProxyClientScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder setProxyId(String proxyId) {
            this.proxyId = proxyId;
            return this;
        }

        public ProxyClientAdminPeerRequest build() {
            return new ProxyClientAdminPeerRequest(this);
        }
    }
}
