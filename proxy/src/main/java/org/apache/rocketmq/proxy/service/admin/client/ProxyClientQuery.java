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
import org.apache.commons.lang3.StringUtils;

public class ProxyClientQuery {
    public static final int DEFAULT_PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 1000;

    private final String group;
    private final String topic;
    private final ClientType clientType;
    private final int pageSize;
    private final String pageToken;
    private final ProxyClientScope scope;
    private final String proxyId;

    private ProxyClientQuery(Builder builder) {
        this.group = StringUtils.trimToNull(builder.group);
        this.topic = StringUtils.trimToNull(builder.topic);
        this.clientType = normalizeClientType(builder.clientType);
        this.pageSize = builder.pageSize;
        this.pageToken = StringUtils.trimToNull(builder.pageToken);
        this.scope = builder.scope == null ? ProxyClientScope.LOCAL_PROXY : builder.scope;
        this.proxyId = StringUtils.trimToNull(builder.proxyId);
    }

    public static Builder newBuilder() {
        return new Builder();
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

    public int getBoundedPageSize() {
        if (pageSize <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }

    private static ClientType normalizeClientType(ClientType clientType) {
        if (clientType == ClientType.CLIENT_TYPE_UNSPECIFIED) {
            return null;
        }
        return clientType;
    }

    public static class Builder {
        private String group;
        private String topic;
        private ClientType clientType;
        private int pageSize = DEFAULT_PAGE_SIZE;
        private String pageToken;
        private ProxyClientScope scope = ProxyClientScope.LOCAL_PROXY;
        private String proxyId;

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

        public ProxyClientQuery build() {
            return new ProxyClientQuery(this);
        }
    }
}
