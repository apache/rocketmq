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
import org.apache.rocketmq.client.Validators;

public class ProxyClientQuery {
    public static final int DEFAULT_PAGE_SIZE = 100;
    public static final int MAX_PAGE_SIZE = 1000;
    private static final int MAX_PROXY_ID_LENGTH = 255;

    private final String group;
    private final String topic;
    private final ClientType clientType;
    private final int pageSize;
    private final String pageToken;
    private final ProxyClientScope scope;
    private final String proxyId;

    private ProxyClientQuery(Builder builder) {
        this.group = normalizeGroup(builder.group);
        this.topic = normalizeTopic(builder.topic);
        this.clientType = normalizeClientType(builder.clientType);
        this.pageSize = builder.pageSize;
        this.scope = builder.scope == null ? ProxyClientScope.LOCAL_PROXY : builder.scope;
        this.pageToken = normalizePageToken(builder.pageToken, this.scope);
        this.proxyId = normalizeProxyId(builder.proxyId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static int boundPageSize(int pageSize) {
        if (pageSize <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }

    public Builder toBuilder() {
        return newBuilder()
            .setGroup(this.group)
            .setTopic(this.topic)
            .setClientType(this.clientType)
            .setPageSize(this.pageSize)
            .setPageToken(this.pageToken)
            .setScope(this.scope)
            .setProxyId(this.proxyId);
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
        return boundPageSize(pageSize);
    }

    private static ClientType normalizeClientType(ClientType clientType) {
        if (clientType == ClientType.CLIENT_TYPE_UNSPECIFIED) {
            return null;
        }
        return clientType;
    }

    private static String normalizeGroup(String group) {
        String normalizedGroup = StringUtils.trimToNull(group);
        if (normalizedGroup == null) {
            return null;
        }
        if (normalizedGroup.length() > Validators.GROUP_MAX_LENGTH) {
            throw new IllegalArgumentException("group length exceeds group max length: "
                + Validators.GROUP_MAX_LENGTH);
        }
        return normalizedGroup;
    }

    private static String normalizeTopic(String topic) {
        String normalizedTopic = StringUtils.trimToNull(topic);
        if (normalizedTopic == null) {
            return null;
        }
        if (normalizedTopic.length() > Validators.TOPIC_MAX_LENGTH) {
            throw new IllegalArgumentException("topic length exceeds topic max length "
                + Validators.TOPIC_MAX_LENGTH);
        }
        return normalizedTopic;
    }

    private static String normalizePageToken(String pageToken, ProxyClientScope scope) {
        String normalizedPageToken = StringUtils.trimToNull(pageToken);
        if (normalizedPageToken == null) {
            return null;
        }
        if (scope != ProxyClientScope.LOCAL_PROXY) {
            return normalizedPageToken;
        }
        return ProxyClientInfo.normalizeClientId(normalizedPageToken, "pageToken");
    }

    private static String normalizeProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            return null;
        }
        if (normalizedProxyId.length() > MAX_PROXY_ID_LENGTH) {
            throw new IllegalArgumentException("proxyId length exceeds " + MAX_PROXY_ID_LENGTH);
        }
        return normalizedProxyId;
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
