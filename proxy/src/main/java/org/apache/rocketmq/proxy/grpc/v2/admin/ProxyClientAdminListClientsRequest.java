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
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminListClientsRequest {
    private static final int MAX_CLIENT_LANGUAGE_LENGTH = 255;

    private final String clientId;
    private final String clientIdPrefix;
    private final String group;
    private final String topic;
    private final String clientLanguage;
    private final Long connectTimeStartMillis;
    private final Long connectTimeEndMillis;
    private final int pageNum;
    private final ClientType clientType;
    private final int pageSize;
    private final String pageToken;
    private final ProxyClientScope scope;
    private final String proxyId;

    protected ProxyClientAdminListClientsRequest(Builder<?> builder) {
        this.clientId = normalizeOptionalClientId(builder.clientId, "clientId");
        this.clientIdPrefix = normalizeClientIdPrefix(builder.clientIdPrefix);
        this.group = normalizeGroup(builder.group);
        this.topic = normalizeTopic(builder.topic);
        this.clientLanguage = normalizeClientLanguage(builder.clientLanguage);
        this.connectTimeStartMillis = normalizeConnectTimeMillis(builder.connectTimeStartMillis,
            "connectTimeStartMillis");
        this.connectTimeEndMillis = normalizeConnectTimeMillis(builder.connectTimeEndMillis,
            "connectTimeEndMillis");
        this.validateConnectTimeRange(this.connectTimeStartMillis, this.connectTimeEndMillis);
        this.pageNum = normalizePageNum(builder.pageNum);
        this.clientType = builder.clientType;
        this.pageSize = ProxyClientQuery.boundPageSize(builder.pageSize);
        this.pageToken = builder.pageToken;
        this.scope = builder.scope == null ? ProxyClientScope.LOCAL_PROXY : builder.scope;
        this.proxyId = this.scope == ProxyClientScope.PROXY_ID ? requireProxyId(builder.proxyId) : null;
    }

    public static Builder<?> newBuilder() {
        return new Builder<>();
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientIdPrefix() {
        return clientIdPrefix;
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public String getClientLanguage() {
        return clientLanguage;
    }

    public Long getConnectTimeStartMillis() {
        return connectTimeStartMillis;
    }

    public Long getConnectTimeEndMillis() {
        return connectTimeEndMillis;
    }

    public int getPageNum() {
        return pageNum;
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
            .setClientId(this.clientId)
            .setClientIdPrefix(this.clientIdPrefix)
            .setGroup(this.group)
            .setTopic(this.topic)
            .setClientLanguage(this.clientLanguage)
            .setConnectTimeStartMillis(this.connectTimeStartMillis)
            .setConnectTimeEndMillis(this.connectTimeEndMillis)
            .setPageNum(this.pageNum)
            .setClientType(this.normalizeClientType(clientType))
            .setPageSize(pageSize)
            .setPageToken(this.pageTokenForQuery())
            .setScope(scope)
            .setProxyId(this.proxyIdForQuery());
    }

    private static String requireProxyId(String proxyId) {
        return ProxyClientAdminPeerIds.requireProxyId(proxyId);
    }

    private static String normalizeOptionalClientId(String clientId, String fieldName) {
        String normalizedClientId = StringUtils.trimToNull(clientId);
        if (normalizedClientId == null) {
            return null;
        }
        return ProxyClientInfo.normalizeClientId(normalizedClientId, fieldName);
    }

    protected static String normalizeGroup(String group) {
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

    protected static String normalizeTopic(String topic) {
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

    private static String normalizeClientIdPrefix(String clientIdPrefix) {
        String normalizedClientIdPrefix = StringUtils.trimToNull(clientIdPrefix);
        if (normalizedClientIdPrefix == null) {
            return null;
        }
        if (normalizedClientIdPrefix.length() > Validators.CHARACTER_MAX_LENGTH) {
            throw new IllegalArgumentException("clientIdPrefix length exceeds " + Validators.CHARACTER_MAX_LENGTH);
        }
        return normalizedClientIdPrefix;
    }

    private static String normalizeClientLanguage(String clientLanguage) {
        String normalizedClientLanguage = StringUtils.trimToNull(clientLanguage);
        if (normalizedClientLanguage == null) {
            return null;
        }
        if (normalizedClientLanguage.length() > MAX_CLIENT_LANGUAGE_LENGTH) {
            throw new IllegalArgumentException("clientLanguage length exceeds " + MAX_CLIENT_LANGUAGE_LENGTH);
        }
        return normalizedClientLanguage;
    }

    private static Long normalizeConnectTimeMillis(Long connectTimeMillis, String fieldName) {
        if (connectTimeMillis == null) {
            return null;
        }
        if (connectTimeMillis < 0) {
            throw new IllegalArgumentException(fieldName + " must be greater than or equal to 0");
        }
        return connectTimeMillis;
    }

    private void validateConnectTimeRange(Long connectTimeStartMillis, Long connectTimeEndMillis) {
        if (connectTimeStartMillis != null && connectTimeEndMillis != null
            && connectTimeStartMillis > connectTimeEndMillis) {
            throw new IllegalArgumentException("connectTimeStartMillis must not exceed connectTimeEndMillis");
        }
    }

    private static int normalizePageNum(int pageNum) {
        if (pageNum < 1) {
            throw new IllegalArgumentException("pageNum must be greater than or equal to 1");
        }
        return pageNum;
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

    private String pageTokenForQuery() {
        if (scope == ProxyClientScope.ALL_PROXIES) {
            return coordinatorPageTokenForQuery();
        }
        return ProxyClientAdminPageTokenCodec.getInstance().decode(pageToken);
    }

    private String coordinatorPageTokenForQuery() {
        if (pageToken == null) {
            return null;
        }
        ProxyClientAdminCoordinatorPageTokenCodec.getInstance().decode(pageToken);
        return pageToken;
    }

    private String proxyIdForQuery() {
        if (scope == ProxyClientScope.PROXY_ID) {
            return proxyId;
        }
        return null;
    }

    public static class Builder<T extends Builder<T>> {
        private String clientId;
        private String clientIdPrefix;
        private String group;
        private String topic;
        private String clientLanguage;
        private Long connectTimeStartMillis;
        private Long connectTimeEndMillis;
        private int pageNum = ProxyClientQuery.DEFAULT_PAGE_NUM;
        private ClientType clientType;
        private int pageSize = ProxyClientQuery.DEFAULT_PAGE_SIZE;
        private String pageToken;
        private ProxyClientScope scope = ProxyClientScope.LOCAL_PROXY;
        private String proxyId;

        public T setClientId(String clientId) {
            this.clientId = clientId;
            return this.self();
        }

        public T setClientIdPrefix(String clientIdPrefix) {
            this.clientIdPrefix = clientIdPrefix;
            return this.self();
        }

        public T setGroup(String group) {
            this.group = group;
            return this.self();
        }

        public T setTopic(String topic) {
            this.topic = topic;
            return this.self();
        }

        public T setClientLanguage(String clientLanguage) {
            this.clientLanguage = clientLanguage;
            return this.self();
        }

        public T setConnectTimeStartMillis(Long connectTimeStartMillis) {
            this.connectTimeStartMillis = connectTimeStartMillis;
            return this.self();
        }

        public T setConnectTimeEndMillis(Long connectTimeEndMillis) {
            this.connectTimeEndMillis = connectTimeEndMillis;
            return this.self();
        }

        public T setPageNum(int pageNum) {
            this.pageNum = pageNum;
            return this.self();
        }

        public T setClientType(ClientType clientType) {
            this.clientType = clientType;
            return this.self();
        }

        public T setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this.self();
        }

        public T setPageToken(String pageToken) {
            this.pageToken = StringUtils.trimToNull(pageToken);
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
            this.proxyId = StringUtils.trimToNull(proxyId);
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
