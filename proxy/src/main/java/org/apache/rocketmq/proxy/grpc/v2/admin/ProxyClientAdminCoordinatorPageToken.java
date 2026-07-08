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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminCoordinatorPageToken {
    private final ProxyClientScope scope;
    private final String group;
    private final String topic;
    private final ClientType clientType;
    private final String proxyId;
    private final String lastClientId;
    private final String lastProxyId;
    private final long createTimeMillis;
    private final Map<String, String> peerPageTokens;

    private ProxyClientAdminCoordinatorPageToken(Builder builder) {
        if (builder.scope == null) {
            throw new IllegalArgumentException("scope is required");
        }
        this.scope = builder.scope;
        this.group = normalizeGroup(builder.group);
        this.topic = normalizeTopic(builder.topic);
        this.clientType = normalizeClientType(builder.clientType);
        this.proxyId = ProxyClientAdminPeerIds.normalizeOptionalProxyId(builder.proxyId, "proxyId");
        this.lastClientId = normalizeOptionalClientCursor(builder.lastClientId, "lastClientId");
        this.lastProxyId = ProxyClientAdminPeerIds.normalizeOptionalProxyId(builder.lastProxyId, "lastProxyId");
        this.createTimeMillis = builder.createTimeMillis;
        this.peerPageTokens = normalizePeerPageTokens(builder.peerPageTokens);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ProxyClientScope getScope() {
        return scope;
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

    public String getProxyId() {
        return proxyId;
    }

    public String getLastClientId() {
        return lastClientId;
    }

    public String getLastProxyId() {
        return lastProxyId;
    }

    public long getCreateTimeMillis() {
        return createTimeMillis;
    }

    public Map<String, String> getPeerPageTokens() {
        return peerPageTokens;
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

    private static Map<String, String> normalizePeerPageTokens(Map<String, String> peerPageTokens) {
        if (peerPageTokens == null || peerPageTokens.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> sortedPeerPageTokens = new TreeMap<>();
        for (Map.Entry<String, String> entry : peerPageTokens.entrySet()) {
            String proxyId = ProxyClientAdminPeerIds.normalizeOptionalProxyId(
                entry.getKey(), "peer page token proxyId"
            );
            String pageToken = normalizeOptionalClientCursor(entry.getValue(), "peer page token");
            if (proxyId != null && pageToken != null) {
                sortedPeerPageTokens.put(proxyId, pageToken);
            }
        }
        if (sortedPeerPageTokens.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(sortedPeerPageTokens));
    }

    private static String normalizeOptionalClientCursor(String clientCursor, String fieldName) {
        String normalizedClientCursor = StringUtils.trimToNull(clientCursor);
        if (normalizedClientCursor == null) {
            return null;
        }
        return ProxyClientInfo.normalizeClientId(normalizedClientCursor, fieldName);
    }

    public static class Builder {
        private ProxyClientScope scope;
        private String group;
        private String topic;
        private ClientType clientType;
        private String proxyId;
        private String lastClientId;
        private String lastProxyId;
        private long createTimeMillis = System.currentTimeMillis();
        private Map<String, String> peerPageTokens = Collections.emptyMap();

        public Builder setScope(ProxyClientScope scope) {
            this.scope = scope;
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

        public Builder setProxyId(String proxyId) {
            this.proxyId = proxyId;
            return this;
        }

        public Builder setLastClientId(String lastClientId) {
            this.lastClientId = lastClientId;
            return this;
        }

        public Builder setLastProxyId(String lastProxyId) {
            this.lastProxyId = lastProxyId;
            return this;
        }

        public Builder setCreateTimeMillis(long createTimeMillis) {
            this.createTimeMillis = createTimeMillis;
            return this;
        }

        public Builder setPeerPageTokens(Map<String, String> peerPageTokens) {
            if (peerPageTokens == null || peerPageTokens.isEmpty()) {
                this.peerPageTokens = Collections.emptyMap();
            } else {
                this.peerPageTokens = new LinkedHashMap<>(peerPageTokens);
            }
            return this;
        }

        public Builder putPeerPageToken(String proxyId, String pageToken) {
            if (this.peerPageTokens.isEmpty()) {
                this.peerPageTokens = new LinkedHashMap<>();
            }
            this.peerPageTokens.put(proxyId, pageToken);
            return this;
        }

        public ProxyClientAdminCoordinatorPageToken build() {
            return new ProxyClientAdminCoordinatorPageToken(this);
        }
    }
}
