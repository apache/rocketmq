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
import com.alibaba.fastjson2.JSON;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public final class ProxyClientAdminCoordinatorPageTokenCodec {
    private static final ProxyClientAdminCoordinatorPageTokenCodec INSTANCE =
        new ProxyClientAdminCoordinatorPageTokenCodec();
    private static final String VERSION_1_PREFIX = "cp1:";
    private static final int MAX_PUBLIC_PAGE_TOKEN_LENGTH = 4096;

    private ProxyClientAdminCoordinatorPageTokenCodec() {
    }

    public static ProxyClientAdminCoordinatorPageTokenCodec getInstance() {
        return INSTANCE;
    }

    public String encode(ProxyClientAdminCoordinatorPageToken coordinatorPageToken) {
        if (coordinatorPageToken == null) {
            return "";
        }

        Payload payload = new Payload();
        payload.setScope(coordinatorPageToken.getScope().name());
        payload.setGroup(coordinatorPageToken.getGroup());
        payload.setTopic(coordinatorPageToken.getTopic());
        payload.setClientType(formatClientType(coordinatorPageToken.getClientType()));
        payload.setProxyId(coordinatorPageToken.getProxyId());
        payload.setLastClientId(coordinatorPageToken.getLastClientId());
        payload.setCreateTimeMillis(coordinatorPageToken.getCreateTimeMillis());
        payload.setPeerPageTokens(coordinatorPageToken.getPeerPageTokens());
        String json = JSON.toJSONString(payload);
        return VERSION_1_PREFIX + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    public ProxyClientAdminCoordinatorPageToken decode(String publicPageToken) {
        String normalizedPublicPageToken = StringUtils.trimToNull(publicPageToken);
        if (normalizedPublicPageToken == null) {
            return null;
        }
        if (normalizedPublicPageToken.length() > MAX_PUBLIC_PAGE_TOKEN_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid coordinator page token: length exceeds " + MAX_PUBLIC_PAGE_TOKEN_LENGTH
            );
        }
        if (!normalizedPublicPageToken.startsWith(VERSION_1_PREFIX)) {
            throw new IllegalArgumentException("Invalid coordinator page token: " + normalizedPublicPageToken);
        }

        String encodedPayload = normalizedPublicPageToken.substring(VERSION_1_PREFIX.length());
        if (StringUtils.isBlank(encodedPayload)) {
            throw new IllegalArgumentException("Invalid coordinator page token: " + normalizedPublicPageToken);
        }
        try {
            String json = new String(Base64.getUrlDecoder().decode(encodedPayload), StandardCharsets.UTF_8);
            Payload payload = JSON.parseObject(json, Payload.class);
            if (payload == null) {
                throw new IllegalArgumentException("Invalid coordinator page token: " + normalizedPublicPageToken);
            }
            return ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(parseScope(payload.getScope()))
                .setGroup(payload.getGroup())
                .setTopic(payload.getTopic())
                .setClientType(parseClientType(payload.getClientType()))
                .setProxyId(payload.getProxyId())
                .setLastClientId(payload.getLastClientId())
                .setCreateTimeMillis(payload.getCreateTimeMillis())
                .setPeerPageTokens(payload.getPeerPageTokens())
                .build();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid coordinator page token: " + normalizedPublicPageToken, e);
        }
    }

    private static String formatClientType(ClientType clientType) {
        if (clientType == null || clientType == ClientType.CLIENT_TYPE_UNSPECIFIED) {
            return null;
        }
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
        return clientType.name();
    }

    private static ProxyClientScope parseScope(String scope) {
        String normalizedScope = StringUtils.trimToNull(scope);
        if (normalizedScope == null) {
            throw new IllegalArgumentException("scope is required");
        }
        return ProxyClientScope.valueOf(normalizedScope);
    }

    private static ClientType parseClientType(String clientType) {
        String normalizedClientType = StringUtils.trimToNull(clientType);
        if (normalizedClientType == null) {
            return null;
        }
        return ClientType.valueOf(normalizedClientType);
    }

    public static class Payload {
        private String scope;
        private String group;
        private String topic;
        private String clientType;
        private String proxyId;
        private String lastClientId;
        private long createTimeMillis;
        private Map<String, String> peerPageTokens;

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getClientType() {
            return clientType;
        }

        public void setClientType(String clientType) {
            this.clientType = clientType;
        }

        public String getProxyId() {
            return proxyId;
        }

        public void setProxyId(String proxyId) {
            this.proxyId = proxyId;
        }

        public String getLastClientId() {
            return lastClientId;
        }

        public void setLastClientId(String lastClientId) {
            this.lastClientId = lastClientId;
        }

        public long getCreateTimeMillis() {
            return createTimeMillis;
        }

        public void setCreateTimeMillis(long createTimeMillis) {
            this.createTimeMillis = createTimeMillis;
        }

        public Map<String, String> getPeerPageTokens() {
            return peerPageTokens;
        }

        public void setPeerPageTokens(Map<String, String> peerPageTokens) {
            this.peerPageTokens = peerPageTokens;
        }
    }
}
