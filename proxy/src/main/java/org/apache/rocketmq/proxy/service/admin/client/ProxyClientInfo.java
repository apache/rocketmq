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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;

public class ProxyClientInfo {
    private static final int MAX_PROXY_ID_LENGTH = 255;

    private final String clientId;
    private final ClientType clientType;
    private final Set<String> groups;
    private final Set<String> topics;
    private final String language;
    private final String remoteAddress;
    private final String localAddress;
    private final String clientVersion;
    private final String proxyId;
    private final long connectTimeMillis;
    private final long lastActiveTimeMillis;

    public ProxyClientInfo(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String language, String remoteAddress, String localAddress, String clientVersion, long connectTimeMillis,
        long lastActiveTimeMillis) {
        this(clientId, clientType, groups, topics, language, remoteAddress, localAddress, clientVersion, null,
            connectTimeMillis, lastActiveTimeMillis);
    }

    public ProxyClientInfo(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String language, String remoteAddress, String localAddress, String clientVersion, String proxyId,
        long connectTimeMillis, long lastActiveTimeMillis) {
        this.clientId = normalizeClientId(clientId);
        this.clientType = normalizeClientType(clientType);
        this.groups = normalizeGroups(groups);
        this.topics = normalizeTopics(topics);
        this.language = language;
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.clientVersion = clientVersion;
        this.proxyId = normalizeProxyId(proxyId);
        this.connectTimeMillis = connectTimeMillis;
        this.lastActiveTimeMillis = lastActiveTimeMillis;
    }

    static String normalizeClientId(String clientId) {
        String normalizedClientId = StringUtils.trimToNull(clientId);
        if (normalizedClientId == null) {
            throw new IllegalArgumentException("clientId is required");
        }
        if (normalizedClientId.length() > Validators.CHARACTER_MAX_LENGTH) {
            throw new IllegalArgumentException("clientId length exceeds " + Validators.CHARACTER_MAX_LENGTH);
        }
        if (isCoordinatorPageTokenPrefix(normalizedClientId)) {
            throw new IllegalArgumentException("clientId must not use reserved page token prefix: "
                + normalizedClientId);
        }
        return normalizedClientId;
    }

    private static boolean isCoordinatorPageTokenPrefix(String clientId) {
        int colonIndex = clientId.indexOf(':');
        if (colonIndex <= 2 || clientId.charAt(0) != 'c' || clientId.charAt(1) != 'p') {
            return false;
        }
        for (int i = 2; i < colonIndex; i++) {
            if (!Character.isDigit(clientId.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static ClientType normalizeClientType(ClientType clientType) {
        if (clientType == ClientType.CLIENT_TYPE_UNSPECIFIED || clientType == ClientType.UNRECOGNIZED) {
            return null;
        }
        return clientType;
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

    private static Set<String> normalizeGroups(Set<String> groups) {
        return normalize(groups, true);
    }

    private static Set<String> normalizeTopics(Set<String> topics) {
        return normalize(topics, false);
    }

    private static Set<String> normalize(Set<String> values, boolean groupValues) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (String value : values) {
            String normalizedValue = StringUtils.trim(value);
            if (StringUtils.isNotBlank(normalizedValue)) {
                validateTopicOrGroup(normalizedValue, groupValues);
                result.add(normalizedValue);
            }
        }
        return Collections.unmodifiableSet(result);
    }

    private static void validateTopicOrGroup(String value, boolean groupValue) {
        if (groupValue && value.length() > Validators.GROUP_MAX_LENGTH) {
            throw new IllegalArgumentException("group length exceeds group max length: "
                + Validators.GROUP_MAX_LENGTH);
        }
        if (!groupValue && value.length() > Validators.TOPIC_MAX_LENGTH) {
            throw new IllegalArgumentException("topic length exceeds topic max length "
                + Validators.TOPIC_MAX_LENGTH);
        }
    }

    public String getClientId() {
        return clientId;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public String getLanguage() {
        return language;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public String getClientVersion() {
        return clientVersion;
    }

    public String getProxyId() {
        return proxyId;
    }

    public long getConnectTimeMillis() {
        return connectTimeMillis;
    }

    public long getLastActiveTimeMillis() {
        return lastActiveTimeMillis;
    }
}
