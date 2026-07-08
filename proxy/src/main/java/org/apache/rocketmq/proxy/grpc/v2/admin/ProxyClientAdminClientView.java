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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;

public class ProxyClientAdminClientView {
    private static final int MAX_PROXY_ID_LENGTH = 255;

    private final String clientId;
    private final ClientType clientType;
    private final List<String> groups;
    private final List<String> topics;
    private final String language;
    private final String remoteAddress;
    private final String localAddress;
    private final String clientVersion;
    private final String proxyId;
    private final long connectTimeMillis;
    private final long lastActiveTimeMillis;

    public ProxyClientAdminClientView(String clientId, ClientType clientType, List<String> groups,
        List<String> topics, String language, String remoteAddress, String localAddress, String clientVersion,
        long connectTimeMillis, long lastActiveTimeMillis) {
        this(clientId, clientType, groups, topics, language, remoteAddress, localAddress, clientVersion, null,
            connectTimeMillis, lastActiveTimeMillis);
    }

    public ProxyClientAdminClientView(String clientId, ClientType clientType, List<String> groups,
        List<String> topics, String language, String remoteAddress, String localAddress, String clientVersion,
        String proxyId, long connectTimeMillis, long lastActiveTimeMillis) {
        this.clientId = normalizeClientId(clientId);
        this.clientType = normalizeClientType(clientType);
        this.groups = immutableCopy(groups, "group", Validators.GROUP_MAX_LENGTH);
        this.topics = immutableCopy(topics, "topic", Validators.TOPIC_MAX_LENGTH);
        this.language = normalizeMetadata(language);
        this.remoteAddress = normalizeMetadata(remoteAddress);
        this.localAddress = normalizeMetadata(localAddress);
        this.clientVersion = normalizeMetadata(clientVersion);
        this.proxyId = normalizeProxyId(proxyId);
        this.connectTimeMillis = connectTimeMillis;
        this.lastActiveTimeMillis = lastActiveTimeMillis;
    }

    private static String normalizeClientId(String clientId) {
        return ProxyClientInfo.normalizeClientId(clientId);
    }

    private static ClientType normalizeClientType(ClientType clientType) {
        if (clientType == null || clientType == ClientType.CLIENT_TYPE_UNSPECIFIED
            || clientType == ClientType.UNRECOGNIZED) {
            return ClientType.CLIENT_TYPE_UNSPECIFIED;
        }
        return clientType;
    }

    private static List<String> immutableCopy(List<String> values, String valueName, int maxLength) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> result = new LinkedHashSet<>();
        for (String value : values) {
            String normalizedValue = StringUtils.trimToNull(value);
            if (normalizedValue != null) {
                validateLength(valueName, normalizedValue, maxLength);
                result.add(normalizedValue);
            }
        }
        if (result.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    private static void validateLength(String valueName, String value, int maxLength) {
        if (value.length() <= maxLength) {
            return;
        }
        if ("group".equals(valueName)) {
            throw new IllegalArgumentException("group length exceeds group max length: " + maxLength);
        }
        throw new IllegalArgumentException("topic length exceeds topic max length " + maxLength);
    }

    private static String normalizeMetadata(String value) {
        return StringUtils.trimToEmpty(value);
    }

    private static String normalizeProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToEmpty(proxyId);
        if (normalizedProxyId.length() > MAX_PROXY_ID_LENGTH) {
            throw new IllegalArgumentException("proxyId length exceeds " + MAX_PROXY_ID_LENGTH);
        }
        return normalizedProxyId;
    }

    public String getClientId() {
        return clientId;
    }

    public ClientType getClientType() {
        return clientType;
    }

    public List<String> getGroups() {
        return groups;
    }

    public List<String> getTopics() {
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
