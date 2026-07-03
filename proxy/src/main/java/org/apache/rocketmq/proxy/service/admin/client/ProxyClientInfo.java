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

public class ProxyClientInfo {
    private final String clientId;
    private final ClientType clientType;
    private final Set<String> groups;
    private final Set<String> topics;
    private final String language;
    private final String remoteAddress;
    private final String localAddress;
    private final String clientVersion;
    private final long connectTimeMillis;
    private final long lastActiveTimeMillis;

    public ProxyClientInfo(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String language, String remoteAddress, String localAddress, String clientVersion, long connectTimeMillis,
        long lastActiveTimeMillis) {
        this.clientId = clientId;
        this.clientType = clientType;
        this.groups = normalize(groups);
        this.topics = normalize(topics);
        this.language = language;
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.clientVersion = clientVersion;
        this.connectTimeMillis = connectTimeMillis;
        this.lastActiveTimeMillis = lastActiveTimeMillis;
    }

    private static Set<String> normalize(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (String value : values) {
            if (value != null && !value.isEmpty()) {
                result.add(value);
            }
        }
        return Collections.unmodifiableSet(result);
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

    public long getConnectTimeMillis() {
        return connectTimeMillis;
    }

    public long getLastActiveTimeMillis() {
        return lastActiveTimeMillis;
    }
}
