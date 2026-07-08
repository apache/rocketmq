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
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public final class ProxyClientAdminRequestConverter {
    private static final ProxyClientAdminRequestConverter INSTANCE = new ProxyClientAdminRequestConverter();

    private ProxyClientAdminRequestConverter() {
    }

    public static ProxyClientAdminRequestConverter getInstance() {
        return INSTANCE;
    }

    public ProxyClientAdminListClientsRequest toListClientsRequest(ClientType clientType, int pageSize,
        String pageToken, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        this.rejectUnrecognizedClientType(clientType);
        return ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminListClientsRequest toListClientsRequest(String clientId, String clientIdPrefix,
        String group, String topic, String clientLanguage, Long connectTimeStartMillis, Long connectTimeEndMillis,
        int pageNum, int pageSize, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        return ProxyClientAdminListClientsRequest.newBuilder()
            .setClientId(clientId)
            .setClientIdPrefix(clientIdPrefix)
            .setGroup(group)
            .setTopic(topic)
            .setClientLanguage(clientLanguage)
            .setConnectTimeStartMillis(connectTimeStartMillis)
            .setConnectTimeEndMillis(connectTimeEndMillis)
            .setPageNum(pageNum)
            .setPageSize(pageSize)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminDescribeClientRequest toDescribeClientRequest(String clientId, String scopeName,
        String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        return ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId(clientId)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminListClientsByGroupRequest toListClientsByGroupRequest(String group, ClientType clientType,
        int pageSize, String pageToken, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        this.rejectUnrecognizedClientType(clientType);
        return ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup(group)
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminListClientsByGroupRequest toListClientsByGroupRequest(String group, String clientId,
        String clientIdPrefix, String clientLanguage, Long connectTimeStartMillis, Long connectTimeEndMillis,
        int pageNum, int pageSize, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        return ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup(group)
            .setClientId(clientId)
            .setClientIdPrefix(clientIdPrefix)
            .setClientLanguage(clientLanguage)
            .setConnectTimeStartMillis(connectTimeStartMillis)
            .setConnectTimeEndMillis(connectTimeEndMillis)
            .setPageNum(pageNum)
            .setPageSize(pageSize)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminListClientsByTopicRequest toListClientsByTopicRequest(String topic, ClientType clientType,
        int pageSize, String pageToken, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        this.rejectUnrecognizedClientType(clientType);
        return ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic(topic)
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    public ProxyClientAdminListClientsByTopicRequest toListClientsByTopicRequest(String topic, String clientId,
        String clientIdPrefix, String clientLanguage, Long connectTimeStartMillis, Long connectTimeEndMillis,
        int pageNum, int pageSize, String scopeName, String proxyId) {
        ProxyClientScope scope = this.decodeScope(scopeName);
        return ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic(topic)
            .setClientId(clientId)
            .setClientIdPrefix(clientIdPrefix)
            .setClientLanguage(clientLanguage)
            .setConnectTimeStartMillis(connectTimeStartMillis)
            .setConnectTimeEndMillis(connectTimeEndMillis)
            .setPageNum(pageNum)
            .setPageSize(pageSize)
            .setScope(scope)
            .setProxyId(this.proxyIdForScope(scope, proxyId))
            .build();
    }

    private ProxyClientScope decodeScope(String scopeName) {
        return ProxyClientAdminScopeMapper.getInstance().decode(scopeName);
    }

    private String proxyIdForScope(ProxyClientScope scope, String proxyId) {
        if (scope == ProxyClientScope.PROXY_ID) {
            return proxyId;
        }
        return null;
    }

    private void rejectUnrecognizedClientType(ClientType clientType) {
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
    }
}
