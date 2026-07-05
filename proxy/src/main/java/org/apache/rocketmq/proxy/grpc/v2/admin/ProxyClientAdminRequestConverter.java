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

public final class ProxyClientAdminRequestConverter {
    private static final ProxyClientAdminRequestConverter INSTANCE = new ProxyClientAdminRequestConverter();

    private ProxyClientAdminRequestConverter() {
    }

    public static ProxyClientAdminRequestConverter getInstance() {
        return INSTANCE;
    }

    public ProxyClientAdminListClientsRequest toListClientsRequest(ClientType clientType, int pageSize,
        String pageToken, String scopeName, String proxyId) {
        return ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScopeName(scopeName)
            .setProxyId(proxyId)
            .build();
    }

    public ProxyClientAdminDescribeClientRequest toDescribeClientRequest(String clientId, String scopeName,
        String proxyId) {
        return ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId(clientId)
            .setScopeName(scopeName)
            .setProxyId(proxyId)
            .build();
    }

    public ProxyClientAdminListClientsByGroupRequest toListClientsByGroupRequest(String group, ClientType clientType,
        int pageSize, String pageToken, String scopeName, String proxyId) {
        return ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup(group)
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScopeName(scopeName)
            .setProxyId(proxyId)
            .build();
    }

    public ProxyClientAdminListClientsByTopicRequest toListClientsByTopicRequest(String topic, ClientType clientType,
        int pageSize, String pageToken, String scopeName, String proxyId) {
        return ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic(topic)
            .setClientType(clientType)
            .setPageSize(pageSize)
            .setPageToken(pageToken)
            .setScopeName(scopeName)
            .setProxyId(proxyId)
            .build();
    }
}
