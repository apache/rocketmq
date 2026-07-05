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
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminRequestTest {

    @Test
    public void listClientsRequestBuildsQueryWithDefaults() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .setPageSize(10)
            .setPageToken("opaque-token")
            .build();

        ProxyClientQuery query = request.toQuery();

        assertThat(query.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(query.getPageSize()).isEqualTo(10);
        assertThat(query.getPageToken()).isEqualTo("opaque-token");
        assertThat(query.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(query.getGroup()).isNull();
        assertThat(query.getTopic()).isNull();
        assertThat(query.getProxyId()).isNull();
    }

    @Test
    public void listClientsRequestPreservesUnsupportedScopeForServiceValidation() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setProxyId("proxy-a")
            .build();

        assertThat(request.toQuery().getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.toQuery().getProxyId()).isEqualTo("proxy-a");
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void listClientsRequestMapsPublicScopeNameForFutureProtoAdapter() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScopeName("PROXY_ID")
            .setProxyId("proxy-a")
            .build();

        assertThat(request.toQuery().getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(request.toQuery().getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void listClientsRequestTreatsUnspecifiedClientTypeAsNoFilter() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build();

        assertThat(request.toQuery().getClientType()).isNull();
    }

    @Test
    public void listClientsRequestDecodesBlankPageTokenAsNoToken() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setPageToken(" ")
            .build();

        assertThat(request.toQuery().getPageToken()).isNull();
    }

    @Test
    public void listClientsRequestRejectsUnrecognizedClientType() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientType(ClientType.UNRECOGNIZED)
            .build();

        assertThatThrownBy(request::toQuery)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported client type")
            .hasMessageContaining("UNRECOGNIZED");
    }

    @Test
    public void describeClientRequestCarriesClientIdAndDefaultsScope() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setProxyId("proxy-a")
            .build();

        assertThat(request.getClientId()).isEqualTo("client-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void describeClientRequestMapsUnspecifiedPublicScopeNameToLocalProxy() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setScopeName("PROXY_SCOPE_UNSPECIFIED")
            .build();

        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsByGroupRequestCarriesGroupAndQueryFilters() {
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup("group-a")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(20)
            .setPageToken("client-b")
            .setProxyId("proxy-a")
            .build();

        ProxyClientQuery query = request.toQuery();

        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(query.getGroup()).isEqualTo("group-a");
        assertThat(query.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(query.getPageSize()).isEqualTo(20);
        assertThat(query.getPageToken()).isEqualTo("client-b");
        assertThat(query.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(query.getProxyId()).isEqualTo("proxy-a");
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void listClientsByTopicRequestCarriesTopicAndOpaquePageToken() {
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic("topic-a")
            .setPageSize(30)
            .setPageToken("opaque-token")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        ProxyClientQuery query = request.toQuery();

        assertThat(request.getTopic()).isEqualTo("topic-a");
        assertThat(query.getTopic()).isEqualTo("topic-a");
        assertThat(query.getPageSize()).isEqualTo(30);
        assertThat(query.getPageToken()).isEqualTo("opaque-token");
        assertThat(query.getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(query.getProxyId()).isEqualTo("proxy-a");
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }
}
