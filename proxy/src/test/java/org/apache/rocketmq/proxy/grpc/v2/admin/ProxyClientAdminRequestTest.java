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
            .setPageToken("v1:Y2xpZW50LWE")
            .build();

        ProxyClientQuery query = request.toQuery();

        assertThat(query.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(query.getPageSize()).isEqualTo(10);
        assertThat(query.getPageToken()).isEqualTo("client-a");
        assertThat(query.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(query.getGroup()).isNull();
        assertThat(query.getTopic()).isNull();
        assertThat(query.getProxyId()).isNull();
    }

    @Test
    public void listClientsRequestBoundsPageSizeAtBoundary() {
        ProxyClientAdminListClientsRequest defaultPageSizeRequest =
            ProxyClientAdminListClientsRequest.newBuilder()
                .setPageSize(0)
                .build();
        ProxyClientAdminListClientsRequest cappedPageSizeRequest =
            ProxyClientAdminListClientsRequest.newBuilder()
                .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE + 1)
                .build();

        assertThat(defaultPageSizeRequest.getPageSize()).isEqualTo(ProxyClientQuery.DEFAULT_PAGE_SIZE);
        assertThat(defaultPageSizeRequest.toQuery().getPageSize()).isEqualTo(ProxyClientQuery.DEFAULT_PAGE_SIZE);
        assertThat(cappedPageSizeRequest.getPageSize()).isEqualTo(ProxyClientQuery.MAX_PAGE_SIZE);
        assertThat(cappedPageSizeRequest.toQuery().getPageSize()).isEqualTo(ProxyClientQuery.MAX_PAGE_SIZE);
    }

    @Test
    public void peerRequestRejectsCoordinatorScopes() {
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported peer request scope")
            .hasMessageContaining("ALL_PROXIES");

        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported peer request scope")
            .hasMessageContaining("PROXY_ID");
    }

    @Test
    public void listClientsRequestPreservesUnsupportedScopeButDropsProxyIdForAllProxies() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setProxyId("proxy-a")
            .build();

        assertThat(request.toQuery().getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.toQuery().getProxyId()).isNull();
        assertThat(request.getProxyId()).isNull();
    }

    @Test
    public void listClientsRequestMapsPublicScopeNameForFutureProtoAdapter() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScopeName("PROXY_SCOPE_PROXY_ID")
            .setProxyId("proxy-a")
            .build();

        assertThat(request.toQuery().getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(request.toQuery().getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void proxyIdScopedRequestsRejectOverlongProxyIds() {
        String proxyId = StringUtils.repeat("p", 256);

        assertThatThrownBy(() -> ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(proxyId)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");

        assertThatThrownBy(() -> ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(proxyId)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");

        assertThatThrownBy(() -> ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup("group-a")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(proxyId)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");

        assertThatThrownBy(() -> ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic("topic-a")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(proxyId)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");
    }

    @Test
    public void listClientsRequestTrimsPageTokenAndIgnoresProxyIdForLocalScope() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setPageToken(" v1:Y2xpZW50LWE ")
            .setProxyId(" proxy-a ")
            .build();

        ProxyClientQuery query = request.toQuery();

        assertThat(request.getPageToken()).isEqualTo("v1:Y2xpZW50LWE");
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getPageToken()).isEqualTo("client-a");
        assertThat(query.getProxyId()).isNull();
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
    public void listClientsRequestRejectsCoordinatorPageTokenForLocalProxyScope() {
        String coordinatorToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-a")
                .putPeerPageToken("proxy-a", "client-a")
                .build()
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setPageToken(coordinatorToken)
            .build();

        assertThatThrownBy(request::toQuery)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid page token")
            .hasMessageContaining("cp1:");
    }

    @Test
    public void listClientsRequestPreservesCoordinatorPageTokenForAllProxiesScope() {
        String coordinatorToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-a")
                .setLastProxyId("proxy-a")
                .putPeerPageToken("proxy-a", "client-a")
                .build()
        );
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageToken(coordinatorToken)
            .build();

        assertThat(request.toQuery().getPageToken()).isEqualTo(coordinatorToken);
    }

    @Test
    public void listClientsRequestRejectsLocalPageTokensForAllProxiesScope() {
        ProxyClientAdminListClientsRequest versionedLocalTokenRequest =
            ProxyClientAdminListClientsRequest.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPageToken("v1:Y2xpZW50LWE")
                .build();
        ProxyClientAdminListClientsRequest legacyBareLocalTokenRequest =
            ProxyClientAdminListClientsRequest.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPageToken("client-a")
                .build();

        assertThatThrownBy(versionedLocalTokenRequest::toQuery)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token")
            .hasMessageContaining("v1:Y2xpZW50LWE");
        assertThatThrownBy(legacyBareLocalTokenRequest::toQuery)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token")
            .hasMessageContaining("client-a");
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
    public void describeClientRequestCarriesClientIdDefaultsScopeAndIgnoresLocalProxyId() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setProxyId("proxy-a")
            .build();

        assertThat(request.getClientId()).isEqualTo("client-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
    }

    @Test
    public void describeClientRequestPreservesProxyIdForFutureProxyIdScope() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        assertThat(request.getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void describeClientRequestDropsProxyIdForAllProxiesScope() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("client-a")
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setProxyId("proxy-a")
            .build();

        assertThat(request.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.getProxyId()).isNull();
    }

    @Test
    public void describeClientRequestTrimsClientIdAndProxyId() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId(" client-a ")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(" proxy-a ")
            .build();

        assertThat(request.getClientId()).isEqualTo("client-a");
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
        assertThat(query.getProxyId()).isNull();
        assertThat(request.getProxyId()).isNull();
    }

    @Test
    public void listClientsByGroupRequestAcceptsLegacyBarePageToken() {
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup("group-a")
            .setPageToken("client-b")
            .build();

        assertThat(request.toQuery().getPageToken()).isEqualTo("client-b");
    }

    @Test
    public void listClientsByGroupRequestTrimsGroup() {
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminListClientsByGroupRequest.newBuilder()
            .setGroup(" group-a ")
            .build();

        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(request.toQuery().getGroup()).isEqualTo("group-a");
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

    @Test
    public void listClientsByTopicRequestTrimsTopic() {
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminListClientsByTopicRequest.newBuilder()
            .setTopic(" topic-a ")
            .build();

        assertThat(request.getTopic()).isEqualTo("topic-a");
        assertThat(request.toQuery().getTopic()).isEqualTo("topic-a");
    }
}
