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

public class ProxyClientAdminRequestConverterTest {

    @Test
    public void toListClientsRequestIgnoresProxyIdForLocalProxyScope() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toListClientsRequest(
                ClientType.PRODUCER,
                10,
                " v1:Y2xpZW50LWE ",
                " PROXY_SCOPE_LOCAL_PROXY ",
                " proxy-a "
            );

        ProxyClientQuery query = request.toQuery();
        assertThat(request.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(request.getPageSize()).isEqualTo(10);
        assertThat(request.getPageToken()).isEqualTo("v1:Y2xpZW50LWE");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getPageToken()).isEqualTo("client-a");
        assertThat(query.getProxyId()).isNull();
    }

    @Test
    public void toListClientsRequestIgnoresProxyIdForAllProxiesScope() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toListClientsRequest(
                ClientType.PRODUCER,
                10,
                "",
                "PROXY_SCOPE_ALL_PROXIES",
                " proxy-a "
            );

        ProxyClientQuery query = request.toQuery();
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getProxyId()).isNull();
    }

    @Test
    public void toListClientsRequestRejectsMissingProxyIdForProxyIdScope() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsRequest(
                ClientType.PRODUCER,
                10,
                "",
                "PROXY_SCOPE_PROXY_ID",
                " "
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
    }

    @Test
    public void toListClientsRequestRejectsUnrecognizedClientTypeAtAdapterBoundary() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsRequest(
                ClientType.UNRECOGNIZED,
                10,
                "",
                "PROXY_SCOPE_LOCAL_PROXY",
                ""
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported client type")
            .hasMessageContaining("UNRECOGNIZED");
    }

    @Test
    public void toDescribeClientRequestMapsPublicFields() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toDescribeClientRequest(
                " client-a ",
                "PROXY_SCOPE_PROXY_ID",
                " proxy-a "
            );

        assertThat(request.getClientId()).isEqualTo("client-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(request.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void toDescribeClientRequestRejectsMissingProxyIdForProxyIdScope() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toDescribeClientRequest(
                "client-a",
                "PROXY_SCOPE_PROXY_ID",
                " "
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
    }

    @Test
    public void toListClientsByGroupRequestMapsPublicFields() {
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setGroup("group-a")
                .setLastClientId("client-b")
                .setLastProxyId("proxy-a")
                .putPeerPageToken("proxy-a", "client-b")
                .build()
        );
        ProxyClientAdminListClientsByGroupRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByGroupRequest(
                " group-a ",
                ClientType.PUSH_CONSUMER,
                20,
                pageToken,
                "PROXY_SCOPE_ALL_PROXIES",
                " proxy-a "
            );

        ProxyClientQuery query = request.toQuery();
        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(request.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getGroup()).isEqualTo("group-a");
        assertThat(query.getPageSize()).isEqualTo(20);
        assertThat(query.getPageToken()).isEqualTo(pageToken);
        assertThat(query.getProxyId()).isNull();
    }

    @Test
    public void toListClientsByGroupRequestRejectsMissingProxyIdForProxyIdScope() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByGroupRequest(
                "group-a",
                ClientType.PUSH_CONSUMER,
                20,
                "",
                "PROXY_SCOPE_PROXY_ID",
                " "
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
    }

    @Test
    public void toListClientsByGroupRequestRejectsOverlongGroupAtAdapterBoundary() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByGroupRequest(
                StringUtils.repeat("g", 121),
                ClientType.PUSH_CONSUMER,
                20,
                "",
                "PROXY_SCOPE_LOCAL_PROXY",
                ""
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("group max length: 120");
    }

    @Test
    public void toListClientsByGroupRequestRejectsUnrecognizedClientTypeAtAdapterBoundary() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByGroupRequest(
                "group-a",
                ClientType.UNRECOGNIZED,
                20,
                "",
                "PROXY_SCOPE_LOCAL_PROXY",
                ""
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported client type")
            .hasMessageContaining("UNRECOGNIZED");
    }

    @Test
    public void toListClientsByTopicRequestMapsPublicFields() {
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByTopicRequest(
                " topic-a ",
                ClientType.SIMPLE_CONSUMER,
                30,
                "",
                "",
                " "
            );

        ProxyClientQuery query = request.toQuery();
        assertThat(request.getTopic()).isEqualTo("topic-a");
        assertThat(request.getClientType()).isEqualTo(ClientType.SIMPLE_CONSUMER);
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getTopic()).isEqualTo("topic-a");
        assertThat(query.getPageSize()).isEqualTo(30);
        assertThat(query.getPageToken()).isNull();
    }

    @Test
    public void toListClientsByTopicRequestRejectsMissingProxyIdForProxyIdScope() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByTopicRequest(
                "topic-a",
                ClientType.SIMPLE_CONSUMER,
                30,
                "",
                "PROXY_SCOPE_PROXY_ID",
                " "
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId is required");
    }

    @Test
    public void toListClientsByTopicRequestRejectsOverlongTopicAtAdapterBoundary() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByTopicRequest(
                StringUtils.repeat("t", 128),
                ClientType.SIMPLE_CONSUMER,
                30,
                "",
                "PROXY_SCOPE_LOCAL_PROXY",
                ""
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topic max length 127");
    }

    @Test
    public void toListClientsByTopicRequestRejectsUnrecognizedClientTypeAtAdapterBoundary() {
        assertThatThrownBy(() -> ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByTopicRequest(
                "topic-a",
                ClientType.UNRECOGNIZED,
                30,
                "",
                "PROXY_SCOPE_LOCAL_PROXY",
                ""
            ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported client type")
            .hasMessageContaining("UNRECOGNIZED");
    }

    @Test
    public void toListClientsByTopicRequestIgnoresProxyIdForUnspecifiedPublicScope() {
        ProxyClientAdminListClientsByTopicRequest request = ProxyClientAdminRequestConverter.getInstance()
            .toListClientsByTopicRequest(
                " topic-a ",
                ClientType.SIMPLE_CONSUMER,
                30,
                "",
                "PROXY_SCOPE_UNSPECIFIED",
                " proxy-a "
            );

        ProxyClientQuery query = request.toQuery();
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
        assertThat(query.getProxyId()).isNull();
    }
}
