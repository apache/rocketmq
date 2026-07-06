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
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminPeerDtoTest {

    @Test
    public void peerListByGroupRequestBuildsLocalQueryWithRawPeerPageToken() {
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setProxyId(" proxy-b ")
            .setGroup(" group-a ")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(20)
            .setPageToken(" client-20 ")
            .build();

        ProxyClientQuery localQuery = request.toLocalQuery();

        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP);
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(request.getProxyId()).isEqualTo("proxy-b");
        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(localQuery.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(localQuery.getProxyId()).isNull();
        assertThat(localQuery.getGroup()).isEqualTo("group-a");
        assertThat(localQuery.getTopic()).isNull();
        assertThat(localQuery.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(localQuery.getPageSize()).isEqualTo(20);
        assertThat(localQuery.getPageToken()).isEqualTo("client-20");
    }

    @Test
    public void peerRequestValidatesOperationSpecificFields() {
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder().build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("operation is required");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId(" ")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("group is required");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topic is required");
    }

    @Test
    public void peerDescribeRequestNormalizesClientAndBuildsLocalScope() {
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(" proxy-b ")
            .setClientId(" client-a ")
            .build();

        ProxyClientAdminDescribeClientRequest localRequest = request.toLocalDescribeClientRequest();

        assertThat(request.getClientId()).isEqualTo("client-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(request.getProxyId()).isEqualTo("proxy-b");
        assertThat(localRequest.getClientId()).isEqualTo("client-a");
        assertThat(localRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(localRequest.getProxyId()).isNull();
    }

    @Test
    public void peerResponseWrapsSuccessAndErrorsWithoutPublicProtoBodies() {
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "remote",
            "local",
            "1.0.0",
            1000L,
            2000L
        );
        ProxyClientPage page = new ProxyClientPage(Collections.singletonList(clientInfo), "client-a");

        ProxyClientAdminPeerResponse<ProxyClientPage> success =
            ProxyClientAdminPeerResponse.success(" proxy-b ", page);
        ProxyClientAdminPeerResponse<ProxyClientPage> error =
            ProxyClientAdminPeerResponse.error(" proxy-c ", " NOT_FOUND ", " missing ");

        assertThat(success.isSuccess()).isTrue();
        assertThat(success.getProxyId()).isEqualTo("proxy-b");
        assertThat(success.getBody()).isSameAs(page);
        assertThat(success.getErrorCode()).isEmpty();
        assertThat(success.getErrorMessage()).isEmpty();
        assertThat(error.isSuccess()).isFalse();
        assertThat(error.getProxyId()).isEqualTo("proxy-c");
        assertThat(error.getBody()).isNull();
        assertThat(error.getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(error.getErrorMessage()).isEqualTo("missing");
    }
}
