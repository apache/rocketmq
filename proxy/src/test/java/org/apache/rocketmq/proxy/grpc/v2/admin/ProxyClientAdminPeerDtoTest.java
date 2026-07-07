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
            .setGroup(" group-a ")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(20)
            .setPageToken(" client-20 ")
            .build();

        ProxyClientQuery localQuery = request.toLocalQuery();

        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP);
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
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
    public void peerRequestBoundsPageSizeAtBoundary() {
        ProxyClientAdminPeerRequest defaultPageSizeRequest = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setPageSize(0)
            .build();
        ProxyClientAdminPeerRequest cappedPageSizeRequest = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE + 1)
            .build();

        assertThat(defaultPageSizeRequest.getPageSize()).isEqualTo(ProxyClientQuery.DEFAULT_PAGE_SIZE);
        assertThat(defaultPageSizeRequest.toLocalQuery().getPageSize()).isEqualTo(ProxyClientQuery.DEFAULT_PAGE_SIZE);
        assertThat(cappedPageSizeRequest.getPageSize()).isEqualTo(ProxyClientQuery.MAX_PAGE_SIZE);
        assertThat(cappedPageSizeRequest.toLocalQuery().getPageSize()).isEqualTo(ProxyClientQuery.MAX_PAGE_SIZE);
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
    public void peerRequestRejectsOperationMismatchedFilters() {
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setGroup("group-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS")
            .hasMessageContaining("group");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .setGroup("group-a")
            .setTopic("topic-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS_BY_GROUP")
            .hasMessageContaining("topic");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC)
            .setTopic("topic-a")
            .setGroup("group-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS_BY_TOPIC")
            .hasMessageContaining("group");
    }

    @Test
    public void peerRequestRejectsOperationMismatchedIdentityAndListFields() {
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId("client-a")
            .setGroup("group-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DESCRIBE_CLIENT")
            .hasMessageContaining("group");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId("client-a")
            .setTopic("topic-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DESCRIBE_CLIENT")
            .hasMessageContaining("topic");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId("client-a")
            .setClientType(ClientType.PRODUCER)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DESCRIBE_CLIENT")
            .hasMessageContaining("clientType");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId("client-a")
            .setPageToken("client-b")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DESCRIBE_CLIENT")
            .hasMessageContaining("pageToken");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS)
            .setClientId("client-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS")
            .hasMessageContaining("clientId");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .setGroup("group-a")
            .setClientId("client-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS_BY_GROUP")
            .hasMessageContaining("clientId");
        assertThatThrownBy(() -> ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC)
            .setTopic("topic-a")
            .setClientId("client-a")
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("LIST_CLIENTS_BY_TOPIC")
            .hasMessageContaining("clientId");
    }

    @Test
    public void peerDescribeRequestNormalizesClientAndBuildsLocalScope() {
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId(" client-a ")
            .build();

        ProxyClientAdminDescribeClientRequest localRequest = request.toLocalDescribeClientRequest();

        assertThat(request.getClientId()).isEqualTo("client-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(request.getProxyId()).isNull();
        assertThat(localRequest.getClientId()).isEqualTo("client-a");
        assertThat(localRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(localRequest.getProxyId()).isNull();
    }

    @Test
    public void peerDescribeRequestRejectsLocalQueryConversion() {
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId("client-a")
            .build();

        assertThatThrownBy(request::toLocalQuery)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("operation is not a list operation: DESCRIBE_CLIENT");
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
