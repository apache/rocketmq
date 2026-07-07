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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminPeerMessageCodecTest {

    @Test
    public void requestCodecRoundTripsPeerListByGroupRequest() {
        ProxyClientAdminPeerRequest request = ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP)
            .setGroup(" group-a ")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(20)
            .setPageToken(" client-10 ")
            .build();

        String encoded = ProxyClientAdminPeerMessageCodec.getInstance().encodeRequest(request);
        ProxyClientAdminPeerRequest decoded =
            ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(encoded);

        assertThat(decoded.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP);
        assertThat(decoded.getGroup()).isEqualTo("group-a");
        assertThat(decoded.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(decoded.getPageSize()).isEqualTo(20);
        assertThat(decoded.getPageToken()).isEqualTo("client-10");
        assertThat(decoded.getScope()).isEqualTo(request.getScope());
        assertThat(decoded.getProxyId()).isNull();
    }

    @Test
    public void pageResponseCodecRoundTripsSuccessfulPeerPage() {
        ProxyClientPage page = new ProxyClientPage(
            Arrays.asList(
                client("client-a", ClientType.PRODUCER, "proxy-a"),
                client("client-b", ClientType.PUSH_CONSUMER, "proxy-a")
            ),
            "client-b"
        );
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerResponse.success(" proxy-a ", page);

        String encoded = ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(response);
        ProxyClientAdminPeerResponse<ProxyClientPage> decoded =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(encoded);

        assertThat(decoded.isSuccess()).isTrue();
        assertThat(decoded.getProxyId()).isEqualTo("proxy-a");
        assertThat(decoded.getErrorCode()).isEmpty();
        assertThat(decoded.getBody().getNextPageToken()).isEqualTo("client-b");
        assertThat(decoded.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a", "client-b");
        assertThat(decoded.getBody().getClients().get(0).getGroups()).containsExactly("group-a");
        assertThat(decoded.getBody().getClients().get(0).getTopics()).containsExactly("topic-a");
        assertThat(decoded.getBody().getClients().get(0).getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void clientResponseCodecRoundTripsSuccessfulPeerClient() {
        ProxyClientAdminPeerResponse<ProxyClientInfo> response =
            ProxyClientAdminPeerResponse.success("proxy-b", client("client-c", ClientType.SIMPLE_CONSUMER, "proxy-b"));

        String encoded = ProxyClientAdminPeerMessageCodec.getInstance().encodeClientResponse(response);
        ProxyClientAdminPeerResponse<ProxyClientInfo> decoded =
            ProxyClientAdminPeerMessageCodec.getInstance().decodeClientResponse(encoded);

        assertThat(decoded.isSuccess()).isTrue();
        assertThat(decoded.getProxyId()).isEqualTo("proxy-b");
        assertThat(decoded.getBody().getClientId()).isEqualTo("client-c");
        assertThat(decoded.getBody().getClientType()).isEqualTo(ClientType.SIMPLE_CONSUMER);
        assertThat(decoded.getBody().getRemoteAddress()).isEqualTo("127.0.0.1:8080");
        assertThat(decoded.getBody().getConnectTimeMillis()).isEqualTo(100L);
        assertThat(decoded.getBody().getLastActiveTimeMillis()).isEqualTo(200L);
    }

    @Test
    public void responseCodecRoundTripsPeerErrorsWithoutBody() {
        ProxyClientAdminPeerResponse<ProxyClientPage> response =
            ProxyClientAdminPeerResponse.error(" proxy-c ", " NOT_FOUND ", " missing client ");

        String encoded = ProxyClientAdminPeerMessageCodec.getInstance().encodePageResponse(response);
        ProxyClientAdminPeerResponse<ProxyClientPage> decoded =
            ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(encoded);

        assertThat(decoded.isSuccess()).isFalse();
        assertThat(decoded.getProxyId()).isEqualTo("proxy-c");
        assertThat(decoded.getBody()).isNull();
        assertThat(decoded.getErrorCode()).isEqualTo("NOT_FOUND");
        assertThat(decoded.getErrorMessage()).isEqualTo("missing client");
    }

    @Test
    public void pageResponseCodecRejectsErrorResponseWithPageBody() {
        String message = "{\"proxyId\":\"proxy-a\",\"success\":false,"
            + "\"errorCode\":\"NOT_FOUND\",\"errorMessage\":\"missing\","
            + "\"page\":{\"clients\":[],\"nextPageToken\":\"\"}}";

        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(message))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer error response must not include body");
    }

    @Test
    public void clientResponseCodecRejectsErrorResponseWithClientBody() {
        String message = "{\"proxyId\":\"proxy-a\",\"success\":false,"
            + "\"errorCode\":\"NOT_FOUND\",\"errorMessage\":\"missing\","
            + "\"client\":{\"clientId\":\"client-a\"}}";

        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeClientResponse(message))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer error response must not include body");
    }

    @Test
    public void pageResponseCodecRejectsSuccessfulResponseWithClientBody() {
        String message = "{\"proxyId\":\"proxy-a\",\"success\":true,"
            + "\"page\":{\"clients\":[],\"nextPageToken\":\"\"},"
            + "\"client\":{\"clientId\":\"client-a\"}}";

        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(message))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer page response must not include client body");
    }

    @Test
    public void pageResponseCodecRejectsSuccessfulPageWithoutClientsArray() {
        String message = "{\"proxyId\":\"proxy-a\",\"success\":true,"
            + "\"page\":{\"nextPageToken\":\"\"}}";

        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse(message))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer page clients are required");
    }

    @Test
    public void pageResponseCodecRejectsMalformedJsonAsResponseBoundaryError() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodePageResponse("{"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid peer response message");
    }

    @Test
    public void clientResponseCodecRejectsSuccessfulResponseWithPageBody() {
        String message = "{\"proxyId\":\"proxy-a\",\"success\":true,"
            + "\"client\":{\"clientId\":\"client-a\"},"
            + "\"page\":{\"clients\":[],\"nextPageToken\":\"\"}}";

        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeClientResponse(message))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer client response must not include page body");
    }

    @Test
    public void requestCodecRejectsMissingMessage() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(" "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer request message is required");
    }

    @Test
    public void requestCodecRejectsMalformedJsonAsBadRequestBoundaryError() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest("{"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid peer request message");
    }

    @Test
    public void requestCodecRejectsOverlongMessageBeforeParsingJson() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance()
            .decodeRequest(StringUtils.repeat("a", 1024 * 1024 + 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer request message length exceeds");
    }

    @Test
    public void requestCodecRejectsUnknownOperationAsBadRequestBoundaryError() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(
            "{\"operation\":\"LIST_CLIENTS_FROM_MARS\"}"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported peer operation")
            .hasMessageContaining("LIST_CLIENTS_FROM_MARS");
    }

    @Test
    public void requestCodecRejectsUnknownClientTypeAsBadRequestBoundaryError() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(
            "{\"operation\":\"LIST_CLIENTS\",\"clientType\":\"MARS_CLIENT\"}"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported peer clientType")
            .hasMessageContaining("MARS_CLIENT");
    }

    @Test
    public void requestCodecRejectsUnknownScopeAsBadRequestBoundaryError() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(
            "{\"operation\":\"LIST_CLIENTS\",\"scope\":\"MARS_PROXY\"}"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported peer scope")
            .hasMessageContaining("MARS_PROXY");
    }

    @Test
    public void requestCodecRejectsProxyIdBecauseTargetingIsOwnedByCoordinator() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance().decodeRequest(
            "{\"operation\":\"LIST_CLIENTS\",\"proxyId\":\"proxy-a\"}"
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer request must not set proxyId");
    }

    @Test
    public void responseCodecRejectsOverlongMessageBeforeParsingJson() {
        assertThatThrownBy(() -> ProxyClientAdminPeerMessageCodec.getInstance()
            .decodePageResponse(StringUtils.repeat("a", 1024 * 1024 + 1)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("peer response message length exceeds");
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String proxyId) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            new HashSet<>(Collections.singletonList("group-a")),
            new HashSet<>(Collections.singletonList("topic-a")),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.2:8080",
            "V5_0_0",
            proxyId,
            100L,
            200L
        );
    }
}
