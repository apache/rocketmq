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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminResponseConverterTest {

    @Test
    public void convertsClientInfoToStablePublicView() {
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            set("group-b", "group-a"),
            set("topic-b", "topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        ProxyClientAdminClientView view = ProxyClientAdminResponseConverter.toClientView(clientInfo);

        assertThat(view.getClientId()).isEqualTo("client-a");
        assertThat(view.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(view.getGroups()).containsExactly("group-a", "group-b");
        assertThat(view.getTopics()).containsExactly("topic-a", "topic-b");
        assertThat(view.getLanguage()).isEqualTo("JAVA");
        assertThat(view.getRemoteAddress()).isEqualTo("127.0.0.1:8080");
        assertThat(view.getLocalAddress()).isEqualTo("192.168.0.1:8080");
        assertThat(view.getClientVersion()).isEqualTo("V5_0_0");
        assertThat(view.getConnectTimeMillis()).isEqualTo(100L);
        assertThat(view.getLastActiveTimeMillis()).isEqualTo(200L);
    }

    @Test
    public void convertsPageToPublicViewAndEncodesOpaquePageToken() {
        ProxyClientPage page = new ProxyClientPage(
            Arrays.asList(client("client-b"), client("client-a")),
            "client-b"
        );

        ProxyClientAdminPageView view = ProxyClientAdminResponseConverter.toPageView(page);

        assertThat(view.getNextPageToken()).isEqualTo("v1:Y2xpZW50LWI");
        assertThat(ProxyClientAdminPageTokenCodec.getInstance().decode(view.getNextPageToken()))
            .isEqualTo("client-b");
        assertThat(view.getClients())
            .extracting(ProxyClientAdminClientView::getClientId)
            .containsExactly("client-b", "client-a");
    }

    @Test
    public void convertsPageToPublicViewAndPreservesCoordinatorPageToken() {
        Map<String, String> peerPageTokens = new LinkedHashMap<>();
        peerPageTokens.put("proxy-a", "client-a");
        String coordinatorPageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-a")
                .setPeerPageTokens(peerPageTokens)
                .build()
        );
        ProxyClientPage page = new ProxyClientPage(
            Collections.singletonList(client("client-a")),
            coordinatorPageToken
        );

        ProxyClientAdminPageView view = ProxyClientAdminResponseConverter.toPageView(page);

        assertThat(view.getNextPageToken()).isEqualTo(coordinatorPageToken);
    }

    @Test
    public void convertsBlankInternalPageTokenToEmptyPublicToken() {
        ProxyClientPage page = new ProxyClientPage(
            Collections.singletonList(client("client-a")),
            " "
        );

        ProxyClientAdminPageView view = ProxyClientAdminResponseConverter.toPageView(page);

        assertThat(view.getNextPageToken()).isEmpty();
    }

    @Test
    public void convertedCollectionsAreImmutableSnapshots() {
        Set<String> groups = new HashSet<>(Collections.singleton("group-a"));
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            groups,
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        ProxyClientAdminClientView view = ProxyClientAdminResponseConverter.toClientView(clientInfo);
        groups.add("group-b");

        assertThat(view.getGroups()).containsExactly("group-a");
        assertThatThrownBy(() -> view.getGroups().add("group-c"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void convertsNullStringFieldsToEmptyPublicViewValues() {
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            null,
            null,
            null,
            null,
            100L,
            200L
        );

        ProxyClientAdminClientView view = ProxyClientAdminResponseConverter.toClientView(clientInfo);

        assertThat(view.getLanguage()).isEmpty();
        assertThat(view.getRemoteAddress()).isEmpty();
        assertThat(view.getLocalAddress()).isEmpty();
        assertThat(view.getClientVersion()).isEmpty();
        assertThat(view.getProxyId()).isEmpty();
    }

    @Test
    public void convertsNullClientTypeToUnspecifiedPublicViewValue() {
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            null,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        ProxyClientAdminClientView view = ProxyClientAdminResponseConverter.toClientView(clientInfo);

        assertThat(view.getClientType()).isEqualTo(ClientType.CLIENT_TYPE_UNSPECIFIED);
    }

    @Test
    public void convertsProxyIdToPublicView() {
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            "proxy-a",
            100L,
            200L
        );

        ProxyClientAdminClientView view = ProxyClientAdminResponseConverter.toClientView(clientInfo);

        assertThat(view.getProxyId()).isEqualTo("proxy-a");
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }

    private static Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}
