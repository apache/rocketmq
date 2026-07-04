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

package org.apache.rocketmq.proxy.service.admin.client;

import apache.rocketmq.v2.ClientType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultClientAdminServiceTest {

    @Test
    public void listClientsDelegatesToReadModel() {
        ProxyClientReadService readService = new ProxyClientReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);
        readService.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        readService.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-b")));

        ProxyClientPage page = adminService.listClients(ProxyClientQuery.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-a");
    }

    @Test
    public void describeClientReturnsExistingClient() {
        ProxyClientReadService readService = new ProxyClientReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);
        ProxyClientInfo clientInfo = client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"));
        readService.upsertClient(clientInfo);

        assertThat(adminService.describeClient("client-a")).isSameAs(clientInfo);
    }

    @Test
    public void describeClientRejectsMissingClientId() {
        ClientAdminService adminService = new DefaultClientAdminService(new ProxyClientReadService());

        assertThatThrownBy(() -> adminService.describeClient(" "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");
    }

    @Test
    public void describeClientRejectsUnknownClient() {
        ClientAdminService adminService = new DefaultClientAdminService(new ProxyClientReadService());

        assertThatThrownBy(() -> adminService.describeClient("missing-client"))
            .isInstanceOf(NoSuchElementException.class)
            .hasMessageContaining("Client not found")
            .hasMessageContaining("missing-client");
    }

    @Test
    public void listClientsByGroupFiltersByGroup() {
        ProxyClientReadService readService = new ProxyClientReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);
        readService.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        readService.upsertClient(client("client-b", ClientType.PRODUCER, set("group-b"), set("topic-a")));

        ProxyClientPage page = adminService.listClientsByGroup("group-a", ProxyClientQuery.newBuilder().build());

        assertThat(clientIds(page.getClients())).containsExactly("client-a");
    }

    @Test
    public void listClientsByTopicFiltersByTopic() {
        ProxyClientReadService readService = new ProxyClientReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);
        readService.upsertClient(client("client-a", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a")));
        readService.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-b")));

        ProxyClientPage page = adminService.listClientsByTopic("topic-b", ProxyClientQuery.newBuilder().build());

        assertThat(clientIds(page.getClients())).containsExactly("client-b");
    }

    @Test
    public void listClientsPropagatesInvalidPageToken() {
        ProxyClientReadService readService = new ProxyClientReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);
        readService.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));

        assertThatThrownBy(() -> adminService.listClients(ProxyClientQuery.newBuilder()
            .setPageToken("missing-client")
            .build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid page token");
    }

    @Test
    public void listClientsRejectsUnsupportedScope() {
        ClientAdminService adminService = new DefaultClientAdminService(new ProxyClientReadService());

        assertThatThrownBy(() -> adminService.listClients(ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported proxy scope")
            .hasMessageContaining("ALL_PROXIES");
    }

    @Test
    public void listClientsByGroupRejectsUnsupportedScope() {
        ClientAdminService adminService = new DefaultClientAdminService(new ProxyClientReadService());

        assertThatThrownBy(() -> adminService.listClientsByGroup("group-a", ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported proxy scope")
            .hasMessageContaining("ALL_PROXIES");
    }

    @Test
    public void listClientsByTopicRejectsUnsupportedScope() {
        ClientAdminService adminService = new DefaultClientAdminService(new ProxyClientReadService());

        assertThatThrownBy(() -> adminService.listClientsByTopic("topic-a", ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported proxy scope")
            .hasMessageContaining("PROXY_ID");
    }

    @Test
    public void listClientsByGroupPreservesProxyIdWhenMergingQuery() {
        CapturingReadService readService = new CapturingReadService();
        ClientAdminService adminService = new DefaultClientAdminService(readService);

        adminService.listClientsByGroup("group-a", ProxyClientQuery.newBuilder()
            .setProxyId("proxy-a")
            .build());

        assertThat(readService.capturedQuery.getGroup()).isEqualTo("group-a");
        assertThat(readService.capturedQuery.getProxyId()).isEqualTo("proxy-a");
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, Set<String> groups, Set<String> topics) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            groups,
            topics,
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

    private static List<String> clientIds(List<ProxyClientInfo> clients) {
        return clients.stream().map(ProxyClientInfo::getClientId).collect(Collectors.toList());
    }

    private static class CapturingReadService extends ProxyClientReadService {
        private ProxyClientQuery capturedQuery;

        @Override
        public synchronized ProxyClientPage listClients(ProxyClientQuery query) {
            this.capturedQuery = query;
            return new ProxyClientPage(Arrays.<ProxyClientInfo>asList(), null);
        }
    }
}
