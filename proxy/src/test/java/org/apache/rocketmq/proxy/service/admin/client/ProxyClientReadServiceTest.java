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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientReadServiceTest {

    @Test
    public void listClientsReturnsStablePagesOrderedByClientId() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-c", ClientType.PRODUCER, set("group-c"), set("topic-c")));
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-b"), set("topic-b")));

        ProxyClientPage firstPage = service.listClients(ProxyClientQuery.newBuilder()
            .setPageSize(2)
            .build());

        assertThat(clientIds(firstPage.getClients())).containsExactly("client-a", "client-b");
        assertThat(firstPage.getNextPageToken()).isEqualTo("client-b");

        ProxyClientPage secondPage = service.listClients(ProxyClientQuery.newBuilder()
            .setPageSize(2)
            .setPageToken(firstPage.getNextPageToken())
            .build());

        assertThat(clientIds(secondPage.getClients())).containsExactly("client-c");
        assertThat(secondPage.getNextPageToken()).isEmpty();
    }

    @Test
    public void upsertClientRefreshesGroupAndTopicIndexes() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));

        service.upsertClient(client("client-a", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-b", "topic-c")));

        assertThat(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-a").build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-a").build()).getClients()).isEmpty();
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-b").build()).getClients()))
            .containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-b").build()).getClients()))
            .containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().setClientType(ClientType.PUSH_CONSUMER).build()).getClients()))
            .containsExactly("client-a");
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientType(ClientType.PRODUCER).build()).getClients()).isEmpty();
    }

    @Test
    public void removeClientDeletesClientAndIndexes() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a")));

        service.removeClient("client-a");

        assertThat(service.getClient("client-a")).isNull();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-a").build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-a").build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientType(ClientType.PUSH_CONSUMER).build()).getClients()).isEmpty();
    }

    @Test
    public void listClientsRejectsInvalidPageToken() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-b"), set("topic-b")));

        assertThatThrownBy(() -> service.listClients(ProxyClientQuery.newBuilder()
            .setPageSize(1)
            .setPageToken("missing-client")
            .build()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid page token");
    }

    @Test
    public void listClientsTreatsUnspecifiedClientTypeAsNoFilter() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-b")));

        ProxyClientPage page = service.listClients(ProxyClientQuery.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-a", "client-b");
    }

    @Test
    public void upsertClientTreatsUnspecifiedClientTypeAsMissingType() {
        ProxyClientReadService service = new ProxyClientReadService();

        service.upsertClient(client(
            "client-a",
            ClientType.CLIENT_TYPE_UNSPECIFIED,
            set("group-a"),
            set("topic-a")
        ));

        ProxyClientReadServiceStats stats = service.snapshotStats();
        assertThat(service.getClient("client-a").getClientType()).isNull();
        assertThat(stats.getClientTypeCount(ClientType.CLIENT_TYPE_UNSPECIFIED)).isEqualTo(0L);
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().build()).getClients()))
            .containsExactly("client-a");
    }

    @Test
    public void snapshotStatsReflectsCurrentReadModel() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a", "topic-b")));
        service.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-a", "group-b"), set("topic-a")));

        ProxyClientReadServiceStats stats = service.snapshotStats();

        assertThat(stats.getTotalClientCount()).isEqualTo(2L);
        assertThat(stats.getGroupIndexCount()).isEqualTo(2L);
        assertThat(stats.getTopicIndexCount()).isEqualTo(2L);
        assertThat(stats.getClientTypeCounts())
            .containsEntry(ClientType.PRODUCER, 1L)
            .containsEntry(ClientType.PUSH_CONSUMER, 1L);

        service.removeClient("client-a");
        stats = service.snapshotStats();

        assertThat(stats.getTotalClientCount()).isEqualTo(1L);
        assertThat(stats.getGroupIndexCount()).isEqualTo(2L);
        assertThat(stats.getTopicIndexCount()).isEqualTo(1L);
        assertThat(stats.getClientTypeCount(ClientType.PRODUCER)).isEqualTo(0L);
        assertThat(stats.getClientTypeCount(ClientType.PUSH_CONSUMER)).isEqualTo(1L);
    }

    @Test
    public void upsertClientIgnoresBlankGroupAndTopicIndexValues() {
        ProxyClientReadService service = new ProxyClientReadService();

        service.upsertClient(client("client-a", ClientType.PRODUCER,
            set("group-a", "", "  "),
            set("topic-a", "\t")));

        ProxyClientReadServiceStats stats = service.snapshotStats();

        assertThat(stats.getGroupIndexCount()).isEqualTo(1L);
        assertThat(stats.getTopicIndexCount()).isEqualTo(1L);
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .build()).getClients())).containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setTopic("topic-a")
            .build()).getClients())).containsExactly("client-a");
    }

    @Test
    public void upsertClientTrimsGroupAndTopicIndexValues() {
        ProxyClientReadService service = new ProxyClientReadService();

        service.upsertClient(client("client-a", ClientType.PRODUCER,
            set(" group-a ", "\tgroup-b\t"),
            set(" topic-a ", "\ttopic-b\t")));

        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .build()).getClients())).containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-b")
            .build()).getClients())).containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setTopic("topic-a")
            .build()).getClients())).containsExactly("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setTopic("topic-b")
            .build()).getClients())).containsExactly("client-a");
    }

    @Test
    public void upsertGetAndRemoveNormalizeClientId() {
        ProxyClientReadService service = new ProxyClientReadService();

        service.upsertClient(client(" client-a ", ClientType.PRODUCER, set("group-a"), set("topic-a")));

        assertThat(service.getClient("client-a").getClientId()).isEqualTo("client-a");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().build()).getClients()))
            .containsExactly("client-a");

        service.removeClient(" client-a ");

        assertThat(service.getClient("client-a")).isNull();
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setTopic("topic-a")
            .build()).getClients()).isEmpty();
    }

    @Test
    public void recordsSuccessfulUpsertAndRemoveOperations() {
        List<ProxyClientReadServiceOperation> operations = new ArrayList<>();
        ProxyClientReadService service = new ProxyClientReadService(operations::add);

        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-b"), set("topic-b")));
        service.removeClient("missing-client");
        service.removeClient("");
        service.removeClient("client-a");

        assertThat(operations).containsExactly(
            ProxyClientReadServiceOperation.UPSERT,
            ProxyClientReadServiceOperation.UPSERT,
            ProxyClientReadServiceOperation.REMOVE
        );
    }

    @Test
    public void operationRecorderFailureDoesNotMaskSuccessfulMutations() {
        ProxyClientReadService service = new ProxyClientReadService(operation -> {
            throw new RuntimeException("metrics down");
        });
        ProxyClientInfo clientInfo = client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"));

        assertThatCode(() -> service.upsertClient(clientInfo)).doesNotThrowAnyException();

        assertThat(service.getClient("client-a")).isSameAs(clientInfo);
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .build()).getClients())).containsExactly("client-a");

        assertThatCode(() -> service.removeClient("client-a")).doesNotThrowAnyException();

        assertThat(service.getClient("client-a")).isNull();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-a").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-a").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientType(ClientType.PRODUCER).build())
            .getClients()).isEmpty();
    }

    @Test
    public void operationRecorderErrorDoesNotMaskSuccessfulMutations() {
        ProxyClientReadService service = new ProxyClientReadService(operation -> {
            throw new LinkageError("metrics linkage down");
        });
        ProxyClientInfo clientInfo = client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"));

        assertThatCode(() -> service.upsertClient(clientInfo)).doesNotThrowAnyException();

        assertThat(service.getClient("client-a")).isSameAs(clientInfo);
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .build()).getClients())).containsExactly("client-a");

        assertThatCode(() -> service.removeClient("client-a")).doesNotThrowAnyException();

        assertThat(service.getClient("client-a")).isNull();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-a").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-a").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientType(ClientType.PRODUCER).build())
            .getClients()).isEmpty();
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
}
