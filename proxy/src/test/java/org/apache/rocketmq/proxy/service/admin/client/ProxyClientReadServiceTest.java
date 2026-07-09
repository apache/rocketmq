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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
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
    public void listClientsIntersectsGroupTopicAndClientTypeIndexesInClientIdOrder() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-d", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-c", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-a")));
        service.upsertClient(client("client-e", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-b")));

        ProxyClientPage page = service.listClients(ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .setTopic("topic-a")
            .setClientType(ClientType.PUSH_CONSUMER)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-b", "client-d");
    }

    @Test
    public void listClientsFiltersByProxyIdAndIntersectsOtherIndexesInClientIdOrder() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-d", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"), "proxy-a"));
        service.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"), "proxy-a"));
        service.upsertClient(client("client-a", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"), "proxy-b"));
        service.upsertClient(client("client-c", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-a"));
        service.upsertClient(client("client-e", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-a"), "proxy-a"));

        ProxyClientPage page = service.listClients(ProxyClientQuery.newBuilder()
            .setProxyId(" proxy-a ")
            .setGroup("group-a")
            .setTopic("topic-a")
            .setClientType(ClientType.PUSH_CONSUMER)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-b", "client-d");
    }

    @Test
    public void listClientsFiltersByContestFieldsInClientIdOrder() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-d", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 150L, 300L));
        service.upsertClient(client("client-b", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 200L, 300L));
        service.upsertClient(client("client-a", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"),
            "CPP", "proxy-a", 150L, 300L));
        service.upsertClient(client("client-c", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-b"),
            "JAVA", "proxy-a", 150L, 300L));
        service.upsertClient(client("other-client", ClientType.PUSH_CONSUMER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 150L, 300L));

        ProxyClientPage page = service.listClients(ProxyClientQuery.newBuilder()
            .setClientIdPrefix("client-")
            .setGroup("group-a")
            .setTopic("topic-a")
            .setClientLanguage("JAVA")
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(200L)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-b", "client-d");
    }

    @Test
    public void listClientsExactClientIdIntersectsOtherContestFields() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 100L, 200L));

        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setClientId("client-a")
            .setClientIdPrefix("client-")
            .setClientLanguage("JAVA")
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(100L)
            .build()).getClients())).containsExactly("client-a");
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setClientId("client-a")
            .setClientIdPrefix("missing-")
            .build()).getClients()).isEmpty();
    }

    @Test
    public void listClientsSupportsOneBasedPageNum() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-c", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-d", ClientType.PRODUCER, set("group-a"), set("topic-a")));
        service.upsertClient(client("client-e", ClientType.PRODUCER, set("group-a"), set("topic-a")));

        ProxyClientPage page = service.listClients(ProxyClientQuery.newBuilder()
            .setPageNum(2)
            .setPageSize(2)
            .build());

        assertThat(clientIds(page.getClients())).containsExactly("client-c", "client-d");
        assertThat(page.getNextPageToken()).isEqualTo("client-d");
    }

    @Test
    public void upsertClientRefreshesLanguageAndConnectTimeIndexes() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 100L, 200L));

        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "CPP", "proxy-a", 300L, 400L));

        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setClientLanguage("JAVA")
            .build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(100L)
            .build()).getClients()).isEmpty();
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setClientLanguage("CPP")
            .setConnectTimeStartMillis(300L)
            .setConnectTimeEndMillis(300L)
            .build()).getClients())).containsExactly("client-a");
    }

    @Test
    public void listClientsSupportsOpenEndedConnectTimeRanges() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 100L, 200L));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 200L, 300L));
        service.upsertClient(client("client-c", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 300L, 400L));

        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setConnectTimeStartMillis(200L)
            .build()).getClients())).containsExactly("client-b", "client-c");
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setConnectTimeEndMillis(200L)
            .build()).getClients())).containsExactly("client-a", "client-b");
    }

    @Test
    public void singleFilterQueriesReuseExistingIndexesForPageBoundedReads() throws Exception {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 100L, 200L));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-a"), set("topic-b"),
            "JAVA", "proxy-a", 100L, 200L));

        Map<String, NavigableSet<String>> clientLanguageIndex = fieldValue(service, "clientLanguageIndex");
        NavigableMap<Long, NavigableSet<String>> connectTimeIndex = fieldValue(service, "connectTimeIndex");

        assertThat(candidateClientIds(service, ProxyClientQuery.newBuilder()
            .setClientLanguage("JAVA")
            .build())).isSameAs(clientLanguageIndex.get("JAVA"));
        assertThat(candidateClientIds(service, ProxyClientQuery.newBuilder()
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(100L)
            .build())).isSameAs(connectTimeIndex.get(100L));
    }

    @Test
    public void removeClientDeletesContestIndexes() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "JAVA", "proxy-a", 100L, 200L));

        service.removeClient("client-a");

        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientId("client-a").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientIdPrefix("client-").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setClientLanguage("JAVA").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(100L)
            .build()).getClients()).isEmpty();
    }

    @Test
    public void upsertClientRefreshesProxyIdIndex() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-a"));

        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-b"));

        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setProxyId("proxy-a")
            .build()).getClients()).isEmpty();
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder()
            .setProxyId("proxy-b")
            .build()).getClients())).containsExactly("client-a");
    }

    @Test
    public void removeClientDeletesProxyIdIndex() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-a"));

        service.removeClient("client-a");

        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setProxyId("proxy-a")
            .build()).getClients()).isEmpty();
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
        assertThat(stats.getProxyIdIndexCount()).isEqualTo(0L);
        assertThat(stats.getClientTypeCounts())
            .containsEntry(ClientType.PRODUCER, 1L)
            .containsEntry(ClientType.PUSH_CONSUMER, 1L);

        service.removeClient("client-a");
        stats = service.snapshotStats();

        assertThat(stats.getTotalClientCount()).isEqualTo(1L);
        assertThat(stats.getGroupIndexCount()).isEqualTo(2L);
        assertThat(stats.getTopicIndexCount()).isEqualTo(1L);
        assertThat(stats.getProxyIdIndexCount()).isEqualTo(0L);
        assertThat(stats.getClientTypeCount(ClientType.PRODUCER)).isEqualTo(0L);
        assertThat(stats.getClientTypeCount(ClientType.PUSH_CONSUMER)).isEqualTo(1L);
    }

    @Test
    public void snapshotStatsReflectsProxyIdIndexCount() {
        ProxyClientReadService service = new ProxyClientReadService();
        service.upsertClient(client("client-a", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-a"));
        service.upsertClient(client("client-b", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-a"));
        service.upsertClient(client("client-c", ClientType.PRODUCER, set("group-a"), set("topic-a"), "proxy-b"));

        assertThat(service.snapshotStats().getProxyIdIndexCount()).isEqualTo(2L);

        service.removeClient("client-c");

        assertThat(service.snapshotStats().getProxyIdIndexCount()).isEqualTo(1L);
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
    public void upsertClientRejectsCoordinatorPageTokenClientIds() {
        ProxyClientReadService service = new ProxyClientReadService();

        assertThatThrownBy(() -> service.upsertClient(
            client("cp1:cursor", ClientType.PRODUCER, set("group-a"), set("topic-a"))
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId must not use reserved page token prefix");
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

    @Test
    public void removeInactiveClientsDeletesClientsAndIndexes() {
        List<ProxyClientReadServiceOperation> operations = new ArrayList<>();
        ProxyClientReadService service = new ProxyClientReadService(operations::add);
        service.upsertClient(client("client-old-a", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "proxy-a", 100L));
        service.upsertClient(client("client-old-b", ClientType.PUSH_CONSUMER, set("group-b"), set("topic-b"),
            "proxy-b", 200L));
        service.upsertClient(client("client-active", ClientType.PRODUCER, set("group-a"), set("topic-a"),
            "proxy-a", 300L));

        int removed = service.removeInactiveClients(200L);

        assertThat(removed).isEqualTo(2);
        assertThat(service.getClient("client-old-a")).isNull();
        assertThat(service.getClient("client-old-b")).isNull();
        assertThat(service.getClient("client-active")).isNotNull();
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().build()).getClients()))
            .containsExactly("client-active");
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setGroup("group-b").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setTopic("topic-b").build()).getClients())
            .isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .build()).getClients()).isEmpty();
        assertThat(service.listClients(ProxyClientQuery.newBuilder().setProxyId("proxy-b").build()).getClients())
            .isEmpty();
        assertThat(clientIds(service.listClients(ProxyClientQuery.newBuilder().setProxyId("proxy-a").build())
            .getClients())).containsExactly("client-active");
        assertThat(operations).containsExactly(
            ProxyClientReadServiceOperation.UPSERT,
            ProxyClientReadServiceOperation.UPSERT,
            ProxyClientReadServiceOperation.UPSERT,
            ProxyClientReadServiceOperation.REMOVE,
            ProxyClientReadServiceOperation.REMOVE
        );
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, Set<String> groups, Set<String> topics) {
        return client(clientId, clientType, groups, topics, null);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String proxyId) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            groups,
            topics,
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            proxyId,
            100L,
            200L
        );
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String proxyId, long lastActiveTimeMillis) {
        return client(clientId, clientType, groups, topics, "JAVA", proxyId, 100L, lastActiveTimeMillis);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, Set<String> groups, Set<String> topics,
        String language, String proxyId, long connectTimeMillis, long lastActiveTimeMillis) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            groups,
            topics,
            language,
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            proxyId,
            connectTimeMillis,
            lastActiveTimeMillis
        );
    }

    private static Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    @SuppressWarnings("unchecked")
    private static <T> T fieldValue(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    @SuppressWarnings("unchecked")
    private static NavigableSet<String> candidateClientIds(ProxyClientReadService service,
        ProxyClientQuery query) throws Exception {
        Method method = ProxyClientReadService.class.getDeclaredMethod("getCandidateClientIds",
            ProxyClientQuery.class);
        method.setAccessible(true);
        return (NavigableSet<String>) method.invoke(service, query);
    }

    private static List<String> clientIds(List<ProxyClientInfo> clients) {
        return clients.stream().map(ProxyClientInfo::getClientId).collect(Collectors.toList());
    }
}
