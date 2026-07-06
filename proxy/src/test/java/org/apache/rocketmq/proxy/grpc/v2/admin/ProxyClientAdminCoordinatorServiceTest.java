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
import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyClientAdminCoordinatorServiceTest {

    @Test
    public void listClientsAllProxiesMergesPeerPagesOrderedByClientId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Arrays.asList(client("client-a"), client("client-c")), "client-c"));
        peerClient.addPage("proxy-b", page(Arrays.asList(client("client-b"), client("client-d")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setClientType(ClientType.PRODUCER)
            .setPageSize(3)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a", "client-b", "client-c");
        ProxyClientAdminCoordinatorPageToken nextToken =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance().decode(result.getBody().getNextPageToken());
        assertThat(nextToken.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(nextToken.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(nextToken.getLastClientId()).isEqualTo("client-c");
        assertThat(nextToken.getPeerPageTokens()).containsEntry("proxy-a", "client-c");
        assertThat(nextToken.getPeerPageTokens()).containsEntry("proxy-b", "client-b");
        assertThat(peerClient.requests("proxy-a").get(0).getPageSize()).isEqualTo(3);
        assertThat(peerClient.requests("proxy-a").get(0).getPageToken()).isNull();
        assertThat(peerClient.requests("proxy-a").get(0).getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(peerClient.requests("proxy-a").get(0).getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsAllProxiesContinuesFromCoordinatorToken() {
        Map<String, String> peerTokens = new LinkedHashMap<>();
        peerTokens.put("proxy-a", "client-c");
        peerTokens.put("proxy-b", "client-b");
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-c")
                .setPeerPageTokens(peerTokens)
                .build()
        );
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.emptyList(), ""));
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-d")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .setPageToken(pageToken)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-d");
        assertThat(result.getBody().getNextPageToken()).isEmpty();
        assertThat(peerClient.requests("proxy-a").get(0).getPageToken()).isEqualTo("client-c");
        assertThat(peerClient.requests("proxy-b").get(0).getPageToken()).isEqualTo("client-b");
    }

    @Test
    public void listClientsAllProxiesRejectsCoordinatorTokenWithUnknownPeer() {
        Map<String, String> peerTokens = new LinkedHashMap<>();
        peerTokens.put("proxy-gone", "client-a");
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-a")
                .setPeerPageTokens(peerTokens)
                .build()
        );
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addPage("proxy-a", page(Collections.emptyList(), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .setPageToken(pageToken)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("unknown peer proxyId").contains("proxy-gone");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsAllProxiesRejectsCoordinatorTokenWithoutPeerProgress() {
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setLastClientId("client-a")
                .build()
        );
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .setPageToken(pageToken)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("progress");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsAllProxiesRejectsCoordinatorTokenWithoutLastClientId() {
        Map<String, String> peerTokens = new LinkedHashMap<>();
        peerTokens.put("proxy-a", "client-a");
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPeerPageTokens(peerTokens)
                .build()
        );
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .setPageToken(pageToken)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getStatus().getMessage()).contains("progress");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsAllProxiesFansOutInStableProxyIdOrder() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-b", "proxy-a");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-a")), ""));
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(peerClient.executedProxyIds()).containsExactly("proxy-a", "proxy-b");
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a", "client-b");
    }

    @Test
    public void listClientsAllProxiesPreservesExhaustedPeerNextPageToken() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Arrays.asList(client("client-a"), client("client-b")), "peer-a-token-2"));
        peerClient.addPage("proxy-b", page(Collections.emptyList(), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a", "client-b");
        ProxyClientAdminCoordinatorPageToken nextToken =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance().decode(result.getBody().getNextPageToken());
        assertThat(nextToken.getPeerPageTokens()).containsEntry("proxy-a", "peer-a-token-2");
    }

    @Test
    public void listClientsAllProxiesRejectsUnmergeableEmptyPeerContinuation() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.emptyList(), "peer-a-token-2"));
        peerClient.addPage("proxy-b", page(Collections.emptyList(), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(2)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("coordinator page token");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsAllProxiesRejectsMismatchedCoordinatorToken() {
        String pageToken = ProxyClientAdminCoordinatorPageTokenCodec.getInstance().encode(
            ProxyClientAdminCoordinatorPageToken.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setClientType(ClientType.PUSH_CONSUMER)
                .setLastClientId("client-a")
                .putPeerPageToken("proxy-a", "client-a")
                .build()
        );
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setClientType(ClientType.PRODUCER)
            .setPageToken(pageToken)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsAllProxiesFailsFastOnPeerError() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-a")), ""));
        peerClient.addResponse("proxy-b", ProxyClientAdminPeerResponse.error("proxy-b", "UNAUTHORIZED", "denied"));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
        assertThat(result.getStatus().getMessage()).isEqualTo("denied");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsAllProxiesRejectsBlankDiscoveredProxyId() {
        BlankDiscoveryPeerClient peerClient = new BlankDiscoveryPeerClient();
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("peer proxyId is required");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.executeCount).isEqualTo(0);
    }

    @Test
    public void listClientsAllProxiesRejectsEmptyPeerDiscovery() {
        EmptyDiscoveryPeerClient peerClient = new EmptyDiscoveryPeerClient();
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("at least one peer proxyId");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.executeCount).isEqualTo(0);
    }

    @Test
    public void listClientsAllProxiesRejectsDuplicateDiscoveredProxyId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", " proxy-a ");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-a")), ""));
        peerClient.addPage(" proxy-a ", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("Duplicate peer proxyId").contains("proxy-a");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsAllProxiesRejectsMismatchedPeerResponseProxyId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addResponse("proxy-a", ProxyClientAdminPeerResponse.success(
            "proxy-b",
            page(Collections.singletonList(client("client-a")), "")
        ));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("proxy-a").contains("proxy-b");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsAllProxiesRejectsPeerPageWithNullClients() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addResponse("proxy-a", ProxyClientAdminPeerResponse.success(
            "proxy-a",
            new InvalidProxyClientPage(null, "")
        ));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("peer page clients are required");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void listClientsProxyIdDelegatesToTargetPeer() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-c")), "client-c"));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(" proxy-b ")
            .setClientType(ClientType.PRODUCER)
            .setPageSize(2)
            .setPageToken("client-b")
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-c");
        assertThat(result.getBody().getNextPageToken()).isEqualTo("client-c");
        assertThat(peerClient.requests("proxy-a")).isEmpty();
        ProxyClientAdminPeerRequest peerRequest = peerClient.requests("proxy-b").get(0);
        assertThat(peerRequest.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS);
        assertThat(peerRequest.getClientType()).isEqualTo(ClientType.PRODUCER);
        assertThat(peerRequest.getPageSize()).isEqualTo(2);
        assertThat(peerRequest.getPageToken()).isEqualTo("client-b");
        assertThat(peerRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsProxyIdRejectsMissingProxyId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result = service.listClients(proxyContext(), query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void listClientsByGroupAllProxiesFansOutGroupRequestAndMergesResults() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-a")), ""));
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(1)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result =
            service.listClientsByGroup(proxyContext(), " group-a ", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a");
        ProxyClientAdminCoordinatorPageToken nextToken =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance().decode(result.getBody().getNextPageToken());
        assertThat(nextToken.getGroup()).isEqualTo("group-a");
        assertThat(nextToken.getPeerPageTokens()).containsEntry("proxy-a", "client-a");
        ProxyClientAdminPeerRequest request = peerClient.requests("proxy-a").get(0);
        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP);
        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsByGroupProxyIdDelegatesToTargetPeer() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-b")), "client-b"));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-b")
            .setPageSize(5)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result =
            service.listClientsByGroup(proxyContext(), " group-a ", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-b");
        assertThat(result.getBody().getNextPageToken()).isEqualTo("client-b");
        assertThat(peerClient.requests("proxy-a")).isEmpty();
        ProxyClientAdminPeerRequest request = peerClient.requests("proxy-b").get(0);
        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP);
        assertThat(request.getGroup()).isEqualTo("group-a");
        assertThat(request.getPageSize()).isEqualTo(5);
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsByTopicAllProxiesFansOutTopicRequestAndMergesResults() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-c")), ""));
        peerClient.addPage("proxy-b", page(Collections.singletonList(client("client-b")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(10)
            .build();

        ProxyClientAdminResult<ProxyClientPage> result =
            service.listClientsByTopic(proxyContext(), " topic-a ", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-b", "client-c");
        assertThat(result.getBody().getNextPageToken()).isEmpty();
        ProxyClientAdminPeerRequest request = peerClient.requests("proxy-b").get(0);
        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC);
        assertThat(request.getTopic()).isEqualTo("topic-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void listClientsByTopicProxyIdDelegatesToTargetPeer() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addPage("proxy-a", page(Collections.singletonList(client("client-a")), ""));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        ProxyClientAdminResult<ProxyClientPage> result =
            service.listClientsByTopic(proxyContext(), " topic-a ", query);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a");
        assertThat(result.getBody().getNextPageToken()).isEmpty();
        assertThat(peerClient.requests("proxy-b")).isEmpty();
        ProxyClientAdminPeerRequest request = peerClient.requests("proxy-a").get(0);
        assertThat(request.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC);
        assertThat(request.getTopic()).isEqualTo("topic-a");
        assertThat(request.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void describeClientProxyIdDelegatesToTargetPeer() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a", "proxy-b");
        peerClient.addResponse("proxy-b", ProxyClientAdminPeerResponse.success("proxy-b", client("client-b")));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(" proxy-b ")
            .setClientId(" client-b ")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClientId()).isEqualTo("client-b");
        assertThat(peerClient.requests("proxy-a")).isEmpty();
        ProxyClientAdminPeerRequest peerRequest = peerClient.requests("proxy-b").get(0);
        assertThat(peerRequest.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT);
        assertThat(peerRequest.getClientId()).isEqualTo("client-b");
        assertThat(peerRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(peerRequest.getProxyId()).isNull();
    }

    @Test
    public void describeClientAllProxiesScansPeersUntilClientIsFound() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-b", "proxy-a");
        peerClient.addResponse("proxy-a", ProxyClientAdminPeerResponse.error("proxy-a", "NOT_FOUND",
            "client missing on proxy-a"));
        peerClient.addResponse("proxy-b", ProxyClientAdminPeerResponse.success("proxy-b", client("client-b")));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setClientId(" client-b ")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.OK);
        assertThat(result.getBody().getClientId()).isEqualTo("client-b");
        assertThat(peerClient.executedProxyIds()).containsExactly("proxy-a", "proxy-b");
        ProxyClientAdminPeerRequest firstRequest = peerClient.requests("proxy-a").get(0);
        assertThat(firstRequest.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT);
        assertThat(firstRequest.getClientId()).isEqualTo("client-b");
        assertThat(firstRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        ProxyClientAdminPeerRequest secondRequest = peerClient.requests("proxy-b").get(0);
        assertThat(secondRequest.getOperation()).isEqualTo(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT);
        assertThat(secondRequest.getClientId()).isEqualTo("client-b");
        assertThat(secondRequest.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void describeClientProxyIdRejectsMissingProxyId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.BAD_REQUEST);
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.requests("proxy-a")).isEmpty();
    }

    @Test
    public void describeClientProxyIdFailsFastOnPeerNotFound() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addResponse("proxy-a", ProxyClientAdminPeerResponse.error("proxy-a", "NOT_FOUND",
            "client missing"));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.NOT_FOUND);
        assertThat(result.getStatus().getMessage()).isEqualTo("client missing");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void describeClientProxyIdRejectsMismatchedPeerResponseProxyId() {
        RecordingPeerClient peerClient = new RecordingPeerClient("proxy-a");
        peerClient.addResponse("proxy-a", ProxyClientAdminPeerResponse.success("proxy-b", client("client-a")));
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("proxy-a").contains("proxy-b");
        assertThat(result.getBody()).isNull();
    }

    @Test
    public void describeClientAllProxiesRejectsEmptyPeerDiscovery() {
        EmptyDiscoveryPeerClient peerClient = new EmptyDiscoveryPeerClient();
        ProxyClientAdminCoordinatorService service = new ProxyClientAdminCoordinatorService(peerClient);
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setClientId("client-a")
            .build();

        ProxyClientAdminResult<ProxyClientInfo> result = service.describeClient(proxyContext(), request);

        assertThat(result.getStatus().getCode()).isEqualTo(Code.INTERNAL_SERVER_ERROR);
        assertThat(result.getStatus().getMessage()).contains("at least one peer proxyId");
        assertThat(result.getBody()).isNull();
        assertThat(peerClient.executeCount).isEqualTo(0);
    }

    private static ProxyClientPage page(List<ProxyClientInfo> clients, String nextPageToken) {
        return new ProxyClientPage(clients, nextPageToken);
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "1.0.0",
            1000L,
            2000L
        );
    }

    private static ProxyContext proxyContext() {
        return ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
    }

    private static class InvalidProxyClientPage extends ProxyClientPage {
        private final List<ProxyClientInfo> clients;

        InvalidProxyClientPage(List<ProxyClientInfo> clients, String nextPageToken) {
            super(Collections.emptyList(), nextPageToken);
            this.clients = clients;
        }

        @Override
        public List<ProxyClientInfo> getClients() {
            return this.clients;
        }
    }

    private static class RecordingPeerClient implements ProxyClientAdminPeerClient {
        private final List<String> proxyIds;
        private final Map<String, List<ProxyClientAdminPeerResponse<?>>> responses = new LinkedHashMap<>();
        private final Map<String, List<ProxyClientAdminPeerRequest>> requests = new LinkedHashMap<>();
        private final List<String> executedProxyIds = new ArrayList<>();

        RecordingPeerClient(String... proxyIds) {
            this.proxyIds = Arrays.asList(proxyIds);
            for (String proxyId : proxyIds) {
                this.responses.put(proxyId, new ArrayList<>());
                this.requests.put(proxyId, new ArrayList<>());
            }
        }

        void addPage(String proxyId, ProxyClientPage page) {
            this.addResponse(proxyId, ProxyClientAdminPeerResponse.success(proxyId, page));
        }

        void addResponse(String proxyId, ProxyClientAdminPeerResponse<?> response) {
            this.responses.get(proxyId).add(response);
        }

        List<ProxyClientAdminPeerRequest> requests(String proxyId) {
            return this.requests.get(proxyId);
        }

        List<String> executedProxyIds() {
            return this.executedProxyIds;
        }

        @Override
        public List<String> listProxyIds() {
            return this.proxyIds;
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            this.executedProxyIds.add(proxyId);
            this.requests.get(proxyId).add(request);
            List<ProxyClientAdminPeerResponse<?>> peerResponses = this.responses.get(proxyId);
            if (peerResponses.isEmpty()) {
                return ProxyClientAdminPeerResponse.error(proxyId, "INTERNAL_SERVER_ERROR", "missing response");
            }
            return peerResponses.remove(0);
        }
    }

    private static class BlankDiscoveryPeerClient implements ProxyClientAdminPeerClient {
        private int executeCount;

        @Override
        public List<String> listProxyIds() {
            return Collections.singletonList(" ");
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            this.executeCount++;
            return ProxyClientAdminPeerResponse.success("proxy-a", page(Collections.emptyList(), ""));
        }
    }

    private static class EmptyDiscoveryPeerClient implements ProxyClientAdminPeerClient {
        private int executeCount;

        @Override
        public List<String> listProxyIds() {
            return Collections.emptyList();
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            this.executeCount++;
            return ProxyClientAdminPeerResponse.success("proxy-a", page(Collections.emptyList(), ""));
        }
    }
}
