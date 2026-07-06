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

import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminCoordinatorService {
    private final ProxyClientAdminPeerClient peerClient;
    private final ProxyClientAdminCoordinatorPageTokenCodec pageTokenCodec;

    public ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient) {
        this(peerClient, ProxyClientAdminCoordinatorPageTokenCodec.getInstance());
    }

    ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient,
        ProxyClientAdminCoordinatorPageTokenCodec pageTokenCodec) {
        if (peerClient == null) {
            throw new IllegalArgumentException("peerClient is required");
        }
        if (pageTokenCodec == null) {
            throw new IllegalArgumentException("pageTokenCodec is required");
        }
        this.peerClient = peerClient;
        this.pageTokenCodec = pageTokenCodec;
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx, ProxyClientQuery query) {
        return this.execute(() -> this.listClientsByScope0(
            ctx,
            this.requireCoordinatorListQuery(query),
            ProxyClientAdminPeerOperation.LIST_CLIENTS
        ));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx, String group,
        ProxyClientQuery query) {
        return this.execute(() -> this.listClientsByScope0(
            ctx,
            this.requireCoordinatorListQuery(query).toBuilder().setGroup(this.requireGroup(group)).build(),
            ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_GROUP
        ));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx, String topic,
        ProxyClientQuery query) {
        return this.execute(() -> this.listClientsByScope0(
            ctx,
            this.requireCoordinatorListQuery(query).toBuilder().setTopic(this.requireTopic(topic)).build(),
            ProxyClientAdminPeerOperation.LIST_CLIENTS_BY_TOPIC
        ));
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        return this.execute(() -> {
            ProxyClientAdminDescribeClientRequest requiredRequest = this.requireCoordinatorDescribeRequest(request);
            switch (requiredRequest.getScope()) {
                case ALL_PROXIES:
                    return this.describeClientAllProxies0(ctx, requiredRequest);
                case PROXY_ID:
                    return this.describeClientProxyId0(ctx, requiredRequest);
                default:
                    throw this.unsupportedCoordinatorScope(requiredRequest.getScope());
            }
        });
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<ProxyClientAdminResult<T>> supplier) {
        try {
            return supplier.get();
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsByScope0(ProxyContext ctx, ProxyClientQuery query,
        ProxyClientAdminPeerOperation operation) {
        switch (query.getScope()) {
            case ALL_PROXIES:
                return this.listClientsAllProxies0(ctx, query, operation);
            case PROXY_ID:
                return this.listClientsProxyId0(ctx, query, operation);
            default:
                throw this.unsupportedCoordinatorScope(query.getScope());
        }
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsAllProxies0(ProxyContext ctx, ProxyClientQuery query,
        ProxyClientAdminPeerOperation operation) {
        ProxyClientAdminCoordinatorPageToken pageToken = this.pageTokenCodec.decode(query.getPageToken());
        this.validatePageToken(query, pageToken);

        List<String> proxyIds = this.requirePeerProxyIds(this.peerClient.listProxyIds());
        Map<String, String> currentPeerTokens = pageToken == null
            ? Collections.emptyMap()
            : pageToken.getPeerPageTokens();
        this.validatePageTokenPeerIds(currentPeerTokens, proxyIds);
        Map<String, ProxyClientPage> peerPages = new LinkedHashMap<>();
        List<Candidate> candidates = new ArrayList<>();
        for (String proxyId : proxyIds) {
            ProxyClientAdminPeerResponse<?> response = this.peerClient.execute(
                ctx,
                proxyId,
                this.toPeerRequest(query, currentPeerTokens.get(proxyId), operation)
            );
            ProxyClientAdminResult<ProxyClientPage> peerPageResult = this.peerPageResult(proxyId, response);
            if (peerPageResult.getStatus().getCode() != Code.OK) {
                return peerPageResult;
            }
            ProxyClientPage peerPage = peerPageResult.getBody();
            peerPages.put(proxyId, peerPage);
            for (ProxyClientInfo clientInfo : peerPage.getClients()) {
                candidates.add(new Candidate(proxyId, clientInfo));
            }
        }

        Collections.sort(candidates, (left, right) -> {
            int result = left.getClientId().compareTo(right.getClientId());
            if (result != 0) {
                return result;
            }
            return left.proxyId.compareTo(right.proxyId);
        });

        int pageSize = query.getBoundedPageSize();
        List<Candidate> selectedCandidates = candidates.subList(0, Math.min(pageSize, candidates.size()));
        List<ProxyClientInfo> selectedClients = new ArrayList<>(selectedCandidates.size());
        Map<String, String> emittedPeerTokens = new LinkedHashMap<>();
        Map<String, Integer> emittedPeerCounts = new HashMap<>();
        String lastClientId = null;
        for (Candidate candidate : selectedCandidates) {
            selectedClients.add(candidate.clientInfo);
            emittedPeerTokens.put(candidate.proxyId, candidate.getClientId());
            emittedPeerCounts.put(candidate.proxyId, emittedPeerCounts.getOrDefault(candidate.proxyId, 0) + 1);
            lastClientId = candidate.getClientId();
        }

        String nextPageToken = "";
        if (!selectedClients.isEmpty() && this.hasMore(candidates, selectedCandidates, peerPages)) {
            nextPageToken = this.buildNextPageToken(query, proxyIds, currentPeerTokens, emittedPeerTokens,
                emittedPeerCounts, peerPages, lastClientId);
        }
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            new ProxyClientPage(selectedClients, nextPageToken)
        );
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsProxyId0(ProxyContext ctx, ProxyClientQuery query,
        ProxyClientAdminPeerOperation operation) {
        String proxyId = this.requireProxyId(query.getProxyId());
        ProxyClientAdminPeerResponse<?> response = this.peerClient.execute(
            ctx,
            proxyId,
            this.toPeerRequest(query, query.getPageToken(), operation)
        );
        return this.peerPageResult(proxyId, response);
    }

    private ProxyClientAdminResult<ProxyClientInfo> describeClientAllProxies0(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        String clientId = this.requireClientId(request.getClientId());
        List<String> proxyIds = this.requirePeerProxyIds(this.peerClient.listProxyIds());
        for (String proxyId : proxyIds) {
            ProxyClientAdminPeerResponse<?> response = this.peerClient.execute(
                ctx,
                proxyId,
                this.toPeerDescribeRequest(clientId)
            );
            ProxyClientAdminResult<ProxyClientInfo> peerInfoResult = this.peerInfoResult(proxyId, response);
            if (peerInfoResult.getStatus().getCode() == Code.OK) {
                return peerInfoResult;
            }
            if (peerInfoResult.getStatus().getCode() != Code.NOT_FOUND) {
                return peerInfoResult;
            }
        }
        return this.errorResult(Code.NOT_FOUND, "Client not found: " + clientId);
    }

    private ProxyClientAdminResult<ProxyClientInfo> describeClientProxyId0(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        String proxyId = this.requireProxyId(request.getProxyId());
        ProxyClientAdminPeerResponse<?> response = this.peerClient.execute(
            ctx,
            proxyId,
            this.toPeerDescribeRequest(request.getClientId())
        );
        return this.peerInfoResult(proxyId, response);
    }

    private ProxyClientQuery requireCoordinatorListQuery(ProxyClientQuery query) {
        ProxyClientQuery effectiveQuery = query == null ? ProxyClientQuery.newBuilder().build() : query;
        if (effectiveQuery.getScope() != ProxyClientScope.ALL_PROXIES
            && effectiveQuery.getScope() != ProxyClientScope.PROXY_ID) {
            throw this.unsupportedCoordinatorScope(effectiveQuery.getScope());
        }
        return effectiveQuery;
    }

    private ProxyClientAdminPeerRequest toPeerRequest(ProxyClientQuery query, String peerPageToken,
        ProxyClientAdminPeerOperation operation) {
        return ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(operation)
            .setGroup(query.getGroup())
            .setTopic(query.getTopic())
            .setClientType(query.getClientType())
            .setPageSize(query.getBoundedPageSize())
            .setPageToken(peerPageToken)
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .build();
    }

    private ProxyClientAdminPeerRequest toPeerDescribeRequest(String clientId) {
        return ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(ProxyClientAdminPeerOperation.DESCRIBE_CLIENT)
            .setClientId(clientId)
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .build();
    }

    private String requireGroup(String group) {
        String normalizedGroup = StringUtils.trimToNull(group);
        if (normalizedGroup == null) {
            throw new IllegalArgumentException("group is required");
        }
        return normalizedGroup;
    }

    private String requireTopic(String topic) {
        String normalizedTopic = StringUtils.trimToNull(topic);
        if (normalizedTopic == null) {
            throw new IllegalArgumentException("topic is required");
        }
        return normalizedTopic;
    }

    private ProxyClientAdminDescribeClientRequest requireCoordinatorDescribeRequest(
        ProxyClientAdminDescribeClientRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        if (request.getScope() != ProxyClientScope.ALL_PROXIES
            && request.getScope() != ProxyClientScope.PROXY_ID) {
            throw new IllegalArgumentException("Unsupported coordinator proxy scope: " + request.getScope());
        }
        this.requireClientId(request.getClientId());
        return request;
    }

    private String requireClientId(String clientId) {
        String normalizedClientId = StringUtils.trimToNull(clientId);
        if (normalizedClientId == null) {
            throw new IllegalArgumentException("clientId is required");
        }
        return normalizedClientId;
    }

    private String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }

    private List<String> requirePeerProxyIds(List<String> proxyIds) {
        if (proxyIds == null) {
            throw new IllegalStateException("peer proxyIds are required");
        }
        List<String> normalizedProxyIds = new ArrayList<>(proxyIds.size());
        Set<String> seenProxyIds = new HashSet<>();
        for (String proxyId : proxyIds) {
            String normalizedProxyId = StringUtils.trimToNull(proxyId);
            if (normalizedProxyId == null) {
                throw new IllegalStateException("peer proxyId is required");
            }
            if (!seenProxyIds.add(normalizedProxyId)) {
                throw new IllegalStateException("Duplicate peer proxyId: " + normalizedProxyId);
            }
            normalizedProxyIds.add(normalizedProxyId);
        }
        Collections.sort(normalizedProxyIds);
        return normalizedProxyIds;
    }

    private IllegalArgumentException unsupportedCoordinatorScope(ProxyClientScope scope) {
        return new IllegalArgumentException("Unsupported coordinator proxy scope: " + scope);
    }

    private void validatePageToken(ProxyClientQuery query, ProxyClientAdminCoordinatorPageToken pageToken) {
        if (pageToken == null) {
            return;
        }
        if (pageToken.getScope() != ProxyClientScope.ALL_PROXIES) {
            throw new IllegalArgumentException("Coordinator page token scope mismatch");
        }
        if (!Objects.equals(pageToken.getGroup(), query.getGroup())
            || !Objects.equals(pageToken.getTopic(), query.getTopic())
            || pageToken.getClientType() != query.getClientType()
            || !Objects.equals(pageToken.getProxyId(), query.getProxyId())) {
            throw new IllegalArgumentException("Coordinator page token filters mismatch");
        }
    }

    private void validatePageTokenPeerIds(Map<String, String> peerPageTokens, List<String> proxyIds) {
        if (peerPageTokens.isEmpty()) {
            return;
        }
        Set<String> discoveredProxyIds = new HashSet<>(proxyIds);
        for (String peerProxyId : peerPageTokens.keySet()) {
            if (!discoveredProxyIds.contains(peerProxyId)) {
                throw new IllegalArgumentException(
                    "Coordinator page token contains unknown peer proxyId: " + peerProxyId
                );
            }
        }
    }

    private boolean hasMore(List<Candidate> candidates, List<Candidate> selectedCandidates,
        Map<String, ProxyClientPage> peerPages) {
        if (candidates.size() > selectedCandidates.size()) {
            return true;
        }
        for (ProxyClientPage peerPage : peerPages.values()) {
            if (StringUtils.isNotBlank(peerPage.getNextPageToken())) {
                return true;
            }
        }
        return false;
    }

    private String buildNextPageToken(ProxyClientQuery query, List<String> proxyIds,
        Map<String, String> currentPeerTokens, Map<String, String> emittedPeerTokens,
        Map<String, Integer> emittedPeerCounts, Map<String, ProxyClientPage> peerPages, String lastClientId) {
        Map<String, String> nextPeerTokens = new LinkedHashMap<>();
        for (String proxyId : proxyIds) {
            String peerToken = this.nextPeerPageToken(
                proxyId,
                currentPeerTokens,
                emittedPeerTokens,
                emittedPeerCounts,
                peerPages
            );
            if (StringUtils.isNotBlank(peerToken)) {
                nextPeerTokens.put(proxyId, peerToken);
            }
        }
        return this.pageTokenCodec.encode(ProxyClientAdminCoordinatorPageToken.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setGroup(query.getGroup())
            .setTopic(query.getTopic())
            .setClientType(query.getClientType())
            .setProxyId(query.getProxyId())
            .setLastClientId(lastClientId)
            .setCreateTimeMillis(System.currentTimeMillis())
            .setPeerPageTokens(nextPeerTokens)
            .build());
    }

    private String nextPeerPageToken(String proxyId, Map<String, String> currentPeerTokens,
        Map<String, String> emittedPeerTokens, Map<String, Integer> emittedPeerCounts,
        Map<String, ProxyClientPage> peerPages) {
        ProxyClientPage peerPage = peerPages.get(proxyId);
        int emittedCount = emittedPeerCounts.getOrDefault(proxyId, 0);
        String peerNextPageToken = peerPage == null ? null : peerPage.getNextPageToken();
        if (emittedCount == 0) {
            if (peerPage != null && peerPage.getClients().isEmpty() && StringUtils.isNotBlank(peerNextPageToken)) {
                return peerNextPageToken;
            }
            return currentPeerTokens.get(proxyId);
        }
        if (peerPage != null
            && emittedCount >= peerPage.getClients().size()
            && StringUtils.isNotBlank(peerNextPageToken)) {
            return peerNextPageToken;
        }
        return emittedPeerTokens.get(proxyId);
    }

    private <T> ProxyClientAdminResult<T> okResult(T body) {
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            body
        );
    }

    private <T> ProxyClientAdminResult<T> peerErrorResult(ProxyClientAdminPeerResponse<?> response) {
        return this.errorResult(this.parseCode(response.getErrorCode()), response.getErrorMessage());
    }

    private ProxyClientAdminResult<ProxyClientPage> peerPageResult(String expectedProxyId,
        ProxyClientAdminPeerResponse<?> response) {
        ProxyClientAdminResult<ProxyClientPage> validationResult =
            this.validatePeerResponse(expectedProxyId, response);
        if (validationResult != null) {
            return validationResult;
        }
        if (!(response.getBody() instanceof ProxyClientPage)) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer page result is required");
        }
        return this.okResult((ProxyClientPage) response.getBody());
    }

    private ProxyClientAdminResult<ProxyClientInfo> peerInfoResult(String expectedProxyId,
        ProxyClientAdminPeerResponse<?> response) {
        ProxyClientAdminResult<ProxyClientInfo> validationResult =
            this.validatePeerResponse(expectedProxyId, response);
        if (validationResult != null) {
            return validationResult;
        }
        if (!(response.getBody() instanceof ProxyClientInfo)) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer client result is required");
        }
        return this.okResult((ProxyClientInfo) response.getBody());
    }

    private <T> ProxyClientAdminResult<T> validatePeerResponse(String expectedProxyId,
        ProxyClientAdminPeerResponse<?> response) {
        if (response == null) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer response is required");
        }
        if (!Objects.equals(expectedProxyId, response.getProxyId())) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR,
                "peer response proxyId mismatch: expected " + expectedProxyId
                    + ", actual " + response.getProxyId());
        }
        if (!response.isSuccess()) {
            return this.peerErrorResult(response);
        }
        return null;
    }

    private <T> ProxyClientAdminResult<T> errorResult(Code code, String message) {
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(code, message),
            null
        );
    }

    private Code parseCode(String code) {
        String normalizedCode = StringUtils.trimToNull(code);
        if (normalizedCode == null) {
            return Code.INTERNAL_SERVER_ERROR;
        }
        try {
            Code result = Code.valueOf(normalizedCode);
            if (result == Code.UNRECOGNIZED) {
                return Code.INTERNAL_SERVER_ERROR;
            }
            return result;
        } catch (RuntimeException ignored) {
            return Code.INTERNAL_SERVER_ERROR;
        }
    }

    private static class Candidate {
        private final String proxyId;
        private final ProxyClientInfo clientInfo;

        private Candidate(String proxyId, ProxyClientInfo clientInfo) {
            this.proxyId = proxyId;
            this.clientInfo = clientInfo;
        }

        private String getClientId() {
            return this.clientInfo.getClientId();
        }
    }
}
