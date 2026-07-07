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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminCoordinatorService {
    private static final long DEFAULT_COORDINATOR_PAGE_TOKEN_TTL_MILLIS = Duration.ofMinutes(5).toMillis();

    private final ProxyClientAdminPeerClient peerClient;
    private final ProxyClientAdminCoordinatorPageTokenCodec pageTokenCodec;
    private final long coordinatorPageTokenTtlMillis;
    private final LongSupplier currentTimeMillisSupplier;

    public ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient) {
        this(peerClient, DEFAULT_COORDINATOR_PAGE_TOKEN_TTL_MILLIS);
    }

    public ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient,
        long coordinatorPageTokenTtlMillis) {
        this(
            peerClient,
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance(),
            coordinatorPageTokenTtlMillis,
            System::currentTimeMillis
        );
    }

    ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient,
        ProxyClientAdminCoordinatorPageTokenCodec pageTokenCodec) {
        this(peerClient, pageTokenCodec, DEFAULT_COORDINATOR_PAGE_TOKEN_TTL_MILLIS, System::currentTimeMillis);
    }

    ProxyClientAdminCoordinatorService(ProxyClientAdminPeerClient peerClient,
        ProxyClientAdminCoordinatorPageTokenCodec pageTokenCodec, long coordinatorPageTokenTtlMillis,
        LongSupplier currentTimeMillisSupplier) {
        if (peerClient == null) {
            throw new IllegalArgumentException("peerClient is required");
        }
        if (pageTokenCodec == null) {
            throw new IllegalArgumentException("pageTokenCodec is required");
        }
        if (coordinatorPageTokenTtlMillis <= 0) {
            throw new IllegalArgumentException("coordinatorPageTokenTtlMillis must be positive");
        }
        if (currentTimeMillisSupplier == null) {
            throw new IllegalArgumentException("currentTimeMillisSupplier is required");
        }
        this.peerClient = peerClient;
        this.pageTokenCodec = pageTokenCodec;
        this.coordinatorPageTokenTtlMillis = coordinatorPageTokenTtlMillis;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
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
            this.restoreInterruptedStatus(t);
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
        this.validatePageTokenPeerIds(pageToken, currentPeerTokens, proxyIds);
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
            ProxyClientAdminResult<ProxyClientPage> peerPageProgressValidationResult =
                this.validatePeerPageProgress(proxyId, peerPage, currentPeerTokens.get(proxyId), pageToken);
            if (peerPageProgressValidationResult != null) {
                return peerPageProgressValidationResult;
            }
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
        String lastProxyId = null;
        for (Candidate candidate : selectedCandidates) {
            selectedClients.add(candidate.clientInfo);
            emittedPeerTokens.put(candidate.proxyId, candidate.getClientId());
            emittedPeerCounts.put(candidate.proxyId, emittedPeerCounts.getOrDefault(candidate.proxyId, 0) + 1);
            lastClientId = candidate.getClientId();
            lastProxyId = candidate.proxyId;
        }

        String nextPageToken = "";
        if (selectedClients.isEmpty() && this.hasMore(candidates, selectedCandidates, peerPages)) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR,
                "Cannot build coordinator page token without an emitted client id");
        }
        if (!selectedClients.isEmpty() && this.hasMore(candidates, selectedCandidates, peerPages)) {
            nextPageToken = this.buildNextPageToken(query, proxyIds, currentPeerTokens, emittedPeerTokens,
                emittedPeerCounts, peerPages, lastClientId, lastProxyId);
        }
        return new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            new ProxyClientPage(selectedClients, nextPageToken)
        );
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsProxyId0(ProxyContext ctx, ProxyClientQuery query,
        ProxyClientAdminPeerOperation operation) {
        String proxyId = this.requireProxyId(query.getProxyId());
        String peerPageToken = ProxyClientAdminPageTokenCodec.getInstance().decode(query.getPageToken());
        ProxyClientAdminPeerResponse<?> response = this.peerClient.execute(
            ctx,
            proxyId,
            this.toPeerRequest(query, peerPageToken, operation)
        );
        ProxyClientAdminResult<ProxyClientPage> peerPageResult = this.peerPageResult(proxyId, response);
        if (peerPageResult.getStatus().getCode() != Code.OK) {
            return peerPageResult;
        }
        ProxyClientAdminResult<ProxyClientPage> peerPageProgressValidationResult =
            this.validatePeerPageProgress(proxyId, peerPageResult.getBody(), peerPageToken);
        if (peerPageProgressValidationResult != null) {
            return peerPageProgressValidationResult;
        }
        return peerPageResult;
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
            ProxyClientAdminResult<ProxyClientInfo> peerInfoResult = this.peerInfoResult(proxyId, clientId, response);
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
        return this.peerInfoResult(proxyId, request.getClientId(), response);
    }

    private ProxyClientQuery requireCoordinatorListQuery(ProxyClientQuery query) {
        ProxyClientQuery effectiveQuery = query == null ? ProxyClientQuery.newBuilder().build() : query;
        if (effectiveQuery.getScope() != ProxyClientScope.ALL_PROXIES
            && effectiveQuery.getScope() != ProxyClientScope.PROXY_ID) {
            throw this.unsupportedCoordinatorScope(effectiveQuery.getScope());
        }
        this.validateClientType(effectiveQuery.getClientType());
        return effectiveQuery;
    }

    private void validateClientType(ClientType clientType) {
        if (clientType == ClientType.UNRECOGNIZED) {
            throw new IllegalArgumentException("Unsupported client type: " + clientType);
        }
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
        if (normalizedProxyIds.isEmpty()) {
            throw new IllegalStateException("at least one peer proxyId is required");
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
        this.validatePageTokenCreateTime(pageToken);
        if (pageToken.getScope() != ProxyClientScope.ALL_PROXIES) {
            throw new IllegalArgumentException("Coordinator page token scope mismatch");
        }
        if (StringUtils.isBlank(pageToken.getLastClientId())
            || StringUtils.isBlank(pageToken.getLastProxyId())
            || pageToken.getPeerPageTokens().isEmpty()) {
            throw new IllegalArgumentException("Coordinator page token progress is required");
        }
        if (!Objects.equals(pageToken.getGroup(), query.getGroup())
            || !Objects.equals(pageToken.getTopic(), query.getTopic())
            || pageToken.getClientType() != query.getClientType()
            || !Objects.equals(pageToken.getProxyId(), query.getProxyId())) {
            throw new IllegalArgumentException("Coordinator page token filters mismatch");
        }
    }

    private void validatePageTokenPeerIds(ProxyClientAdminCoordinatorPageToken pageToken,
        Map<String, String> peerPageTokens, List<String> proxyIds) {
        if (pageToken == null && peerPageTokens.isEmpty()) {
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
        if (pageToken != null && !discoveredProxyIds.contains(pageToken.getLastProxyId())) {
            throw new IllegalArgumentException(
                "Coordinator page token contains unknown last proxyId: " + pageToken.getLastProxyId()
            );
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
        Map<String, Integer> emittedPeerCounts, Map<String, ProxyClientPage> peerPages, String lastClientId,
        String lastProxyId) {
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
            .setLastProxyId(lastProxyId)
            .setCreateTimeMillis(this.currentTimeMillisSupplier.getAsLong())
            .setPeerPageTokens(nextPeerTokens)
            .build());
    }

    private void validatePageTokenCreateTime(ProxyClientAdminCoordinatorPageToken pageToken) {
        if (pageToken.getCreateTimeMillis() <= 0) {
            throw new IllegalArgumentException("Coordinator page token create time is required");
        }
        long nowMillis = this.currentTimeMillisSupplier.getAsLong();
        if (pageToken.getCreateTimeMillis() > nowMillis) {
            throw new IllegalArgumentException("Coordinator page token create time is in the future");
        }
        if (nowMillis - pageToken.getCreateTimeMillis() > this.coordinatorPageTokenTtlMillis) {
            throw new IllegalArgumentException("Coordinator page token expired");
        }
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
        ProxyClientPage peerPage = (ProxyClientPage) response.getBody();
        ProxyClientAdminResult<ProxyClientPage> peerPageValidationResult =
            this.validatePeerPage(expectedProxyId, peerPage);
        if (peerPageValidationResult != null) {
            return peerPageValidationResult;
        }
        return this.okResult(peerPage);
    }

    private ProxyClientAdminResult<ProxyClientInfo> peerInfoResult(String expectedProxyId, String expectedClientId,
        ProxyClientAdminPeerResponse<?> response) {
        ProxyClientAdminResult<ProxyClientInfo> validationResult =
            this.validatePeerResponse(expectedProxyId, response);
        if (validationResult != null) {
            return validationResult;
        }
        if (!(response.getBody() instanceof ProxyClientInfo)) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer client result is required");
        }
        ProxyClientInfo clientInfo = (ProxyClientInfo) response.getBody();
        ProxyClientAdminResult<ProxyClientInfo> clientValidationResult =
            this.validatePeerClientInfo(expectedProxyId, clientInfo);
        if (clientValidationResult != null) {
            return clientValidationResult;
        }
        String normalizedExpectedClientId = this.requireClientId(expectedClientId);
        if (!Objects.equals(normalizedExpectedClientId, clientInfo.getClientId())) {
            return this.errorResult(
                Code.INTERNAL_SERVER_ERROR,
                "peer client id mismatch: expected " + normalizedExpectedClientId
                    + ", actual " + clientInfo.getClientId()
            );
        }
        return this.okResult(clientInfo);
    }

    private ProxyClientAdminResult<ProxyClientPage> validatePeerPage(String proxyId, ProxyClientPage peerPage) {
        List<ProxyClientInfo> clients = peerPage.getClients();
        if (clients == null) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer page clients are required");
        }
        String previousClientId = null;
        for (ProxyClientInfo clientInfo : clients) {
            if (clientInfo == null) {
                return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer page client is required");
            }
            ProxyClientAdminResult<ProxyClientPage> clientValidationResult =
                this.validatePeerClientInfo(proxyId, clientInfo);
            if (clientValidationResult != null) {
                return clientValidationResult;
            }
            String clientId = clientInfo.getClientId();
            if (previousClientId != null && clientId.compareTo(previousClientId) <= 0) {
                return this.errorResult(
                    Code.INTERNAL_SERVER_ERROR,
                    "peer page client ids must be strictly increasing: proxyId=" + proxyId
                        + ", previousClientId=" + previousClientId
                        + ", clientId=" + clientId
                );
            }
            previousClientId = clientId;
        }
        return null;
    }

    private ProxyClientAdminResult<ProxyClientPage> validatePeerPageProgress(String proxyId, ProxyClientPage peerPage,
        String peerPageToken, ProxyClientAdminCoordinatorPageToken pageToken) {
        String normalizedPeerPageToken = StringUtils.trimToNull(peerPageToken);
        String coordinatorLastClientId = pageToken == null ? null : StringUtils.trimToNull(pageToken.getLastClientId());
        String coordinatorLastProxyId = pageToken == null ? null : StringUtils.trimToNull(pageToken.getLastProxyId());
        if (normalizedPeerPageToken == null && coordinatorLastClientId == null) {
            return null;
        }
        for (ProxyClientInfo clientInfo : peerPage.getClients()) {
            if (this.isBeforeOrAtCoordinatorCursor(
                proxyId,
                clientInfo.getClientId(),
                normalizedPeerPageToken,
                coordinatorLastClientId,
                coordinatorLastProxyId
            )) {
                return this.errorResult(
                    Code.INTERNAL_SERVER_ERROR,
                    "peer page client id is not after coordinator page token: proxyId=" + proxyId
                        + ", clientId=" + clientInfo.getClientId()
                        + ", peerPageToken="
                        + (normalizedPeerPageToken == null ? coordinatorLastClientId : normalizedPeerPageToken)
                        + ", lastProxyId=" + coordinatorLastProxyId
                        + ", lastClientId=" + coordinatorLastClientId
                );
            }
        }
        return null;
    }

    private boolean isBeforeOrAtCoordinatorCursor(String proxyId, String clientId, String peerPageToken,
        String coordinatorLastClientId, String coordinatorLastProxyId) {
        if (peerPageToken != null) {
            return clientId.compareTo(peerPageToken) <= 0;
        }
        int clientIdComparison = clientId.compareTo(coordinatorLastClientId);
        if (clientIdComparison != 0) {
            return clientIdComparison < 0;
        }
        return coordinatorLastProxyId == null || proxyId.compareTo(coordinatorLastProxyId) <= 0;
    }

    private ProxyClientAdminResult<ProxyClientPage> validatePeerPageProgress(String proxyId, ProxyClientPage peerPage,
        String peerPageToken) {
        String normalizedPeerPageToken = StringUtils.trimToNull(peerPageToken);
        if (normalizedPeerPageToken == null) {
            return null;
        }
        for (ProxyClientInfo clientInfo : peerPage.getClients()) {
            if (clientInfo.getClientId().compareTo(normalizedPeerPageToken) <= 0) {
                return this.errorResult(
                    Code.INTERNAL_SERVER_ERROR,
                    "peer page client id is not after page token: proxyId=" + proxyId
                        + ", clientId=" + clientInfo.getClientId()
                        + ", pageToken=" + normalizedPeerPageToken
                );
            }
        }
        return null;
    }

    private <T> ProxyClientAdminResult<T> validatePeerClientInfo(String expectedProxyId,
        ProxyClientInfo clientInfo) {
        if (StringUtils.isBlank(clientInfo.getClientId())) {
            return this.errorResult(Code.INTERNAL_SERVER_ERROR, "peer client id is required");
        }
        String clientProxyId = StringUtils.trimToNull(clientInfo.getProxyId());
        if (clientProxyId != null && !Objects.equals(expectedProxyId, clientProxyId)) {
            return this.errorResult(
                Code.INTERNAL_SERVER_ERROR,
                "peer client proxyId mismatch: expected " + expectedProxyId + ", actual " + clientProxyId
            );
        }
        return null;
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
            if (result == Code.OK || result == Code.UNRECOGNIZED) {
                return Code.INTERNAL_SERVER_ERROR;
            }
            return result;
        } catch (RuntimeException ignored) {
            return Code.INTERNAL_SERVER_ERROR;
        }
    }

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
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
