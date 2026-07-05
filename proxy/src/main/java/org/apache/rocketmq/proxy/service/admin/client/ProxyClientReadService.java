/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ProxyClientReadService {
    private static final Consumer<ProxyClientReadServiceOperation> NOOP_OPERATION_RECORDER = operation -> {
    };
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final Map<String, ProxyClientInfo> clientIdTable = new HashMap<>();
    private final NavigableSet<String> clientIdIndex = new TreeSet<>();
    private final Map<String, NavigableSet<String>> groupIndex = new HashMap<>();
    private final Map<String, NavigableSet<String>> topicIndex = new HashMap<>();
    private final Map<ClientType, NavigableSet<String>> clientTypeIndex = new HashMap<>();
    private final Map<String, NavigableSet<String>> proxyIdIndex = new HashMap<>();
    private final Consumer<ProxyClientReadServiceOperation> operationRecorder;

    public ProxyClientReadService() {
        this(NOOP_OPERATION_RECORDER);
    }

    public ProxyClientReadService(Consumer<ProxyClientReadServiceOperation> operationRecorder) {
        this.operationRecorder = operationRecorder == null ? NOOP_OPERATION_RECORDER : operationRecorder;
    }

    public synchronized void upsertClient(ProxyClientInfo clientInfo) {
        String clientId = clientInfo == null ? null : normalizeClientId(clientInfo.getClientId());
        if (clientId == null) {
            throw new IllegalArgumentException("clientId is required");
        }
        ProxyClientInfo oldClientInfo = this.clientIdTable.put(clientId, clientInfo);
        if (oldClientInfo != null) {
            this.removeIndexes(oldClientInfo);
        }
        this.clientIdIndex.add(clientId);
        this.addIndexes(clientInfo);
        this.recordOperation(ProxyClientReadServiceOperation.UPSERT);
    }

    public synchronized void removeClient(String clientId) {
        String normalizedClientId = normalizeClientId(clientId);
        if (normalizedClientId == null) {
            return;
        }
        ProxyClientInfo oldClientInfo = this.clientIdTable.remove(normalizedClientId);
        if (oldClientInfo != null) {
            this.removeIndexes(oldClientInfo);
            this.clientIdIndex.remove(normalizedClientId);
            this.recordOperation(ProxyClientReadServiceOperation.REMOVE);
        }
    }

    public synchronized int removeInactiveClients(long maxLastActiveTimeMillis) {
        List<String> inactiveClientIds = new ArrayList<>();
        for (ProxyClientInfo clientInfo : this.clientIdTable.values()) {
            if (clientInfo.getLastActiveTimeMillis() <= maxLastActiveTimeMillis) {
                inactiveClientIds.add(clientInfo.getClientId());
            }
        }
        for (String clientId : inactiveClientIds) {
            this.removeClient(clientId);
        }
        return inactiveClientIds.size();
    }

    public synchronized ProxyClientInfo getClient(String clientId) {
        return this.clientIdTable.get(normalizeClientId(clientId));
    }

    public synchronized ProxyClientReadServiceStats snapshotStats() {
        Map<ClientType, Long> clientTypeCounts = new HashMap<>();
        for (Map.Entry<ClientType, NavigableSet<String>> entry : this.clientTypeIndex.entrySet()) {
            clientTypeCounts.put(entry.getKey(), (long) entry.getValue().size());
        }
        return new ProxyClientReadServiceStats(
            this.clientIdTable.size(),
            this.groupIndex.size(),
            this.topicIndex.size(),
            this.proxyIdIndex.size(),
            clientTypeCounts
        );
    }

    public synchronized ProxyClientPage listClients(ProxyClientQuery query) {
        ProxyClientQuery effectiveQuery = query == null ? ProxyClientQuery.newBuilder().build() : query;
        NavigableSet<String> clientIds = this.getCandidateClientIds(effectiveQuery);
        String pageToken = effectiveQuery.getPageToken();
        if (StringUtils.isNotBlank(pageToken)) {
            if (!clientIds.contains(pageToken)) {
                throw new IllegalArgumentException("Invalid page token: " + pageToken);
            }
            clientIds = clientIds.tailSet(pageToken, false);
        }

        int pageSize = effectiveQuery.getBoundedPageSize();
        List<ProxyClientInfo> clients = new ArrayList<>(pageSize);
        Iterator<String> iterator = clientIds.iterator();
        while (iterator.hasNext() && clients.size() < pageSize) {
            clients.add(this.clientIdTable.get(iterator.next()));
        }

        String nextPageToken = "";
        if (iterator.hasNext() && !clients.isEmpty()) {
            nextPageToken = clients.get(clients.size() - 1).getClientId();
        }
        return new ProxyClientPage(clients, nextPageToken);
    }

    private NavigableSet<String> getCandidateClientIds(ProxyClientQuery query) {
        List<NavigableSet<String>> candidateIndexes = new ArrayList<>(4);
        if (StringUtils.isNotBlank(query.getGroup())) {
            candidateIndexes.add(this.getIndexClientIds(this.groupIndex, query.getGroup()));
        }
        if (StringUtils.isNotBlank(query.getTopic())) {
            candidateIndexes.add(this.getIndexClientIds(this.topicIndex, query.getTopic()));
        }
        if (query.getClientType() != null) {
            candidateIndexes.add(this.getIndexClientIds(this.clientTypeIndex, query.getClientType()));
        }
        if (StringUtils.isNotBlank(query.getProxyId())) {
            candidateIndexes.add(this.getIndexClientIds(this.proxyIdIndex, query.getProxyId()));
        }
        if (candidateIndexes.isEmpty()) {
            return this.clientIdIndex;
        }

        NavigableSet<String> smallestCandidateIndex = this.smallestCandidateIndex(candidateIndexes);
        NavigableSet<String> clientIds = new TreeSet<>(smallestCandidateIndex);
        for (NavigableSet<String> candidateIndex : candidateIndexes) {
            if (candidateIndex != smallestCandidateIndex) {
                clientIds.retainAll(candidateIndex);
            }
        }
        return clientIds;
    }

    private NavigableSet<String> smallestCandidateIndex(List<NavigableSet<String>> candidateIndexes) {
        NavigableSet<String> result = candidateIndexes.get(0);
        for (NavigableSet<String> candidateIndex : candidateIndexes) {
            if (candidateIndex.size() < result.size()) {
                result = candidateIndex;
            }
        }
        return result;
    }

    private <T> NavigableSet<String> getIndexClientIds(Map<T, NavigableSet<String>> index, T key) {
        NavigableSet<String> clientIds = index.get(key);
        if (clientIds == null) {
            return new TreeSet<>();
        }
        return new TreeSet<>(clientIds);
    }

    private void addIndexes(ProxyClientInfo clientInfo) {
        String clientId = clientInfo.getClientId();
        for (String group : clientInfo.getGroups()) {
            this.addIndex(this.groupIndex, group, clientId);
        }
        for (String topic : clientInfo.getTopics()) {
            this.addIndex(this.topicIndex, topic, clientId);
        }
        if (clientInfo.getClientType() != null) {
            this.addIndex(this.clientTypeIndex, clientInfo.getClientType(), clientId);
        }
        if (clientInfo.getProxyId() != null) {
            this.addIndex(this.proxyIdIndex, clientInfo.getProxyId(), clientId);
        }
    }

    private void removeIndexes(ProxyClientInfo clientInfo) {
        String clientId = clientInfo.getClientId();
        for (String group : clientInfo.getGroups()) {
            this.removeIndex(this.groupIndex, group, clientId);
        }
        for (String topic : clientInfo.getTopics()) {
            this.removeIndex(this.topicIndex, topic, clientId);
        }
        if (clientInfo.getClientType() != null) {
            this.removeIndex(this.clientTypeIndex, clientInfo.getClientType(), clientId);
        }
        if (clientInfo.getProxyId() != null) {
            this.removeIndex(this.proxyIdIndex, clientInfo.getProxyId(), clientId);
        }
    }

    private <T> void addIndex(Map<T, NavigableSet<String>> index, T key, String clientId) {
        NavigableSet<String> clientIds = index.get(key);
        if (clientIds == null) {
            clientIds = new TreeSet<>();
            index.put(key, clientIds);
        }
        clientIds.add(clientId);
    }

    private <T> void removeIndex(Map<T, NavigableSet<String>> index, T key, String clientId) {
        NavigableSet<String> clientIds = index.get(key);
        if (clientIds == null) {
            return;
        }
        clientIds.remove(clientId);
        if (clientIds.isEmpty()) {
            index.remove(key);
        }
    }

    private void recordOperation(ProxyClientReadServiceOperation operation) {
        try {
            this.operationRecorder.accept(operation);
        } catch (Throwable e) {
            log.warn("record proxy client read model operation failed. operation:{}", operation, e);
        }
    }

    private static String normalizeClientId(String clientId) {
        return StringUtils.trimToNull(clientId);
    }
}
