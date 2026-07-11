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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
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
    private final Map<String, NavigableSet<String>> clientLanguageIndex = new HashMap<>();
    private final NavigableMap<Long, NavigableSet<String>> connectTimeIndex = new TreeMap<>();
    private final Consumer<ProxyClientReadServiceOperation> operationRecorder;
    private List<String> clientIdPageAnchors = new ArrayList<>();
    private boolean clientIdPageAnchorsDirty = true;

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
        if (this.clientIdIndex.add(clientId)) {
            this.clientIdPageAnchorsDirty = true;
        }
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
            if (this.clientIdIndex.remove(normalizedClientId)) {
                this.clientIdPageAnchorsDirty = true;
            }
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
        List<NavigableSet<String>> candidateIndexes = this.getCandidateIndexes(effectiveQuery);
        NavigableSet<String> drivingIndex = this.smallestCandidateIndex(candidateIndexes);
        String pageToken = effectiveQuery.getPageToken();
        if (StringUtils.isNotBlank(pageToken)) {
            if (!this.matchesAllCandidateIndexes(pageToken, candidateIndexes)) {
                throw new IllegalArgumentException("Invalid page token: " + pageToken);
            }
            drivingIndex = drivingIndex.tailSet(pageToken, false);
        }

        int pageSize = effectiveQuery.getBoundedPageSize();
        long skipCount = StringUtils.isBlank(pageToken)
            ? ((long) effectiveQuery.getPageNum() - 1L) * pageSize
            : 0L;
        if (StringUtils.isBlank(pageToken) && candidateIndexes.size() == 1
            && drivingIndex == this.clientIdIndex && skipCount > 0) {
            ClientIdPageAnchor anchor = this.getClientIdPageAnchor(skipCount);
            if (anchor != null) {
                drivingIndex = this.clientIdIndex.tailSet(anchor.clientId, true);
                skipCount -= anchor.offset;
            }
        }
        List<ProxyClientInfo> clients = new ArrayList<>(pageSize);
        Iterator<String> iterator = drivingIndex.iterator();
        while (skipCount > 0 && this.nextMatchingClientId(iterator, candidateIndexes, drivingIndex) != null) {
            skipCount--;
        }
        String clientId;
        while (clients.size() < pageSize
            && (clientId = this.nextMatchingClientId(iterator, candidateIndexes, drivingIndex)) != null) {
            clients.add(this.clientIdTable.get(clientId));
        }

        String nextPageToken = "";
        if (!clients.isEmpty() && this.nextMatchingClientId(iterator, candidateIndexes, drivingIndex) != null) {
            nextPageToken = clients.get(clients.size() - 1).getClientId();
        }
        return new ProxyClientPage(clients, nextPageToken);
    }

    private NavigableSet<String> getCandidateClientIds(ProxyClientQuery query) {
        List<NavigableSet<String>> candidateIndexes = this.getCandidateIndexes(query);
        if (candidateIndexes.size() == 1) {
            return candidateIndexes.get(0);
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

    private List<NavigableSet<String>> getCandidateIndexes(ProxyClientQuery query) {
        List<NavigableSet<String>> candidateIndexes = new ArrayList<>(8);
        if (StringUtils.isNotBlank(query.getClientId())) {
            candidateIndexes.add(this.getClientIdCandidate(query.getClientId()));
        }
        if (StringUtils.isNotBlank(query.getClientIdPrefix())) {
            candidateIndexes.add(this.getClientIdPrefixCandidates(query.getClientIdPrefix()));
        }
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
        if (StringUtils.isNotBlank(query.getClientLanguage())) {
            candidateIndexes.add(this.getIndexClientIds(this.clientLanguageIndex, query.getClientLanguage()));
        }
        if (query.getConnectTimeStartMillis() != null || query.getConnectTimeEndMillis() != null) {
            candidateIndexes.add(this.getConnectTimeClientIds(query.getConnectTimeStartMillis(),
                query.getConnectTimeEndMillis()));
        }
        if (candidateIndexes.isEmpty()) {
            candidateIndexes.add(this.clientIdIndex);
        }
        return candidateIndexes;
    }

    private String nextMatchingClientId(Iterator<String> iterator,
        List<NavigableSet<String>> candidateIndexes, NavigableSet<String> drivingIndex) {
        while (iterator.hasNext()) {
            String clientId = iterator.next();
            if (this.matchesAllCandidateIndexes(clientId, candidateIndexes, drivingIndex)) {
                return clientId;
            }
        }
        return null;
    }

    private boolean matchesAllCandidateIndexes(String clientId, List<NavigableSet<String>> candidateIndexes) {
        return this.matchesAllCandidateIndexes(clientId, candidateIndexes, null);
    }

    private boolean matchesAllCandidateIndexes(String clientId, List<NavigableSet<String>> candidateIndexes,
        NavigableSet<String> drivingIndex) {
        for (NavigableSet<String> candidateIndex : candidateIndexes) {
            if (candidateIndex != drivingIndex && !candidateIndex.contains(clientId)) {
                return false;
            }
        }
        return true;
    }

    private NavigableSet<String> getClientIdCandidate(String clientId) {
        NavigableSet<String> clientIds = new TreeSet<>();
        if (this.clientIdTable.containsKey(clientId)) {
            clientIds.add(clientId);
        }
        return clientIds;
    }

    private NavigableSet<String> getClientIdPrefixCandidates(String clientIdPrefix) {
        String upperBound = nextPrefix(clientIdPrefix);
        if (upperBound == null) {
            return this.clientIdIndex.tailSet(clientIdPrefix, true);
        }
        return this.clientIdIndex.subSet(clientIdPrefix, true, upperBound, false);
    }

    private static String nextPrefix(String prefix) {
        char[] chars = prefix.toCharArray();
        for (int i = chars.length - 1; i >= 0; i--) {
            if (chars[i] != Character.MAX_VALUE) {
                chars[i]++;
                return new String(chars, 0, i + 1);
            }
        }
        return null;
    }

    private ClientIdPageAnchor getClientIdPageAnchor(long skipCount) {
        this.refreshClientIdPageAnchors();
        if (this.clientIdPageAnchors.isEmpty()) {
            return null;
        }
        long requestedAnchorIndex = skipCount / ProxyClientQuery.MAX_PAGE_SIZE;
        int anchorIndex = (int) Math.min(requestedAnchorIndex, this.clientIdPageAnchors.size() - 1L);
        return new ClientIdPageAnchor(
            this.clientIdPageAnchors.get(anchorIndex),
            (long) anchorIndex * ProxyClientQuery.MAX_PAGE_SIZE
        );
    }

    private void refreshClientIdPageAnchors() {
        if (!this.clientIdPageAnchorsDirty) {
            return;
        }
        List<String> anchors = new ArrayList<>(
            (this.clientIdIndex.size() + ProxyClientQuery.MAX_PAGE_SIZE - 1)
                / ProxyClientQuery.MAX_PAGE_SIZE
        );
        int index = 0;
        for (String clientId : this.clientIdIndex) {
            if (index % ProxyClientQuery.MAX_PAGE_SIZE == 0) {
                anchors.add(clientId);
            }
            index++;
        }
        this.clientIdPageAnchors = anchors;
        this.clientIdPageAnchorsDirty = false;
    }

    private NavigableSet<String> getConnectTimeClientIds(Long connectTimeStartMillis, Long connectTimeEndMillis) {
        NavigableMap<Long, NavigableSet<String>> rangeIndex;
        if (connectTimeStartMillis != null && connectTimeEndMillis != null) {
            rangeIndex = this.connectTimeIndex.subMap(connectTimeStartMillis, true, connectTimeEndMillis, true);
        } else if (connectTimeStartMillis != null) {
            rangeIndex = this.connectTimeIndex.tailMap(connectTimeStartMillis, true);
        } else {
            rangeIndex = this.connectTimeIndex.headMap(connectTimeEndMillis, true);
        }

        if (rangeIndex.isEmpty()) {
            return new TreeSet<>();
        }
        if (rangeIndex.size() == 1) {
            return rangeIndex.firstEntry().getValue();
        }

        NavigableSet<String> clientIds = new TreeSet<>();
        for (NavigableSet<String> indexedClientIds : rangeIndex.values()) {
            clientIds.addAll(indexedClientIds);
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
        return clientIds;
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
        String clientLanguage = normalizeIndexValue(clientInfo.getLanguage());
        if (clientLanguage != null) {
            this.addIndex(this.clientLanguageIndex, clientLanguage, clientId);
        }
        this.addIndex(this.connectTimeIndex, clientInfo.getConnectTimeMillis(), clientId);
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
        String clientLanguage = normalizeIndexValue(clientInfo.getLanguage());
        if (clientLanguage != null) {
            this.removeIndex(this.clientLanguageIndex, clientLanguage, clientId);
        }
        this.removeIndex(this.connectTimeIndex, clientInfo.getConnectTimeMillis(), clientId);
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

    private static String normalizeIndexValue(String value) {
        return StringUtils.trimToNull(value);
    }

    private static class ClientIdPageAnchor {
        private final String clientId;
        private final long offset;

        private ClientIdPageAnchor(String clientId, long offset) {
            this.clientId = clientId;
            this.offset = offset;
        }
    }
}
