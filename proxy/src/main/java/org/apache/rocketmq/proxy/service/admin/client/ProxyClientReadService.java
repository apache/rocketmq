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
import org.apache.commons.lang3.StringUtils;

public class ProxyClientReadService {
    private final Map<String, ProxyClientInfo> clientIdTable = new HashMap<>();
    private final Map<String, NavigableSet<String>> groupIndex = new HashMap<>();
    private final Map<String, NavigableSet<String>> topicIndex = new HashMap<>();
    private final Map<ClientType, NavigableSet<String>> clientTypeIndex = new HashMap<>();

    public synchronized void upsertClient(ProxyClientInfo clientInfo) {
        if (clientInfo == null || StringUtils.isBlank(clientInfo.getClientId())) {
            throw new IllegalArgumentException("clientId is required");
        }
        ProxyClientInfo oldClientInfo = this.clientIdTable.put(clientInfo.getClientId(), clientInfo);
        if (oldClientInfo != null) {
            this.removeIndexes(oldClientInfo);
        }
        this.addIndexes(clientInfo);
    }

    public synchronized void removeClient(String clientId) {
        if (StringUtils.isBlank(clientId)) {
            return;
        }
        ProxyClientInfo oldClientInfo = this.clientIdTable.remove(clientId);
        if (oldClientInfo != null) {
            this.removeIndexes(oldClientInfo);
        }
    }

    public synchronized ProxyClientInfo getClient(String clientId) {
        return this.clientIdTable.get(clientId);
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
        NavigableSet<String> clientIds = new TreeSet<>(this.clientIdTable.keySet());
        if (StringUtils.isNotBlank(query.getGroup())) {
            clientIds.retainAll(this.getIndexClientIds(this.groupIndex, query.getGroup()));
        }
        if (StringUtils.isNotBlank(query.getTopic())) {
            clientIds.retainAll(this.getIndexClientIds(this.topicIndex, query.getTopic()));
        }
        if (query.getClientType() != null) {
            clientIds.retainAll(this.getIndexClientIds(this.clientTypeIndex, query.getClientType()));
        }
        return clientIds;
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
}
