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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ProxyClientReadServiceStats {
    private final long totalClientCount;
    private final long groupIndexCount;
    private final long topicIndexCount;
    private final long proxyIdIndexCount;
    private final Map<ClientType, Long> clientTypeCounts;

    public ProxyClientReadServiceStats(long totalClientCount, long groupIndexCount, long topicIndexCount,
        Map<ClientType, Long> clientTypeCounts) {
        this(totalClientCount, groupIndexCount, topicIndexCount, 0L, clientTypeCounts);
    }

    public ProxyClientReadServiceStats(long totalClientCount, long groupIndexCount, long topicIndexCount,
        long proxyIdIndexCount, Map<ClientType, Long> clientTypeCounts) {
        this.totalClientCount = totalClientCount;
        this.groupIndexCount = groupIndexCount;
        this.topicIndexCount = topicIndexCount;
        this.proxyIdIndexCount = proxyIdIndexCount;
        Map<ClientType, Long> copiedClientTypeCounts = new HashMap<>();
        if (clientTypeCounts != null) {
            copiedClientTypeCounts.putAll(clientTypeCounts);
        }
        this.clientTypeCounts = Collections.unmodifiableMap(copiedClientTypeCounts);
    }

    public long getTotalClientCount() {
        return totalClientCount;
    }

    public long getGroupIndexCount() {
        return groupIndexCount;
    }

    public long getTopicIndexCount() {
        return topicIndexCount;
    }

    public long getProxyIdIndexCount() {
        return proxyIdIndexCount;
    }

    public Map<ClientType, Long> getClientTypeCounts() {
        return clientTypeCounts;
    }

    public long getClientTypeCount(ClientType clientType) {
        Long count = this.clientTypeCounts.get(clientType);
        if (count == null) {
            return 0L;
        }
        return count;
    }
}
