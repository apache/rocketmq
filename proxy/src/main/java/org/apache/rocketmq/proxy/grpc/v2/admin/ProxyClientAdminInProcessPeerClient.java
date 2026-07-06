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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;

public class ProxyClientAdminInProcessPeerClient implements ProxyClientAdminPeerClient {
    private final Map<String, ProxyClientAdminPeerLocalExecutor> executors;
    private final List<String> proxyIds;

    public ProxyClientAdminInProcessPeerClient(Map<String, ProxyClientAdminPeerLocalExecutor> executors) {
        if (executors == null) {
            throw new IllegalArgumentException("executors is required");
        }
        TreeMap<String, ProxyClientAdminPeerLocalExecutor> sortedExecutors = new TreeMap<>();
        for (Map.Entry<String, ProxyClientAdminPeerLocalExecutor> entry : executors.entrySet()) {
            String proxyId = requireProxyId(entry.getKey());
            if (sortedExecutors.containsKey(proxyId)) {
                throw new IllegalArgumentException("Duplicate proxyId: " + proxyId);
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("executor is required");
            }
            String executorProxyId = entry.getValue().getLocalProxyId();
            if (!proxyId.equals(executorProxyId)) {
                throw new IllegalArgumentException(
                    "executor proxyId mismatch: expected " + proxyId + ", actual " + executorProxyId
                );
            }
            sortedExecutors.put(proxyId, entry.getValue());
        }
        if (sortedExecutors.isEmpty()) {
            throw new IllegalArgumentException("at least one executor is required");
        }
        this.executors = Collections.unmodifiableMap(new LinkedHashMap<>(sortedExecutors));
        this.proxyIds = Collections.unmodifiableList(new ArrayList<>(sortedExecutors.keySet()));
    }

    @Override
    public List<String> listProxyIds() {
        return this.proxyIds;
    }

    @Override
    public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
        ProxyClientAdminPeerRequest request) {
        String requiredProxyId = requireProxyId(proxyId);
        ProxyClientAdminPeerLocalExecutor executor = this.executors.get(requiredProxyId);
        if (executor == null) {
            return ProxyClientAdminPeerResponse.error(
                requiredProxyId,
                Code.NOT_FOUND.name(),
                "Proxy not found: " + requiredProxyId
            );
        }
        try {
            return executor.execute(ctx, request);
        } catch (Throwable t) {
            return ProxyClientAdminPeerResponse.error(
                requiredProxyId,
                Code.INTERNAL_SERVER_ERROR.name(),
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
            );
        }
    }

    private static String requireProxyId(String proxyId) {
        String normalizedProxyId = StringUtils.trimToNull(proxyId);
        if (normalizedProxyId == null) {
            throw new IllegalArgumentException("proxyId is required");
        }
        return normalizedProxyId;
    }
}
