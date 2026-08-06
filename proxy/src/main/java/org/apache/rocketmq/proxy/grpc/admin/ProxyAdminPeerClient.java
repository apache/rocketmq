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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.ClientInstance;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyScope;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * RIP-2 D3 cluster-wide aggregation ("PROXY_SCOPE_ALL_PROXIES").
 *
 * <p>Each proxy always serves its own local view; when the caller requests the
 * ALL_PROXIES scope, this client fans the query out to the configured peer
 * proxy admin endpoints ({@code proxyAdminPeerEndpoints}) in parallel and
 * merges the per-node local views. Client instances are deduplicated by
 * {@code client_id} (a client is attached to exactly one proxy at a time);
 * the local node's view wins on duplicates. Every merged instance keeps the
 * {@code proxy_endpoint}/{@code epoch} tag of the node that owns it, so the
 * result is auditable.
 *
 * <p>Peer failures never fail the aggregated call: an unreachable peer is
 * skipped (with a warning log) and the merged view of the remaining nodes is
 * returned.
 */
public class ProxyAdminPeerClient implements StartAndShutdown {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    /**
     * Fan-out requests always use LOCAL scope towards peers to avoid recursion.
     */
    private static final int MAX_PEER_PAGE_SIZE = 1000;

    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final org.apache.rocketmq.auth.config.AuthConfig authConfig;

    public ProxyAdminPeerClient() {
        this(null);
    }

    public ProxyAdminPeerClient(org.apache.rocketmq.auth.config.AuthConfig authConfig) {
        this.authConfig = authConfig;
    }

    /**
     * Aggregate ListClients across all proxies.
     *
     * @param localView this node's already-filtered local instances
     * @param request original request (filter is forwarded to peers, scope forced to LOCAL)
     * @param peerEndpoints peer admin endpoints (host:port)
     * @param timeoutMillis per-peer timeout
     * @return merged view, local instances first
     */
    public List<ClientInstance> listClientsAllProxies(List<ClientInstance> localView, ListClientsRequest request,
        List<String> peerEndpoints, long timeoutMillis) {
        ListClientsRequest peerRequest = request.toBuilder()
            .setScope(ProxyScope.PROXY_SCOPE_LOCAL_PROXY)
            .setPageSize(MAX_PEER_PAGE_SIZE)
            .clearNextToken()
            .build();
        List<CompletableFuture<List<ClientInstance>>> futures = new ArrayList<>();
        for (String endpoint : peerEndpoints) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ProxyAdminServiceGrpc.ProxyAdminServiceFutureStub stub = futureStub(endpoint, timeoutMillis);
                    ListClientsResponse response = stub.listClients(peerRequest)
                        .get(timeoutMillis, TimeUnit.MILLISECONDS);
                    if (response.getStatus().getCode() != Code.OK) {
                        log.warn("RIP-2 peer listClients returned non-OK. peer:{}, status:{}", endpoint,
                            response.getStatus());
                        return new ArrayList<ClientInstance>();
                    }
                    return response.getClientsList();
                } catch (Throwable t) {
                    log.warn("RIP-2 peer listClients failed, skip peer. peer:{}", endpoint, t);
                    return new ArrayList<ClientInstance>();
                }
            }));
        }
        return merge(localView, futures, timeoutMillis);
    }

    /**
     * Aggregate ListClientsByGroup across all proxies.
     */
    public List<ClientInstance> listClientsByGroupAllProxies(List<ClientInstance> localView,
        ListClientsByGroupRequest request, List<String> peerEndpoints, long timeoutMillis) {
        ListClientsByGroupRequest peerRequest = request.toBuilder()
            .setScope(ProxyScope.PROXY_SCOPE_LOCAL_PROXY)
            .setPageSize(MAX_PEER_PAGE_SIZE)
            .clearNextToken()
            .build();
        List<CompletableFuture<List<ClientInstance>>> futures = new ArrayList<>();
        for (String endpoint : peerEndpoints) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ProxyAdminServiceGrpc.ProxyAdminServiceFutureStub stub = futureStub(endpoint, timeoutMillis);
                    ListClientsByGroupResponse response = stub.listClientsByGroup(peerRequest)
                        .get(timeoutMillis, TimeUnit.MILLISECONDS);
                    if (response.getStatus().getCode() != Code.OK) {
                        log.warn("RIP-2 peer listClientsByGroup returned non-OK. peer:{}, status:{}", endpoint,
                            response.getStatus());
                        return new ArrayList<ClientInstance>();
                    }
                    return response.getClientsList();
                } catch (Throwable t) {
                    log.warn("RIP-2 peer listClientsByGroup failed, skip peer. peer:{}", endpoint, t);
                    return new ArrayList<ClientInstance>();
                }
            }));
        }
        return merge(localView, futures, timeoutMillis);
    }

    /**
     * Aggregate ListClientsByTopic across all proxies.
     */
    public List<ClientInstance> listClientsByTopicAllProxies(List<ClientInstance> localView,
        ListClientsByTopicRequest request, List<String> peerEndpoints, long timeoutMillis) {
        ListClientsByTopicRequest peerRequest = request.toBuilder()
            .setScope(ProxyScope.PROXY_SCOPE_LOCAL_PROXY)
            .setPageSize(MAX_PEER_PAGE_SIZE)
            .clearNextToken()
            .build();
        List<CompletableFuture<List<ClientInstance>>> futures = new ArrayList<>();
        for (String endpoint : peerEndpoints) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ProxyAdminServiceGrpc.ProxyAdminServiceFutureStub stub = futureStub(endpoint, timeoutMillis);
                    ListClientsByTopicResponse response = stub.listClientsByTopic(peerRequest)
                        .get(timeoutMillis, TimeUnit.MILLISECONDS);
                    if (response.getStatus().getCode() != Code.OK) {
                        log.warn("RIP-2 peer listClientsByTopic returned non-OK. peer:{}, status:{}", endpoint,
                            response.getStatus());
                        return new ArrayList<ClientInstance>();
                    }
                    return response.getClientsList();
                } catch (Throwable t) {
                    log.warn("RIP-2 peer listClientsByTopic failed, skip peer. peer:{}", endpoint, t);
                    return new ArrayList<ClientInstance>();
                }
            }));
        }
        return merge(localView, futures, timeoutMillis);
    }

    private List<ClientInstance> merge(List<ClientInstance> localView,
        List<CompletableFuture<List<ClientInstance>>> peerFutures, long timeoutMillis) {
        Map<String, ClientInstance> merged = new LinkedHashMap<>();
        for (ClientInstance instance : localView) {
            merged.putIfAbsent(instance.getClientId(), instance);
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        for (CompletableFuture<List<ClientInstance>> future : peerFutures) {
            long remaining = Math.max(1, deadline - System.currentTimeMillis());
            try {
                List<ClientInstance> peerView = future.get(remaining, TimeUnit.MILLISECONDS);
                for (ClientInstance instance : peerView) {
                    merged.putIfAbsent(instance.getClientId(), instance);
                }
            } catch (Throwable t) {
                future.cancel(true);
                log.warn("RIP-2 peer aggregation timed out or failed, skip peer result", t);
            }
        }
        return new ArrayList<>(merged.values());
    }

    private ProxyAdminServiceGrpc.ProxyAdminServiceFutureStub futureStub(String endpoint, long timeoutMillis) {
        ManagedChannel channel = channels.computeIfAbsent(endpoint, key ->
            NettyChannelBuilder.forTarget(key)
                .usePlaintext()
                .build());
        ProxyAdminServiceGrpc.ProxyAdminServiceFutureStub stub =
            ProxyAdminServiceGrpc.newFutureStub(channel)
                .withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS);
        ProxyAdminClientAuthInterceptor authInterceptor = buildAuthInterceptor();
        if (authInterceptor != null) {
            stub = stub.withInterceptors(authInterceptor);
        }
        return stub;
    }

    /**
     * Fan-out requests authenticate as the proxy's inner client (SUPER user seeded via
     * innerClientAuthenticationCredentials) when cluster authentication is enabled.
     */
    private ProxyAdminClientAuthInterceptor buildAuthInterceptor() {
        if (authConfig == null || !authConfig.isAuthenticationEnabled()) {
            return null;
        }
        String credentialsJson = authConfig.getInnerClientAuthenticationCredentials();
        if (credentialsJson == null || credentialsJson.isEmpty()) {
            return null;
        }
        try {
            org.apache.rocketmq.acl.common.SessionCredentials credentials =
                com.alibaba.fastjson.JSON.parseObject(credentialsJson,
                    org.apache.rocketmq.acl.common.SessionCredentials.class);
            if (credentials == null || credentials.getAccessKey() == null
                || credentials.getSecretKey() == null) {
                return null;
            }
            return new ProxyAdminClientAuthInterceptor(credentials.getAccessKey(), credentials.getSecretKey());
        } catch (Throwable t) {
            log.warn("RIP-2 peer auth credentials are invalid, fan-out will be unauthenticated", t);
            return null;
        }
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void shutdown() throws Exception {
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown();
                if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (Throwable t) {
                log.warn("RIP-2 peer channel shutdown failed", t);
            }
        }
        channels.clear();
    }
}
