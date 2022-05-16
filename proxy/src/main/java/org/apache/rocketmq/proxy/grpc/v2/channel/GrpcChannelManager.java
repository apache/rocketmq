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

package org.apache.rocketmq.proxy.grpc.v2.channel;

import io.grpc.Context;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.proxy.service.out.ProxyOutService;

public class GrpcChannelManager {
    private final ProxyOutService proxyOutService;
    protected final ConcurrentMap<String /* group */, Map<String, GrpcClientChannel>/* clientId */> groupClientIdChannelMap = new ConcurrentHashMap<>();

    protected final AtomicLong nonceIdGenerator = new AtomicLong(0);
    protected final ConcurrentMap<String /* nonce */, CompletableFuture<?>> resultNonceFutureMap = new ConcurrentHashMap<>();

    public GrpcChannelManager(ProxyOutService proxyOutService) {
        this.proxyOutService = proxyOutService;
    }

    public GrpcClientChannel createChannel(Context ctx, String group, String clientId) {
        this.groupClientIdChannelMap.compute(group, (groupKey, clientIdMap) -> {
            if (clientIdMap == null) {
                clientIdMap = new ConcurrentHashMap<>();
            }
            clientIdMap.computeIfAbsent(clientId, clientIdKey -> new GrpcClientChannel(proxyOutService, this, ctx, group, clientId));
            return clientIdMap;
        });
        return getChannel(group, clientId);
    }

    public GrpcClientChannel getChannel(String group, String clientId) {
        Map<String, GrpcClientChannel> clientIdChannelMap = this.groupClientIdChannelMap.get(group);
        if (clientIdChannelMap == null) {
            return null;
        }
        return clientIdChannelMap.get(clientId);
    }

    public GrpcClientChannel removeChannel(String group, String clientId)  {
        AtomicReference<GrpcClientChannel> channelRef = new AtomicReference<>();
        this.groupClientIdChannelMap.computeIfPresent(group, (groupKey, clientIdMap) -> {
            channelRef.set(clientIdMap.remove(clientId));
            if (clientIdMap.isEmpty()) {
                return null;
            }
            return clientIdMap;
        });
        return channelRef.get();
    }

    public String addResponseFuture(CompletableFuture<?> responseFuture) {
        String nonce = this.nextNonce();
        this.resultNonceFutureMap.put(nonce, responseFuture);
        return nonce;
    }

    public <T> CompletableFuture<T> getAndRemoveResponseFuture(String nonce) {
        return (CompletableFuture<T>) this.resultNonceFutureMap.remove(nonce);
    }

    protected String nextNonce() {
        return String.valueOf(this.nonceIdGenerator.getAndIncrement());
    }
}
