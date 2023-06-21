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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

public class GrpcChannelManager implements StartAndShutdown {
    private final ProxyRelayService proxyRelayService;
    private final GrpcClientSettingsManager grpcClientSettingsManager;
    protected final ConcurrentMap<String, GrpcClientChannel> clientIdChannelMap = new ConcurrentHashMap<>();

    protected final AtomicLong nonceIdGenerator = new AtomicLong(0);
    protected final ConcurrentMap<String /* nonce */, ResultFuture> resultNonceFutureMap = new ConcurrentHashMap<>();

    protected final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("GrpcChannelManager_")
    );

    public GrpcChannelManager(ProxyRelayService proxyRelayService, GrpcClientSettingsManager grpcClientSettingsManager) {
        this.proxyRelayService = proxyRelayService;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.init();
    }

    protected void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(
            this::scanExpireResultFuture,
            10, 1, TimeUnit.SECONDS
        );
    }

    public GrpcClientChannel createChannel(ProxyContext ctx, String clientId) {
        return this.clientIdChannelMap.computeIfAbsent(clientId,
            k -> new GrpcClientChannel(proxyRelayService, grpcClientSettingsManager, this, ctx, clientId));
    }

    public GrpcClientChannel getChannel(String clientId) {
        return clientIdChannelMap.get(clientId);
    }

    public GrpcClientChannel removeChannel(String clientId) {
        return this.clientIdChannelMap.remove(clientId);
    }

    public <T> String addResponseFuture(CompletableFuture<ProxyRelayResult<T>> responseFuture) {
        String nonce = this.nextNonce();
        this.resultNonceFutureMap.put(nonce, new ResultFuture<>(responseFuture));
        return nonce;
    }

    public <T> CompletableFuture<ProxyRelayResult<T>> getAndRemoveResponseFuture(String nonce) {
        ResultFuture<T> resultFuture = this.resultNonceFutureMap.remove(nonce);
        if (resultFuture != null) {
            return resultFuture.future;
        }
        return null;
    }

    protected String nextNonce() {
        return String.valueOf(this.nonceIdGenerator.getAndIncrement());
    }

    protected void scanExpireResultFuture() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        long timeOutMs = TimeUnit.SECONDS.toMillis(proxyConfig.getGrpcProxyRelayRequestTimeoutInSeconds());

        Set<String> nonceSet = this.resultNonceFutureMap.keySet();
        for (String nonce : nonceSet) {
            ResultFuture<?> resultFuture = this.resultNonceFutureMap.get(nonce);
            if (resultFuture == null) {
                continue;
            }
            if (System.currentTimeMillis() - resultFuture.createTime > timeOutMs) {
                resultFuture = this.resultNonceFutureMap.remove(nonce);
                if (resultFuture != null) {
                    resultFuture.future.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_BUSY, "call remote timeout", null));
                }
            }
        }
    }

    @Override
    public void shutdown() throws Exception {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void start() throws Exception {

    }

    protected static class ResultFuture<T> {
        public CompletableFuture<ProxyRelayResult<T>> future;
        public long createTime = System.currentTimeMillis();

        public ResultFuture(CompletableFuture<ProxyRelayResult<T>> future) {
            this.future = future;
        }
    }
}
