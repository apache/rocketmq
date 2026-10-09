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
package org.apache.rocketmq.proxy.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIExt;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData;

/**
 * Sends lightweight heartbeats over channels that Proxy clients have already established with brokers.
 *
 * <p>The service deliberately uses the active-channel snapshot instead of topic route data. It therefore never opens
 * a connection merely to keep it alive and does not fan out to every broker in a large cluster.</p>
 */
public class ProxyBrokerHeartbeatService implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final String CLIENT_ID_PREFIX = "ProxyBrokerHeartbeat_";

    private final List<MQClientAPIFactory> clientFactories;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean enabled;
    private final long heartbeatIntervalMillis;
    private final long heartbeatTimeoutMillis;
    private final String clientId;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean roundRunning = new AtomicBoolean();
    private final AtomicReference<HeartbeatRoundStats> lastRoundStats =
        new AtomicReference<>(HeartbeatRoundStats.empty());

    private volatile ScheduledFuture<?> scheduledFuture;

    public ProxyBrokerHeartbeatService(List<MQClientAPIFactory> clientFactories,
        ScheduledExecutorService scheduledExecutorService, ProxyConfig proxyConfig) {
        this(clientFactories, scheduledExecutorService, proxyConfig.isEnableProxyBrokerHeartbeat(),
            proxyConfig.getProxyBrokerHeartbeatIntervalMillis(), proxyConfig.getProxyBrokerHeartbeatTimeoutMillis(),
            CLIENT_ID_PREFIX + proxyConfig.getProxyName());
    }

    ProxyBrokerHeartbeatService(List<MQClientAPIFactory> clientFactories,
        ScheduledExecutorService scheduledExecutorService, boolean enabled, long heartbeatIntervalMillis,
        long heartbeatTimeoutMillis, String clientId) {
        this.clientFactories = clientFactories == null
            ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(clientFactories));
        this.scheduledExecutorService = scheduledExecutorService;
        this.enabled = enabled;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.clientId = clientId;
    }

    @Override
    public void start() {
        if (!enabled || !started.compareAndSet(false, true)) {
            return;
        }
        try {
            this.scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(
                this::runHeartbeatRoundSafely,
                heartbeatIntervalMillis,
                heartbeatIntervalMillis,
                TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            started.set(false);
            throw e;
        }
        log.info("Proxy-to-Broker heartbeat service started. intervalMillis={}, timeoutMillis={}",
            heartbeatIntervalMillis, heartbeatTimeoutMillis);
    }

    @Override
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        ScheduledFuture<?> future = this.scheduledFuture;
        this.scheduledFuture = null;
        if (future != null) {
            future.cancel(false);
        }
        log.info("Proxy-to-Broker heartbeat service stopped");
    }

    void runHeartbeatRoundSafely() {
        try {
            runHeartbeatRound();
        } catch (Throwable t) {
            log.error("Unexpected error while sending Proxy-to-Broker heartbeats", t);
        }
    }

    void runHeartbeatRound() {
        if (!enabled) {
            return;
        }
        if (!roundRunning.compareAndSet(false, true)) {
            log.warn("Skip Proxy-to-Broker heartbeat round because the previous round is still running");
            return;
        }

        long startTimestamp = System.currentTimeMillis();
        MutableRoundStats stats = new MutableRoundStats();
        try {
            for (MQClientAPIFactory clientFactory : clientFactories) {
                collectFactoryHeartbeats(clientFactory, stats);
            }
        } finally {
            HeartbeatRoundStats snapshot = stats.snapshot(startTimestamp, System.currentTimeMillis());
            lastRoundStats.set(snapshot);
            roundRunning.set(false);
            if (snapshot.getFailedHeartbeatCount() > 0 || snapshot.getFailedClientCount() > 0) {
                log.warn("Proxy-to-Broker heartbeat round completed with failures. {}", snapshot);
            } else {
                log.debug("Proxy-to-Broker heartbeat round completed. {}", snapshot);
            }
        }
    }

    private void collectFactoryHeartbeats(MQClientAPIFactory clientFactory, MutableRoundStats stats) {
        if (clientFactory == null) {
            return;
        }

        final MQClientAPIExt[] clients;
        try {
            clients = clientFactory.getClients();
        } catch (Throwable t) {
            stats.failedClientCount++;
            log.warn("Failed to get clients while preparing Proxy-to-Broker heartbeat", t);
            return;
        }
        if (clients == null) {
            return;
        }

        for (MQClientAPIExt client : clients) {
            collectClientHeartbeats(client, stats);
        }
    }

    private void collectClientHeartbeats(MQClientAPIExt client, MutableRoundStats stats) {
        if (client == null) {
            return;
        }
        stats.clientCount++;

        final Set<String> brokerAddresses;
        try {
            brokerAddresses = client.getActiveBrokerAddresses();
        } catch (Throwable t) {
            stats.failedClientCount++;
            log.warn("Failed to list active broker channels while preparing Proxy-to-Broker heartbeat", t);
            return;
        }
        if (brokerAddresses == null || brokerAddresses.isEmpty()) {
            return;
        }

        for (String brokerAddress : brokerAddresses) {
            if (StringUtils.isBlank(brokerAddress)) {
                continue;
            }
            sendHeartbeat(client, brokerAddress, stats);
        }
    }

    private void sendHeartbeat(MQClientAPIExt client, String brokerAddress, MutableRoundStats stats) {
        stats.attemptedHeartbeatCount++;
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(clientId);
        try {
            CompletableFuture<Void> future = client.sendHeartbeatOneway(
                brokerAddress, heartbeatData, heartbeatTimeoutMillis);
            if (future == null) {
                stats.failedHeartbeatCount++;
                log.warn("Failed to send Proxy-to-Broker heartbeat to {}: no completion future returned",
                    brokerAddress);
            } else if (future.isCompletedExceptionally()) {
                stats.failedHeartbeatCount++;
                future.whenComplete((result, throwable) ->
                    log.warn("Failed to send Proxy-to-Broker heartbeat to {}", brokerAddress, throwable));
            } else {
                stats.successfulHeartbeatCount++;
            }
        } catch (Throwable t) {
            stats.failedHeartbeatCount++;
            log.warn("Failed to send Proxy-to-Broker heartbeat to {}", brokerAddress, t);
        }
    }

    HeartbeatRoundStats getLastRoundStats() {
        return lastRoundStats.get();
    }

    boolean isStarted() {
        return started.get();
    }

    boolean isRoundRunning() {
        return roundRunning.get();
    }

    static class HeartbeatRoundStats {
        private final long startTimestamp;
        private final long finishTimestamp;
        private final int clientCount;
        private final int failedClientCount;
        private final int attemptedHeartbeatCount;
        private final int successfulHeartbeatCount;
        private final int failedHeartbeatCount;

        HeartbeatRoundStats(long startTimestamp, long finishTimestamp, int clientCount, int failedClientCount,
            int attemptedHeartbeatCount, int successfulHeartbeatCount, int failedHeartbeatCount) {
            this.startTimestamp = startTimestamp;
            this.finishTimestamp = finishTimestamp;
            this.clientCount = clientCount;
            this.failedClientCount = failedClientCount;
            this.attemptedHeartbeatCount = attemptedHeartbeatCount;
            this.successfulHeartbeatCount = successfulHeartbeatCount;
            this.failedHeartbeatCount = failedHeartbeatCount;
        }

        static HeartbeatRoundStats empty() {
            return new HeartbeatRoundStats(0, 0, 0, 0, 0, 0, 0);
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public long getFinishTimestamp() {
            return finishTimestamp;
        }

        public long getElapsedMillis() {
            return Math.max(0, finishTimestamp - startTimestamp);
        }

        public int getClientCount() {
            return clientCount;
        }

        public int getFailedClientCount() {
            return failedClientCount;
        }

        public int getAttemptedHeartbeatCount() {
            return attemptedHeartbeatCount;
        }

        public int getSuccessfulHeartbeatCount() {
            return successfulHeartbeatCount;
        }

        public int getFailedHeartbeatCount() {
            return failedHeartbeatCount;
        }

        @Override
        public String toString() {
            return "HeartbeatRoundStats{" +
                "elapsedMillis=" + getElapsedMillis() +
                ", clientCount=" + clientCount +
                ", failedClientCount=" + failedClientCount +
                ", attemptedHeartbeatCount=" + attemptedHeartbeatCount +
                ", successfulHeartbeatCount=" + successfulHeartbeatCount +
                ", failedHeartbeatCount=" + failedHeartbeatCount +
                '}';
        }
    }

    private static class MutableRoundStats {
        private int clientCount;
        private int failedClientCount;
        private int attemptedHeartbeatCount;
        private int successfulHeartbeatCount;
        private int failedHeartbeatCount;

        private HeartbeatRoundStats snapshot(long startTimestamp, long finishTimestamp) {
            return new HeartbeatRoundStats(startTimestamp, finishTimestamp, clientCount, failedClientCount,
                attemptedHeartbeatCount, successfulHeartbeatCount, failedHeartbeatCount);
        }
    }
}
