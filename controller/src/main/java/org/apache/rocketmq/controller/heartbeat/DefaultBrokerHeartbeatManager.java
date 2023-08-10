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
package org.apache.rocketmq.controller.heartbeat;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class DefaultBrokerHeartbeatManager implements BrokerHeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 10;
    private ScheduledExecutorService scheduledService;
    private ExecutorService executor;

    private final ControllerConfig controllerConfig;

    private final Map<BrokerSetIdentity/*clusterName:brokerName*/, Map<Long/*brokerId*/, BrokerLiveInfo>> brokerLiveTable;
    private final List<BrokerLifecycleListener> brokerLifecycleListeners;

    public DefaultBrokerHeartbeatManager(final ControllerConfig controllerConfig) {
        this.controllerConfig = controllerConfig;
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.brokerLifecycleListeners = new ArrayList<>();
    }

    @Override
    public void start() {
        this.scheduledService.scheduleAtFixedRate(this::scanNotActiveBroker, 2000, this.controllerConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        this.scheduledService.shutdown();
        this.executor.shutdown();
    }

    @Override
    public void initialize() {
        this.scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_scheduledService_"));
        this.executor = Executors.newFixedThreadPool(2, new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_executorService_"));
    }

    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            final Iterator<Map.Entry<BrokerSetIdentity, Map<Long, BrokerLiveInfo>>> brokerSetIterator = this.brokerLiveTable.entrySet().iterator();
            while (brokerSetIterator.hasNext()) {
                final Map.Entry<BrokerSetIdentity, Map<Long, BrokerLiveInfo>> brokerSetEntry = brokerSetIterator.next();
                final String clusterName = brokerSetEntry.getKey().getClusterName();
                final String brokerName = brokerSetEntry.getKey().getBrokerName();
                Iterator<Map.Entry<Long, BrokerLiveInfo>> brokerIterator = brokerSetEntry.getValue().entrySet().iterator();
                while (brokerIterator.hasNext()) {
                    Map.Entry<Long, BrokerLiveInfo> brokerEntry = brokerIterator.next();
                    long lastHeartbeatMs = brokerEntry.getValue().getLastUpdateTimestamp();
                    long timeoutMs = brokerEntry.getValue().getHeartbeatTimeoutMillis();
                    if (UtilAll.computeElapsedTimeMilliseconds(lastHeartbeatMs) > timeoutMs) {
                        final Channel channel = brokerEntry.getValue().getChannel();
                        brokerIterator.remove();
                        if (channel != null) {
                            RemotingHelper.closeChannel(channel);
                        }
                        this.executor.submit(() -> notifyBrokerInActive(clusterName, brokerName, brokerEntry.getKey()));
                        log.warn("The broker channel expired, brokerInfo {}, expired time(ms) {}",
                            brokerEntry.getValue().toString(), timeoutMs);
                    }
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    private void notifyBrokerInActive(String clusterName, String brokerName, Long brokerId) {
        for (BrokerLifecycleListener listener : this.brokerLifecycleListeners) {
            listener.onBrokerInactive(new BrokerSetIdentity(clusterName, brokerName), brokerId);
        }
    }

    @Override
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {
        this.brokerLifecycleListeners.add(listener);
    }

    @Override
    public void onBrokerHeartbeat(String clusterName, String brokerName, String brokerAddr, Long brokerId,
        Long timeoutMillis, Channel channel, Integer epoch, Long maxOffset, Long confirmOffset,
        Integer electionPriority) {
        BrokerSetIdentity brokerSetIdentity = new BrokerSetIdentity(clusterName, brokerName);
        this.brokerLiveTable.putIfAbsent(brokerSetIdentity, new HashMap<>());
        Map<Long, BrokerLiveInfo> brokerLiveInfoMap = this.brokerLiveTable.get(brokerSetIdentity);
        BrokerLiveInfo prev = brokerLiveInfoMap.get(brokerId);
        int realEpoch = Optional.ofNullable(epoch).orElse(-1);
        long realBrokerId = Optional.ofNullable(brokerId).orElse(-1L);
        long realMaxOffset = Optional.ofNullable(maxOffset).orElse(-1L);
        long realConfirmOffset = Optional.ofNullable(confirmOffset).orElse(-1L);
        long realTimeoutMillis = Optional.ofNullable(timeoutMillis).orElse(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        int realElectionPriority = Optional.ofNullable(electionPriority).orElse(Integer.MAX_VALUE);
        if (null == prev) {
            brokerLiveInfoMap.put(realBrokerId,
                new BrokerLiveInfo(clusterName,
                    brokerName,
                    brokerAddr,
                    realBrokerId,
                    System.currentTimeMillis(),
                    realTimeoutMillis,
                    channel,
                    realEpoch,
                    realMaxOffset,
                    realElectionPriority));
            log.info("new broker registered, {}, brokerId:{}", brokerSetIdentity, realBrokerId);
        } else {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
            prev.setHeartbeatTimeoutMillis(realTimeoutMillis);
            prev.setElectionPriority(realElectionPriority);
            if (realEpoch > prev.getEpoch() || realEpoch == prev.getEpoch() && realMaxOffset > prev.getMaxOffset()) {
                prev.setEpoch(realEpoch);
                prev.setMaxOffset(realMaxOffset);
                prev.setConfirmOffset(realConfirmOffset);
            }
        }

    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        this.brokerLiveTable.values().stream().flatMap(map -> map.values().stream()).filter(info -> info.getChannel() == channel).forEach(info -> {
            log.info("Channel {} inactive, broker {}, addr:{}, id:{}", info.getChannel(), info.getBrokerName(), info.getBrokerAddr(), info.getBrokerId());
            BrokerSetIdentity brokerSetIdentity = new BrokerSetIdentity(info.getClusterName(), info.getBrokerName());
            this.brokerLiveTable.computeIfPresent(brokerSetIdentity, (brokerSet, brokerLiveInfoMap) -> {
                brokerLiveInfoMap.remove(info.getBrokerId());
                return brokerLiveInfoMap;
            });
            this.executor.submit(() ->
                notifyBrokerInActive(info.getClusterName(), info.getBrokerName(), info.getBrokerId()));
        });
    }

    @Override
    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId) {
        return this.brokerLiveTable.getOrDefault(new BrokerSetIdentity(clusterName, brokerName), Collections.emptyMap()).get(brokerId);
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerName, Long brokerId) {
        final BrokerLiveInfo info = this.brokerLiveTable.getOrDefault(new BrokerSetIdentity(clusterName, brokerName), Collections.emptyMap()).get(brokerId);
        if (info != null) {
            long last = info.getLastUpdateTimestamp();
            long timeoutMillis = info.getHeartbeatTimeoutMillis();
            return UtilAll.computeElapsedTimeMilliseconds(last) <= timeoutMillis;
        }
        return false;
    }

    @Override
    public Map<BrokerSetIdentity, Integer/*brokers num*/> getActiveBrokersNum() {
        return this.brokerLiveTable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
    }

    @Override
    public Set<Long> getActiveBrokerIds(String clusterName, String brokerName) {
        return this.brokerLiveTable.getOrDefault(new BrokerSetIdentity(clusterName, brokerName), Collections.emptyMap()).entrySet().stream().filter(entry -> {
            long last = entry.getValue().getLastUpdateTimestamp();
            long timeoutMillis = entry.getValue().getHeartbeatTimeoutMillis();
            return UtilAll.computeElapsedTimeMilliseconds(last) <= timeoutMillis;
        }).map(Map.Entry::getKey).collect(Collectors.toSet());
    }

}
