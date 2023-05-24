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
package org.apache.rocketmq.controller.impl;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BrokerAddrInfo;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.BrokerLiveInfo;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class DefaultBrokerHeartbeatManager implements BrokerHeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private static final long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 10;
    private final ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_scheduledService_"));
    private final ExecutorService executor = Executors.newFixedThreadPool(2, new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_executorService_"));

    private final ControllerConfig controllerConfig;
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
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

    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            final Iterator<Map.Entry<BrokerAddrInfo, BrokerLiveInfo>> iterator = this.brokerLiveTable.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<BrokerAddrInfo, BrokerLiveInfo> next = iterator.next();
                long last = next.getValue().getLastUpdateTimestamp();
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    final Channel channel = next.getValue().getChannel();
                    iterator.remove();
                    if (channel != null) {
                        RemotingHelper.closeChannel(channel);
                    }
                    this.executor.submit(() ->
                        notifyBrokerInActive(next.getKey().getClusterName(), next.getValue().getBrokerName(), next.getKey().getBrokerAddr(), next.getValue().getBrokerId()));
                    log.warn("The broker channel {} expired, brokerInfo {}, expired {}ms", next.getValue().getChannel(), next.getKey(), timeoutMillis);
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    private void notifyBrokerInActive(String clusterName, String brokerName, String brokerAddr, Long brokerId) {
        for (BrokerLifecycleListener listener : this.brokerLifecycleListeners) {
            listener.onBrokerInactive(clusterName, brokerName, brokerAddr, brokerId);
        }
    }

    @Override
    public void addBrokerLifecycleListener(BrokerLifecycleListener listener) {
        this.brokerLifecycleListeners.add(listener);
    }

    @Override public void onBrokerHeartbeat(String clusterName, String brokerName, String brokerAddr, Long brokerId,
        Long timeoutMillis, Channel channel, Integer epoch, Long maxOffset, Long confirmOffset, Integer electionPriority) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        int realEpoch = Optional.ofNullable(epoch).orElse(-1);
        long realBrokerId = Optional.ofNullable(brokerId).orElse(-1L);
        long realMaxOffset = Optional.ofNullable(maxOffset).orElse(-1L);
        long realConfirmOffset = Optional.ofNullable(confirmOffset).orElse(-1L);
        long realTimeoutMillis = Optional.ofNullable(timeoutMillis).orElse(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        int realElectionPriority = Optional.ofNullable(electionPriority).orElse(Integer.MAX_VALUE);
        if (null == prev) {
            this.brokerLiveTable.put(addrInfo,
                new BrokerLiveInfo(brokerName,
                    brokerAddr,
                    realBrokerId,
                    System.currentTimeMillis(),
                    realTimeoutMillis,
                    channel,
                    realEpoch,
                    realMaxOffset,
                    realElectionPriority));
            log.info("new broker registered, {}, brokerId:{}", addrInfo, realBrokerId);
        } else {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
            prev.setHeartbeatTimeoutMillis(realTimeoutMillis);
            prev.setElectionPriority(realElectionPriority);
            prev.setBrokerId(realBrokerId);
            if (realEpoch > prev.getEpoch() || realEpoch == prev.getEpoch() && realMaxOffset > prev.getMaxOffset()) {
                prev.setEpoch(realEpoch);
                prev.setMaxOffset(realMaxOffset);
                prev.setConfirmOffset(realConfirmOffset);
            }
        }

    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        BrokerAddrInfo addrInfo = null;
        for (Map.Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
            if (entry.getValue().getChannel() == channel) {
                log.info("Channel {} inactive, broker {}, addr:{}, id:{}", entry.getValue().getChannel(), entry.getValue().getBrokerName(), entry.getKey().getBrokerAddr(), entry.getValue().getBrokerId());
                addrInfo = entry.getKey();
                this.executor.submit(() ->
                    notifyBrokerInActive(entry.getKey().getClusterName(), entry.getValue().getBrokerName(), entry.getKey().getBrokerAddr(), entry.getValue().getBrokerId()));
                break;
            }
        }
        if (addrInfo != null) {
            this.brokerLiveTable.remove(addrInfo);
        }
    }

    @Override
    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerAddr) {
        return this.brokerLiveTable.get(new BrokerAddrInfo(clusterName, brokerAddr));
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerAddr) {
        final BrokerLiveInfo info = this.brokerLiveTable.get(new BrokerAddrInfo(clusterName, brokerAddr));
        if (info != null) {
            long last = info.getLastUpdateTimestamp();
            long timeoutMillis = info.getHeartbeatTimeoutMillis();
            return (last + timeoutMillis) >= System.currentTimeMillis();
        }
        return false;
    }

}
