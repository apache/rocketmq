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
package org.apache.rocketmq.controller.impl.heartbeat;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class DefaultBrokerHeartbeatManager implements BrokerHeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private ScheduledExecutorService scheduledService;
    private ExecutorService executor;

    private final ControllerConfig controllerConfig;
    private final Map<BrokerIdentityInfo/* brokerIdentity*/, BrokerLiveInfo> brokerLiveTable;
    private final List<BrokerLifecycleListener> brokerLifecycleListeners;
    private final ConcurrentMap<Channel, Object> closingChannels = new ConcurrentHashMap<>();
    private final ReadWriteLock channelLifecycleLock = new ReentrantReadWriteLock();

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
        this.closingChannels.clear();
    }

    @Override
    public void initialize() {
        this.scheduledService = ThreadUtils.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_scheduledService_"));
        this.executor = Executors.newFixedThreadPool(2, new ThreadFactoryImpl("DefaultBrokerHeartbeatManager_executorService_"));
    }

    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            for (Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                final BrokerIdentityInfo identity = entry.getKey();
                final BrokerLiveInfo info = entry.getValue();
                long last = info.getLastUpdateTimestamp();
                long timeoutMillis = info.getHeartbeatTimeoutMillis();
                if (System.currentTimeMillis() - last <= timeoutMillis) {
                    continue;
                }
                final AtomicReference<BrokerLiveInfo> expiredRef = new AtomicReference<>();
                final AtomicReference<Channel> expiredChannelRef = new AtomicReference<>();
                final AtomicLong expiredTimeoutRef = new AtomicLong();
                this.brokerLiveTable.computeIfPresent(identity, (key, current) -> {
                    long currentLast = current.getLastUpdateTimestamp();
                    long currentTimeout = current.getHeartbeatTimeoutMillis();
                    if (System.currentTimeMillis() - currentLast > currentTimeout) {
                        expiredRef.set(current);
                        expiredChannelRef.set(current.getChannel());
                        expiredTimeoutRef.set(currentTimeout);
                        return null;
                    }
                    return current;
                });
                final BrokerLiveInfo expired = expiredRef.get();
                if (expired != null) {
                    final Channel channel = expiredChannelRef.get();
                    final String brokerName = expired.getBrokerName();
                    final long brokerId = expired.getBrokerId();
                    closeChannelIfUnused(channel);
                    this.executor.submit(() ->
                        notifyBrokerInActive(identity.getClusterName(), brokerName, brokerId));
                    log.warn("The broker channel {} expired, brokerInfo {}, expired {}ms",
                        channel, identity, expiredTimeoutRef.get());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    private void closeChannelIfUnused(Channel channel) {
        if (channel == null || isChannelInUse(channel)) {
            return;
        }
        this.channelLifecycleLock.writeLock().lock();
        try {
            if (!isChannelInUse(channel)) {
                final Object closeToken = new Object();
                this.closingChannels.put(channel, closeToken);
                final ChannelFuture closeFuture = channel.closeFuture();
                if (closeFuture != null) {
                    closeFuture.addListener(future -> this.closingChannels.remove(channel, closeToken));
                }
                boolean closeListenerAttached = false;
                try {
                    final ChannelFuture opFuture = closeChannel(channel);
                    if (opFuture != null) {
                        closeListenerAttached = true;
                        opFuture.addListener((ChannelFutureListener) future -> {
                            if (!future.isSuccess() || closeFuture == null || !channel.isActive()) {
                                this.closingChannels.remove(channel, closeToken);
                            }
                        });
                    }
                } finally {
                    if (!closeListenerAttached || !channel.isActive()) {
                        this.closingChannels.remove(channel, closeToken);
                    }
                }
            }
        } finally {
            this.channelLifecycleLock.writeLock().unlock();
        }
    }

    private ChannelFuture closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        final ChannelFuture closeFuture = channel.close();
        if (closeFuture != null && addrRemote != null && !addrRemote.isEmpty()) {
            closeFuture.addListener((ChannelFutureListener) future ->
                log.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                    future.isSuccess()));
        }
        return closeFuture;
    }

    private boolean isChannelInUse(Channel channel) {
        for (BrokerLiveInfo liveInfo : this.brokerLiveTable.values()) {
            if (liveInfo.getChannel() == channel) {
                return true;
            }
        }
        return false;
    }

    private boolean isChannelClosingOrInactive(Channel channel) {
        return this.closingChannels.containsKey(channel) || !channel.isActive();
    }

    private void notifyBrokerInActive(String clusterName, String brokerName, Long brokerId) {
        for (BrokerLifecycleListener listener : this.brokerLifecycleListeners) {
            listener.onBrokerInactive(clusterName, brokerName, brokerId);
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
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo(clusterName, brokerName, brokerId);
        int realEpoch = Optional.ofNullable(epoch).orElse(-1);
        long realBrokerId = Optional.ofNullable(brokerId).orElse(-1L);
        long realMaxOffset = Optional.ofNullable(maxOffset).orElse(-1L);
        long realConfirmOffset = Optional.ofNullable(confirmOffset).orElse(-1L);
        long realTimeoutMillis = Optional.ofNullable(timeoutMillis).orElse(DEFAULT_BROKER_CHANNEL_EXPIRED_TIME);
        int realElectionPriority = Optional.ofNullable(electionPriority).orElse(Integer.MAX_VALUE);
        final AtomicBoolean acceptedChannel = new AtomicBoolean(false);
        if (channel != null) {
            this.channelLifecycleLock.readLock().lock();
        }
        try {
            // Serialize creation, timestamp refresh, and channel rebinding per broker identity, while
            // rejecting already-closed or expiry-condemned channels so late heartbeats cannot resurrect
            // or rebind stale connections. Holding readLock coordinates non-blocking heartbeats with
            // writeLock in closeChannelIfUnused() so a shared BrokerContainer channel cannot be closed
            // concurrently with a sibling broker rebinding onto it.
            this.brokerLiveTable.compute(brokerIdentityInfo, (key, prev) -> {
                if (channel != null && isChannelClosingOrInactive(channel)) {
                    log.warn("Ignore heartbeat from inactive channel {}, brokerInfo:{}", channel, brokerIdentityInfo);
                    return prev;
                }
                if (channel != null) {
                    acceptedChannel.set(true);
                }
                long now = System.currentTimeMillis();
                if (prev == null) {
                    log.info("new broker registered, {}, brokerId:{}", brokerIdentityInfo, realBrokerId);
                    return new BrokerLiveInfo(brokerName,
                        brokerAddr,
                        realBrokerId,
                        now,
                        realTimeoutMillis,
                        channel,
                        realEpoch,
                        realMaxOffset,
                        realElectionPriority);
                }
                prev.setLastUpdateTimestamp(now);
                prev.setHeartbeatTimeoutMillis(realTimeoutMillis);
                prev.setElectionPriority(realElectionPriority);
                if (channel != null && channel != prev.getChannel()) {
                    prev.setChannel(channel);
                }
                if (realEpoch > prev.getEpoch() || realEpoch == prev.getEpoch() && realMaxOffset > prev.getMaxOffset()) {
                    prev.setEpoch(realEpoch);
                    prev.setMaxOffset(realMaxOffset);
                    prev.setConfirmOffset(realConfirmOffset);
                }
                return prev;
            });
        } finally {
            if (channel != null) {
                this.channelLifecycleLock.readLock().unlock();
            }
        }
        if (acceptedChannel.get() && isChannelClosingOrInactive(channel)) {
            evictBrokerIfChannelMatches(brokerIdentityInfo, channel);
        }
    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        if (channel == null) {
            return;
        }
        for (BrokerIdentityInfo brokerIdentity : this.brokerLiveTable.keySet()) {
            evictBrokerIfChannelMatches(brokerIdentity, channel);
        }
    }

    private boolean evictBrokerIfChannelMatches(BrokerIdentityInfo brokerIdentity, Channel channel) {
        final AtomicReference<BrokerLiveInfo> closedRef = new AtomicReference<>();
        this.brokerLiveTable.computeIfPresent(brokerIdentity, (key, live) -> {
            if (live.getChannel() != channel) {
                return live;
            }
            closedRef.set(live);
            return null;
        });
        final BrokerLiveInfo closed = closedRef.get();
        if (closed != null) {
            final String brokerName = closed.getBrokerName();
            final String brokerAddr = closed.getBrokerAddr();
            final long brokerId = closed.getBrokerId();
            log.info("Channel {} inactive, broker {}, addr:{}, id:{}", channel, brokerName, brokerAddr, brokerId);
            this.executor.submit(() -> notifyBrokerInActive(brokerIdentity.getClusterName(),
                brokerName, brokerId));
            return true;
        }
        return false;
    }

    @Override
    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId) {
        return this.brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerName, Long brokerId) {
        final BrokerLiveInfo info = this.brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
        if (info != null) {
            long last = info.getLastUpdateTimestamp();
            long timeoutMillis = info.getHeartbeatTimeoutMillis();
            return (last + timeoutMillis) >= System.currentTimeMillis();
        }
        return false;
    }

    @Override
    public Map<String, Map<String, Integer>> getActiveBrokersNum() {
        Map<String, Map<String, Integer>> map = new HashMap<>();
        this.brokerLiveTable.keySet().stream()
            .filter(brokerIdentity -> this.isBrokerActive(brokerIdentity.getClusterName(), brokerIdentity.getBrokerName(), brokerIdentity.getBrokerId()))
            .forEach(id -> {
                map.computeIfAbsent(id.getClusterName(), k -> new HashMap<>());
                map.get(id.getClusterName()).compute(id.getBrokerName(), (broker, num) ->
                    num == null ? 1 : num + 1
                );
            });
        return map;
    }
}
