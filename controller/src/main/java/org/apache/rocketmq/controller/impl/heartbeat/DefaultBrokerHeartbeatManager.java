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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    // Reverse index from channel to active broker identities or a unique per-close closing token.
    // Lock order is strictly brokerLiveTable[identity] -> channelBrokerTable[channel], never nested across channels.
    private final ConcurrentMap<Channel, ChannelState> channelBrokerTable = new ConcurrentHashMap<>();

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
        this.channelBrokerTable.clear();
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
                final AtomicReference<ChannelState> closingTokenRef = new AtomicReference<>();
                this.brokerLiveTable.computeIfPresent(identity, (key, current) -> {
                    long currentLast = current.getLastUpdateTimestamp();
                    long currentTimeout = current.getHeartbeatTimeoutMillis();
                    if (System.currentTimeMillis() - currentLast > currentTimeout) {
                        expiredRef.set(current);
                        final Channel currentChannel = current.getChannel();
                        expiredChannelRef.set(currentChannel);
                        expiredTimeoutRef.set(currentTimeout);
                        closingTokenRef.set(unbindChannel(currentChannel, key, true));
                        return null;
                    }
                    return current;
                });
                final BrokerLiveInfo expired = expiredRef.get();
                if (expired != null) {
                    final Channel channel = expiredChannelRef.get();
                    final String brokerName = expired.getBrokerName();
                    final long brokerId = expired.getBrokerId();
                    closeExpiredChannel(channel, closingTokenRef.get());
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

    private boolean bindChannel(Channel channel, BrokerIdentityInfo identity) {
        final AtomicBoolean bound = new AtomicBoolean(false);
        this.channelBrokerTable.compute(channel, (ch, state) -> {
            if (state != null && state.isClosing() || !ch.isActive()) {
                return state;
            }
            final ChannelState next = state == null ? ChannelState.active() : state;
            next.add(identity);
            bound.set(true);
            return next;
        });
        return bound.get();
    }

    private ChannelState unbindChannel(Channel channel, BrokerIdentityInfo identity, boolean closeIfLast) {
        if (channel == null) {
            return null;
        }
        final AtomicReference<ChannelState> closingTokenRef = new AtomicReference<>();
        this.channelBrokerTable.computeIfPresent(channel, (ch, state) -> {
            if (state.isClosing()) {
                return state;
            }
            state.remove(identity);
            if (state.isEmpty()) {
                if (closeIfLast) {
                    final ChannelState token = ChannelState.closing();
                    closingTokenRef.set(token);
                    return token;
                }
                return null;
            }
            return state;
        });
        return closingTokenRef.get();
    }

    private void closeExpiredChannel(Channel channel, ChannelState closeToken) {
        if (channel == null || closeToken == null) {
            return;
        }
        boolean closeListenerAttached = false;
        try {
            final ChannelFuture opFuture = closeChannel(channel);
            if (opFuture != null) {
                closeListenerAttached = true;
                opFuture.addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess() || !channel.isActive()) {
                        this.channelBrokerTable.remove(channel, closeToken);
                        return;
                    }
                    final ChannelFuture closeFuture = channel.closeFuture();
                    if (closeFuture != null) {
                        closeFuture.addListener(cf -> this.channelBrokerTable.remove(channel, closeToken));
                        if (closeFuture.isDone() || !channel.isActive()) {
                            this.channelBrokerTable.remove(channel, closeToken);
                        }
                    } else {
                        this.channelBrokerTable.remove(channel, closeToken);
                    }
                });
            }
        } finally {
            if (!closeListenerAttached) {
                this.channelBrokerTable.remove(channel, closeToken);
            }
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

    private boolean isChannelClosingOrInactive(Channel channel) {
        final ChannelState state = this.channelBrokerTable.get(channel);
        return state != null && state.isClosing() || !channel.isActive();
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
        // Serialize creation, timestamp refresh, and channel rebinding per broker identity, while
        // rejecting already-closed or expiry-closing channels via the per-channel reverse index.
        this.brokerLiveTable.compute(brokerIdentityInfo, (key, prev) -> {
            final Channel oldChannel = prev == null ? null : prev.getChannel();
            if (channel != null) {
                if (oldChannel == channel) {
                    if (isChannelClosingOrInactive(channel)) {
                        log.warn("Ignore heartbeat from inactive channel {}, brokerInfo:{}", channel, brokerIdentityInfo);
                        return prev;
                    }
                } else if (!bindChannel(channel, key)) {
                    log.warn("Ignore heartbeat from inactive channel {}, brokerInfo:{}", channel, brokerIdentityInfo);
                    return prev;
                }
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
            if (channel != null && channel != oldChannel) {
                prev.setChannel(channel);
                unbindChannel(oldChannel, key, false);
            }
            if (realEpoch > prev.getEpoch() || realEpoch == prev.getEpoch() && realMaxOffset > prev.getMaxOffset()) {
                prev.setEpoch(realEpoch);
                prev.setMaxOffset(realMaxOffset);
                prev.setConfirmOffset(realConfirmOffset);
            }
            return prev;
        });
        if (channel != null && acceptedChannel.get() && isChannelClosingOrInactive(channel)) {
            evictBrokerIfChannelMatches(brokerIdentityInfo, channel);
        }
    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        if (channel == null) {
            return;
        }
        final ChannelState state = this.channelBrokerTable.get(channel);
        if (state == null || state.isClosing()) {
            return;
        }
        for (BrokerIdentityInfo brokerIdentity : state.snapshot()) {
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
            unbindChannel(channel, key, false);
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

    private static final class ChannelState {
        private final Set<BrokerIdentityInfo> brokerIdentities;

        private ChannelState(Set<BrokerIdentityInfo> brokerIdentities) {
            this.brokerIdentities = brokerIdentities;
        }

        private static ChannelState active() {
            return new ChannelState(ConcurrentHashMap.newKeySet());
        }

        private static ChannelState closing() {
            return new ChannelState(null);
        }

        private boolean isClosing() {
            return this.brokerIdentities == null;
        }

        private void add(BrokerIdentityInfo identity) {
            this.brokerIdentities.add(identity);
        }

        private void remove(BrokerIdentityInfo identity) {
            this.brokerIdentities.remove(identity);
        }

        private boolean isEmpty() {
            return this.brokerIdentities.isEmpty();
        }

        private List<BrokerIdentityInfo> snapshot() {
            return new ArrayList<>(this.brokerIdentities);
        }
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
