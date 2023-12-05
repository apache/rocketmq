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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.Channel;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.helper.BrokerLifecycleListener;
import org.apache.rocketmq.controller.impl.JRaftController;
import org.apache.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import org.apache.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import org.apache.rocketmq.controller.impl.task.GetBrokerLiveInfoResponse;
import org.apache.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RaftBrokerHeartBeatManager implements BrokerHeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);
    private JRaftController controller;
    private final List<BrokerLifecycleListener> brokerLifecycleListeners = new ArrayList<>();
    private final ScheduledExecutorService scheduledService;
    private final ExecutorService executor;
    private final ControllerConfig controllerConfig;

    private final Map<Channel, BrokerIdentityInfo> brokerChannelIdentityInfoMap = new HashMap<>();

    public RaftBrokerHeartBeatManager(ControllerConfig controllerConfig) {
        this.scheduledService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RaftBrokerHeartbeatManager_scheduledService_"));
        this.executor = Executors.newFixedThreadPool(2, new ThreadFactoryImpl("RaftBrokerHeartbeatManager_executorService_"));
        this.controllerConfig = controllerConfig;
    }

    public void setController(JRaftController controller) {
        this.controller = controller;
    }

    @Override
    public void initialize() {

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
    public void registerBrokerLifecycleListener(BrokerLifecycleListener listener) {
        brokerLifecycleListeners.add(listener);
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
        BrokerLiveInfo liveInfo = new BrokerLiveInfo(brokerName,
            brokerAddr,
            realBrokerId,
            System.currentTimeMillis(),
            realTimeoutMillis,
            null,
            realEpoch,
            realMaxOffset,
            realElectionPriority,
            realConfirmOffset);
        log.info("broker {} heart beat", brokerIdentityInfo);
        RaftBrokerHeartBeatEventRequest requestHeader = new RaftBrokerHeartBeatEventRequest(brokerIdentityInfo, liveInfo);
        CompletableFuture<RemotingCommand> future = controller.onBrokerHeartBeat(requestHeader);
        try {
            RemotingCommand remotingCommand = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (remotingCommand.getCode() != ResponseCode.SUCCESS && remotingCommand.getCode() != ResponseCode.CONTROLLER_NOT_LEADER) {
                throw new RuntimeException("on broker heartbeat return invalid code, code: " + remotingCommand.getCode());
            }
        } catch (ExecutionException | InterruptedException | TimeoutException | RuntimeException e) {
            log.error("on broker heartbeat through raft failed", e);
        }
        brokerChannelIdentityInfoMap.put(channel, brokerIdentityInfo);
    }

    @Override
    public void onBrokerChannelClose(Channel channel) {
        BrokerIdentityInfo brokerIdentityInfo = brokerChannelIdentityInfoMap.get(channel);
        log.info("Channel {} inactive, broker identity info: {}", channel, brokerIdentityInfo);
        if (brokerIdentityInfo != null) {
            BrokerCloseChannelRequest requestHeader = new BrokerCloseChannelRequest(brokerIdentityInfo);
            CompletableFuture<RemotingCommand> future = controller.onBrokerCloseChannel(requestHeader);
            try {
                RemotingCommand remotingCommand = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (remotingCommand.getCode() != ResponseCode.SUCCESS) {
                    throw new RuntimeException("on broker close channel return invalid code, code: " + remotingCommand.getCode());
                }
                this.executor.submit(() -> notifyBrokerInActive(brokerIdentityInfo.getClusterName(), brokerIdentityInfo.getBrokerName(), brokerIdentityInfo.getBrokerId()));
                brokerChannelIdentityInfoMap.remove(channel);
            } catch (ExecutionException | InterruptedException | TimeoutException | RuntimeException e) {
                log.error("on broker close channel through raft failed", e);
            }
        }
    }

    /**
     * @param brokerIdentityInfo null means get broker live info of all brokers
     */
    private Map<BrokerIdentityInfo, BrokerLiveInfo> getBrokerLiveInfo(BrokerIdentityInfo brokerIdentityInfo) {
        GetBrokerLiveInfoRequest requestHeader;
        if (brokerIdentityInfo == null) {
            requestHeader = new GetBrokerLiveInfoRequest();
        } else {
            requestHeader = new GetBrokerLiveInfoRequest(brokerIdentityInfo);
        }
        CompletableFuture<RemotingCommand> future = controller.getBrokerLiveInfo(requestHeader);
        try {
            RemotingCommand remotingCommand = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (remotingCommand.getCode() != ResponseCode.SUCCESS) {
                throw new RuntimeException("get broker live info return invalid code, code: " + remotingCommand.getCode());
            }
            GetBrokerLiveInfoResponse getBrokerLiveInfoResponse = (GetBrokerLiveInfoResponse) remotingCommand.decodeCommandCustomHeader(GetBrokerLiveInfoResponse.class);
            return JSON.parseObject(remotingCommand.getBody(), new TypeReference<Map<BrokerIdentityInfo, BrokerLiveInfo>>() {
            }.getType());
        } catch (Throwable e) {
            log.error("get broker live info through raft failed", e);
        }
        return new HashMap<>();
    }

    private void scanNotActiveBroker() {
        if (!controller.isLeaderState()) {
            log.info("current node is not leader, skip scan not active broker");
            return;
        }
        log.info("start scan not active broker");
        CheckNotActiveBrokerRequest requestHeader = new CheckNotActiveBrokerRequest();
        CompletableFuture<RemotingCommand> future = this.controller.checkNotActiveBroker(requestHeader);
        try {
            RemotingCommand remotingCommand = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            if (remotingCommand.getCode() != ResponseCode.SUCCESS) {
                throw new RuntimeException("check not active broker return invalid code, code: " + remotingCommand.getCode());
            }
            List<BrokerIdentityInfo> notActiveAndNeedReElectBrokerIdentityInfoList = JSON.parseObject(remotingCommand.getBody(), new TypeReference<List<BrokerIdentityInfo>>() {
            }.getType());
            if (notActiveAndNeedReElectBrokerIdentityInfoList != null && !notActiveAndNeedReElectBrokerIdentityInfoList.isEmpty()) {
                notActiveAndNeedReElectBrokerIdentityInfoList.forEach(brokerIdentityInfo -> {
                    Iterator<Map.Entry<Channel, BrokerIdentityInfo>> iterator = brokerChannelIdentityInfoMap.entrySet().iterator();
                    Channel channel = null;
                    while (iterator.hasNext()) {
                        Map.Entry<Channel, BrokerIdentityInfo> entry = iterator.next();
                        if (entry.getValue().getBrokerId() == null) {
                            continue;
                        }
                        if (entry.getValue().equals(brokerIdentityInfo)) {
                            channel = entry.getKey();
                            RemotingHelper.closeChannel(entry.getKey());
                            iterator.remove();
                            break;
                        }
                    }
                    this.executor.submit(() -> notifyBrokerInActive(brokerIdentityInfo.getClusterName(), brokerIdentityInfo.getBrokerName(), brokerIdentityInfo.getBrokerId()));
                    log.warn("The broker channel {} expired, brokerInfo {}", channel, brokerIdentityInfo);
                });
            }
        } catch (Throwable e) {
            log.error("check not active broker through raft failed", e);
        }
    }

    @Override
    public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId) {
        log.info("get broker live info, clusterName: {}, brokerName: {}, brokerId: {}", clusterName, brokerName, brokerId);
        BrokerIdentityInfo brokerIdentityInfo = new BrokerIdentityInfo(clusterName, brokerName, brokerId);
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveInfoMap = getBrokerLiveInfo(brokerIdentityInfo);
        return brokerLiveInfoMap.get(brokerIdentityInfo);
    }

    @Override
    public boolean isBrokerActive(String clusterName, String brokerName, Long brokerId) {
        BrokerLiveInfo info = null;
        try {
            info = getBrokerLiveInfo(clusterName, brokerName, brokerId);
        } catch (RuntimeException e) {
            log.error("get broker live info failed", e);
            return false;
        }

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
        Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveInfoMap = getBrokerLiveInfo(null);
        brokerLiveInfoMap.keySet().stream()
            .filter(brokerIdentity -> this.isBrokerActive(brokerIdentity.getClusterName(), brokerIdentity.getBrokerName(), brokerIdentity.getBrokerId()))
            .forEach(id -> {
                map.computeIfAbsent(id.getClusterName(), k -> new HashMap<>());
                map.get(id.getClusterName()).compute(id.getBrokerName(), (broker, num) ->
                    num == null ? 0 : num + 1
                );
            });
        return map;
    }

    private void notifyBrokerInActive(String clusterName, String brokerName, Long brokerId) {
        log.info("Broker {}-{}-{} inactive", clusterName, brokerName, brokerId);
        for (BrokerLifecycleListener listener : this.brokerLifecycleListeners) {
            listener.onBrokerInactive(clusterName, brokerName, brokerId);
        }
    }
}
