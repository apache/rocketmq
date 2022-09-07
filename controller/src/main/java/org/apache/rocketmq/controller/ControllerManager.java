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
package org.apache.rocketmq.controller;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.common.protocol.header.NotifyBrokerRoleChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ControllerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final ControllerConfig controllerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final BrokerHousekeepingService brokerHousekeepingService;
    private final Configuration configuration;
    private final RemotingClient remotingClient;
    private Controller controller;
    private BrokerHeartbeatManager heartbeatManager;
    private ExecutorService controllerRequestExecutor;
    private BlockingQueue<Runnable> controllerRequestThreadPoolQueue;

    public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig,
                             NettyClientConfig nettyClientConfig) {
        this.controllerConfig = controllerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log, this.controllerConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.controllerConfig, "configStorePath");
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }

    public boolean initialize() {
        this.controllerRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.controllerConfig.getControllerRequestThreadPoolQueueCapacity());
        this.controllerRequestExecutor = new ThreadPoolExecutor(
                this.controllerConfig.getControllerThreadPoolNums(),
                this.controllerConfig.getControllerThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.controllerRequestThreadPoolQueue,
                new ThreadFactoryImpl("ControllerRequestExecutorThread_")) {
            @Override
            protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
                return new FutureTaskExt<T>(runnable, value);
            }
        };
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(this.controllerConfig);
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerPeers())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerPeers of ControllerConfig is null or empty");
        }
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerSelfId())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerSelfId of ControllerConfig is null or empty");
        }
        this.controller = new DLedgerController(this.controllerConfig, this.heartbeatManager::isBrokerActive,
                this.nettyServerConfig, this.nettyClientConfig, this.brokerHousekeepingService,
                new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo));

        // Register broker inactive listener
        this.heartbeatManager.addBrokerLifecycleListener(this::onBrokerInactive);
        registerProcessor();
        return true;
    }

    /**
     * When the heartbeatManager detects the "Broker is not active",
     * we call this method to elect a master and do something else.
     * @param clusterName The cluster name of this inactive broker
     * @param brokerName The inactive broker name
     * @param brokerAddress The inactive broker address(ip)
     * @param brokerId The inactive broker id
     */
    private void onBrokerInactive(String clusterName, String brokerName, String brokerAddress, long brokerId) {
        if (brokerId == MixAll.MASTER_ID) {
            if (controller.isLeaderState()) {
                final CompletableFuture<RemotingCommand> future = controller.electMaster(new ElectMasterRequestHeader(brokerName));
                try {
                    final RemotingCommand response = future.get(5, TimeUnit.SECONDS);
                    final ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.readCustomHeader();
                    if (responseHeader != null) {
                        log.info("Broker {}'s master {} shutdown, elect a new master done, result:{}", brokerName, brokerAddress, responseHeader);
                        if (StringUtils.isNotEmpty(responseHeader.getNewMasterAddress())) {
                            heartbeatManager.changeBrokerMetadata(clusterName, responseHeader.getNewMasterAddress(), MixAll.MASTER_ID);
                        }
                        if (controllerConfig.isNotifyBrokerRoleChanged()) {
                            notifyBrokerRoleChanged(responseHeader, clusterName);
                        }
                    }
                } catch (Exception ignored) {
                }
            } else {
                log.info("Broker{}' master shutdown", brokerName);
            }
        }
    }

    /**
     * Notify master and all slaves for a broker that the master role changed.
     */
    public void notifyBrokerRoleChanged(final ElectMasterResponseHeader electMasterResult, final String clusterName) {
        final BrokerMemberGroup memberGroup = electMasterResult.getBrokerMemberGroup();
        if (memberGroup != null) {
            // First, inform the master
            final String master = electMasterResult.getNewMasterAddress();
            if (StringUtils.isNoneEmpty(master) && this.heartbeatManager.isBrokerActive(clusterName, master)) {
                doNotifyBrokerRoleChanged(master, MixAll.MASTER_ID, electMasterResult);
            }

            // Then, inform all slaves
            final Map<Long, String> brokerIdAddrs = memberGroup.getBrokerAddrs();
            for (Map.Entry<Long, String> broker : brokerIdAddrs.entrySet()) {
                if (!broker.getValue().equals(master) && this.heartbeatManager.isBrokerActive(clusterName, broker.getValue())) {
                    doNotifyBrokerRoleChanged(broker.getValue(), broker.getKey(), electMasterResult);
                }
            }

        }
    }

    public void doNotifyBrokerRoleChanged(final String brokerAddr, final Long brokerId,
                                          final ElectMasterResponseHeader responseHeader) {
        if (StringUtils.isNoneEmpty(brokerAddr)) {
            log.info("Try notify broker {} with id {} that role changed, responseHeader:{}", brokerAddr, brokerId, responseHeader);
            final NotifyBrokerRoleChangedRequestHeader requestHeader = new NotifyBrokerRoleChangedRequestHeader(responseHeader.getNewMasterAddress(),
                    responseHeader.getMasterEpoch(), responseHeader.getSyncStateSetEpoch(), brokerId);
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_BROKER_ROLE_CHANGED, requestHeader);
            try {
                this.remotingClient.invokeOneway(brokerAddr, request, 3000);
            } catch (final Exception e) {
                log.error("Failed to notify broker {} with id {} that role changed", brokerAddr, brokerId, e);
            }
        }
    }

    public void registerProcessor() {
        final ControllerRequestProcessor controllerRequestProcessor = new ControllerRequestProcessor(this);
        final RemotingServer controllerRemotingServer = this.controller.getRemotingServer();
        assert controllerRemotingServer != null;
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_ELECT_MASTER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_REGISTER_BROKER, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_REPLICA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_METADATA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.BROKER_HEARTBEAT, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.UPDATE_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.GET_CONTROLLER_CONFIG, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CLEAN_BROKER_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
    }

    public void start() {
        this.heartbeatManager.start();
        this.controller.startup();
        this.remotingClient.start();
    }

    public void shutdown() {
        this.heartbeatManager.shutdown();
        this.controllerRequestExecutor.shutdown();
        this.controller.shutdown();
        this.remotingClient.shutdown();
    }

    public BrokerHeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public ControllerConfig getControllerConfig() {
        return controllerConfig;
    }

    public Controller getController() {
        return controller;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BrokerHousekeepingService getBrokerHousekeepingService() {
        return brokerHousekeepingService;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
