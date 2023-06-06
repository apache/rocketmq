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
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;

import org.apache.rocketmq.controller.elect.impl.DefaultElectPolicy;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.controller.impl.heartbeat.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.metrics.ControllerMetricsManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.RoleChangeNotifyEntry;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.remoting.protocol.header.NotifyBrokerRoleChangedRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;

public class ControllerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

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

    private NotifyService notifyService;

    private ControllerMetricsManager controllerMetricsManager;

    public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig) {
        this.controllerConfig = controllerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(log, this.controllerConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.controllerConfig, "configStorePath");
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(this.controllerConfig);
        this.notifyService = new NotifyService();
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
        this.notifyService.initialize();
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerPeers())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerPeers of ControllerConfig is null or empty");
        }
        if (StringUtils.isEmpty(this.controllerConfig.getControllerDLegerSelfId())) {
            throw new IllegalArgumentException("Attribute value controllerDLegerSelfId of ControllerConfig is null or empty");
        }
        this.controller = new DLedgerController(this.controllerConfig, this.heartbeatManager::isBrokerActive,
            this.nettyServerConfig, this.nettyClientConfig, this.brokerHousekeepingService,
            new DefaultElectPolicy(this.heartbeatManager::isBrokerActive, this.heartbeatManager::getBrokerLiveInfo));

        // Initialize the basic resources
        this.heartbeatManager.initialize();

        // Register broker inactive listener
        this.heartbeatManager.registerBrokerLifecycleListener(this::onBrokerInactive);
        this.controller.registerBrokerLifecycleListener(this::onBrokerInactive);
        registerProcessor();
        this.controllerMetricsManager = ControllerMetricsManager.getInstance(this);
        return true;
    }

    /**
     * When the heartbeatManager detects the "Broker is not active", we call this method to elect a master and do
     * something else.
     *
     * @param clusterName The cluster name of this inactive broker
     * @param brokerName The inactive broker name
     * @param brokerId The inactive broker id, null means that the election forced to be triggered
     */
    private void onBrokerInactive(String clusterName, String brokerName, Long brokerId) {
        if (controller.isLeaderState()) {
            if (brokerId == null) {
                // Means that force triggering election for this broker-set
                triggerElectMaster(brokerName);
                return;
            }
            final CompletableFuture<RemotingCommand> replicaInfoFuture = controller.getReplicaInfo(new GetReplicaInfoRequestHeader(brokerName));
            replicaInfoFuture.whenCompleteAsync((replicaInfoResponse, err) -> {
                if (err != null || replicaInfoResponse == null) {
                    log.error("Failed to get replica-info for broker-set: {} when OnBrokerInactive", brokerName, err);
                    return;
                }
                final GetReplicaInfoResponseHeader replicaInfoResponseHeader = (GetReplicaInfoResponseHeader) replicaInfoResponse.readCustomHeader();
                // Not master broker offline
                if (!brokerId.equals(replicaInfoResponseHeader.getMasterBrokerId())) {
                    log.warn("The broker with brokerId: {} in broker-set: {} has been inactive", brokerId, brokerName);
                    return;
                }
                // Trigger election
                triggerElectMaster(brokerName);
            });
        } else {
            log.warn("The broker with brokerId: {} in broker-set: {} has been inactive", brokerId, brokerName);
        }
    }

    private void triggerElectMaster(String brokerName) {
        final CompletableFuture<RemotingCommand> electMasterFuture = controller.electMaster(ElectMasterRequestHeader.ofControllerTrigger(brokerName));
        electMasterFuture.whenCompleteAsync((electMasterResponse, err) -> {
            if (err != null || electMasterResponse == null) {
                log.error("Failed to trigger elect-master in broker-set: {}", brokerName, err);
                return;
            }
            if (electMasterResponse.getCode() == ResponseCode.SUCCESS) {
                log.info("Elect a new master in broker-set: {} done, result: {}", brokerName, electMasterResponse);
                if (controllerConfig.isNotifyBrokerRoleChanged()) {
                    notifyBrokerRoleChanged(RoleChangeNotifyEntry.convert(electMasterResponse));
                }
            }
        });
    }

    /**
     * Notify master and all slaves for a broker that the master role changed.
     */
    public void notifyBrokerRoleChanged(final RoleChangeNotifyEntry entry) {
        final BrokerMemberGroup memberGroup = entry.getBrokerMemberGroup();
        if (memberGroup != null) {
            final Long masterBrokerId = entry.getMasterBrokerId();
            String clusterName = memberGroup.getCluster();
            String brokerName = memberGroup.getBrokerName();
            if (masterBrokerId == null) {
                log.warn("Notify broker role change failed, because member group is not null but the new master brokerId is empty, entry:{}", entry);
                return;
            }
            // Inform all active brokers
            final Map<Long, String> brokerAddrs = memberGroup.getBrokerAddrs();
            brokerAddrs.entrySet().stream().filter(x -> this.heartbeatManager.isBrokerActive(clusterName, brokerName, x.getKey()))
                    .forEach(x -> this.notifyService.notifyBroker(x.getValue(), entry));
        }
    }

    /**
     * Notify broker that there are roles-changing in controller
     * @param brokerAddr target broker's address to notify
     * @param entry role change entry
     */
    public void doNotifyBrokerRoleChanged(final String brokerAddr, final RoleChangeNotifyEntry entry) {
        if (StringUtils.isNoneEmpty(brokerAddr)) {
            log.info("Try notify broker {} that role changed, RoleChangeNotifyEntry:{}", brokerAddr, entry);
            final NotifyBrokerRoleChangedRequestHeader requestHeader = new NotifyBrokerRoleChangedRequestHeader(entry.getMasterAddress(), entry.getMasterBrokerId(),
                    entry.getMasterEpoch(), entry.getSyncStateSetEpoch());
            final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_BROKER_ROLE_CHANGED, requestHeader);
            request.setBody(new SyncStateSet(entry.getSyncStateSet(), entry.getSyncStateSetEpoch()).encode());
            try {
                this.remotingClient.invokeOneway(brokerAddr, request, 3000);
            } catch (final Exception e) {
                log.error("Failed to notify broker {} that role changed", brokerAddr, e);
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
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID, controllerRequestProcessor, this.controllerRequestExecutor);
        controllerRemotingServer.registerProcessor(RequestCode.CONTROLLER_APPLY_BROKER_ID, controllerRequestProcessor, this.controllerRequestExecutor);
    }

    public void start() {
        this.heartbeatManager.start();
        this.controller.startup();
        this.remotingClient.start();
    }

    public void shutdown() {
        this.heartbeatManager.shutdown();
        this.controllerRequestExecutor.shutdown();
        this.notifyService.shutdown();
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

    class NotifyService {
        private ExecutorService executorService;

        private Map<String/*brokerAddress*/, NotifyTask/*currentNotifyTask*/> currentNotifyFutures;

        public NotifyService() {
        }

        public void initialize() {
            this.executorService = Executors.newFixedThreadPool(3, new ThreadFactoryImpl("ControllerManager_NotifyService_"));
            this.currentNotifyFutures = new ConcurrentHashMap<>();
        }

        public void notifyBroker(String brokerAddress, RoleChangeNotifyEntry entry) {
            int masterEpoch = entry.getMasterEpoch();
            NotifyTask oldTask = this.currentNotifyFutures.get(brokerAddress);
            if (oldTask != null && masterEpoch > oldTask.getMasterEpoch()) {
                // cancel current future
                Future oldFuture = oldTask.getFuture();
                if (oldFuture != null && !oldFuture.isDone()) {
                    oldFuture.cancel(true);
                }
            }
            final NotifyTask task = new NotifyTask(masterEpoch, null);
            Runnable runnable = () -> {
                doNotifyBrokerRoleChanged(brokerAddress, entry);
                this.currentNotifyFutures.remove(brokerAddress, task);
            };
            this.currentNotifyFutures.put(brokerAddress, task);
            Future<?> future = this.executorService.submit(runnable);
            task.setFuture(future);
        }

        public void shutdown() {
            if (!this.executorService.isShutdown()) {
                this.executorService.shutdownNow();
            }
        }

        class NotifyTask extends Pair<Integer/*epochMaster*/, Future/*notifyFuture*/> {
            public NotifyTask(Integer masterEpoch, Future future) {
                super(masterEpoch, future);
            }

            public Integer getMasterEpoch() {
                return super.getObject1();
            }

            public Future getFuture() {
                return super.getObject2();
            }

            public void setFuture(Future future) {
                super.setObject2(future);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(super.getObject1());
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (!(obj instanceof NotifyTask)) {
                    return false;
                }
                NotifyTask task = (NotifyTask) obj;
                return super.getObject1().equals(task.getObject1());
            }
        }
    }
}
