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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.ControllerConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.controller.impl.DLedgerController;
import org.apache.rocketmq.controller.impl.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ControllerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private final ControllerConfig controllerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final BrokerHousekeepingService brokerHousekeepingService;
    private final Configuration configuration;
    private Controller controller;
    private BrokerHeartbeatManager heartbeatManager;
    private RemotingServer remotingServer;

    private ExecutorService controllerRequestExecutor;
    private BlockingQueue<Runnable> controllerRequestThreadPoolQueue;

    public ControllerManager(ControllerConfig controllerConfig, NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig) {
        this.controllerConfig = controllerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(
            log,
            this.controllerConfig, this.nettyServerConfig
        );
        this.configuration.setStorePathFromConfig(this.controllerConfig, "configStorePath");
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
        this.controller = new DLedgerController(this.controllerConfig, (cluster, brokerAddr) -> this.heartbeatManager.isBrokerActive(cluster, brokerAddr),
            this.nettyServerConfig, this.nettyClientConfig, this.brokerHousekeepingService);

        // Register broker inactive listener
        this.heartbeatManager.addBrokerLifecycleListener(new BrokerHeartbeatManager.BrokerLifecycleListener() {
            @Override
            public void onBrokerInactive(String brokerName, String brokerAddress, long brokerId) {
                if (brokerId == MixAll.MASTER_ID) {
                    if (controller.isLeaderState()) {
                        final CompletableFuture<RemotingCommand> future = controller.electMaster(new ElectMasterRequestHeader(brokerName));
                        try {
                            final RemotingCommand response = future.get(5, TimeUnit.SECONDS);
                            final ElectMasterResponseHeader responseHeader = (ElectMasterResponseHeader) response.readCustomHeader();
                            if (responseHeader != null) {
                                log.info("Broker{}'s master shutdown, elect a new master:{}", brokerName, responseHeader);
                            }
                        } catch (Exception ignored) {
                        }
                    } else {
                        log.info("Broker{}' master shutdown", brokerName);
                    }
                }
            }
        });
        return true;
    }

    public void registerProcessor() {
        final ControllerRequestProcessor controllerRequestProcessor = new ControllerRequestProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_ELECT_MASTER, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_REGISTER_BROKER, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_GET_REPLICA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_GET_METADATA_INFO, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, controllerRequestProcessor, this.controllerRequestExecutor);
        this.remotingServer.registerProcessor(RequestCode.BROKER_HEARTBEAT, controllerRequestProcessor, this.controllerRequestExecutor);
    }

    public void start() {
        this.remotingServer.start();
        this.heartbeatManager.start();
        this.remotingServer = this.controller.getRemotingServer();
        registerProcessor();
        this.controller.startup();
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.heartbeatManager.shutdown();
        this.controller.shutdown();
        this.controllerRequestExecutor.shutdown();
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

    public RemotingServer getRemotingServer() {
        return remotingServer;
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
