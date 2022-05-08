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

package org.apache.rocketmq.broker.hacontroller;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

/**
 * The manager of broker replicas, including:
 * 1.regularly syncing metadata from controllers, and changing broker roles and master if needed, both master and slave will start this timed task.
 * 2.regularly expanding and Shrinking syncStateSet, only master will start this timed task.
 */
public class ReplicasManager {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final ScheduledExecutorService syncMetadataService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ReplicasManager_SyncMetadata_"));
    private final ScheduledExecutorService checkSyncStateSetService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ReplicasManager_CheckSyncStateSet_"));
    private final BrokerController brokerController;
    private final DefaultMessageStore messageStore;
    private final AutoSwitchHAService haService;
    private final HaControllerProxy proxy;
    private final String clusterName;
    private final String brokerName;
    private final String localAddress;
    private final String localHaAddress;

    private ScheduledFuture<?> checkSyncStateSetTaskFuture;
    private ScheduledFuture<?> slaveSyncFuture;
    private Set<String> syncStateSet;
    private int syncStateSetEpoch = 0;
    private BrokerRole currentRole = BrokerRole.SLAVE;
    private Long brokerId = -1L;
    private String masterAddress = "";
    private int masterEpoch = 0;

    public ReplicasManager(final AutoSwitchHAService haService, final BrokerController brokerController,
        final DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
        this.haService = haService;
        final BrokerConfig brokerConfig = brokerController.getBrokerConfig();
        final String controllerPaths = brokerConfig.getMetaDataHosts();
        final String[] controllers = controllerPaths.split(";");
        assert controllers.length > 0;
        this.proxy = new HaControllerProxy(brokerController.getNettyClientConfig(), Arrays.asList(controllers));
        this.syncStateSet = new HashSet<>();
        this.clusterName = brokerConfig.getBrokerClusterName();
        this.brokerName = brokerConfig.getBrokerName();
        this.localAddress = brokerController.getBrokerAddr();
        this.localHaAddress = brokerController.getHAServerAddr();
    }

    public boolean start() {
        if (!this.proxy.start()) {
            LOGGER.error("Failed to start controller proxy");
            return false;
        }
        // First, register this broker to controller, get brokerId and masterAddress.
        try {
            final RegisterBrokerResponseHeader registerResponse = this.proxy.registerBroker(this.clusterName, this.brokerName, this.localAddress, this.localHaAddress);
            this.brokerId = registerResponse.getBrokerId();
            final String newMasterAddress = registerResponse.getMasterAddress();
            if (newMasterAddress != null && !newMasterAddress.isEmpty()) {
                if (newMasterAddress.equals(this.localAddress)) {
                    changeToMaster(registerResponse.getMasterEpoch(), registerResponse.getSyncStateSetEpoch());
                } else {
                    changeToSlave(newMasterAddress, registerResponse.getMasterEpoch(), registerResponse.getMasterHaAddress());
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Failed to register broker to controller", e);
            return false;
        }

        // Then, scheduling sync broker metadata.
        this.syncMetadataService.scheduleAtFixedRate(this::doSyncMetaData, 0, 2, TimeUnit.SECONDS);
        return true;
    }

    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to master brokerName:{}, new Epoch:{}", this.brokerController.getBrokerConfig().getBrokerName(), newMasterEpoch);

                // Change record
                this.currentRole = BrokerRole.SYNC_MASTER;
                this.brokerId = MixAll.MASTER_ID;
                this.masterAddress = this.localAddress;
                this.masterEpoch = newMasterEpoch;

                // Change sync state set
                final HashSet<String> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(this.localAddress);
                changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);
                startCheckSyncStateSetService();

                // Notify ha service
                this.haService.changeToMaster(newMasterEpoch);

                // Handle the slave synchronise
                handleSlaveSynchronize(BrokerRole.SYNC_MASTER);

                this.brokerController.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);

                // Last, register broker to name-srv
                try {
                    this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
                } catch (final Throwable ignored) {
                }

                LOGGER.info("Change broker {} to master, masterEpoch, syncStateSetEpoch:{}", this.localAddress, newMasterEpoch, syncStateSetEpoch);
            }
        }
    }

    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, final String masterHaAddress) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to slave brokerName={} brokerId={}", this.brokerName, this.brokerId);

                // Change record
                this.currentRole = BrokerRole.SLAVE;
                this.masterAddress = newMasterAddress;
                this.masterEpoch = newMasterEpoch;
                stopCheckSyncStateSetService();

                // Notify ha service
                this.haService.changeToSlave(newMasterAddress, masterHaAddress, newMasterEpoch);

                // Change config
                this.brokerController.getBrokerConfig().setBrokerId(this.brokerId); //TO DO check
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
                this.brokerController.changeSpecialServiceStatus(false);

                // Handle the slave synchronise
                handleSlaveSynchronize(BrokerRole.SLAVE);

                // Last, register broker to name-srv
                try {
                    this.brokerController.registerBrokerAll(true, true, this.brokerController.getBrokerConfig().isForceRegister());
                } catch (final Throwable ignored) {
                }
                LOGGER.info("Finish to change broker {} to slave, newMasterAddress:{}, newMasterEpoch:{}, newMasterHaAddress:{}", newMasterAddress, newMasterEpoch, masterHaAddress);
            }
        }
    }

    private void changeSyncStateSet(final Set<String> newSyncStateSet, final int newSyncStateSetEpoch) {
        synchronized (this) {
            if (newSyncStateSetEpoch > this.syncStateSetEpoch) {
                LOGGER.info("Sync state set changed from {} to {}", this.syncStateSet, newSyncStateSet);
                this.syncStateSetEpoch = newSyncStateSetEpoch;
                this.syncStateSet = new HashSet<>(newSyncStateSet);
                this.haService.setSyncStateSet(newSyncStateSet);
            }
        }
    }

    private void handleSlaveSynchronize(final BrokerRole role) {
        if (role == BrokerRole.SLAVE) {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(this.masterAddress);
            slaveSyncFuture = this.brokerController.getScheduledExecutorService().scheduleAtFixedRate(() -> {
                try {
                    brokerController.getSlaveSynchronize().syncAll();
                } catch (Throwable e) {
                    LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                }
            }, 1000 * 3, 1000 * 10, TimeUnit.MILLISECONDS);
        } else {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    private void doSyncMetaData() {
        try {
            final GetReplicaInfoResponseHeader info = this.proxy.getReplicaInfo(this.brokerName);
            final String newMasterAddress = info.getMasterAddress();
            final int newMasterEpoch = info.getMasterEpoch();
            synchronized (this) {
                // Check if master changed
                if (newMasterAddress != null && !newMasterAddress.isEmpty() && !this.masterAddress.equals(newMasterAddress) && newMasterEpoch > this.masterEpoch) {
                    if (newMasterAddress.equals(this.localAddress)) {
                        changeToMaster(newMasterEpoch, info.getSyncStateSetEpoch());
                    } else {
                        changeToSlave(newMasterAddress, newMasterEpoch, info.getMasterHaAddress());
                    }
                } else {
                    // Check if sync state set changed
                    if (this.currentRole == BrokerRole.SYNC_MASTER) {
                        changeSyncStateSet(info.getSyncStateSet(), info.getSyncStateSetEpoch());
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error happen when get broker {}'s metadata", this.brokerName, e);
        }
    }

    private void startCheckSyncStateSetService() {
        this.checkSyncStateSetTaskFuture = this.checkSyncStateSetService.scheduleAtFixedRate(() -> {
            final Set<String> newSyncStateSet = this.haService.getLatestSyncStateSet();
            newSyncStateSet.add(this.localAddress);
            synchronized (this) {
                if (this.syncStateSet != null) {
                    // Check if syncStateSet changed
                    if (this.syncStateSet.size() == newSyncStateSet.size() && this.syncStateSet.containsAll(newSyncStateSet)) {
                        return;
                    }
                }
            }
            try {
                final AlterSyncStateSetResponseHeader header = this.proxy.alterSyncStateSet(this.brokerName, this.masterAddress, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
                changeSyncStateSet(header.getNewSyncStateSet(), header.getNewSyncStateSetEpoch());
            } catch (final Exception e) {
                LOGGER.error("Error happen when change sync state set, broker:{}, masterAddress:{}, masterEpoch, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}",
                    this.brokerName, this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
            }
        }, 0, 4, TimeUnit.SECONDS);
    }

    private void stopCheckSyncStateSetService() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
            this.checkSyncStateSetTaskFuture = null;
        }
    }

    public void shutdown() {
        this.syncMetadataService.shutdown();
        this.checkSyncStateSetService.shutdown();
    }
}
