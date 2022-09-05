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

package org.apache.rocketmq.broker.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.RegisterBrokerToControllerResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

import static org.apache.rocketmq.common.protocol.ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST;

/**
 * The manager of broker replicas, including: 0.regularly syncing controller metadata, change controller leader address,
 * both master and slave will start this timed task. 1.regularly syncing metadata from controllers, and changing broker
 * roles and master if needed, both master and slave will start this timed task. 2.regularly expanding and Shrinking
 * syncStateSet, only master will start this timed task.
 */
public class ReplicasManager {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private static final int RETRY_INTERVAL_SECOND = 5;

    private final ScheduledExecutorService scheduledService;
    private final ExecutorService executorService;
    private final BrokerController brokerController;
    private final AutoSwitchHAService haService;
    private final BrokerConfig brokerConfig;
    private final String localAddress;
    private final BrokerOuterAPI brokerOuterAPI;
    private final List<String> controllerAddresses;

    private volatile String controllerLeaderAddress = "";
    private volatile State state = State.INITIAL;

    private ScheduledFuture<?> checkSyncStateSetTaskFuture;
    private ScheduledFuture<?> slaveSyncFuture;

    private Set<String> syncStateSet;
    private int syncStateSetEpoch = 0;
    private String masterAddress = "";
    private int masterEpoch = 0;
    private long lastSyncTimeMs = System.currentTimeMillis();

    public ReplicasManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
        this.scheduledService = Executors.newScheduledThreadPool(3, new ThreadFactoryImpl("ReplicasManager_ScheduledService_", brokerController.getBrokerIdentity()));
        this.executorService = Executors.newFixedThreadPool(3, new ThreadFactoryImpl("ReplicasManager_ExecutorService_", brokerController.getBrokerIdentity()));
        this.haService = (AutoSwitchHAService) brokerController.getMessageStore().getHaService();
        this.brokerConfig = brokerController.getBrokerConfig();
        final String controllerPaths = this.brokerConfig.getControllerAddr();
        final String[] controllers = controllerPaths.split(";");
        assert controllers.length > 0;
        this.controllerAddresses = new ArrayList<>(Arrays.asList(controllers));
        this.syncStateSet = new HashSet<>();
        this.localAddress = brokerController.getBrokerAddr();
        this.haService.setLocalAddress(this.localAddress);
    }

    public long getConfirmOffset() {
        return this.haService.getConfirmOffset();
    }

    enum State {
        INITIAL,
        FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE,
        RUNNING,
        SHUTDOWN,
    }

    public void start() {
        if (!startBasicService()) {
            LOGGER.error("Failed to start replicasManager");
            this.executorService.submit(() -> {
                int retryTimes = 0;
                do {
                    try {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL_SECOND);
                    } catch (InterruptedException ignored) {

                    }
                    retryTimes++;
                    LOGGER.warn("Failed to start replicasManager, retry times:{}, current state:{}, try it again", retryTimes, this.state);
                }
                while (!startBasicService());

                LOGGER.info("Start replicasManager success, retry times:{}", retryTimes);
            });
        }
    }

    private boolean startBasicService() {
        if (this.state == State.INITIAL) {
            if (schedulingSyncControllerMetadata()) {
                LOGGER.info("First time sync controller metadata success");
                this.state = State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE;
            } else {
                return false;
            }
        }

        if (this.state == State.FIRST_TIME_SYNC_CONTROLLER_METADATA_DONE) {
            if (registerBrokerToController()) {
                LOGGER.info("First time register broker success");
                this.state = State.RUNNING;
            } else {
                return false;
            }
        }

        schedulingSyncBrokerMetadata();

        // Register syncStateSet changed listener.
        this.haService.registerSyncStateSetChangedListener(this::doReportSyncStateSetChanged);
        return true;
    }

    public void shutdown() {
        this.state = State.SHUTDOWN;
        this.executorService.shutdown();
        this.scheduledService.shutdown();
    }

    public synchronized void changeBrokerRole(final String newMasterAddress, final int newMasterEpoch,
        final int syncStateSetEpoch, final long brokerId) {
        if (StringUtils.isNoneEmpty(newMasterAddress) && newMasterEpoch > this.masterEpoch) {
            if (StringUtils.equals(newMasterAddress, this.localAddress)) {
                changeToMaster(newMasterEpoch, syncStateSetEpoch);
            } else {
                changeToSlave(newMasterAddress, newMasterEpoch, brokerId);
            }
        }
    }

    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to master, brokerName:{}, replicas:{}, new Epoch:{}", this.brokerConfig.getBrokerName(), this.localAddress, newMasterEpoch);

                this.masterEpoch = newMasterEpoch;

                // Change sync state set
                final HashSet<String> newSyncStateSet = new HashSet<>();
                newSyncStateSet.add(this.localAddress);
                changeSyncStateSet(newSyncStateSet, syncStateSetEpoch);

                // Change record
                this.masterAddress = this.localAddress;

                // Handle the slave synchronise
                handleSlaveSynchronize(BrokerRole.SYNC_MASTER);

                // Notify ha service, change to master
                this.haService.changeToMaster(newMasterEpoch);

                this.brokerController.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);
                this.brokerController.changeSpecialServiceStatus(true);

                schedulingCheckSyncStateSet();

                this.executorService.submit(() -> {
                    // Register broker to name-srv
                    try {
                        this.brokerController.registerBrokerAll(true, false, this.brokerController.getBrokerConfig().isForceRegister());
                    } catch (final Throwable e) {
                        LOGGER.error("Error happen when register broker to name-srv, Failed to change broker to master", e);
                        return;
                    }
                    LOGGER.info("Change broker {} to master success, masterEpoch {}, syncStateSetEpoch:{}", this.localAddress, newMasterEpoch, syncStateSetEpoch);
                });
            }
        }
    }

    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, long brokerId) {
        synchronized (this) {
            if (newMasterEpoch > this.masterEpoch) {
                LOGGER.info("Begin to change to slave, brokerName={}, replicas={}, brokerId={}", this.brokerConfig.getBrokerName(), this.localAddress, brokerId);

                // Change record
                this.masterAddress = newMasterAddress;
                this.masterEpoch = newMasterEpoch;

                stopCheckSyncStateSet();

                // Change config
                this.brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
                this.brokerController.changeSpecialServiceStatus(false);
                this.brokerConfig.setBrokerId(brokerId);

                // Handle the slave synchronise
                handleSlaveSynchronize(BrokerRole.SLAVE);

                // Notify ha service, change to slave
                this.haService.changeToSlave(newMasterAddress, newMasterEpoch, this.brokerConfig.getBrokerId());

                this.executorService.submit(() -> {
                    // Register broker to name-srv
                    try {
                        this.brokerController.registerBrokerAll(true, false, this.brokerController.getBrokerConfig().isForceRegister());
                    } catch (final Throwable e) {
                        LOGGER.error("Error happen when register broker to name-srv, Failed to change broker to slave", e);
                        return;
                    }

                    LOGGER.info("Change broker {} to slave, newMasterAddress:{}, newMasterEpoch:{}", this.localAddress, newMasterAddress, newMasterEpoch);
                });
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
                    if (System.currentTimeMillis() - lastSyncTimeMs > 10 * 1000) {
                        brokerController.getSlaveSynchronize().syncAll();
                        lastSyncTimeMs = System.currentTimeMillis();
                    }
                    //timer checkpoint, latency-sensitive, so sync it more frequently
                    brokerController.getSlaveSynchronize().syncTimerCheckPoint();
                } catch (final Throwable e) {
                    LOGGER.error("ScheduledTask SlaveSynchronize syncAll error.", e);
                }
            }, 1000 * 3, 1000 * 3, TimeUnit.MILLISECONDS);

        } else {
            if (this.slaveSyncFuture != null) {
                this.slaveSyncFuture.cancel(false);
            }
            this.brokerController.getSlaveSynchronize().setMasterAddr(null);
        }
    }

    private boolean registerBrokerToController() {
        // Register this broker to controller, get brokerId and masterAddress.
        try {
            final RegisterBrokerToControllerResponseHeader registerResponse = this.brokerOuterAPI.registerBrokerToController(this.controllerLeaderAddress,
                this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.localAddress,
                this.haService.getLastEpoch(), this.brokerController.getMessageStore().getMaxPhyOffset());
            final String newMasterAddress = registerResponse.getMasterAddress();
            if (StringUtils.isNoneEmpty(newMasterAddress)) {
                if (StringUtils.equals(newMasterAddress, this.localAddress)) {
                    changeToMaster(registerResponse.getMasterEpoch(), registerResponse.getSyncStateSetEpoch());
                } else {
                    changeToSlave(newMasterAddress, registerResponse.getMasterEpoch(), registerResponse.getBrokerId());
                }
                // Set isolated to false, make broker can register to namesrv regularly
                brokerController.setIsolated(false);
            } else {
                LOGGER.warn("No master in controller");
                return false;
            }
            return true;
        } catch (final Exception e) {
            LOGGER.error("Failed to register broker to controller", e);
            return false;
        }
    }

    /**
     * Scheduling sync broker metadata form controller.
     */
    private void schedulingSyncBrokerMetadata() {
        this.scheduledService.scheduleAtFixedRate(() -> {
            try {
                final Pair<GetReplicaInfoResponseHeader, SyncStateSet> result = this.brokerOuterAPI.getReplicaInfo(this.controllerLeaderAddress, this.brokerConfig.getBrokerName(), this.localAddress);
                final GetReplicaInfoResponseHeader info = result.getObject1();
                final SyncStateSet syncStateSet = result.getObject2();
                final String newMasterAddress = info.getMasterAddress();
                final int newMasterEpoch = info.getMasterEpoch();
                final long brokerId = info.getBrokerId();
                synchronized (this) {
                    // Check if master changed
                    if (newMasterEpoch > this.masterEpoch) {
                        if (StringUtils.isNoneEmpty(newMasterAddress)) {
                            if (StringUtils.equals(newMasterAddress, this.localAddress)) {
                                changeToMaster(newMasterEpoch, syncStateSet.getSyncStateSetEpoch());
                            } else {
                                if (brokerId > 0) {
                                    changeToSlave(newMasterAddress, newMasterEpoch, brokerId);
                                } else if (brokerId < 0) {
                                    // If the brokerId is no existed, we should try register again.
                                    registerBrokerToController();
                                }
                            }
                        } else {
                            // In this case, the master in controller is null, try register to controller again, this will trigger the electMasterEvent in controller.
                            registerBrokerToController();
                        }
                    } else if (newMasterEpoch == this.masterEpoch) {
                        // Check if sync state set changed
                        if (isMasterState()) {
                            changeSyncStateSet(syncStateSet.getSyncStateSet(), syncStateSet.getSyncStateSetEpoch());
                        }
                    }
                }
            } catch (final MQBrokerException exception) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), exception);
                if (exception.getResponseCode() == CONTROLLER_BROKER_METADATA_NOT_EXIST) {
                    try {
                        registerBrokerToController();
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ignore) {

                    }
                }
            } catch (final Exception e) {
                LOGGER.warn("Error happen when get broker {}'s metadata", this.brokerConfig.getBrokerName(), e);
            }
        }, 3 * 1000, this.brokerConfig.getSyncBrokerMetadataPeriod(), TimeUnit.MILLISECONDS);
    }

    /**
     * Scheduling sync controller medata.
     */
    private boolean schedulingSyncControllerMetadata() {
        // Get controller metadata first.
        int tryTimes = 0;
        while (tryTimes < 3) {
            boolean flag = updateControllerMetadata();
            if (flag) {
                this.scheduledService.scheduleAtFixedRate(this::updateControllerMetadata, 1000 * 3, this.brokerConfig.getSyncControllerMetadataPeriod(), TimeUnit.MILLISECONDS);
                return true;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException ignore) {

            }
            tryTimes++;
        }
        LOGGER.error("Failed to init controller metadata, maybe the controllers in {} is not available", this.controllerAddresses);
        return false;
    }

    /**
     * Update controller leader address by rpc.
     */
    private boolean updateControllerMetadata() {
        for (String address : this.controllerAddresses) {
            try {
                final GetMetaDataResponseHeader responseHeader = this.brokerOuterAPI.getControllerMetaData(address);
                if (responseHeader != null && StringUtils.isNoneEmpty(responseHeader.getControllerLeaderAddress())) {
                    this.controllerLeaderAddress = responseHeader.getControllerLeaderAddress();
                    LOGGER.info("Update controller leader address to {}", this.controllerLeaderAddress);
                    return true;
                }
            } catch (final Exception e) {
                LOGGER.error("Failed to update controller metadata", e);
            }
        }
        return false;
    }

    /**
     * Scheduling check syncStateSet.
     */
    private void schedulingCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
        this.checkSyncStateSetTaskFuture = this.scheduledService.scheduleAtFixedRate(() -> {
            final Set<String> newSyncStateSet = this.haService.maybeShrinkInSyncStateSet();
            newSyncStateSet.add(this.localAddress);
            synchronized (this) {
                if (this.syncStateSet != null) {
                    // Check if syncStateSet changed
                    if (this.syncStateSet.size() == newSyncStateSet.size() && this.syncStateSet.containsAll(newSyncStateSet)) {
                        return;
                    }
                }
            }
            doReportSyncStateSetChanged(newSyncStateSet);
        }, 3 * 1000, this.brokerConfig.getCheckSyncStateSetPeriod(), TimeUnit.MILLISECONDS);
    }

    private void doReportSyncStateSetChanged(Set<String> newSyncStateSet) {
        try {
            final SyncStateSet result = this.brokerOuterAPI.alterSyncStateSet(this.controllerLeaderAddress, this.brokerConfig.getBrokerName(), this.masterAddress, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
            if (result != null) {
                changeSyncStateSet(result.getSyncStateSet(), result.getSyncStateSetEpoch());
            }
        } catch (final Exception e) {
            LOGGER.error("Error happen when change sync state set, broker:{}, masterAddress:{}, masterEpoch:{}, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}",
                this.brokerConfig.getBrokerName(), this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
        }
    }

    private void stopCheckSyncStateSet() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(false);
        }
    }

    public int getLastEpoch() {
        return this.haService.getLastEpoch();
    }

    public BrokerRole getBrokerRole() {
        return this.brokerController.getMessageStoreConfig().getBrokerRole();
    }

    public boolean isMasterState() {
        return getBrokerRole() == BrokerRole.SYNC_MASTER;
    }

    public SyncStateSet getSyncStateSet() {
        return new SyncStateSet(this.syncStateSet, this.syncStateSetEpoch);
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public int getMasterEpoch() {
        return masterEpoch;
    }

    public List<String> getControllerAddresses() {
        return controllerAddresses;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.haService.getEpochEntries();
    }
}
