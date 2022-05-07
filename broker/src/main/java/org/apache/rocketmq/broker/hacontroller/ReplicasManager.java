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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.AlterSyncStateSetResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.controller.GetReplicaInfoResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
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

    /**
     * Todo:
     * 1.How to get local address?
     * 2.How to report ha address to controller?
     * 3.add sync state set to ha service.
     * 4.register to controller when startup.
     */
    private final AutoSwitchHAService haService;
    private final ControllerProxy proxy;
    private final String brokerName;
    private final String localAddress;
    private volatile ScheduledFuture<?> checkSyncStateSetTaskFuture;
    private volatile Set<String> syncStateSet;
    private volatile int syncStateSetEpoch = 0;
    private volatile BrokerRole currentRole = BrokerRole.SLAVE;
    private volatile String masterAddress = "";
    private volatile int masterEpoch = 0;

    public ReplicasManager(final AutoSwitchHAService haService, final ControllerProxy proxy, final String brokerName) {
        this.haService = haService;
        this.proxy = proxy;
        this.syncStateSet = new HashSet<>();
        this.brokerName = brokerName;
        this.localAddress = "";
    }

    public void start() {
        this.syncMetadataService.scheduleAtFixedRate(() -> {
            try {
                final GetReplicaInfoResponseHeader info = this.proxy.getReplicaInfo(brokerName);
                final String newMasterAddress = info.getMasterAddress();
                final int newMasterEpoch = info.getMasterEpoch();
                // Check if master changed
                if (newMasterAddress != null && !this.masterAddress.equals(newMasterAddress) && newMasterEpoch > masterEpoch) {
                    if (newMasterAddress.equals(this.localAddress)) {
                        changeToMaster(newMasterEpoch, info.getSyncStateSetEpoch());
                    } else {
                        // Todo: add haAddress to controller
                        changeToSlave(newMasterAddress, newMasterEpoch, "");
                    }
                } else {
                    // Check if sync state set changed
                    if (this.currentRole == BrokerRole.SYNC_MASTER) {
                        changeSyncStateSet(info.getSyncStateSet(), info.getSyncStateSetEpoch());
                    }
                }
            } catch (final Exception e) {
                LOGGER.error("Error happen when get broker {}'s metadata", brokerName, e);
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    public void changeToMaster(final int newMasterEpoch, final int syncStateSetEpoch) {
        synchronized(this) {
            if (newMasterEpoch > this.masterEpoch) {
                this.currentRole = BrokerRole.SYNC_MASTER;
                this.masterAddress = this.localAddress;
                this.masterEpoch = newMasterEpoch;
                this.syncStateSet = new HashSet<>();
                this.syncStateSet.add(this.localAddress);
                this.syncStateSetEpoch = syncStateSetEpoch;
                startCheckSyncStateSetService();
                this.haService.changeToMaster(newMasterEpoch);
            }
        }
    }

    public void changeToSlave(final String newMasterAddress, final int newMasterEpoch, final String masterHaAddress) {
        synchronized(this) {
            if (newMasterEpoch > this.masterEpoch) {
                this.currentRole = BrokerRole.SLAVE;
                this.masterAddress = newMasterAddress;
                this.masterEpoch = newMasterEpoch;
                stopCheckSyncStateSetService();
                this.haService.changeToSlave(newMasterAddress, masterHaAddress, newMasterEpoch);
            }
        }
    }

    private void changeSyncStateSet(final Set<String> newSyncStateSet, final int newSyncStateSetEpoch) {
        synchronized(this) {
            if (newSyncStateSetEpoch > this.syncStateSetEpoch) {
                this.syncStateSetEpoch = newSyncStateSetEpoch;
                this.syncStateSet = new HashSet<>(newSyncStateSet);
                // Todo: add syncStateSet into ha service.
            }
        }
    }

    private void startCheckSyncStateSetService() {
        this.checkSyncStateSetTaskFuture = this.checkSyncStateSetService.scheduleAtFixedRate(() -> {
            final Set<String> newSyncStateSet = this.haService.checkSyncStateSetChanged();
            if (this.syncStateSet != null) {
                // Check if syncStateSet changed
                if (this.syncStateSet.size() != newSyncStateSet.size() || !this.syncStateSet.containsAll(newSyncStateSet)) {
                    try {
                        final AlterSyncStateSetResponseHeader header = this.proxy.alterSyncStateSet(this.brokerName, this.masterAddress, this.masterEpoch, newSyncStateSet, this.syncStateSetEpoch);
                        changeSyncStateSet(header.getNewSyncStateSet(), header.getNewSyncStateSetEpoch());
                    } catch (final Exception e) {
                        LOGGER.error("Error happen when change sync state set, broker:{}, masterAddress:{}, masterEpoch, oldSyncStateSet:{}, newSyncStateSet:{}, syncStateSetEpoch:{}",
                            this.brokerName, this.masterAddress, this.masterEpoch, this.syncStateSet, newSyncStateSet, this.syncStateSetEpoch, e);
                    }
                }
            }
        }, 1, 4, TimeUnit.SECONDS);
    }

    private void stopCheckSyncStateSetService() {
        if (this.checkSyncStateSetTaskFuture != null) {
            this.checkSyncStateSetTaskFuture.cancel(true);
            this.checkSyncStateSetTaskFuture = null;
        }
    }

    public void shutdown() {
        this.syncMetadataService.shutdown();
        this.checkSyncStateSetService.shutdown();
    }
}
