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

package org.apache.rocketmq.container;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerSyncInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class InnerSalveBrokerController extends InnerBrokerController {

    private final Lock lock = new ReentrantLock();

    public InnerSalveBrokerController(final BrokerContainer brokerContainer,
        final BrokerConfig brokerConfig,
        final MessageStoreConfig storeConfig) {
        super(brokerContainer, brokerConfig, storeConfig);
        // Check configs
        checkSlaveBrokerConfig();
    }

    private void checkSlaveBrokerConfig() {
        Preconditions.checkNotNull(brokerConfig.getBrokerClusterName());
        Preconditions.checkNotNull(brokerConfig.getBrokerName());
        Preconditions.checkArgument(brokerConfig.getBrokerId() != MixAll.MASTER_ID);
    }

    private void onMasterOffline() {
        // close channels with master broker
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            this.brokerOuterAPI.getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, MixAll.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        this.slaveSynchronize.setMasterAddr(null);
        this.messageStore.updateHaMasterAddress(null);
    }

    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = this.messageStore.getMasterFlushedOffset() == 0
            && this.messageStoreConfig.isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            try {
                BrokerSyncInfo brokerSyncInfo = this.brokerOuterAPI.retrieveBrokerHaInfo(masterAddr);

                if (needSyncMasterFlushOffset) {
                    LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                    this.messageStore.setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
                }

                if (masterHaAddr == null) {
                    this.messageStore.updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                    this.messageStore.updateMasterAddress(brokerSyncInfo.getMasterAddress());
                }
            } catch (Exception e) {
                LOG.error("retrieve master ha info exception, {}", e);
            }
        }

        // set master HA address.
        if (masterHaAddr != null) {
            this.messageStore.updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        this.messageStore.wakeupHAClient();
    }

    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            onMasterOffline();
        }

        if (minBrokerId == MixAll.MASTER_ID && minBrokerAddr != null) {
            // master online
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        if (this.minBrokerIdInGroup == MixAll.MASTER_ID) {
            this.pullRequestHoldService.notifyMasterOnline();
        }
    }

    @Override
    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        if (lock.tryLock()) {
            try {
                if (minBrokerId != this.minBrokerIdInGroup) {
                    String offlineBrokerAddr = null;
                    if (minBrokerId > this.minBrokerIdInGroup) {
                        offlineBrokerAddr = this.minBrokerAddrInGroup;
                    }
                    onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, null);
                }
            } finally {
                lock.unlock();
            }

        }
    }

    @Override
    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        try {
            if (lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                try {
                    if (minBrokerId != this.minBrokerIdInGroup) {
                        onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
                    }
                } finally {
                    lock.unlock();
                }

            }
        } catch (InterruptedException e) {
            LOG.error("Update min broker error, {}", e);
        }
    }

    @Override
    public BrokerController peekMasterBroker() {
        return this.brokerContainer.peekMasterBroker();
    }
}
