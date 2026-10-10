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

package org.apache.rocketmq.store.ha;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

/**
 * GroupTransferService Service
 */
public class GroupTransferService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private final DefaultMessageStore defaultMessageStore;
    private final HAService haService;
    private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile List<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

    public GroupTransferService(final HAService haService, final DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        wakeup();
    }

    public void notifyTransferSome() {
        this.notifyTransferObject.wakeup();
    }

    private void swapRequests() {
        lock.lock();
        try {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }

    private void doWaitTransfer() {
        if (!this.requestsRead.isEmpty()) {
            for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                boolean transferOK = false;

                long deadLine = req.getDeadLine();
                final boolean allAckInSyncStateSet = req.getAckNums() == MixAll.ALL_ACK_IN_SYNC_STATE_SET;

                for (int i = 0; !transferOK && deadLine - System.nanoTime() > 0; i++) {
                    if (i > 0) {
                        this.notifyTransferObject.waitForRunning(1);
                    }

                    //handle only one slave ack, ackNums <= 2 means master + 1 slave
                    if (!allAckInSyncStateSet && req.getAckNums() <= 2) {
                        transferOK = haService.getPush2SlaveMaxOffset().get() >= req.getNextOffset();
                        continue;
                    }

                    if (allAckInSyncStateSet && this.haService instanceof AutoSwitchHAService) {
                        // In this mode, we must wait for all replicas that in SyncStateSet.
                        final AutoSwitchHAService autoSwitchHAService = (AutoSwitchHAService) this.haService;
                        final Set<Long> syncStateSet = autoSwitchHAService.getSyncStateSet();
                        if (syncStateSet.size() <= 1) {
                            // Only master
                            transferOK = true;
                            break;
                        }

                        // Include master
                        int ackNums = 1;
                        for (HAConnection conn : haService.getConnectionList()) {
                            final AutoSwitchHAConnection autoSwitchHAConnection = (AutoSwitchHAConnection) conn;
                            if (syncStateSet.contains(autoSwitchHAConnection.getSlaveId()) && autoSwitchHAConnection.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= syncStateSet.size()) {
                                transferOK = true;
                                break;
                            }
                        }
                    } else {
                        // Include master
                        int ackNums = 1;
                        for (HAConnection conn : haService.getConnectionList()) {
                            // TODO: We must ensure every HAConnection represents a different slave
                            // Solution: Consider assign a unique and fixed IP:ADDR for each different slave
                            if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= req.getAckNums()) {
                                transferOK = true;
                                break;
                            }
                        }
                    }
                }

                if (!transferOK) {
                    log.warn("transfer message to slave timeout, offset : {}, request acks: {}",
                        req.getNextOffset(), req.getAckNums());
                }

                req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
            }

            this.requestsRead = new LinkedList<>();
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                this.doWaitTransfer();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + GroupTransferService.class.getSimpleName();
        }
        return GroupTransferService.class.getSimpleName();
    }
}
