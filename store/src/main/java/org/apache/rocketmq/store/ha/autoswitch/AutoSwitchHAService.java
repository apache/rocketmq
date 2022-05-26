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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.ha.DefaultHAService;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService extends DefaultHAService {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("AutoSwitchHAService_Executor_"));
    private final List<Consumer<Set<String>>> syncStateSetChangedListeners = new ArrayList<>();
    private final CopyOnWriteArraySet<String> syncStateSet = new CopyOnWriteArraySet<>();
    private String localAddress;

    private EpochFileCache epochCache;
    private AutoSwitchHAClient haClient;

    public AutoSwitchHAService() {
    }

    @Override public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.epochCache = new EpochFileCache(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        this.epochCache.initCacheFromFile();
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService = new AutoSwitchAcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override public void shutdown() {
        super.shutdown();
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.executorService.shutdown();
    }

    @Override public boolean changeToMaster(int masterEpoch) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (masterEpoch < lastEpoch) {
            return false;
        }
        destroyConnections();
        // Stop ha client if needed
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        // Truncate dirty file
        final long truncateOffset = truncateInvalidMsg();
        if (truncateOffset >= 0) {
            this.epochCache.truncateSuffixByOffset(truncateOffset);
        }

        // Append new epoch to epochFile
        final EpochEntry newEpochEntry = new EpochEntry(masterEpoch, this.defaultMessageStore.getMaxPhyOffset());
        if (this.epochCache.lastEpoch() >= masterEpoch) {
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
        }
        this.epochCache.appendEntry(newEpochEntry);

        this.defaultMessageStore.recoverTopicQueueTable();

        final HashSet<String> newSyncStateSet = new HashSet<>();
        newSyncStateSet.add(this.localAddress);
        setSyncStateSet(newSyncStateSet);
        LOGGER.info("Change ha to master success, newMasterEpoch:{}, startOffset:{}", masterEpoch, newEpochEntry.getStartOffset());
        return true;
    }

    @Override public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        final int lastEpoch = this.epochCache.lastEpoch();
        if (newMasterEpoch <= lastEpoch) {
            return false;
        }
        try {
            destroyConnections();
            if (this.haClient == null) {
                this.haClient = new AutoSwitchHAClient(this, defaultMessageStore, this.epochCache);
            } else {
                this.haClient.reOpen();
            }
            this.haClient.setLocalAddress(this.localAddress);
            this.haClient.updateSlaveId(slaveId);
            this.haClient.updateMasterAddress(newMasterAddr);
            this.haClient.start();
            LOGGER.info("Change ha to slave success, newMasterAddress:{}, newMasterEpoch:{}", newMasterAddr, newMasterEpoch);
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error happen when change ha to slave", e);
            return false;
        }
    }

    @Override public HAClient getHAClient() {
        return this.haClient;
    }

    @Override public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override public void updateMasterAddress(String newAddr) {
    }

    public void registerSyncStateSetChangedListener(final Consumer<Set<String>> listener) {
        this.syncStateSetChangedListeners.add(listener);
    }

    public void notifySyncStateSetChanged(final Set<String> newSyncStateSet) {
        this.executorService.submit(() -> {
            for (Consumer<Set<String>> listener : syncStateSetChangedListeners) {
                listener.accept(newSyncStateSet);
            }
        });
    }

    /**
     * Check and maybe shrink the inSyncStateSet.
     * A slave will be removed from inSyncStateSet if (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public Set<String> maybeShrinkInSyncStateSet() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        final HashSet<String> newSyncStateSet = new HashSet<>(currentSyncStateSet);
        final long haMaxTimeSlaveNotCatchup = this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        for (HAConnection haConnection : this.connectionList) {
            final AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;
            final String slaveAddress = connection.getSlaveAddress();
            if (currentSyncStateSet.contains(slaveAddress)) {
                if (this.defaultMessageStore.getMaxPhyOffset() == connection.getSlaveAckOffset()) {
                    continue;
                }
                if ((System.currentTimeMillis() - connection.getLastCatchUpTimeMs()) > haMaxTimeSlaveNotCatchup) {
                    newSyncStateSet.remove(slaveAddress);
                }
            }
        }
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to inSyncStateSet.
     * A slave will be added to inSyncStateSet if its slaveMaxOffset >= current confirmOffset, and it is caught up to
     * an offset within the current leader epoch.
     */
    public void maybeExpandInSyncStateSet(final String slaveAddress, final long slaveMaxOffset) {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        if (currentSyncStateSet.contains(slaveAddress)) {
            return;
        }
        final long confirmOffset = getConfirmOffset();
        if (slaveMaxOffset >= confirmOffset) {
            final EpochEntry currentLeaderEpoch = this.epochCache.lastEntry();
            if (slaveMaxOffset >= currentLeaderEpoch.getStartOffset()) {
                currentSyncStateSet.add(slaveAddress);
                // Notify the upper layer that syncStateSet changed.
                notifySyncStateSetChanged(currentSyncStateSet);
            }
        }
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members)
     */
    public long getConfirmOffset() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        long confirmOffset = this.defaultMessageStore.getMaxPhyOffset();
        for (HAConnection connection : this.connectionList) {
            if (currentSyncStateSet.contains(connection.getClientAddress())) {
                confirmOffset = Math.min(confirmOffset, connection.getSlaveAckOffset());
            }
        }
        return confirmOffset;
    }

    public synchronized void setSyncStateSet(final Set<String> syncStateSet) {
        this.syncStateSet.clear();
        this.syncStateSet.addAll(syncStateSet);
    }

    public synchronized Set<String> getSyncStateSet() {
        return new HashSet<>(this.syncStateSet);
    }

    public void truncateEpochFilePrefix(final long offset) {
        this.epochCache.truncatePrefixByOffset(offset);
    }

    public void truncateEpochFileSuffix(final long offset) {
        this.epochCache.truncateSuffixByOffset(offset);
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    /**
     * Try to truncate incomplete msg transferred from master.
     */
    public long truncateInvalidMsg() {
        long dispatchBehind = this.defaultMessageStore.dispatchBehindBytes();
        if (dispatchBehind <= 0) {
            LOGGER.info("Dispatch complete, skip truncate");
            return -1;
        }

        long reputFromOffset = this.defaultMessageStore.getMaxPhyOffset() - dispatchBehind;

        boolean doNext = true;
        while (reputFromOffset < this.defaultMessageStore.getMaxPhyOffset() && doNext) {
            SelectMappedBufferResult result = this.defaultMessageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            try {
                reputFromOffset = result.getStartOffset();

                int readSize = 0;
                while (readSize < result.getSize()) {
                    DispatchRequest dispatchRequest = this.defaultMessageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                    int size = dispatchRequest.getMsgSize();

                    if (dispatchRequest.isSuccess()) {
                        if (size > 0) {
                            reputFromOffset += size;
                            readSize += size;
                        } else {
                            reputFromOffset = this.defaultMessageStore.getCommitLog().rollNextFile(reputFromOffset);
                            break;
                        }
                    } else {
                        doNext = false;
                        break;
                    }
                }
            } finally {
                result.release();
            }
        }

        LOGGER.info("AutoRecoverHAClient truncate commitLog to {}", reputFromOffset);
        this.defaultMessageStore.truncateDirtyFiles(reputFromOffset);
        return reputFromOffset;
    }

    class AutoSwitchAcceptSocketService extends AcceptSocketService {

        public AutoSwitchAcceptSocketService(int port) {
            super(port);
        }

        @Override public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getLoggerIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return AutoSwitchAcceptSocketService.class.getSimpleName();
        }

        @Override protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new AutoSwitchHAConnection(AutoSwitchHAService.this, sc, AutoSwitchHAService.this.epochCache);
        }
    }
}
