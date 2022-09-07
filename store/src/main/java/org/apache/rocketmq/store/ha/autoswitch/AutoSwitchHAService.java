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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationRequest;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.WaitNotifyObject;
import org.apache.rocketmq.store.ha.netty.NettyTransferServer;
import org.apache.rocketmq.store.ha.netty.TransferMessage;
import org.apache.rocketmq.store.ha.netty.TransferType;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService implements HAService {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryImpl("TransferServiceExecutor"));

    private long currentMasterEpoch = -1L;
    private long confirmOffset = -1L;
    private String localAddress;
    private EpochStore epochStore;
    private DefaultMessageStore defaultMessageStore;
    private GroupTransferService groupTransferService;

    private AutoSwitchHAClient haClient;
    private NettyTransferServer nettyTransferServer;
    private CopyOnWriteArraySet<String> syncStateSet = new CopyOnWriteArraySet<>();
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final List<Consumer<Set<String>>> syncStateSetChangedListeners = new ArrayList<>();
    private HAConnectionStateNotificationService haConnectionStateNotificationService;

    public AutoSwitchHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.epochStore = new EpochStoreService(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        this.epochStore.initStateFromFile();
        this.defaultMessageStore = defaultMessageStore;
        this.nettyTransferServer = new NettyTransferServer(this);
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService =
            new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void start() throws Exception {
        this.groupTransferService.start();
        this.nettyTransferServer.start();
        this.haConnectionStateNotificationService.start();
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.executorService.shutdown();
        this.nettyTransferServer.shutdown();
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
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
     * Try to truncate incomplete msg transferred from master.
     */
    public long truncateInvalidMsg() {
        long dispatchBehind = this.defaultMessageStore.dispatchBehindBytes();
        if (dispatchBehind <= 0) {
            LOGGER.info("Dispatch complete, skip truncate");
            return -1L;
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
                    DispatchRequest dispatchRequest =
                        this.defaultMessageStore.getCommitLog().checkMessageAndReturnSize(
                            result.getByteBuffer(), false, false);
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

        LOGGER.info("Truncate commitLog to {}", reputFromOffset);
        this.defaultMessageStore.truncateDirtyFiles(reputFromOffset);
        return reputFromOffset;
    }

    private void doWaitMessageStoreDispatch() {
        while (defaultMessageStore.dispatchBehindBytes() > 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (Exception ignored) {
                // ignored
            }
        }
    }

    @Override
    public boolean changeToMaster(int masterEpoch) {
        if (masterEpoch < this.epochStore.getLastEpoch()) {
            LOGGER.error("Broker change to master failed, not allow elect unclean master, oldEpoch:{}, newEpoch:{}",
                masterEpoch, this.epochStore.getLastEpoch());
            return false;
        }

        nettyTransferServer.destroyConnections();
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        final long truncateOffset = truncateInvalidMsg();
        final long maxPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        final long minPhyOffset = this.defaultMessageStore.getMinPhyOffset();
        LOGGER.info("Broker truncate message file, newMasterEpoch:{}, storeOffset: {}-{}, truncateOffset:{}",
            minPhyOffset, maxPhyOffset, masterEpoch, truncateOffset);

        // When broker change to master, accept uncommitted message in store
        updateConfirmOffset(computeConfirmOffset());

        this.epochStore.truncateSuffixByEpoch(masterEpoch);
        this.epochStore.truncateSuffixByOffset(maxPhyOffset);
        this.epochStore.tryAppendEpochEntry(new EpochEntry(masterEpoch, maxPhyOffset));

        this.currentMasterEpoch = masterEpoch;
        this.doWaitMessageStoreDispatch();
        this.defaultMessageStore.recoverTopicQueueTable();

        this.setSyncStateSet(new HashSet<>(Collections.singletonList(this.localAddress)));
        notifySyncStateSetChanged(syncStateSet);

        LOGGER.info("Broker change to master success, newMasterEpoch:{}", masterEpoch);
        return true;
    }

    @Override
    public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        try {
            nettyTransferServer.destroyConnections();
            setSyncStateSet(new HashSet<>());

            if (this.haClient == null) {
                this.haClient = new AutoSwitchHAClient(this);
            } else {
                this.haClient.shutdown();
            }

            this.currentMasterEpoch = newMasterEpoch;

            this.haClient.init();
            this.haClient.updateSlaveId(slaveId);
            this.haClient.updateHaMasterAddress(newMasterAddr);
            this.haClient.start();

            LOGGER.info("Broker change to slave success, newMasterEpoch:{}, newMasterAddress:{}, brokerId:{}",
                newMasterAddr, newMasterEpoch, slaveId);
            return true;
        } catch (Exception e) {
            LOGGER.error("Broker change to slave failed, newMasterEpoch:{}, newMasterAddress:{}, brokerId:{}",
                newMasterAddr, newMasterEpoch, slaveId, e);
        }
        return false;
    }

    public boolean isSlaveEpochMatchMaster(long epoch) {
        return this.currentMasterEpoch == epoch;
    }

    public long getCurrentMasterEpoch() {
        return currentMasterEpoch;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return new AtomicLong(this.confirmOffset);
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members) for master
     */
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
            if (this.syncStateSet.size() == 1) {
                return this.defaultMessageStore.getMaxPhyOffset();
            }
            // First time compute confirmOffset.
            if (this.confirmOffset <= 0) {
                this.confirmOffset = computeConfirmOffset();
            }
        }
        return confirmOffset;
    }

    public void updateConfirmOffsetWhenSlaveAck(final String slaveAddress) {
        if (this.syncStateSet.contains(slaveAddress)) {
            this.confirmOffset = computeConfirmOffset();
        }
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();
        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);
            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);
            info.setMasterCommitLogMaxOffset(masterPutWhere);
            for (HAConnection haConnection : this.getConnectionList()) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();
                long slaveAckOffset = haConnection.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(haConnection.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(haConnection.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(haConnection.getTransferFromWhere());
                cInfo.setInSync(syncStateSet.contains(haConnection.getClientAddress()));
                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(syncStateSet.size() - 1);
        }
        return info;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return this.waitNotifyObject;
    }

    @Override
    public boolean isSlaveOK(long masterPutWhere) {
        boolean result = false;
        try {
            result = this.getConnectionCount().get() > 0;
            long maxConfirmOffset = 0;
            for (HAConnection haConnection : this.getConnectionList()) {
                if (syncStateSet.contains(haConnection.getClientAddress())) {
                    maxConfirmOffset = Math.max(maxConfirmOffset, haConnection.getSlaveAckOffset());
                }
            }
            result &= maxConfirmOffset < this.defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync();
        } catch (Exception e) {
            LOGGER.error("Check slave status error, masterPutOffset:{}", masterPutWhere, e);
        }
        return result;
    }

    @Override
    public void updateMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        return syncStateSet.size();
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        return masterPutWhere - conn.getSlaveAckOffset() <
            this.defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync();
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return new AtomicInteger(this.nettyTransferServer.getChannelGroup().size());
    }

    @Override
    public void putRequest(CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        List<HAConnection> connectionList = new ArrayList<>();
        this.nettyTransferServer.getChannelGroup().forEach(new Consumer<Channel>() {
            @Override
            public void accept(Channel channel) {
                HAConnection haConnection = (HAConnection) channel.attr(AttributeKey.valueOf("connection")).get();
                connectionList.add(haConnection);
            }
        });
        return connectionList;
    }

    /**
     * Check and maybe shrink the inSyncStateSet.
     * A slave will be removed from inSyncStateSet if
     * (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public synchronized Set<String> maybeShrinkInSyncStateSet() {
        final HashSet<String> newSyncStateSet = new HashSet<>();
        final long haMaxTimeSlaveNotCatchup =
            this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        for (HAConnection haConnection : this.getConnectionList()) {
            final AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;
            final String slaveAddress = connection.getClientAddress();
            final boolean firstInit = connection.getSlaveAckOffset() < 0;
            final boolean asyncLearner = connection.isSlaveAsyncLearner();
            final boolean slaveNotCatchup =
                (System.currentTimeMillis() - connection.getSlaveAckTimestamp()) > haMaxTimeSlaveNotCatchup;
            if (firstInit || asyncLearner || slaveNotCatchup) {
                continue;
            }
            newSyncStateSet.add(slaveAddress);
        }
        newSyncStateSet.add(this.localAddress);
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to inSyncStateSet.
     * A slave will be added to inSyncStateSet if its slaveAckOffset >= current confirmOffset
     * and it is caught up to an offset within the current leader epoch.
     */
    public synchronized void maybeExpandInSyncStateSet(HAConnection haConnection, final long slaveAckOffset) {
        AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;

        final Set<String> currentSyncStateSet = getSyncStateSet();
        String brokerAddr = connection.getClientAddress();
        if (brokerAddr == null || currentSyncStateSet.contains(brokerAddr)) {
            return;
        }

        final long confirmOffset = getConfirmOffset();
        final EpochEntry currentLeaderEpoch = this.epochStore.getLastEntry();
        if (slaveAckOffset >= confirmOffset && slaveAckOffset >= currentLeaderEpoch.getStartOffset()) {
            currentSyncStateSet.add(brokerAddr);
            setSyncStateSet(currentSyncStateSet);
            // Notify the upper layer that syncStateSet changed.
            notifySyncStateSetChanged(currentSyncStateSet);
            LOGGER.info("Expanding in sync state set for broker:{}, current syncStateSet:{}, slaveAckOffset:{}",
                haConnection.getClientAddress(), currentSyncStateSet, slaveAckOffset);
        }
    }

    public void updateConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members)
     */
    public long computeConfirmOffset() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        long confirmOffset = this.defaultMessageStore.getMaxPhyOffset();
        for (HAConnection connection : this.getConnectionList()) {
            final String slaveAddress = connection.getClientAddress();
            if (currentSyncStateSet.contains(slaveAddress)) {
                confirmOffset = Math.min(confirmOffset, connection.getSlaveAckOffset());
            }
        }
        return confirmOffset;
    }

    public synchronized Set<String> getSyncStateSet() {
        return new HashSet<>(this.syncStateSet);
    }

    public synchronized void setSyncStateSet(final Set<String> syncStateSet) {
        this.syncStateSet = new CopyOnWriteArraySet<>(syncStateSet);
    }

    public void truncateEpochFilePrefix(final long offset) {
        this.epochStore.truncatePrefixByOffset(offset);
    }

    public NettyTransferServer getNettyTransferServer() {
        return nettyTransferServer;
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.epochStore.getAllEntries();
    }

    public HandshakeResult verifySlaveIdentity(HandshakeSlave handshakeSlave) {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();

        if (!StringUtils.equals(brokerConfig.getBrokerClusterName(), handshakeSlave.getClusterName())) {
            return HandshakeResult.CLUSTER_NAME_NOT_MATCH;
        }

        if (!StringUtils.equals(brokerConfig.getBrokerName(), handshakeSlave.getBrokerName())) {
            return HandshakeResult.BROKER_NAME_NOT_MATCH;
        }

        if (handshakeSlave.getBrokerId() == 0L) {
            return HandshakeResult.BROKER_ID_ERROR;
        }

        if (MQVersion.Version.V5_0_0_SNAPSHOT.ordinal() > handshakeSlave.getBrokerAppVersion()) {
            return HandshakeResult.PROTOCOL_NOT_SUPPORT;
        }

        return HandshakeResult.ACCEPT;
    }

    public HandshakeMaster buildHandshakeResult(HandshakeResult handshakeResult) {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();
        HandshakeMaster handshakeMaster = new HandshakeMaster();
        handshakeMaster.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeMaster.setBrokerName(brokerConfig.getBrokerName());
        handshakeMaster.setBrokerId(brokerConfig.getBrokerId());
        handshakeMaster.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeMaster.setLanguageCode(LanguageCode.JAVA);
        handshakeMaster.setHaProtocolVersion(2);
        handshakeMaster.setHandshakeResult(handshakeResult);
        return handshakeMaster;
    }

    public void notifyTransferSome() {
        this.confirmOffset = computeConfirmOffset();
        this.groupTransferService.notifyTransferSome();
    }

    public TransferMessage buildMessage(TransferType messageType) {
        return new TransferMessage(messageType, this.currentMasterEpoch);
    }

    public EpochStore getEpochStore() {
        return epochStore;
    }
}
