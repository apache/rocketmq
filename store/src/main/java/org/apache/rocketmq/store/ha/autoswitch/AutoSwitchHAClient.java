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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.netty.NettyTransferClient;
import org.apache.rocketmq.store.ha.netty.TransferMessage;
import org.apache.rocketmq.store.ha.netty.TransferType;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

import static org.apache.rocketmq.store.ha.HAConnectionState.HANDSHAKE;
import static org.apache.rocketmq.store.ha.HAConnectionState.READY;
import static org.apache.rocketmq.store.ha.HAConnectionState.TRANSFER;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final AtomicReference<Long> slaveId = new AtomicReference<>();

    private final AutoSwitchHAService haService;
    private final EpochStore epochCache;
    private final DefaultMessageStore defaultMessageStore;
    private final NettyTransferClient nettyTransferClient;

    private volatile HAConnectionState currentState = HAConnectionState.SHUTDOWN;
    private volatile long currentReceivedEpoch = -1L;
    private volatile long currentTransferOffset = -1L;

    public AutoSwitchHAClient(AutoSwitchHAService haService) {
        this.haService = haService;
        this.defaultMessageStore = haService.getDefaultMessageStore();
        this.epochCache = haService.getEpochStore();
        this.nettyTransferClient = new NettyTransferClient(this);
        this.nettyTransferClient.init();
    }

    public void init() throws IOException {
        // init offset
        this.currentReceivedEpoch = -1L;
        this.currentTransferOffset = -1L;
        changeCurrentState(HAConnectionState.READY);
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getLoggerIdentifier()
                + AutoSwitchHAClient.class.getSimpleName();
        }
        return AutoSwitchHAClient.class.getSimpleName();
    }

    public void updateSlaveId(Long newId) {
        Long currentId = this.slaveId.get();
        this.slaveId.set(newId);
        LOGGER.info("Update slave Id, OLD: {}, New: {}", currentId, newId);
    }

    public long getCurrentMasterEpoch() {
        return this.haService.getCurrentMasterEpoch();
    }

    public AutoSwitchHAService getHaService() {
        return haService;
    }

    @Override
    public void updateMasterAddress(String newAddress) {
        String currentAddr = this.masterAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("Update master address, OLD: " + currentAddr + " NEW: " + newAddress);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddress) {
        String currentAddr = this.masterHaAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterHaAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("Update master ha address, OLD: " + currentAddr + " NEW: " + newAddress);
            wakeup();
        }
    }

    @Override
    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    @Override
    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public void setLastReadTimestamp(long lastReadTimestamp) {
        this.nettyTransferClient.setLastReadTimestamp(lastReadTimestamp);
    }

    @Override
    public long getLastReadTimestamp() {
        return this.nettyTransferClient.getLastReadTimestamp();
    }

    @Override
    public long getLastWriteTimestamp() {
        return this.nettyTransferClient.getLastWriteTimestamp();
    }

    @Override
    public HAConnectionState getCurrentState() {
        return this.currentState;
    }

    @Override
    public void changeCurrentState(HAConnectionState haConnectionState) {
        LOGGER.info("change ha state from {} to {}", this.currentState, haConnectionState);
        this.currentState = haConnectionState;
    }

    @Override
    public void closeMaster() {
        this.nettyTransferClient.close();
        LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.nettyTransferClient.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.closeMaster();
        super.shutdown();
    }

    private boolean doHandshakeWithMaster() {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();
        HandshakeSlave handshakeSlave = new HandshakeSlave();
        handshakeSlave.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeSlave.setBrokerName(brokerConfig.getBrokerName());
        handshakeSlave.setBrokerId(brokerConfig.getBrokerId());
        handshakeSlave.setBrokerAddr(
            ((AutoSwitchHAService) defaultMessageStore.getHaService()).getLocalAddress());
        handshakeSlave.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeSlave.setLanguageCode(LanguageCode.JAVA);
        handshakeSlave.setHaProtocolVersion(2);
        TransferMessage transferMessage = this.haService.buildMessage(TransferType.HANDSHAKE_SLAVE);
        transferMessage.appendBody(RemotingSerializable.encode(handshakeSlave));

        boolean result = nettyTransferClient.writeMessageAndWait(transferMessage);
        if (result) {
            Object object = nettyTransferClient.getRpcResponseObject();
            if (object instanceof HandshakeMaster) {
                HandshakeMaster shake = (HandshakeMaster) object;
                return this.masterHandshake(shake);
            }
        }
        return false;
    }

    public NettyTransferClient getNettyTransferClient() {
        return nettyTransferClient;
    }

    public boolean masterHandshake(HandshakeMaster handshakeMaster) {
        if (handshakeMaster != null
            && HandshakeResult.ACCEPT.equals(handshakeMaster.getHandshakeResult())) {
            return true;
        }
        LOGGER.error("Master reject build connection, {}", handshakeMaster);
        return false;
    }

    private boolean doQueryMasterEpoch() {
        TransferMessage transferMessage = haService.buildMessage(TransferType.QUERY_EPOCH);
        boolean result = nettyTransferClient.writeMessageAndWait(transferMessage);
        if (result) {
            List<EpochEntry> entryList = (List<EpochEntry>) nettyTransferClient.getRpcResponseObject();
            this.doConsistencyRepairWithMaster(entryList);
            this.sendConfirmTruncateToMaster(currentTransferOffset);
            return true;
        }
        return false;
    }

    private boolean doFlowMonitor() {
        return true;
    }

    public synchronized void doConsistencyRepairWithMaster(List<EpochEntry> entryList) {
        if (!doTruncateFiles(entryList)) {
            this.closeMaster();
            return;
        }

        long masterMinOffset = entryList.get(0).getStartOffset();
        long masterMaxOffset = entryList.get(entryList.size() - 1).getEndOffset();

        // only take effect when slave commitLog is empty
        if (currentTransferOffset == -1L) {
            boolean fromLast = this.defaultMessageStore.getMessageStoreConfig().isSyncFromLastFile();
            currentTransferOffset = fromLast ? masterMaxOffset : masterMinOffset;
        } else {
            currentTransferOffset = Math.max(currentTransferOffset, masterMinOffset);
            currentTransferOffset = Math.min(currentTransferOffset, masterMaxOffset);
        }
    }

    private void sendConfirmTruncateToMaster(long startOffset) {
        boolean syncFromLastFile = this.defaultMessageStore.getMessageStoreConfig().isSyncFromLastFile();
        ConfirmTruncate confirmTruncate = new ConfirmTruncate(syncFromLastFile, startOffset);
        TransferMessage transferMessage = this.haService.buildMessage(TransferType.CONFIRM_TRUNCATE);
        transferMessage.appendBody(RemotingSerializable.encode(confirmTruncate));
        nettyTransferClient.writeMessage(transferMessage);
    }

    public void sendPushCommitLogAck() {
        boolean asyncLearner = defaultMessageStore.getMessageStoreConfig().isAsyncLearner();
        PushCommitLogAck pushCommitLogAck = new PushCommitLogAck(this.currentTransferOffset, asyncLearner);
        TransferMessage transferMessage = this.haService.buildMessage(TransferType.TRANSFER_ACK);
        transferMessage.appendBody(RemotingSerializable.encode(pushCommitLogAck));
        nettyTransferClient.writeMessage(transferMessage);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case READY:
                        if (this.doHandshakeWithMaster()) {
                            this.changeCurrentState(HANDSHAKE);
                        } else {
                            this.closeMaster();
                            this.changeCurrentState(READY);
                            this.waitForRunning(100);
                        }
                        continue;
                    case HANDSHAKE:
                        if (this.doQueryMasterEpoch()) {
                            this.changeCurrentState(TRANSFER);
                        } else {
                            this.closeMaster();
                            this.changeCurrentState(READY);
                            this.waitForRunning(1000);
                        }
                        continue;
                    case TRANSFER:
                    case SUSPEND:
                        if (!this.doFlowMonitor()) {
                            this.closeMaster();
                            this.changeCurrentState(READY);
                            this.waitForRunning(1000);
                        }
                        continue;
                    case SHUTDOWN:
                    default:
                        this.waitForRunning(1000);
                }
            } catch (Exception e) {
                System.out.println(e);
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                closeMaster();
            }
        }
    }

    /**
     * Compare the master and slave's epoch file, find consistent point, do truncate.
     */
    private boolean doTruncateFiles(List<EpochEntry> masterEpochEntries) {
        // If epochMap is empty, means the broker is a new replicas
        if (this.epochCache.getAllEntries().size() == 0) {
            LOGGER.info("Slave local epochCache is empty, skip truncate log");
            return true;
        }

        final EpochStore masterEpochCache = new EpochStoreService();
        masterEpochCache.initStateFromEntries(masterEpochEntries);

        final EpochStore localEpochCache = new EpochStoreService();
        final List<EpochEntry> localEpochEntries = this.epochCache.getAllEntries();
        localEpochCache.initStateFromEntries(localEpochEntries);
        localEpochCache.setLastEpochEntryEndOffset(this.defaultMessageStore.getMaxPhyOffset());

        // If truncateOffset < 0, means we can't find a consistent point
        final long truncateOffset = localEpochCache.findLastConsistentPoint(masterEpochCache);
        if (truncateOffset < 0) {
            LOGGER.error("Failed to find a consistent point between masterEpoch:{} and slaveEpoch:{}",
                masterEpochEntries, localEpochEntries);
        }

        // Truncate invalid msg first
        if (haService.truncateInvalidMsg() >= 0) {
            this.epochCache.truncateSuffixByOffset(truncateOffset);
            LOGGER.info("Truncate slave log to {} success, change to transfer state", truncateOffset);
        }

        this.currentReceivedEpoch = this.epochCache.getLastEpoch();
        this.currentTransferOffset = truncateOffset;
        return true;
    }

    public void doPutCommitLog(PushCommitLogData pushCommitLogData, ByteBuffer byteBuffer) {
        long currentBlockEpoch = pushCommitLogData.getEpoch();
        long currentBlockStartOffset = pushCommitLogData.getBlockStartOffset();
        long currentEpochStartOffset = pushCommitLogData.getEpochStartOffset();
        long replicaConfirmOffset = pushCommitLogData.getConfirmOffset();

        if (byteBuffer.hasRemaining()) {
            this.defaultMessageStore.appendToCommitLog(
                currentBlockStartOffset, byteBuffer.array(), 32, byteBuffer.remaining());
        }

        this.haService.updateConfirmOffset(Math.min(replicaConfirmOffset, this.defaultMessageStore.getMaxPhyOffset()));

        // If epoch changed to bigger, last epoch record would be terminated
        if (this.currentReceivedEpoch < currentBlockEpoch) {
            this.currentReceivedEpoch = currentBlockEpoch;
            this.epochCache.tryAppendEpochEntry(new EpochEntry(currentBlockEpoch, currentEpochStartOffset));
        }

        this.currentTransferOffset = this.defaultMessageStore.getMaxPhyOffset();
    }
}
