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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.ha.io.HAWriter;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    /**
     * Handshake header buffer size. Schema: state ordinal + Two flags + slaveBrokerId. Format:
     *
     * <pre>
     *                   ┌──────────────────┬───────────────┐
     *                   │isSyncFromLastFile│ isAsyncLearner│
     *                   │     (2bytes)     │   (2bytes)    │
     *                   └──────────────────┴───────────────┘
     *                     \                              /
     *                      \                            /
     *                       ╲                          /
     *                        ╲                        /
     * ┌───────────────────────┬───────────────────────┬───────────────────────┐
     * │      current state    │          Flags        │      slaveBrokerId    │
     * │         (4bytes)      │         (4bytes)      │         (8bytes)      │
     * ├───────────────────────┴───────────────────────┴───────────────────────┤
     * │                                                                       │
     * │                          HANDSHAKE  Header                            │
     * </pre>
     * <p>
     * Flag: isSyncFromLastFile(short), isAsyncLearner(short)... we can add more flags in the future if needed
     */
    public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8;

    /**
     * Header + slaveAddress, Format:
     * <pre>
     *                   ┌──────────────────┬───────────────┐
     *                   │isSyncFromLastFile│ isAsyncLearner│
     *                   │     (2bytes)     │   (2bytes)    │
     *                   └──────────────────┴───────────────┘
     *                     \                              /
     *                      \                            /
     *                       ╲                          /
     *                        ╲                        /
     * ┌───────────────────────┬───────────────────────┬───────────────────────┬───────────────────────────────┐
     * │      current state    │          Flags        │  slaveAddressLength   │          slaveAddress         │
     * │         (4bytes)      │         (4bytes)      │         (4bytes)      │             (50bytes)         │
     * ├───────────────────────┴───────────────────────┴───────────────────────┼───────────────────────────────┤
     * │                                                                       │                               │
     * │                        HANDSHAKE  Header                              │               body            │
     * </pre>
     */
    @Deprecated
    public static final int HANDSHAKE_SIZE = HANDSHAKE_HEADER_SIZE + 50;

    /**
     * Transfer header buffer size. Schema: state ordinal + maxOffset. Format:
     * <pre>
     * ┌───────────────────────┬───────────────────────┐
     * │      current state    │        maxOffset      │
     * │         (4bytes)      │         (8bytes)      │
     * ├───────────────────────┴───────────────────────┤
     * │                                               │
     * │                TRANSFER  Header               │
     * </pre>
     */
    public static final int TRANSFER_HEADER_SIZE = 4 + 8;
    public static final int MIN_HEADER_SIZE = Math.min(HANDSHAKE_HEADER_SIZE, TRANSFER_HEADER_SIZE);
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final ByteBuffer handshakeHeaderBuffer = ByteBuffer.allocate(HANDSHAKE_HEADER_SIZE);
    private final ByteBuffer transferHeaderBuffer = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
    private final AutoSwitchHAService haService;
    private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private final DefaultMessageStore messageStore;
    private final EpochFileCache epochCache;

    private final Long brokerId;

    private SocketChannel socketChannel;
    private Selector selector;
    private AbstractHAReader haReader;
    private HAWriter haWriter;
    private FlowMonitor flowMonitor;
    /**
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp;
    /**
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp;

    private long currentReportedOffset;
    private int processPosition;
    private volatile HAConnectionState currentState;
    /**
     * Current epoch
     */
    private volatile int currentReceivedEpoch;

    public AutoSwitchHAClient(AutoSwitchHAService haService, DefaultMessageStore defaultMessageStore,
        EpochFileCache epochCache, Long brokerId) throws IOException {
        this.haService = haService;
        this.messageStore = defaultMessageStore;
        this.epochCache = epochCache;
        this.brokerId = brokerId;
        init();
    }

    public void init() throws IOException {
        this.selector = NetworkUtil.openSelector();
        this.flowMonitor = new FlowMonitor(this.messageStore.getMessageStoreConfig());
        this.haReader = new HAClientReader();
        haReader.registerHook(readSize -> {
            if (readSize > 0) {
                AutoSwitchHAClient.this.flowMonitor.addByteCountTransferred(readSize);
                lastReadTimestamp = System.currentTimeMillis();
            }
        });
        this.haWriter = new HAWriter();
        haWriter.registerHook(writeSize -> {
            if (writeSize > 0) {
                lastWriteTimestamp = System.currentTimeMillis();
            }
        });
        changeCurrentState(HAConnectionState.READY);
        this.currentReceivedEpoch = -1;
        this.currentReportedOffset = 0;
        this.processPosition = 0;
        this.lastReadTimestamp = System.currentTimeMillis();
        this.lastWriteTimestamp = System.currentTimeMillis();
    }

    public void reOpen() throws IOException {
        shutdown();
        init();
    }

    @Override
    public String getServiceName() {
        if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
            return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + AutoSwitchHAClient.class.getSimpleName();
        }
        return AutoSwitchHAClient.class.getSimpleName();
    }

    @Override
    public void updateMasterAddress(String newAddress) {
        String currentAddr = this.masterAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master address, OLD: " + currentAddr + " NEW: " + newAddress);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddress) {
        String currentAddr = this.masterHaAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterHaAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddress);
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

    @Override
    public long getLastReadTimestamp() {
        return this.lastReadTimestamp;
    }

    @Override
    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return this.currentState;
    }

    @Override
    public void changeCurrentState(HAConnectionState haConnectionState) {
        LOGGER.info("change state to {}", haConnectionState);
        this.currentState = haConnectionState;
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    @Override
    public void closeMaster() {
        if (null != this.socketChannel) {
            try {
                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();
                this.socketChannel = null;

                LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                LOGGER.warn("CloseMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.processPosition = 0;

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        // Shutdown thread firstly
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            LOGGER.warn("Close the selector of AutoSwitchHAClient error, ", e);
        }
    }

    private boolean isTimeToReportOffset() {
        long interval = this.messageStore.now() - this.lastWriteTimestamp;
        return interval > this.messageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean sendHandshakeHeader() throws IOException {
        this.handshakeHeaderBuffer.position(0);
        this.handshakeHeaderBuffer.limit(HANDSHAKE_HEADER_SIZE);
        // Original state
        this.handshakeHeaderBuffer.putInt(HAConnectionState.HANDSHAKE.ordinal());
        // IsSyncFromLastFile
        short isSyncFromLastFile = this.haService.getDefaultMessageStore().getMessageStoreConfig().isSyncFromLastFile() ? (short) 1 : (short) 0;
        this.handshakeHeaderBuffer.putShort(isSyncFromLastFile);
        // IsAsyncLearner role
        short isAsyncLearner = this.haService.getDefaultMessageStore().getMessageStoreConfig().isAsyncLearner() ? (short) 1 : (short) 0;
        this.handshakeHeaderBuffer.putShort(isAsyncLearner);
        // Slave brokerId
        this.handshakeHeaderBuffer.putLong(this.brokerId);

        this.handshakeHeaderBuffer.flip();
        return this.haWriter.write(this.socketChannel, this.handshakeHeaderBuffer);
    }

    private void handshakeWithMaster() throws IOException {
        boolean result = this.sendHandshakeHeader();
        if (!result) {
            closeMasterAndWait();
        }

        this.selector.select(5000);

        result = this.haReader.read(this.socketChannel, this.byteBufferRead);
        if (!result) {
            closeMasterAndWait();
        }
    }

    private boolean reportSlaveOffset(HAConnectionState currentState, final long offsetToReport) throws IOException {
        this.transferHeaderBuffer.position(0);
        this.transferHeaderBuffer.limit(TRANSFER_HEADER_SIZE);
        this.transferHeaderBuffer.putInt(currentState.ordinal());
        this.transferHeaderBuffer.putLong(offsetToReport);
        this.transferHeaderBuffer.flip();
        return this.haWriter.write(this.socketChannel, this.transferHeaderBuffer);
    }

    private boolean reportSlaveMaxOffset(HAConnectionState currentState) throws IOException {
        boolean result = true;
        final long maxPhyOffset = this.messageStore.getMaxPhyOffset();
        if (maxPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = maxPhyOffset;
            result = reportSlaveOffset(currentState, this.currentReportedOffset);
        }
        return result;
    }

    public boolean connectMaster() throws IOException {
        if (null == this.socketChannel) {
            String addr = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(addr)) {
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    LOGGER.info("AutoSwitchHAClient connect to master {}", addr);
                    changeCurrentState(HAConnectionState.HANDSHAKE);
                }
            }
            this.currentReportedOffset = this.messageStore.getMaxPhyOffset();
            this.lastReadTimestamp = System.currentTimeMillis();
        }
        return this.socketChannel != null;
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        if (isTimeToReportOffset()) {
            LOGGER.info("Slave report current offset {}", this.currentReportedOffset);
            result = reportSlaveOffset(HAConnectionState.TRANSFER, this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        this.selector.select(1000);

        result = this.haReader.read(this.socketChannel, this.byteBufferRead);
        if (!result) {
            return false;
        }

        return this.reportSlaveMaxOffset(HAConnectionState.TRANSFER);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        this.flowMonitor.start();
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        this.flowMonitor.shutdown(true);
                        return;
                    case READY:
                        // Truncate invalid msg first
                        final long truncateOffset = AutoSwitchHAClient.this.haService.truncateInvalidMsg();
                        if (truncateOffset >= 0) {
                            AutoSwitchHAClient.this.epochCache.truncateSuffixByOffset(truncateOffset);
                        }
                        if (!connectMaster()) {
                            LOGGER.warn("AutoSwitchHAClient connect to master {} failed", this.masterHaAddress.get());
                            waitForRunning(1000 * 5);
                        }
                        continue;
                    case HANDSHAKE:
                        handshakeWithMaster();
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    case SUSPEND:
                    default:
                        waitForRunning(1000 * 5);
                        continue;
                }
                long interval = this.messageStore.now() - this.lastReadTimestamp;
                if (interval > this.messageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    LOGGER.warn("AutoSwitchHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    closeMaster();
                    LOGGER.warn("AutoSwitchHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                closeMasterAndWait();
            }
        }

        this.flowMonitor.shutdown(true);
        LOGGER.info(this.getServiceName() + " service end");
    }

    /**
     * Compare the master and slave's epoch file, find consistent point, do truncate.
     */
    private boolean doTruncate(List<EpochEntry> masterEpochEntries, long masterEndOffset) throws Exception {
        if (this.epochCache.getEntrySize() == 0) {
            // If epochMap is empty, means the broker is a new replicas
            LOGGER.info("Slave local epochCache is empty, skip truncate log");
            changeCurrentState(HAConnectionState.TRANSFER);
            this.currentReportedOffset = 0;
        } else {
            final EpochFileCache masterEpochCache = new EpochFileCache();
            masterEpochCache.initCacheFromEntries(masterEpochEntries);
            masterEpochCache.setLastEpochEntryEndOffset(masterEndOffset);
            final List<EpochEntry> localEpochEntries = this.epochCache.getAllEntries();
            final EpochFileCache localEpochCache = new EpochFileCache();
            localEpochCache.initCacheFromEntries(localEpochEntries);
            localEpochCache.setLastEpochEntryEndOffset(this.messageStore.getMaxPhyOffset());

            LOGGER.info("master epoch entries is {}", masterEpochCache.getAllEntries());
            LOGGER.info("local epoch entries is {}", localEpochEntries);

            final long truncateOffset = localEpochCache.findConsistentPoint(masterEpochCache);

            LOGGER.info("truncateOffset is {}", truncateOffset);

            if (truncateOffset < 0) {
                // If truncateOffset < 0, means we can't find a consistent point
                LOGGER.error("Failed to find a consistent point between masterEpoch:{} and slaveEpoch:{}", masterEpochEntries, localEpochEntries);
                return false;
            }
            if (!this.messageStore.truncateFiles(truncateOffset)) {
                LOGGER.error("Failed to truncate slave log to {}", truncateOffset);
                return false;
            }
            this.epochCache.truncateSuffixByOffset(truncateOffset);
            LOGGER.info("Truncate slave log to {} success, change to transfer state", truncateOffset);
            changeCurrentState(HAConnectionState.TRANSFER);
            this.currentReportedOffset = truncateOffset;
        }
        if (!reportSlaveMaxOffset(HAConnectionState.TRANSFER)) {
            LOGGER.error("AutoSwitchHAClient report max offset to master failed");
            return false;
        }
        return true;
    }

    class HAClientReader extends AbstractHAReader {

        @Override
        protected boolean processReadResult(ByteBuffer byteBufferRead) {
            int readSocketPos = byteBufferRead.position();
            try {
                while (true) {
                    int diff = byteBufferRead.position() - AutoSwitchHAClient.this.processPosition;
                    if (diff >= AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE) {
                        final int processPosition = AutoSwitchHAClient.this.processPosition;
                        int masterState = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE - 20);
                        int bodySize = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE - 16);
                        long masterOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE - 12);
                        int masterEpoch = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE - 4);
                        long masterEpochStartOffset = 0;
                        long confirmOffset = 0;
                        // If master send transfer header data, set masterEpochStartOffset and confirmOffset value.
                        if (masterState == HAConnectionState.TRANSFER.ordinal() && diff >= AutoSwitchHAConnection.TRANSFER_HEADER_SIZE) {
                            masterEpochStartOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.TRANSFER_HEADER_SIZE - 16);
                            confirmOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.TRANSFER_HEADER_SIZE - 8);
                        }
                        if (masterState != AutoSwitchHAClient.this.currentState.ordinal()) {
                            int headerSize = masterState == HAConnectionState.TRANSFER.ordinal() ? AutoSwitchHAConnection.TRANSFER_HEADER_SIZE : AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE;
                            AutoSwitchHAClient.this.processPosition += headerSize + bodySize;
                            AutoSwitchHAClient.this.waitForRunning(1);
                            LOGGER.error("State not matched, masterState:{}, slaveState:{}, bodySize:{}, offset:{}, masterEpoch:{}, masterEpochStartOffset:{}, confirmOffset:{}",
                                HAConnectionState.values()[masterState], AutoSwitchHAClient.this.currentState, bodySize, masterOffset, masterEpoch, masterEpochStartOffset, confirmOffset);
                            return false;
                        }

                        // Flag whether the received data is complete
                        boolean isComplete = true;
                        switch (AutoSwitchHAClient.this.currentState) {
                            case HANDSHAKE: {
                                if (diff < AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE + bodySize) {
                                    // The received HANDSHAKE data is not complete
                                    isComplete = false;
                                    break;
                                }
                                AutoSwitchHAClient.this.processPosition += AutoSwitchHAConnection.HANDSHAKE_HEADER_SIZE;
                                // Truncate log
                                int entrySize = AutoSwitchHAConnection.EPOCH_ENTRY_SIZE;
                                final int entryNums = bodySize / entrySize;
                                final ArrayList<EpochEntry> epochEntries = new ArrayList<>(entryNums);
                                for (int i = 0; i < entryNums; i++) {
                                    int epoch = byteBufferRead.getInt(AutoSwitchHAClient.this.processPosition + i * entrySize);
                                    long startOffset = byteBufferRead.getLong(AutoSwitchHAClient.this.processPosition + i * entrySize + 4);
                                    epochEntries.add(new EpochEntry(epoch, startOffset));
                                }
                                byteBufferRead.position(readSocketPos);
                                AutoSwitchHAClient.this.processPosition += bodySize;
                                LOGGER.info("Receive handshake, masterMaxPosition {}, masterEpochEntries:{}, try truncate log", masterOffset, epochEntries);
                                if (!doTruncate(epochEntries, masterOffset)) {
                                    waitForRunning(1000 * 2);
                                    LOGGER.error("AutoSwitchHAClient truncate log failed in handshake state");
                                    return false;
                                }
                            }
                            break;
                            case TRANSFER: {
                                if (diff < AutoSwitchHAConnection.TRANSFER_HEADER_SIZE + bodySize) {
                                    // The received TRANSFER data is not complete
                                    isComplete = false;
                                    break;
                                }
                                byte[] bodyData = new byte[bodySize];
                                byteBufferRead.position(AutoSwitchHAClient.this.processPosition + AutoSwitchHAConnection.TRANSFER_HEADER_SIZE);
                                byteBufferRead.get(bodyData);
                                byteBufferRead.position(readSocketPos);
                                AutoSwitchHAClient.this.processPosition += AutoSwitchHAConnection.TRANSFER_HEADER_SIZE + bodySize;
                                long slavePhyOffset = AutoSwitchHAClient.this.messageStore.getMaxPhyOffset();
                                if (slavePhyOffset != 0) {
                                    if (slavePhyOffset != masterOffset) {
                                        LOGGER.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                            + slavePhyOffset + " MASTER: " + masterOffset);
                                        return false;
                                    }
                                }

                                // If epoch changed
                                if (masterEpoch != AutoSwitchHAClient.this.currentReceivedEpoch) {
                                    AutoSwitchHAClient.this.currentReceivedEpoch = masterEpoch;
                                    AutoSwitchHAClient.this.epochCache.appendEntry(new EpochEntry(masterEpoch, masterEpochStartOffset));
                                }

                                if (bodySize > 0) {
                                    AutoSwitchHAClient.this.messageStore.appendToCommitLog(masterOffset, bodyData, 0, bodyData.length);
                                }

                                haService.getDefaultMessageStore().setConfirmOffset(Math.min(confirmOffset, messageStore.getMaxPhyOffset()));

                                if (!reportSlaveMaxOffset(HAConnectionState.TRANSFER)) {
                                    LOGGER.error("AutoSwitchHAClient report max offset to master failed");
                                    return false;
                                }
                                break;
                            }
                            default:
                                break;
                        }
                        if (isComplete) {
                            continue;
                        }

                    }

                    if (!byteBufferRead.hasRemaining()) {
                        byteBufferRead.position(AutoSwitchHAClient.this.processPosition);
                        byteBufferRead.compact();
                        AutoSwitchHAClient.this.processPosition = 0;
                    }

                    break;
                }
            } catch (final Exception e) {
                LOGGER.error("Error when ha client process read request", e);
            }
            return true;
        }
    }
}
