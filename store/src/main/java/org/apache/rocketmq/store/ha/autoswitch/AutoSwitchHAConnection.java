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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.ha.io.HAWriter;

public class AutoSwitchHAConnection implements HAConnection {

    /**
     * Handshake data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬────────────────────────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   EpochEntrySize * EpochEntryNums  │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (12bytes * EpochEntryNums)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┼────────────────────────────────────┤
     * │                       Header                            │             Body                   │
     * │                                                         │                                    │
     * </pre>
     * Handshake Header protocol Format:
     * current state + body size + offset + epoch
     */
    public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8 + 4;

    /**
     * Transfer data protocol in syncing msg from master. Format:
     * <pre>
     * ┌─────────────────┬───────────────┬───────────┬───────────┬─────────────────────┬──────────────────┬──────────────────┐
     * │  current state  │   body size   │   offset  │   epoch   │   epochStartOffset  │   confirmOffset  │    log data      │
     * │     (4bytes)    │   (4bytes)    │  (8bytes) │  (4bytes) │      (8bytes)       │      (8bytes)    │   (data size)    │
     * ├─────────────────┴───────────────┴───────────┴───────────┴─────────────────────┴──────────────────┼──────────────────┤
     * │                                               Header                                             │       Body       │
     * │                                                                                                  │                  │
     * </pre>
     * Transfer Header protocol Format:
     * current state + body size + offset + epoch  + epochStartOffset + additionalInfo(confirmOffset)
     */
    public static final int TRANSFER_HEADER_SIZE = HANDSHAKE_HEADER_SIZE + 8 + 8;
    public static final int EPOCH_ENTRY_SIZE = 12;
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final AutoSwitchHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;
    private final EpochFileCache epochCache;
    private final AbstractWriteSocketService writeSocketService;
    private final ReadSocketService readSocketService;
    private final FlowMonitor flowMonitor;

    private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;
    private volatile long slaveRequestOffset = -1;
    private volatile long slaveAckOffset = -1;
    /**
     * Whether the slave have already sent a handshake message
     */
    private volatile boolean isSlaveSendHandshake = false;
    private volatile int currentTransferEpoch = -1;
    private volatile long currentTransferEpochEndOffset = 0;
    private volatile boolean isSyncFromLastFile = false;
    private volatile boolean isAsyncLearner = false;
    private volatile long slaveId = -1;

    /**
     * Last endOffset when master transfer data to slave
     */
    private volatile long lastMasterMaxOffset = -1;
    /**
     * Last time ms when transfer data to slave.
     */
    private volatile long lastTransferTimeMs = 0;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, SocketChannel socketChannel,
        EpochFileCache epochCache) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.epochCache = epochCache;
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.HANDSHAKE);
        this.flowMonitor.start();
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown(true);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    @Override
    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (final IOException e) {
                LOGGER.error("", e);
            }
        }
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
        this.currentState = connectionState;
    }

    public long getSlaveId() {
        return slaveId;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return this.writeSocketService.getNextTransferFromWhere();
    }

    private void changeTransferEpochToNext(final EpochEntry entry) {
        this.currentTransferEpoch = entry.getEpoch();
        this.currentTransferEpochEndOffset = entry.getEndOffset();
        if (entry.getEpoch() == this.epochCache.lastEpoch()) {
            // Use -1 to stand for Long.max
            this.currentTransferEpochEndOffset = -1;
        }
    }

    public boolean isAsyncLearner() {
        return isAsyncLearner;
    }

    public boolean isSyncFromLastFile() {
        return isSyncFromLastFile;
    }

    private synchronized void updateLastTransferInfo() {
        this.lastMasterMaxOffset = this.haService.getDefaultMessageStore().getMaxPhyOffset();
        this.lastTransferTimeMs = System.currentTimeMillis();
    }

    private synchronized void maybeExpandInSyncStateSet(long slaveMaxOffset) {
        if (!this.isAsyncLearner && slaveMaxOffset >= this.lastMasterMaxOffset) {
            long caughtUpTimeMs = this.haService.getDefaultMessageStore().getMaxPhyOffset() == slaveMaxOffset ? System.currentTimeMillis() : this.lastTransferTimeMs;
            this.haService.updateConnectionLastCaughtUpTime(this.slaveId, caughtUpTimeMs);
            this.haService.maybeExpandInSyncStateSet(this.slaveId, slaveMaxOffset);
        }
    }

    class ReadSocketService extends ServiceThread {
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private final AbstractHAReader haReader;
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
            haReader = new HAServerReader();
            haReader.registerHook(readSize -> {
                if (readSize > 0) {
                    ReadSocketService.this.lastReadTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.haReader.read(this.socketChannel, this.byteBufferRead);
                    if (!ok) {
                        AutoSwitchHAConnection.LOGGER.error("processReadEvent error");
                        break;
                    }

                    long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        LOGGER.warn("ha housekeeping, found this connection[" + clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            changeCurrentState(HAConnectionState.SHUTDOWN);

            writeSocketService.makeStop();

            haService.removeConnection(AutoSwitchHAConnection.this);

            haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }

            flowMonitor.shutdown(true);

            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        class HAServerReader extends AbstractHAReader {
            @Override
            protected boolean processReadResult(ByteBuffer byteBufferRead) {
                while (true) {
                    boolean processSuccess = true;
                    int readSocketPos = byteBufferRead.position();
                    int diff = byteBufferRead.position() - ReadSocketService.this.processPosition;
                    if (diff >= AutoSwitchHAClient.MIN_HEADER_SIZE) {
                        int readPosition = ReadSocketService.this.processPosition;
                        HAConnectionState slaveState = HAConnectionState.values()[byteBufferRead.getInt(readPosition)];

                        switch (slaveState) {
                            case HANDSHAKE:
                                // SlaveBrokerId
                                Long slaveBrokerId = byteBufferRead.getLong(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 8);
                                AutoSwitchHAConnection.this.slaveId = slaveBrokerId;
                                // Flag(isSyncFromLastFile)
                                short syncFromLastFileFlag = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 12);
                                if (syncFromLastFileFlag == 1) {
                                    AutoSwitchHAConnection.this.isSyncFromLastFile = true;
                                }
                                // Flag(isAsyncLearner role)
                                short isAsyncLearner = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 10);
                                if (isAsyncLearner == 1) {
                                    AutoSwitchHAConnection.this.isAsyncLearner = true;
                                }

                                isSlaveSendHandshake = true;
                                byteBufferRead.position(readSocketPos);
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE;
                                LOGGER.info("Receive slave handshake, slaveBrokerId:{}, isSyncFromLastFile:{}, isAsyncLearner:{}",
                                    AutoSwitchHAConnection.this.slaveId, AutoSwitchHAConnection.this.isSyncFromLastFile, AutoSwitchHAConnection.this.isAsyncLearner);
                                break;
                            case TRANSFER:
                                long slaveMaxOffset = byteBufferRead.getLong(readPosition + 4);
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.TRANSFER_HEADER_SIZE;

                                AutoSwitchHAConnection.this.slaveAckOffset = slaveMaxOffset;
                                if (slaveRequestOffset < 0) {
                                    slaveRequestOffset = slaveMaxOffset;
                                }
                                byteBufferRead.position(readSocketPos);
                                maybeExpandInSyncStateSet(slaveMaxOffset);
                                AutoSwitchHAConnection.this.haService.updateConfirmOffsetWhenSlaveAck(AutoSwitchHAConnection.this.slaveId);
                                AutoSwitchHAConnection.this.haService.notifyTransferSome(AutoSwitchHAConnection.this.slaveAckOffset);
                                break;
                            default:
                                LOGGER.error("Current state illegal {}", currentState);
                                return false;
                        }

                        if (!slaveState.equals(currentState)) {
                            LOGGER.warn("Master change state from {} to {}", currentState, slaveState);
                            changeCurrentState(slaveState);
                        }
                        if (processSuccess) {
                            continue;
                        }
                    }

                    if (!byteBufferRead.hasRemaining()) {
                        byteBufferRead.position(ReadSocketService.this.processPosition);
                        byteBufferRead.compact();
                        ReadSocketService.this.processPosition = 0;
                    }
                    break;
                }

                return true;
            }
        }
    }

    class WriteSocketService extends AbstractWriteSocketService {
        private SelectMappedBufferResult selectMappedBufferResult;

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            super(socketChannel);
        }

        @Override
        protected int getNextTransferDataSize() {
            SelectMappedBufferResult selectResult = haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
            if (selectResult == null || selectResult.getSize() <= 0) {
                return 0;
            }
            this.selectMappedBufferResult = selectResult;
            return selectResult.getSize();
        }

        @Override
        protected void releaseData() {
            this.selectMappedBufferResult.release();
            this.selectMappedBufferResult = null;
        }

        @Override
        protected boolean transferData(int maxTransferSize) throws Exception {

            if (null != this.selectMappedBufferResult && maxTransferSize >= 0) {
                this.selectMappedBufferResult.getByteBuffer().limit(maxTransferSize);
            }

            // Write Header
            boolean result = haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }

            if (null == this.selectMappedBufferResult) {
                return true;
            }

            // Write Body
            result = haWriter.write(this.socketChannel, this.selectMappedBufferResult.getByteBuffer());

            if (result) {
                releaseData();
            }
            return result;
        }

        @Override
        protected void onStop() {
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }
    }

    abstract class AbstractWriteSocketService extends ServiceThread {
        protected final Selector selector;
        protected final SocketChannel socketChannel;
        protected final HAWriter haWriter;

        protected final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
        // Store master epochFileCache: (Epoch + startOffset) * 1000
        private final ByteBuffer handShakeBuffer = ByteBuffer.allocate(EPOCH_ENTRY_SIZE * 1000);
        protected long nextTransferFromWhere = -1;
        protected boolean lastWriteOver = true;
        protected long lastWriteTimestamp = System.currentTimeMillis();
        protected long lastPrintTimestamp = System.currentTimeMillis();
        protected long transferOffset = 0;

        public AbstractWriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
            haWriter = new HAWriter();
            haWriter.registerHook(writeSize -> {
                flowMonitor.addByteCountTransferred(writeSize);
                if (writeSize > 0) {
                    AbstractWriteSocketService.this.lastWriteTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        public long getNextTransferFromWhere() {
            return this.nextTransferFromWhere;
        }

        private boolean buildHandshakeBuffer() {
            final List<EpochEntry> epochEntries = AutoSwitchHAConnection.this.epochCache.getAllEntries();
            final int lastEpoch = AutoSwitchHAConnection.this.epochCache.lastEpoch();
            final long maxPhyOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getMaxPhyOffset();
            this.byteBufferHeader.position(0);
            this.byteBufferHeader.limit(HANDSHAKE_HEADER_SIZE);
            // State
            this.byteBufferHeader.putInt(currentState.ordinal());
            // Body size
            this.byteBufferHeader.putInt(epochEntries.size() * EPOCH_ENTRY_SIZE);
            // Offset
            this.byteBufferHeader.putLong(maxPhyOffset);
            // Epoch
            this.byteBufferHeader.putInt(lastEpoch);
            this.byteBufferHeader.flip();

            // EpochEntries
            this.handShakeBuffer.position(0);
            this.handShakeBuffer.limit(EPOCH_ENTRY_SIZE * epochEntries.size());
            for (final EpochEntry entry : epochEntries) {
                if (entry != null) {
                    this.handShakeBuffer.putInt(entry.getEpoch());
                    this.handShakeBuffer.putLong(entry.getStartOffset());
                }
            }
            this.handShakeBuffer.flip();
            LOGGER.info("Master build handshake header: maxEpoch:{}, maxOffset:{}, epochEntries:{}", lastEpoch, maxPhyOffset, epochEntries);
            return true;
        }

        private boolean handshakeWithSlave() throws IOException {
            // Write Header
            boolean result = this.haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }

            // Write Body
            return this.haWriter.write(this.socketChannel, this.handShakeBuffer);
        }

        // Normal transfer method
        private void buildTransferHeaderBuffer(long nextOffset, int bodySize) {

            EpochEntry entry = AutoSwitchHAConnection.this.epochCache.getEntry(AutoSwitchHAConnection.this.currentTransferEpoch);

            if (entry == null) {

                // If broker is started on empty disk and no message entered (nextOffset = -1 and currentTransferEpoch = -1), do not output error log when sending heartbeat
                if (nextOffset != -1 || currentTransferEpoch != -1 || bodySize > 0) {
                    LOGGER.error("Failed to find epochEntry with epoch {} when build msg header", AutoSwitchHAConnection.this.currentTransferEpoch);
                }

                if (bodySize > 0) {
                    return;
                }
                // Maybe it's used for heartbeat
                entry = AutoSwitchHAConnection.this.epochCache.firstEntry();
            }
            // Build Header
            this.byteBufferHeader.position(0);
            this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
            // State
            this.byteBufferHeader.putInt(currentState.ordinal());
            // Body size
            this.byteBufferHeader.putInt(bodySize);
            // Offset
            this.byteBufferHeader.putLong(nextOffset);
            // Epoch
            this.byteBufferHeader.putInt(entry.getEpoch());
            // EpochStartOffset
            this.byteBufferHeader.putLong(entry.getStartOffset());
            // Additional info(confirm offset)
            final long confirmOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getConfirmOffset();
            this.byteBufferHeader.putLong(confirmOffset);
            this.byteBufferHeader.flip();
        }

        private boolean sendHeartbeatIfNeeded() throws Exception {
            long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
            if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                buildTransferHeaderBuffer(this.nextTransferFromWhere, 0);
                return this.transferData(0);
            }
            return true;
        }

        private void transferToSlave() throws Exception {
            if (this.lastWriteOver) {
                this.lastWriteOver = sendHeartbeatIfNeeded();
            } else {
                // maxTransferSize == -1 means to continue transfer remaining data.
                this.lastWriteOver = this.transferData(-1);
            }
            if (!this.lastWriteOver) {
                return;
            }

            int size = this.getNextTransferDataSize();
            if (size > 0) {
                if (size > haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                    size = haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                }
                int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                if (size > canTransferMaxBytes) {
                    if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                        LOGGER.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                            String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                            String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                        lastPrintTimestamp = System.currentTimeMillis();
                    }
                    size = canTransferMaxBytes;
                }
                if (size <= 0) {
                    this.releaseData();
                    this.waitForRunning(100);
                    return;
                }

                // We must ensure that the transmitted logs are within the same epoch
                // If currentEpochEndOffset == -1, means that currentTransferEpoch = last epoch, so the endOffset = Long.max
                final long currentEpochEndOffset = AutoSwitchHAConnection.this.currentTransferEpochEndOffset;
                if (currentEpochEndOffset != -1 && this.nextTransferFromWhere + size > currentEpochEndOffset) {
                    final EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.nextEntry(AutoSwitchHAConnection.this.currentTransferEpoch);
                    if (epochEntry == null) {
                        LOGGER.error("Can't find a bigger epochEntry than epoch {}", AutoSwitchHAConnection.this.currentTransferEpoch);
                        waitForRunning(100);
                        return;
                    }
                    size = (int) (currentEpochEndOffset - this.nextTransferFromWhere);
                    changeTransferEpochToNext(epochEntry);
                }

                this.transferOffset = this.nextTransferFromWhere;
                this.nextTransferFromWhere += size;
                updateLastTransferInfo();

                // Build Header
                buildTransferHeaderBuffer(this.transferOffset, size);

                this.lastWriteOver = this.transferData(size);
            } else {
                // If size == 0, we should update the lastCatchupTimeMs
                AutoSwitchHAConnection.this.haService.updateConnectionLastCaughtUpTime(AutoSwitchHAConnection.this.slaveId, System.currentTimeMillis());
                haService.getWaitNotifyObject().allWaitForRunning(100);
            }
        }

        @Override
        public void run() {
            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    switch (currentState) {
                        case HANDSHAKE:
                            // Wait until the slave send it handshake msg to master.
                            if (!isSlaveSendHandshake) {
                                this.waitForRunning(10);
                                continue;
                            }

                            if (this.lastWriteOver) {
                                if (!buildHandshakeBuffer()) {
                                    LOGGER.error("AutoSwitchHAConnection build handshake buffer failed");
                                    this.waitForRunning(5000);
                                    continue;
                                }
                            }

                            this.lastWriteOver = handshakeWithSlave();
                            if (this.lastWriteOver) {
                                // change flag to {false} to wait for slave notification
                                isSlaveSendHandshake = false;
                            }
                            break;
                        case TRANSFER:
                            if (-1 == slaveRequestOffset) {
                                this.waitForRunning(10);
                                continue;
                            }

                            if (-1 == this.nextTransferFromWhere) {
                                if (0 == slaveRequestOffset) {
                                    // We must ensure that the starting point of syncing log
                                    // must be the startOffset of a file (maybe the last file, or the minOffset)
                                    final MessageStoreConfig config = haService.getDefaultMessageStore().getMessageStoreConfig();
                                    if (AutoSwitchHAConnection.this.isSyncFromLastFile) {
                                        long masterOffset = haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                                        masterOffset = masterOffset - (masterOffset % config.getMappedFileSizeCommitLog());
                                        if (masterOffset < 0) {
                                            masterOffset = 0;
                                        }
                                        this.nextTransferFromWhere = masterOffset;
                                    } else {
                                        this.nextTransferFromWhere = haService.getDefaultMessageStore().getCommitLog().getMinOffset();
                                    }
                                } else {
                                    this.nextTransferFromWhere = slaveRequestOffset;
                                }

                                // nextTransferFromWhere is not found. It may be empty disk and no message is entered
                                if (this.nextTransferFromWhere == -1) {
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                // Setup initial transferEpoch
                                EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.findEpochEntryByOffset(this.nextTransferFromWhere);
                                if (epochEntry == null) {
                                    LOGGER.error("Failed to find an epochEntry to match nextTransferFromWhere {}", this.nextTransferFromWhere);
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                changeTransferEpochToNext(epochEntry);
                                LOGGER.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}",
                                    AutoSwitchHAConnection.this.clientAddress, this.nextTransferFromWhere, epochEntry);
                            }
                            transferToSlave();
                            break;
                        default:
                            throw new Exception("unexpected state " + currentState);
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.onStop();

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(AutoSwitchHAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }

            flowMonitor.shutdown(true);

            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        abstract protected int getNextTransferDataSize();

        abstract protected void releaseData();

        abstract protected boolean transferData(int maxTransferSize) throws Exception;

        abstract protected void onStop();
    }
}
