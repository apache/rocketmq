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

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DefaultHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
    private SocketChannel socketChannel;
    private Selector selector;
    /**
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();

    private long currentReportedOffset = 0;
    private int dispatchPosition = 0;
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = RemotingUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }

    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    private boolean isTimeToReportOffset() {
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean reportSlaveMaxOffset(final long maxOffset) {
        this.reportOffset.position(0);
        this.reportOffset.limit(8);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(8);

        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                    + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        return !this.reportOffset.hasRemaining();
    }

    private void reallocateByteBuffer() {
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }

        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    flowMonitor.addByteCountTransferred(readSize);
                    readSizeZeroTimes = 0;
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        return false;
                    }
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        return true;
    }

    private boolean dispatchReadRequest() {
        final int msgHeaderSize = 8 + 4; // phyoffset + size
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            if (diff >= msgHeaderSize) {
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();

                if (slavePhyOffset != 0) {
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                            + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }

                if (diff >= (msgHeaderSize + bodySize)) {
                    byte[] bodyData = byteBufferRead.array();
                    int dataStart = this.dispatchPosition + msgHeaderSize;

                    this.defaultMessageStore.appendToCommitLog(
                        masterPhyOffset, bodyData, dataStart, bodySize);

                    this.byteBufferRead.position(readSocketPos);
                    this.dispatchPosition += msgHeaderSize + bodySize;

                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }

                    continue;
                }
            }

            if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = currentPhyOffset;
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingUtil.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }

            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();

            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }

    public void closeMaster() {
        if (null != this.socketChannel) {
            try {

                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();

                this.socketChannel = null;

                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        return;
                    case READY:
                        if (!this.connectMaster()) {
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            this.waitForRunning(1000 * 5);
                        }
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }
                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        this.selector.select(1000);

        result = this.processReadEvent();
        if (!result) {
            return false;
        }

        return reportSlaveMaxOffsetPlus();
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getLoggerIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}
