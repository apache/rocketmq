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
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.netty.TransferMessage;
import org.apache.rocketmq.store.ha.netty.TransferType;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class AutoSwitchHAConnection implements HAConnection {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final EpochStore epochStore;
    private final AutoSwitchHAService haService;
    private final NettyTransferService transferService;

    private final Channel channel;
    private final FlowMonitor flowMonitor;
    private EpochEntry currentTransferEpochEntry;
    private SelectMappedBufferResult currentTransferBuffer;
    private final AtomicLong currentTransferOffset = new AtomicLong(-1L);

    private volatile long slaveAckOffset = -1L;
    private volatile long slaveAckTimestamp = 0L;
    private volatile long slaveBrokerId = -1L;
    private volatile String clientAddress;
    private volatile boolean slaveAsyncLearner = false;
    private volatile HAConnectionState currentState = HAConnectionState.READY;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, Channel channel, EpochStore epochStore) {
        this.haService = haService;
        this.channel = channel;
        this.epochStore = epochStore;
        this.transferService = new NettyTransferService();
        this.flowMonitor = new FlowMonitor(this.haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.READY);
        this.transferService.start();
        this.flowMonitor.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown(true);
        this.close();
    }

    @Override
    public void close() {
        try {
            channel.disconnect().sync();
            channel.close();
        } catch (InterruptedException e) {
            LOGGER.warn("TransferService shutdown exception", e);
        }
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("Change connection state from {} to {}", this.currentState, connectionState);
        this.currentState = connectionState;
    }

    public Channel getChannel() {
        return channel;
    }

    public long getCurrentTransferOffset() {
        return currentTransferOffset.get();
    }

    public void setCurrentTransferOffset(long currentTransferOffset) {
        this.currentTransferOffset.set(currentTransferOffset);
    }

    public long getSlaveAckTimestamp() {
        return slaveAckTimestamp;
    }

    public void setSlaveAckTimestamp(long slaveAckTimestamp) {
        this.slaveAckTimestamp = slaveAckTimestamp;
    }

    public long getSlaveBrokerId() {
        return slaveBrokerId;
    }

    public void setSlaveBrokerId(long slaveBrokerId) {
        this.slaveBrokerId = slaveBrokerId;
    }

    public boolean isSlaveAsyncLearner() {
        return slaveAsyncLearner;
    }

    public void setSlaveAsyncLearner(boolean slaveAsyncLearner) {
        this.slaveAsyncLearner = slaveAsyncLearner;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return null;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    public void updateSlaveTransferProgress(long slaveAckOffset) {
        this.slaveAckOffset = slaveAckOffset;
        this.slaveAckTimestamp = System.currentTimeMillis();
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return currentTransferOffset.get();
    }

    public AutoSwitchHAService getHaService() {
        return haService;
    }

    class NettyTransferService extends ServiceThread {

        @Override
        public String getServiceName() {
            DefaultMessageStore defaultMessageStore = haService.getDefaultMessageStore();
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getLoggerIdentifier() + this.getClass().getSimpleName();
            }
            return this.getClass().getSimpleName();
        }

        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    switch (currentState) {
                        case READY:
                        case HANDSHAKE:
                            this.waitForRunning(10);
                            continue;
                        case TRANSFER:
                            if (!this.pushCommitLogDataToSlave()) {
                                close();
                            }
                            continue;
                        case SHUTDOWN:
                            // remove
                            break;
                    }
                } catch (Exception e) {
                    LOGGER.error(this.getServiceName() + " service has exception.", e);
                }
            }
        }

        public synchronized boolean pushCommitLogDataToSlave() {
            if (-1 == currentTransferOffset.get()) {
                return false;
            }

            // Correct transferEpoch
            if (currentTransferEpochEntry == null) {
                currentTransferEpochEntry = epochStore.findEpochEntryByOffset(currentTransferOffset.get());
                if (currentTransferEpochEntry == null) {
                    LOGGER.error("Failed to find an epochEntry to match request offset {}", currentTransferOffset);
                    return false;
                }
            }

            pushCommitLogDataToSlave0();
            return true;
        }

        protected int getNextTransferDataSize() {
            if (currentTransferBuffer == null || currentTransferBuffer.getSize() <= 0) {
                return 0;
            }
            return Math.min(currentTransferBuffer.getSize(),
                haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize());
        }

        private void pushCommitLogDataToSlave0() {
            currentTransferBuffer = haService.getDefaultMessageStore().getCommitLogData(currentTransferOffset.get());
            int size = this.getNextTransferDataSize();
            if (size == 0) {
                if (currentTransferOffset.get() == currentTransferEpochEntry.getEndOffset()) {
                    long currentEpoch = currentTransferEpochEntry.getEpoch();
                    currentTransferEpochEntry = epochStore.findCeilingEntryByEpoch(currentEpoch);
                }
                doNettyTransferData(0);
                this.releaseData();
                this.waitForRunning(50);
                return;
            }

            // We must ensure that the transmitted logs are within the same epoch
            // currentTransferEpoch == last epoch && endOffset = Long.MAX_VALUE
            currentTransferOffset.set(currentTransferBuffer.getStartOffset());
            final long currentEpochEndOffset = currentTransferEpochEntry.getEndOffset();
            boolean separated = currentTransferOffset.get() + size > currentEpochEndOffset;
            if (separated) {
                size = (int) (currentEpochEndOffset - currentTransferOffset.get());
                long currentEpoch = currentTransferEpochEntry.getEpoch();
                currentTransferEpochEntry = epochStore.findCeilingEntryByEpoch(currentEpoch);
                if (currentTransferEpochEntry == null) {
                    LOGGER.error("Can't find a bigger epochEntry than epoch {}", currentTransferOffset);
                    waitForRunning(100);
                }
            }

            currentTransferBuffer.getByteBuffer().limit(size);
            doNettyTransferData(size);
            currentTransferOffset.addAndGet(size);
        }

        private void doNettyTransferData(int maxTransferSize) {
            PushCommitLogData pushCommitLogData = new PushCommitLogData();
            pushCommitLogData.setEpoch(currentTransferEpochEntry.getEpoch());
            pushCommitLogData.setEpochStartOffset(currentTransferEpochEntry.getStartOffset());
            pushCommitLogData.setConfirmOffset(haService.computeConfirmOffset());
            pushCommitLogData.setBlockStartOffset(currentTransferOffset.get());

            TransferMessage transferMessage = haService.buildMessage(TransferType.TRANSFER_DATA);
            transferMessage.appendBody(pushCommitLogData.encode());
            if (maxTransferSize > 0) {
                transferMessage.appendBody(currentTransferBuffer.getByteBuffer());
            }

            try {
                channel.writeAndFlush(transferMessage).sync();
                if (maxTransferSize > 0) {
                    releaseData();
                }
            } catch (InterruptedException e) {
                LOGGER.error("NettyTransferService transfer data error", e);
                waitForRunning(100);
            }
        }

        protected void releaseData() {
            if (currentTransferBuffer != null) {
                currentTransferBuffer.release();
                currentTransferBuffer = null;
            }
        }
    }
}
