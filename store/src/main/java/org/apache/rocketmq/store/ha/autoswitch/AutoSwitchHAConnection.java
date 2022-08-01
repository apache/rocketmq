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
import org.apache.rocketmq.store.ha.netty.HAMessage;
import org.apache.rocketmq.store.ha.netty.HAMessageType;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class AutoSwitchHAConnection implements HAConnection {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService haService;
    private final EpochStore epochStore;
    private final FlowMonitor flowMonitor;
    private final AtomicLong currentTransferOffset = new AtomicLong(-1L);
    private final NettyTransferService transferService;
    private final Channel channel;
    private EpochEntry currentTransferEpochEntry;
    private SelectMappedBufferResult currentTransferBuffer;

    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private volatile long slaveBrokerId = -1L;
    private volatile long slaveAckOffset = -1L;
    private volatile long slaveAckTimestamp = 0L;
    private volatile String slaveBrokerAddress;
    private volatile boolean slaveAsyncLearner = false;

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
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel.close();
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
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

    public String getSlaveBrokerAddress() {
        return slaveBrokerAddress;
    }

    public void setSlaveBrokerAddress(String slaveBrokerAddress) {
        this.slaveBrokerAddress = slaveBrokerAddress;
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
        return slaveBrokerAddress;
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
                            if (this.pushCommitLogDataToSlave()) {
                                this.waitForRunning(10);
                            } else {
                                System.out.println("server exit");
                                close();
                            }
                            continue;
                        case SHUTDOWN:
                            // remove
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
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
                    LOGGER.error("Failed to find an epochEntry to match slaveRequestOffset {}", currentTransferOffset);
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
                this.waitForRunning(100);
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
            HAMessage haMessage = new HAMessage(HAMessageType.PUSH_DATA, haService.getCurrentMasterEpoch());
            PushCommitLogData pushCommitLogData = new PushCommitLogData();
            pushCommitLogData.setEpoch(currentTransferEpochEntry.getEpoch());
            pushCommitLogData.setEpochStartOffset(currentTransferEpochEntry.getStartOffset());
            pushCommitLogData.setConfirmOffset(haService.computeConfirmOffset());
            pushCommitLogData.setStartOffset(currentTransferOffset.get());
            haMessage.appendBody(pushCommitLogData.encode());

            if (maxTransferSize > 0) {
                haMessage.appendBody(currentTransferBuffer.getByteBuffer());
            }

            try {
                channel.writeAndFlush(haMessage).sync();
                if (maxTransferSize > 0) {
                    releaseData();
                }
            } catch (InterruptedException e) {
                LOGGER.error("Netty transfer data error", e);
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
