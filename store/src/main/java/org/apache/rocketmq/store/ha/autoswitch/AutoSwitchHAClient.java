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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import java.io.IOException;
import java.net.SocketAddress;
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
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.netty.TransferMessage;
import org.apache.rocketmq.store.ha.netty.TransferType;
import org.apache.rocketmq.store.ha.netty.NettyTransferClientHandler;
import org.apache.rocketmq.store.ha.netty.NettyTransferDecoder;
import org.apache.rocketmq.store.ha.netty.NettyTransferEncoder;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final AtomicReference<Long> slaveId = new AtomicReference<>();

    private final AutoSwitchHAService haService;
    private final EpochStore epochCache;
    private final DefaultMessageStore defaultMessageStore;
    public EventLoopGroup workerGroup;
    public Bootstrap bootstrap;
    private FlowMonitor flowMonitor;
    private volatile HAConnectionState currentState = HAConnectionState.SHUTDOWN;
    private volatile long currentReceivedEpoch = -1L;
    private volatile long currentTransferOffset = -1L;
    private volatile long lastReadTimestamp;
    private volatile long lastWriteTimestamp;
    private ChannelFuture future;
    private ChannelPromise channelPromise;

    public AutoSwitchHAClient(AutoSwitchHAService haService) {
        this.haService = haService;
        this.defaultMessageStore = haService.getDefaultMessageStore();
        this.epochCache = haService.getEpochStore();
    }

    public void init() throws IOException {
        if (this.flowMonitor == null) {
            this.flowMonitor = new FlowMonitor(this.defaultMessageStore.getMessageStoreConfig());
        }

        // init offset
        this.currentReceivedEpoch = -1L;
        this.currentTransferOffset = -1L;

        startNettyClient();
        changeCurrentState(HAConnectionState.READY);
    }

    public void changePromise(boolean success) {
        if (this.channelPromise != null && !this.channelPromise.isDone()) {
            if (success) {
                this.channelPromise.setSuccess();
            } else {
                this.channelPromise.setFailure(new RuntimeException("promise failure"));
            }
        }
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
        this.slaveId.set(newId);
    }

    public long getCurrentMasterEpoch() {
        return this.haService.getCurrentMasterEpoch();
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
        this.lastReadTimestamp = lastReadTimestamp;
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

    @Override
    public void closeMaster() {
        if (channelPromise != null) {
            channelPromise.setFailure(new RuntimeException("epoch not match"));
        }
        // close channel
        if (future != null && future.channel() != null) {
            try {
                future.channel().close().sync();
                future = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        closeMaster();
        this.flowMonitor.shutdown();
        super.shutdown();
    }

    private void sendHandshakeSlave(Channel channel) {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();
        HandshakeSlave handshakeSlave = new HandshakeSlave();
        handshakeSlave.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeSlave.setBrokerName(brokerConfig.getBrokerName());
        handshakeSlave.setBrokerId(brokerConfig.getBrokerId());
        handshakeSlave.setBrokerAddr(((AutoSwitchHAService) defaultMessageStore.getHaService()).getLocalAddress());
        handshakeSlave.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeSlave.setLanguageCode(LanguageCode.JAVA);
        handshakeSlave.setHaProtocolVersion(2);

        TransferMessage transferMessage = this.haService.buildMessage(TransferType.HANDSHAKE_SLAVE);
        transferMessage.appendBody(RemotingSerializable.encode(handshakeSlave));

        this.lastWriteTimestamp = System.currentTimeMillis();
        channel.writeAndFlush(transferMessage);
    }

    public synchronized void startNettyClient() {
        AutoSwitchHAClient haClient = this;
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_SNDBUF, AutoSwitchHAService.WRITE_MAX_BUFFER_SIZE)
            .option(ChannelOption.SO_RCVBUF, AutoSwitchHAService.READ_MAX_BUFFER_SIZE)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new IdleStateHandler(15, 15, 0))
                        .addLast(new NettyTransferDecoder(haService))
                        .addLast(new NettyTransferEncoder(haService))
                        .addLast(new NettyTransferClientHandler(haClient));
                }
            });
    }

    public void doNettyConnect() throws InterruptedException {
        if (future != null && future.channel() != null && future.channel().isActive()) {
            return;
        }

        if (StringUtils.isBlank(this.masterHaAddress.get())) {
            return;
        }

        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(this.masterHaAddress.get());
        future = bootstrap.connect(socketAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                LOGGER.info("Client connect to server successfully!");
            } else {
                LOGGER.info("Failed to connect to server, try connect after 1000 ms");
            }
        });

        try {
            future.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("HAClient start server InterruptedException", e);
        }
    }

    public synchronized boolean tryConnectToMaster() throws InterruptedException {
        try {
            String address = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(address)) {
                doNettyConnect();
                channelPromise = new DefaultChannelPromise(future.channel());
                sendHandshakeSlave(future.channel());
                channelPromise.await(5000);
                if (channelPromise.isSuccess()) {
                    channelPromise = null;
                    changeCurrentState(HAConnectionState.HANDSHAKE);
                    return true;
                } else {
                    LOGGER.warn("Connector to master failed");
                }
                channelPromise = null;
            }
        } catch (InterruptedException e) {
            LOGGER.error("HAClient send handshake but not receive response, masterAddr:{}", masterHaAddress.get(), e);
            future.channel().close().sync();
        }
        return false;
    }

    private boolean checkConnectionTimeout() {
        long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
        if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
            LOGGER.warn("NettyHAClient housekeeping, found this connection {} expired, interval={}",
                this.masterHaAddress, interval);
            return false;
        }
        return true;
    }

    public void masterHandshake(HandshakeMaster handshakeMaster) {
        if (handshakeMaster != null
            && HandshakeResult.ACCEPT.equals(handshakeMaster.getHandshakeResult())) {
            channelPromise.setSuccess();
            return;
        }
        LOGGER.error("Master reject build connection, {}", handshakeMaster);
        channelPromise.setFailure(new Exception("Master reject build connection"));
    }

    private boolean queryMasterEpoch() throws InterruptedException {
        try {
            TransferMessage transferMessage = haService.buildMessage(TransferType.QUERY_EPOCH);
            channelPromise = new DefaultChannelPromise(future.channel());
            this.lastWriteTimestamp = System.currentTimeMillis();
            future.channel().writeAndFlush(transferMessage);
            channelPromise.await(5000);
            if (channelPromise.isSuccess()) {
                channelPromise = null;
                changeCurrentState(HAConnectionState.TRANSFER);
                return true;
            }
            channelPromise = null;
            return false;
        } catch (InterruptedException e) {
            future.channel().close().sync();
        }
        return true;
    }

    public synchronized void doConsistencyRepairWithMaster(List<EpochEntry> entryList) {
        channelPromise.setSuccess();
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

        sendConfirmTruncateToMaster(currentTransferOffset);
    }

    private void sendConfirmTruncateToMaster(long startOffset) {
        boolean syncFromLastFile = this.defaultMessageStore.getMessageStoreConfig().isSyncFromLastFile();
        ConfirmTruncate confirmTruncate = new ConfirmTruncate(syncFromLastFile, startOffset);
        TransferMessage transferMessage = this.haService.buildMessage(TransferType.CONFIRM_TRUNCATE);
        transferMessage.appendBody(RemotingSerializable.encode(confirmTruncate));
        this.lastWriteTimestamp = System.currentTimeMillis();
        future.channel().writeAndFlush(transferMessage);
    }

    public synchronized void sendPushCommitLogAck() {
        boolean asyncLearner = defaultMessageStore.getMessageStoreConfig().isAsyncLearner();
        PushCommitLogAck pushCommitLogAck = new PushCommitLogAck(this.currentTransferOffset, asyncLearner);
        TransferMessage transferMessage = this.haService.buildMessage(TransferType.TRANSFER_ACK);
        transferMessage.appendBody(RemotingSerializable.encode(pushCommitLogAck));
        this.lastWriteTimestamp = System.currentTimeMillis();
        future.channel().writeAndFlush(transferMessage);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        this.flowMonitor.start();
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case READY:
                        if (!tryConnectToMaster()) {
                            closeMaster();
                            this.waitForRunning(3000);
                        }
                        continue;
                    case HANDSHAKE:
                        if (!queryMasterEpoch()) {
                            closeMaster();
                            this.waitForRunning(3000);
                        }
                        continue;
                    case TRANSFER:
                    case SUSPEND:
                        if (!checkConnectionTimeout()) {
                            closeMaster();
                            break;
                        }
                        this.waitForRunning(500);
                        continue;
                    case SHUTDOWN:
                    default:
                        waitForRunning(1000);
                }
            } catch (Exception e) {
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
