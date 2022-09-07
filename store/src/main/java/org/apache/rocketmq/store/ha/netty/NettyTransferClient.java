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

package org.apache.rocketmq.store.ha.netty;

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
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAClient;
import org.apache.rocketmq.store.ha.autoswitch.EpochStore;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;

public class NettyTransferClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAClient haClient;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private ChannelFuture channelFuture;
    private Object rpcResponseObject;
    private ChannelPromise channelPromise;
    private FlowMonitor flowMonitor;

    private Long lastReadTimestamp;
    private Long lastWriteTimestamp;

    public NettyTransferClient(AutoSwitchHAClient autoSwitchHAClient) {
        this.haClient = autoSwitchHAClient;
    }

    public void init() {
        if (this.flowMonitor == null) {
            this.flowMonitor = new FlowMonitor(
                this.haClient.getHaService().getDefaultMessageStore().getMessageStoreConfig());
            this.flowMonitor.start();
        }

        eventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_SNDBUF, NettyTransferServer.WRITE_MAX_BUFFER_SIZE)
            .option(ChannelOption.SO_RCVBUF, NettyTransferServer.READ_MAX_BUFFER_SIZE)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast(new IdleStateHandler(15, 15, 0))
                        .addLast(new NettyTransferDecoder())
                        .addLast(new NettyTransferEncoder())
                        .addLast(new NettyTransferClientHandler(haClient));
                }
            });
    }

    public void close() {
        // close channel
        if (channelFuture != null && channelFuture.channel() != null) {
            try {
                channelFuture.channel().close().sync();
                channelFuture = null;
            } catch (InterruptedException e) {
                LOGGER.error("close channel error", e);
            }
        }
    }

    public void shutdown() {
        this.flowMonitor.shutdown();
        this.eventLoopGroup.shutdownGracefully();
    }

    public boolean doNettyConnect() {
        if (channelFuture != null && channelFuture.channel() != null && channelFuture.channel().isActive()) {
            return true;
        }

        String serverAddress = this.haClient.getHaMasterAddress();
        if (StringUtils.isBlank(serverAddress)) {
            return false;
        }

        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(serverAddress);
        channelFuture = bootstrap.connect(socketAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                LOGGER.info("Client connect to server successfully!");
            } else {
                LOGGER.info("Failed to connect to server, try connect after 1000 ms");
            }
        });

        try {
            channelFuture.await();
            return true;
        } catch (InterruptedException e) {
            LOGGER.warn("ha client connect to server interrupt", e);
        }
        return false;
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

    private boolean checkConnectionTimeout() {
        DefaultMessageStore defaultMessageStore = this.haClient.getHaService().getDefaultMessageStore();
        long interval = defaultMessageStore.now() - this.lastReadTimestamp;
        if (interval > defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
            LOGGER.warn("NettyHAClient housekeeping, found this connection {} expired, interval={}",
                this.haClient.getHaMasterAddress(), interval);
            return false;
        }
        return true;
    }

    public boolean writeMessageAndWait(TransferMessage transferMessage) {
        try {
            boolean connect = doNettyConnect();
            if (!connect) {
                return false;
            }
            Channel channel = channelFuture.channel();
            channelPromise = new DefaultChannelPromise(channel);
            channel.writeAndFlush(transferMessage).syncUninterruptibly();
            channelPromise.await(5, TimeUnit.SECONDS);
            return channelPromise.isSuccess();
        } catch (InterruptedException e) {
            LOGGER.error("transfer client send message but not receive response", e);
        }
        channelPromise = null;
        return false;
    }

    public void writeMessage(TransferMessage transferMessage) {
        channelFuture.channel().writeAndFlush(transferMessage);
    }

    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    public void setLastReadTimestamp(long lastReadTimestamp) {
        this.lastReadTimestamp = lastReadTimestamp;
    }

    public long getLastWriteTimestamp() {
        return lastWriteTimestamp;
    }

    public void setLastWriteTimestamp(long lastWriteTimestamp) {
        this.lastWriteTimestamp = lastWriteTimestamp;
    }

    public Object getRpcResponseObject() {
        return rpcResponseObject;
    }

    public void setRpcResponseObject(Object rpcResponseObject) {
        this.rpcResponseObject = rpcResponseObject;
    }
}
