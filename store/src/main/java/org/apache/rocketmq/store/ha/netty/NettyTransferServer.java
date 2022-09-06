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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

public class NettyTransferServer {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
    protected static final int WRITE_MAX_BUFFER_SIZE = 1024 * 1024;

    private final AutoSwitchHAService autoSwitchHAService;
    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;

    public NettyTransferServer(AutoSwitchHAService autoSwitchHAService) {
        this.autoSwitchHAService = autoSwitchHAService;
    }

    public void start() {
        Integer port = autoSwitchHAService.getDefaultMessageStore().getMessageStoreConfig().getHaListenPort();
        try {
            bossEventLoopGroup = new NioEventLoopGroup(2);
            workerEventLoopGroup = new NioEventLoopGroup(2);
            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossEventLoopGroup, workerEventLoopGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_SNDBUF, WRITE_MAX_BUFFER_SIZE)
                .childOption(ChannelOption.SO_RCVBUF, READ_MAX_BUFFER_SIZE)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                    new WriteBufferWaterMark(32 * 1024, 1024 * 1024))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        channel.pipeline()
                            .addLast(new IdleStateHandler(15, 15, 0))
                            .addLast(new NettyTransferDecoder())
                            .addLast(new NettyTransferEncoder())
                            .addLast("serverHandler", new NettyTransferServerHandler(autoSwitchHAService));
                    }
                });
            bootstrap.bind(port).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    LOGGER.info("NettyTransferService start listen at port: {}", port);
                } else {
                    LOGGER.info("NettyTransferService start listen at port: {} failed", port);
                }
            }).sync();
        } catch (Exception e) {
            LOGGER.error("NettyTransferService start exception, port: {} failed", port, e);
            throw new RuntimeException("NettyTransferService start exception", e);
        }
    }

    public void shutdown() {
        if (bossEventLoopGroup != null) {
            bossEventLoopGroup.shutdownGracefully();
        }

        if (workerEventLoopGroup != null) {
            workerEventLoopGroup.shutdownGracefully();
        }
    }

    public void destroyConnections() {
        channelGroup.forEach(channel -> channel.close().syncUninterruptibly());
    }

    public void addChannelToGroup(Channel channel) {
        this.channelGroup.add(channel);
    }

    public ChannelGroup getChannelGroup() {
        return channelGroup;
    }
}
