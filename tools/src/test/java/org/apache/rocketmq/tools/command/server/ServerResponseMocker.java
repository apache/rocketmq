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
package org.apache.rocketmq.tools.command.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/**
 * mock server response for command
 */
public abstract class ServerResponseMocker {

    private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    @Before
    public void before() {
        start();
    }

    @After
    public void shutdown() {
        if (eventLoopGroup.isShutdown()) {
            return;
        }
        Future<?> future = eventLoopGroup.shutdownGracefully();
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected abstract int getPort();

    protected abstract byte[] getBody();

    public void start() {
        start(null);
    }

    public void start(HashMap<String, String> extMap) {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 65535)
                .childOption(ChannelOption.SO_RCVBUF, 65535)
                .localAddress(new InetSocketAddress(getPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(eventLoopGroup,
                                        new NettyEncoder(),
                                        new NettyDecoder(),
                                        new IdleStateHandler(0, 0, 120),
                                        new ChannelDuplexHandler(),
                                        new NettyServerHandler(extMap)
                                );
                    }
                });
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }
    }

    @ChannelHandler.Sharable
    private class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        private HashMap<String, String> extMap;

        public NettyServerHandler(HashMap<String, String> extMap) {
            this.extMap = extMap;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            String remark = "mock data";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, remark);
            response.setOpaque(msg.getOpaque());
            response.setBody(getBody());

            if (extMap != null && extMap.size() > 0) {
                response.setExtFields(extMap);
            }
            ctx.writeAndFlush(response);
        }
    }

    public static ServerResponseMocker startServer(int port, byte[] body) {
        return startServer(port, body, null);
    }


    public static ServerResponseMocker startServer(int port, byte[] body, HashMap<String, String> extMap) {
        ServerResponseMocker mocker = new ServerResponseMocker() {
            @Override
            protected int getPort() {
                return port;
            }

            @Override
            protected byte[] getBody() {
                return body;
            }
        };
        mocker.start(extMap);
        // add jvm hook, close connection when jvm down
        Runtime.getRuntime().addShutdownHook(new Thread(mocker::shutdown));
        return mocker;
    }
}
