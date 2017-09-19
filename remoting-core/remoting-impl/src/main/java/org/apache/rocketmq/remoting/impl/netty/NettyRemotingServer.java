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

package org.apache.rocketmq.remoting.impl.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingServer;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.channel.NettyChannelImpl;
import org.apache.rocketmq.remoting.impl.netty.handler.ChannelStatistics;
import org.apache.rocketmq.remoting.impl.netty.handler.ProtocolSelector;
import org.apache.rocketmq.remoting.internal.JvmUtils;

public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private final RemotingConfig serverConfig;

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup ioGroup;
    private EventExecutorGroup workerGroup;
    private Class<? extends ServerSocketChannel> socketChannelClass;

    private int port;
    private SslContext sslContext;

    NettyRemotingServer(final RemotingConfig serverConfig) {
        super(serverConfig);

        this.serverBootstrap = new ServerBootstrap();
        this.serverConfig = serverConfig;

        if (JvmUtils.isLinux() && this.serverConfig.isServerNativeEpollEnable()) {
            this.ioGroup = new EpollEventLoopGroup(serverConfig.getServerIoThreads(), ThreadUtils.newGenericThreadFactory("NettyEpollIoThreads",
                serverConfig.getServerIoThreads()));

            this.bossGroup = new EpollEventLoopGroup(serverConfig.getServerAcceptorThreads(), ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                serverConfig.getServerAcceptorThreads()));

            this.socketChannelClass = EpollServerSocketChannel.class;
        } else {
            this.bossGroup = new NioEventLoopGroup(serverConfig.getServerAcceptorThreads(), ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                serverConfig.getServerAcceptorThreads()));

            this.ioGroup = new NioEventLoopGroup(serverConfig.getServerIoThreads(), ThreadUtils.newGenericThreadFactory("NettyNioIoThreads",
                serverConfig.getServerIoThreads()));

            this.socketChannelClass = NioServerSocketChannel.class;
        }

        this.workerGroup = new DefaultEventExecutorGroup(serverConfig.getServerWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyWorkerThreads", serverConfig.getServerWorkerThreads()));

        buildHttp2SslContext();
    }

    private void buildHttp2SslContext() {
        try {
            SslProvider provider = OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
            SelfSignedCertificate ssc;
            //NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
            //Please refer to the HTTP/2 specification for cipher requirements.
            ssc = new SelfSignedCertificate();
            sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .sslProvider(provider)
                .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE).build();
        } catch (Exception e) {
            LOG.error("Can not build SSL context !", e);
        }
    }

    private void applyOptions(ServerBootstrap bootstrap) {
        //option() is for the NioServerSocketChannel that accepts incoming connections.
        //childOption() is for the Channels accepted by the parent ServerChannel, which is NioServerSocketChannel in this case
        if (null != serverConfig) {
            if (serverConfig.getTcpSoBacklogSize() > 0) {
                bootstrap.option(ChannelOption.SO_BACKLOG, serverConfig.getTcpSoBacklogSize());
            }

            if (serverConfig.getTcpSoLinger() > 0) {
                bootstrap.option(ChannelOption.SO_LINGER, serverConfig.getTcpSoLinger());
            }

            if (serverConfig.getTcpSoSndBufSize() > 0) {
                bootstrap.childOption(ChannelOption.SO_SNDBUF, serverConfig.getTcpSoSndBufSize());
            }
            if (serverConfig.getTcpSoRcvBufSize() > 0) {
                bootstrap.childOption(ChannelOption.SO_RCVBUF, serverConfig.getTcpSoRcvBufSize());
            }

            bootstrap.option(ChannelOption.SO_REUSEADDR, serverConfig.isTcpSoReuseAddress()).
                childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isTcpSoKeepAlive()).
                childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpSoNoDelay()).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, serverConfig.getTcpSoTimeout());
        }

        if (serverConfig.isServerPooledBytebufAllocatorEnable()) {
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
    }

    @Override
    public void start() {
        super.start();

        final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.serverBootstrap.group(this.bossGroup, this.ioGroup).
            channel(socketChannelClass).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                channels.add(ch);

                ChannelPipeline cp = ch.pipeline();

                cp.addLast(ChannelStatistics.NAME, new ChannelStatistics(channels));

                cp.addFirst(ProtocolSelector.NAME, new ProtocolSelector(sslContext));
                cp.addLast(workerGroup, new IdleStateHandler(serverConfig.getConnectionChannelReaderIdleSeconds(),
                        serverConfig.getConnectionChannelWriterIdleSeconds(),
                        serverConfig.getConnectionChannelIdleSeconds()),
                    new ServerConnectionHandler(),
                    new EventDispatcher());
            }
        });

        applyOptions(serverBootstrap);

        ChannelFuture channelFuture = this.serverBootstrap.bind(this.serverConfig.getServerListenPort()).syncUninterruptibly();
        this.port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();

        startUpHouseKeepingService();
    }

    @Override
    public void stop() {
        try {

            ThreadUtils.shutdownGracefully(houseKeepingService, 3000, TimeUnit.MILLISECONDS);

            ThreadUtils.shutdownGracefully(channelEventExecutor);

            this.bossGroup.shutdownGracefully().syncUninterruptibly();

            this.ioGroup.shutdownGracefully().syncUninterruptibly();

            this.workerGroup.shutdownGracefully().syncUninterruptibly();
        } catch (Exception e) {
            LOG.error("RemotingServer stopped error !", e);
        }

        super.stop();
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public RemotingCommand invoke(final RemotingChannel remotingChannel, final RemotingCommand request,
        final long timeoutMillis) {
        return invokeWithInterceptor(((NettyChannelImpl) remotingChannel).getChannel(), request, timeoutMillis);
    }

    @Override
    public void invokeAsync(final RemotingChannel remotingChannel, final RemotingCommand request,
        final AsyncHandler asyncHandler,
        final long timeoutMillis) {
        invokeAsyncWithInterceptor(((NettyChannelImpl) remotingChannel).getChannel(), request, asyncHandler, timeoutMillis);
    }

    @Override
    public void invokeOneWay(final RemotingChannel remotingChannel, final RemotingCommand request,
        final long timeoutMillis) {
        invokeOnewayWithInterceptor(((NettyChannelImpl) remotingChannel).getChannel(), request, timeoutMillis);
    }

    private class ServerConnectionHandler extends ChannelDuplexHandler {
        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            LOG.warn("Channel {} channelWritabilityChanged event triggered - bytesBeforeUnwritable:{},bytesBeforeWritable:{}", ctx.channel(),
                ctx.channel().bytesBeforeUnwritable(), ctx.channel().bytesBeforeWritable());
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.ACTIVE, ctx.channel()));
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.INACTIVE, ctx.channel()));
        }

        @Override
        public void userEventTriggered(final ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                final IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    ctx.channel().close().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            LOG.warn("Close channel {} because of event {},result is {}", ctx.channel(), event, future.isSuccess());
                        }
                    });

                    putNettyEvent(new NettyChannelEvent(NettyChannelEventType.IDLE, ctx.channel()));
                }
            }
            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.EXCEPTION, ctx.channel(), cause));

            ctx.channel().close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOG.warn("Close channel {} because of error {},result is {}", ctx.channel(), cause, future.isSuccess());
                }
            });
        }
    }
}
