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
package org.apache.rocketmq.remoting.transport.http2;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.netty.ChannelStatisticsHandler;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.NettyRemotingServerAbstract;
import org.apache.rocketmq.remoting.transport.rocketmq.NettyDecoder;
import org.apache.rocketmq.remoting.transport.rocketmq.NettyEncoder;
import org.apache.rocketmq.remoting.util.JvmUtils;
import org.apache.rocketmq.remoting.util.ThreadUtils;

public class Http2ServerImpl extends NettyRemotingServerAbstract implements RemotingServer {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private ServerBootstrap serverBootstrap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup ioGroup;
    private EventExecutorGroup workerGroup;
    private Class<? extends ServerSocketChannel> socketChannelClass;
    private ServerConfig serverConfig;
    private ChannelEventListener channelEventListener;
    private ExecutorService publicExecutor;
    private int port;
    private InterceptorGroup interceptorGroup;

    public Http2ServerImpl(ServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        init(nettyServerConfig, channelEventListener);
    }

    public Http2ServerImpl() {
        super();
    }

    @Override
    public RemotingServer init(ServerConfig nettyServerConfig, ChannelEventListener channelEventListener) {
        super.init(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.serverConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;
        this.publicExecutor = ThreadUtils.newFixedThreadPool(
            serverConfig.getServerCallbackExecutorThreads(),
            10000, "Remoting-PublicExecutor", true);
        if (JvmUtils.isLinux() && this.serverConfig.isUseEpollNativeSelector()) {
            this.ioGroup = new EpollEventLoopGroup(serverConfig.getServerSelectorThreads(), ThreadUtils.newGenericThreadFactory("NettyEpollIoThreads",
                serverConfig.getServerSelectorThreads()));
            this.bossGroup = new EpollEventLoopGroup(serverConfig.getServerAcceptorThreads(), ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                serverConfig.getServerAcceptorThreads()));
            this.socketChannelClass = EpollServerSocketChannel.class;
        } else {
            this.bossGroup = new NioEventLoopGroup(serverConfig.getServerAcceptorThreads(), ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                serverConfig.getServerAcceptorThreads()));
            this.ioGroup = new NioEventLoopGroup(serverConfig.getServerSelectorThreads(), ThreadUtils.newGenericThreadFactory("NettyNioIoThreads",
                serverConfig.getServerSelectorThreads()));
            this.socketChannelClass = NioServerSocketChannel.class;
        }

        this.workerGroup = new DefaultEventExecutorGroup(serverConfig.getServerWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyWorkerThreads", serverConfig.getServerWorkerThreads()));
        this.port = nettyServerConfig.getListenPort();
        buildHttp2SslContext();
        return this;
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
            log.error("Can not build SSL context !", e);
        }
    }

    @Override
    public void registerProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        executor = executor == null ? this.publicExecutor : executor;
        registerNettyProcessor(requestCode, processor, executor);
    }

    @Override
    public void registerDefaultProcessor(RequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<RequestProcessor, ExecutorService>(processor, executor);
    }

    @Override
    public int localListenPort() {
        return this.port;
    }

    @Override
    public Pair<RequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTable.get(requestCode);
    }

    @Override
    public void push(RemotingChannel remotingChannel, RemotingCommand request,
        long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

    }

    @Override
    public RemotingCommand invokeSync(final RemotingChannel remotingChannel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(((NettyChannelImpl) remotingChannel).getChannel(), request, timeoutMillis);
    }

    @Override
    public void invokeAsync(RemotingChannel remotingChannel, RemotingCommand request, long timeoutMillis,
        InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(((NettyChannelImpl) remotingChannel).getChannel(), request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(RemotingChannel remotingChannel, RemotingCommand request,
        long timeoutMillis) throws InterruptedException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(((NettyChannelImpl) remotingChannel).getChannel(), request, timeoutMillis);
    }

    private void applyOptions(ServerBootstrap bootstrap) {
        if (null != serverConfig) {
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, serverConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, serverConfig.getServerSocketRcvBufSize());
        }

        if (serverConfig.isServerPooledByteBufAllocatorEnable()) {
            bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
    }

    @Override
    public void start() {
        super.start();
        final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        this.serverBootstrap.group(this.bossGroup, this.ioGroup).channel(socketChannelClass).childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                channels.add(ch);
                ChannelPipeline cp = ch.pipeline();
                cp.addLast(ChannelStatisticsHandler.NAME, new ChannelStatisticsHandler(channels));
                cp.addLast(workerGroup,
                    Http2Handler.newHandler(true),
                    new NettyEncoder(),
                    new NettyDecoder(),
                    new IdleStateHandler(serverConfig.getConnectionChannelReaderIdleSeconds(),
                        serverConfig.getConnectionChannelWriterIdleSeconds(),
                        serverConfig.getServerChannelMaxIdleTimeSeconds()),
                    new NettyConnectManageHandler(),
                    new NettyServerHandler());
            }
        });
        applyOptions(serverBootstrap);

        ChannelFuture channelFuture = this.serverBootstrap.bind(this.port).syncUninterruptibly();
        this.port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        startUpHouseKeepingService();
    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
            if (this.bossGroup != null) {
                this.bossGroup.shutdownGracefully();
            }
            if (this.ioGroup != null) {
                this.ioGroup.shutdownGracefully();
            }
            if (this.workerGroup != null) {
                this.workerGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("Http2RemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("Http2RemotingServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public void registerInterceptorGroup(InterceptorGroup interceptorGroup) {
        this.interceptorGroup = interceptorGroup;
    }

    @Override
    public InterceptorGroup getInterceptorGroup() {
        return this.interceptorGroup;
    }

    @Override
    protected RemotingChannel getAndCreateChannel(String addr, long timeout) throws InterruptedException {
        return null;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

}
