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
package org.apache.rocketmq.remoting.transport.mqtt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.NoSuchElementException;
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
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.netty.FileRegionEncoder;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.NettyRemotingServerAbstract;
import org.apache.rocketmq.remoting.util.JvmUtils;
import org.apache.rocketmq.remoting.util.ThreadUtils;

public class MqttRemotingServer extends NettyRemotingServerAbstract implements RemotingServer {

    private static final InternalLogger log = InternalLoggerFactory
            .getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private ServerBootstrap serverBootstrap;
    private EventLoopGroup eventLoopGroupSelector;
    private EventLoopGroup eventLoopGroupBoss;
    private ServerConfig nettyServerConfig;

    private ExecutorService publicExecutor;
    private ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private Class<? extends ServerSocketChannel> socketChannelClass;

    private int port = 1883;
    private InterceptorGroup interceptorGroup;

    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    public MqttRemotingServer() {
        super();
    }

    public MqttRemotingServer(final ServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public MqttRemotingServer(final ServerConfig nettyServerConfig,
            final ChannelEventListener channelEventListener) {
        init(nettyServerConfig, channelEventListener);
    }

    @Override
    public RemotingServer init(ServerConfig serverConfig,
            ChannelEventListener channelEventListener) {
        this.nettyServerConfig = serverConfig;
        super.init(nettyServerConfig.getServerOnewaySemaphoreValue(),
                nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }
        this.publicExecutor = ThreadUtils.newFixedThreadPool(
                publicThreadNums,
                10000, "Remoting-PublicExecutor", true);
        if (JvmUtils.isUseEpoll() && this.nettyServerConfig.isUseEpollNativeSelector()) {
            this.eventLoopGroupSelector = new EpollEventLoopGroup(
                    serverConfig.getServerSelectorThreads(),
                    ThreadUtils.newGenericThreadFactory("NettyEpollIoThreads",
                            serverConfig.getServerSelectorThreads()));
            this.eventLoopGroupBoss = new EpollEventLoopGroup(
                    serverConfig.getServerAcceptorThreads(),
                    ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                            serverConfig.getServerAcceptorThreads()));
            this.socketChannelClass = EpollServerSocketChannel.class;
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(serverConfig.getServerAcceptorThreads(),
                    ThreadUtils.newGenericThreadFactory("NettyBossThreads",
                            serverConfig.getServerAcceptorThreads()));
            this.eventLoopGroupSelector = new NioEventLoopGroup(
                    serverConfig.getServerSelectorThreads(),
                    ThreadUtils.newGenericThreadFactory("NettyNioIoThreads",
                            serverConfig.getServerSelectorThreads()));
            this.socketChannelClass = NioServerSocketChannel.class;
        }
        this.port = nettyServerConfig.getMqttListenPort();
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                serverConfig.getServerWorkerThreads(),
                ThreadUtils.newGenericThreadFactory("NettyWorkerThreads",
                        serverConfig.getServerWorkerThreads()));
        loadSslContext();
        return this;
    }

    public void loadSslContext() {
        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        log.info("Server is running in TLS {} mode", tlsMode.getName());
        if (tlsMode != TlsMode.DISABLED) {
            try {
                sslContext = TlsHelper.buildSslContext(false);
                log.info("SSLContext created for server");
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext for server", e);
            } catch (IOException e) {
                log.error("Failed to create SSLContext for server", e);
            }
        }
    }

    @Override
    public void start() {
        super.start();
        ServerBootstrap childHandler =
                this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                        .channel(socketChannelClass)
                        .option(ChannelOption.SO_BACKLOG, 1024)
                        .option(ChannelOption.SO_REUSEADDR, true)
                        .option(ChannelOption.SO_KEEPALIVE, false)
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .childOption(ChannelOption.SO_SNDBUF,
                                nettyServerConfig.getServerSocketSndBufSize())
                        .childOption(ChannelOption.SO_RCVBUF,
                                nettyServerConfig.getServerSocketRcvBufSize())
                        .localAddress(new InetSocketAddress(this.port))
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline()
                                        .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME,
                                                new HandshakeHandler(TlsSystemConfig.tlsMode))
                                        .addLast(defaultEventExecutorGroup,
                                                new MqttDecoder(),
                                                MqttEncoder.INSTANCE,
                                                new Mqtt2RemotingCommandHandler(),
                                                new RemotingCommand2MqttHandler(),
                                                new IdleStateHandler(nettyServerConfig
                                                        .getConnectionChannelReaderIdleSeconds(),
                                                        nettyServerConfig
                                                                .getConnectionChannelWriterIdleSeconds(),
                                                        nettyServerConfig
                                                                .getServerChannelMaxIdleTimeSeconds()),
                                                new NettyConnectManageHandler(),
                                                new NettyServerHandler()

                                        );
                            }
                        });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException",
                    e1);
        }
        startUpHouseKeepingService();
        registerMessageHandler();
    }

    private void registerMessageHandler() {

    }
    @Override
    public void shutdown() {
        try {
            super.shutdown();
            if (this.eventLoopGroupBoss != null) {
                this.eventLoopGroupBoss.shutdownGracefully();
            }
            if (this.eventLoopGroupSelector != null) {
                this.eventLoopGroupSelector.shutdownGracefully();
            }
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }
        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    @Override
    public void registerInterceptorGroup(InterceptorGroup interceptorGroup) {
        this.interceptorGroup = interceptorGroup;
    }

    @Override
    public void registerProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        executor = executor == null ? this.publicExecutor : executor;
        registerNettyProcessor(requestCode, processor, executor);
    }

    @Override
    public void registerDefaultProcessor(RequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
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
    public RemotingCommand invokeSync(final RemotingChannel remotingChannel,
            final RemotingCommand request,
            final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        return this.invokeSyncImpl(((NettyChannelImpl) remotingChannel).getChannel(), request,
                timeoutMillis);
    }

    @Override
    public void invokeAsync(RemotingChannel remotingChannel, RemotingCommand request,
            long timeoutMillis,
            InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(((NettyChannelImpl) remotingChannel).getChannel(), request,
                timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(RemotingChannel remotingChannel, RemotingCommand request,
            long timeoutMillis) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOnewayImpl(((NettyChannelImpl) remotingChannel).getChannel(), request,
                timeoutMillis);
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    @Override
    public InterceptorGroup getInterceptorGroup() {
        return this.interceptorGroup;
    }

    @Override
    protected RemotingChannel getAndCreateChannel(String addr, long timeout)
            throws InterruptedException {
        return null;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode) {
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {

            // mark the current position so that we can peek the first byte to determine if the content is starting with
            // TLS handshake
            msg.markReaderIndex();

            byte b = msg.getByte(0);

            if (b == HANDSHAKE_MAGIC_CODE) {
                switch (tlsMode) {
                    case DISABLED:
                        ctx.close();
                        log.warn(
                                "Clients intend to establish a SSL connection while this server is running in SSL disabled mode");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if (null != sslContext) {
                            ctx.pipeline()
                                    .addAfter(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME,
                                            TLS_HANDLER_NAME,
                                            sslContext.newHandler(ctx.channel().alloc()))
                                    .addAfter(defaultEventExecutorGroup, TLS_HANDLER_NAME,
                                            FILE_REGION_ENCODER_NAME, new FileRegionEncoder());
                            log.info(
                                    "Handlers prepended to channel pipeline to establish SSL connection");
                        } else {
                            ctx.close();
                            log.error(
                                    "Trying to establish a SSL connection but sslContext is null");
                        }
                        break;

                    default:
                        log.warn("Unknown TLS mode");
                        break;
                }
            } else if (tlsMode == TlsMode.ENFORCING) {
                ctx.close();
                log.warn(
                        "Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
            }

            // reset the reader index so that handshake negotiation may proceed as normal.
            msg.resetReaderIndex();

            try {
                // Remove this handler
                ctx.pipeline().remove(this);
            } catch (NoSuchElementException e) {
                log.error("Error while removing HandshakeHandler", e);
            }

            // Hand over this message to the next .
            ctx.fireChannelRead(msg.retain());
        }
    }

    @Override
    public void push(RemotingChannel remotingChannel, RemotingCommand request,
            long timeoutMillis) throws InterruptedException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        this.invokeOneway(remotingChannel, request, timeoutMillis);
    }
}
