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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.NettyRemotingClientAbstract;
import org.apache.rocketmq.remoting.util.ThreadUtils;

public class MqttRemotingClient extends NettyRemotingClientAbstract implements RemotingClient {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private ClientConfig nettyClientConfig;
    private Bootstrap bootstrap = new Bootstrap();
    private EventLoopGroup eventLoopGroupWorker;

    private ExecutorService publicExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private InterceptorGroup interceptorGroup;

    public MqttRemotingClient() {
        super();
    }

    public MqttRemotingClient(final ClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public MqttRemotingClient(final ClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        init(nettyClientConfig, channelEventListener);
    }

    @Override
    public RemotingClient init(ClientConfig nettyClientConfig, ChannelEventListener channelEventListener) {
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;
        this.eventLoopGroupWorker = new NioEventLoopGroup(nettyClientConfig.getClientWorkerThreads(), ThreadUtils.newGenericThreadFactory("NettyClientEpollIoThreads",
            nettyClientConfig.getClientWorkerThreads()));
        this.publicExecutor = ThreadUtils.newFixedThreadPool(
            nettyClientConfig.getClientCallbackExecutorThreads(),
            10000, "Remoting-PublicExecutor", true);
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyClientWorkerThreads", nettyClientConfig.getClientWorkerThreads()));
        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                log.info("SSL enabled for client");
            } catch (IOException e) {
                log.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                log.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
        return this;
    }

    @Override
    public Bootstrap getBootstrap() {
        return this.bootstrap;
    }

    @Override
    public void start() {
        bootstrap = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            log.info("Prepend SSL handler");
                        } else {
                            log.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        MqttEncoder.INSTANCE,
                        new MqttDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });
        startUpHouseKeepingService();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            clearChannels();
            if (this.eventLoopGroupWorker != null) {
                this.eventLoopGroupWorker.shutdownGracefully();
            }
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
            if (this.publicExecutor != null) {
                this.publicExecutor.shutdown();
            }
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }
    }

    @Override
    public void registerInterceptorGroup(InterceptorGroup interceptorGroup) {
        this.interceptorGroup = interceptorGroup;
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        super.updateNameServerAddressList(addrs);
    }

    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        final RemotingChannel remotingChannel = this.getAndCreateChannel(addr, timeoutMillis);
        if (remotingChannel != null && remotingChannel.isActive()) {
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call timeout");
                }
                RemotingCommand response = this.invokeSyncWithInterceptor(remotingChannel, request, timeoutMillis - costTime);
                return response;
            } catch (RemotingException remotingException) {
                if (remotingException instanceof RemotingSendRequestException) {
                    log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                    this.closeRemotingChannel(addr, remotingChannel);
                    throw (RemotingSendRequestException) remotingException;
                }
                if (remotingException instanceof RemotingTimeoutException) {
                    if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                        this.closeRemotingChannel(addr, remotingChannel);
                        log.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                    }
                    log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                    throw (RemotingTimeoutException) remotingException;
                }
            }
        } else {
            this.closeRemotingChannel(addr, remotingChannel);
            throw new RemotingConnectException(addr);
        }
        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final RemotingChannel remotingChannel = this.getAndCreateChannel(addr, timeoutMillis);
        Channel channel = null;
        if (remotingChannel instanceof NettyChannelImpl) {
            channel = ((NettyChannelImpl) remotingChannel).getChannel();
        }

        if (channel != null && channel.isActive()) {
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final RemotingChannel remotingChannel = getAndCreateChannel(addr, timeoutMillis);
        if (remotingChannel != null && remotingChannel.isActive()) {
            try {
                this.invokeOnewayWithInterceptor(remotingChannel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("InvokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeRemotingChannel(addr, remotingChannel);
                throw e;
            }
        } else {
            this.closeRemotingChannel(addr, remotingChannel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<RequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public InterceptorGroup getInterceptorGroup() {
        return this.interceptorGroup;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

}
