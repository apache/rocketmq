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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.interceptor.InterceptorGroup;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.NettyRemotingClientAbstract;
import org.apache.rocketmq.remoting.transport.rocketmq.NettyDecoder;
import org.apache.rocketmq.remoting.transport.rocketmq.NettyEncoder;
import org.apache.rocketmq.remoting.util.ThreadUtils;

public class Http2ClientImpl extends NettyRemotingClientAbstract implements RemotingClient {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private ClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private EventLoopGroup ioGroup;
    private ExecutorService publicExecutor;
    private ExecutorService callbackExecutor;
    private ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private InterceptorGroup interceptorGroup;

    public Http2ClientImpl(final ClientConfig clientConfig,
        final ChannelEventListener channelEventListener) {
        super(clientConfig.getClientOnewaySemaphoreValue(), clientConfig.getClientAsyncSemaphoreValue());
        init(clientConfig, channelEventListener);
    }

    public Http2ClientImpl() {
        super();
    }

    @Override
    public RemotingClient init(ClientConfig clientConfig, ChannelEventListener channelEventListener) {
        this.nettyClientConfig = clientConfig;
        this.channelEventListener = channelEventListener;
        this.ioGroup = new NioEventLoopGroup(clientConfig.getClientWorkerThreads(), ThreadUtils.newGenericThreadFactory("NettyClientEpollIoThreads",
            clientConfig.getClientWorkerThreads()));
        this.publicExecutor = ThreadUtils.newFixedThreadPool(
            clientConfig.getClientCallbackExecutorThreads(),
            10000, "Remoting-PublicExecutor", true);
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(clientConfig.getClientWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyClientWorkerThreads", clientConfig.getClientWorkerThreads()));
        buildSslContext();
        return this;
    }

    private void buildSslContext() {
        SslProvider provider = OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        try {
            sslContext = SslContextBuilder.forClient()
                .sslProvider(provider)
                /* NOTE: the cipher filter may not include all ciphers required by the HTTP/2 specification.
                 * Please refer to the HTTP/2 specification for cipher requirements. */
                .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
        } catch (SSLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void start() {
        super.start();
        this.bootstrap.group(this.ioGroup).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(
                        defaultEventExecutorGroup,
                        Http2Handler.newHandler(false),
                        new NettyEncoder(),
                        new NettyDecoder(),
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

            if (this.ioGroup != null) {
                this.ioGroup.shutdownGracefully();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("Http2ClientImpl shutdown exception, ", e);
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
    public void updateNameServerAddressList(List<String> addrs) {
        super.updateNameServerAddressList(addrs);
    }

    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        final RemotingChannel remotingChannel = this.getAndCreateChannel(addr, timeoutMillis);
        if (remotingChannel != null && remotingChannel.isActive()) {
            try {
                RemotingCommand response = this.invokeSyncWithInterceptor(remotingChannel, request, timeoutMillis);
                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("InvokeSync: send request exception, so close the channel[{}]", addr);
                this.closeRemotingChannel(addr, remotingChannel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeRemotingChannel(addr, remotingChannel);
                    log.warn("InvokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                log.warn("InvokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeRemotingChannel(addr, remotingChannel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        final RemotingChannel remotingChannel = this.getAndCreateChannel(addr, timeoutMillis);

        if (remotingChannel != null && remotingChannel.isActive()) {
            try {

                this.invokeAsyncWithInterceptor(remotingChannel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeRemotingChannel(addr, remotingChannel);
                throw e;
            }
        } else {
            this.closeRemotingChannel(addr, remotingChannel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final RemotingChannel remotingChannel = this.getAndCreateChannel(addr, timeoutMillis);
        if (remotingChannel != null && remotingChannel.isActive()) {
            try {
                this.invokeOnewayWithInterceptor(remotingChannel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeRemotingChannel(addr, remotingChannel);
                throw e;
            }
        } else {
            this.closeRemotingChannel(addr, remotingChannel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public Bootstrap getBootstrap() {
        return this.bootstrap;
    }

    @Override
    public void registerProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        executor = executor == null ? this.publicExecutor : executor;
        registerNettyProcessor(requestCode, processor, executor);
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
