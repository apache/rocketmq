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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.remoting.api.command.TrafficType;
import org.apache.rocketmq.remoting.api.exception.RemoteConnectFailureException;
import org.apache.rocketmq.remoting.api.exception.RemoteTimeoutException;
import org.apache.rocketmq.remoting.api.protocol.Protocol;
import org.apache.rocketmq.remoting.common.RemotingCommandFactoryMeta;
import org.apache.rocketmq.remoting.config.RemotingConfig;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.netty.handler.Decoder;
import org.apache.rocketmq.remoting.impl.netty.handler.Encoder;
import org.apache.rocketmq.remoting.impl.netty.handler.ExceptionHandler;
import org.apache.rocketmq.remoting.impl.netty.handler.Http2Handler;
import org.apache.rocketmq.remoting.internal.JvmUtils;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private final Bootstrap clientBootstrap = new Bootstrap();
    private final EventLoopGroup ioGroup;
    private final Class<? extends SocketChannel> socketChannelClass;

    private final RemotingConfig clientConfig;

    private final ConcurrentHashMap<String, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();
    private final Lock lockChannelTables = new ReentrantLock();
    private EventExecutorGroup workerGroup;
    private SslContext sslContext;

    NettyRemotingClient(final RemotingConfig clientConfig) {
        super(clientConfig, new RemotingCommandFactoryMeta(clientConfig.getProtocolName(), clientConfig.getSerializerName()));
        this.clientConfig = clientConfig;

        if (JvmUtils.isLinux() && this.clientConfig.isClientNativeEpollEnable()) {
            this.ioGroup = new EpollEventLoopGroup(clientConfig.getClientWorkerThreads(), ThreadUtils.newGenericThreadFactory("NettyClientEpollIoThreads",
                clientConfig.getClientWorkerThreads()));
            socketChannelClass = EpollSocketChannel.class;
        } else {
            this.ioGroup = new NioEventLoopGroup(clientConfig.getClientWorkerThreads(), ThreadUtils.newGenericThreadFactory("NettyClientNioIoThreads",
                clientConfig.getClientWorkerThreads()));
            socketChannelClass = NioSocketChannel.class;
        }

        this.workerGroup = new DefaultEventExecutorGroup(clientConfig.getClientWorkerThreads(),
            ThreadUtils.newGenericThreadFactory("NettyClientWorkerThreads", clientConfig.getClientWorkerThreads()));

        if (Protocol.HTTP2.equals(clientConfig.getProtocolName())) {
            buildSslContext();
        }
    }

    private void applyOptions(Bootstrap bootstrap) {
        if (null != clientConfig) {
            if (clientConfig.getTcpSoLinger() > 0) {
                bootstrap.option(ChannelOption.SO_LINGER, clientConfig.getTcpSoLinger());
            }

            if (clientConfig.getTcpSoSndBufSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, clientConfig.getTcpSoSndBufSize());
            }
            if (clientConfig.getTcpSoRcvBufSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, clientConfig.getTcpSoRcvBufSize());
            }

            bootstrap.option(ChannelOption.SO_REUSEADDR, clientConfig.isTcpSoReuseAddress()).
                option(ChannelOption.SO_KEEPALIVE, clientConfig.isTcpSoKeepAlive()).
                option(ChannelOption.TCP_NODELAY, clientConfig.isTcpSoNoDelay()).
                option(ChannelOption.CONNECT_TIMEOUT_MILLIS, clientConfig.getTcpSoTimeout()).
                option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(clientConfig.getWriteBufLowWaterMark(),
                    clientConfig.getWriteBufHighWaterMark()));
        }
    }

    @Override
    public void start() {
        super.start();

        this.clientBootstrap.group(this.ioGroup).channel(socketChannelClass)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    if (Protocol.HTTP2.equals(clientConfig.getProtocolName())) {
                        ch.pipeline().addFirst(sslContext.newHandler(ch.alloc()), Http2Handler.newHandler(false));
                    }
                    ch.pipeline().addLast(workerGroup, new Decoder(), new Encoder(), new IdleStateHandler(clientConfig.getConnectionChannelReaderIdleSeconds(),
                            clientConfig.getConnectionChannelWriterIdleSeconds(), clientConfig.getConnectionChannelIdleSeconds()),
                        new ClientConnectionHandler(), new EventDispatcher(), new ExceptionHandler());
                }
            });

        applyOptions(clientBootstrap);

        startUpHouseKeepingService();
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
    public void stop() {
        // try {
        ThreadUtils.shutdownGracefully(houseKeepingService, 3000, TimeUnit.MILLISECONDS);

        for (ChannelWrapper cw : this.channelTables.values()) {
            this.closeChannel(null, cw.getChannel());
        }

        this.channelTables.clear();

        this.ioGroup.shutdownGracefully();

        ThreadUtils.shutdownGracefully(channelEventExecutor);

        this.workerGroup.shutdownGracefully();
        /*
        } catch (Exception e) {
            LOG.error("RemotingClient stopped error !", e);
        }
        */

        super.stop();
    }

    private void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            return;

        final String addrRemote = null == addr ? extractRemoteAddress(channel) : addr;
        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = this.channelTables.get(addrRemote);
                    //Workaround for null
                    if (null == prevCW) {
                        return;
                    }

                    LOG.info("Begin to close the remote address {} channel {}", addrRemote, prevCW);

                    if (prevCW.getChannel() != channel) {
                        LOG.info("Channel {} has been closed,this is a new channel.", prevCW.getChannel(), channel);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOG.info("Channel {} has been removed !", addrRemote);
                    }

                    channel.close().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            LOG.warn("Close channel {} {}", channel, future.isSuccess());
                        }
                    });
                } catch (Exception e) {
                    LOG.error("Close channel error !", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOG.warn("Can not lock channel table in {} ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOG.error("Close channel error !", e);
        }
    }

    private Channel createIfAbsent(final String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isActive()) {
            return cw.getChannel();
        }
        return this.createChannel(addr);
    }

    //FIXME need test to verify
    private Channel createChannel(final String addr) {
        ChannelWrapper cw = null;
        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean createNewConnection;
                    cw = this.channelTables.get(addr);
                    if (cw != null) {
                        if (cw.isActive()) {
                            return cw.getChannel();
                        } else if (!cw.getChannelFuture().isDone()) {
                            createNewConnection = false;
                        } else {
                            this.channelTables.remove(addr);
                            createNewConnection = true;
                        }
                    } else {
                        createNewConnection = true;
                    }

                    if (createNewConnection) {
                        String[] s = addr.split(":");
                        SocketAddress socketAddress = new InetSocketAddress(s[0], Integer.valueOf(s[1]));
                        ChannelFuture channelFuture = this.clientBootstrap.connect(socketAddress);
                        LOG.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                        cw = new ChannelWrapper(channelFuture);
                        this.channelTables.put(addr, cw);
                    }
                } catch (Exception e) {
                    LOG.error("createChannel: create channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOG.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(this.clientConfig.getClientConnectionFutureAwaitTimeoutMillis())) {
                if (cw.isActive()) {
                    LOG.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    LOG.warn("createChannel: connect remote host[" + addr + "] failed, and destroy the channel" + channelFuture.toString(), channelFuture.cause());
                    this.closeChannel(addr, cw.getChannel());
                }
            } else {
                LOG.warn("createChannel: connect remote host[{}] timeout {}ms, {}, and destroy the channel", addr, this.clientConfig.getClientConnectionFutureAwaitTimeoutMillis(),
                    channelFuture.toString());
                this.closeChannel(addr, cw.getChannel());
            }
        }
        return null;
    }

    private void closeChannel(final Channel channel) {
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;

                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = entry.getKey();
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        LOG.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOG.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        //RemotingHelper.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOG.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOG.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOG.error("closeChannel exception", e);
        }
    }

    @Override
    public RemotingCommand invoke(final String address, final RemotingCommand request, final long timeoutMillis) {
        request.trafficType(TrafficType.REQUEST_SYNC);

        Channel channel = this.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            try {
                return this.invokeWithInterceptor(channel, request, timeoutMillis);

            } catch (RemoteTimeoutException e) {
                if (this.clientConfig.isClientCloseSocketIfTimeout()) {
                    LOG.warn("invoke: timeout, so close the socket {} ms, {}", timeoutMillis, address);
                    this.closeChannel(address, channel);
                }

                LOG.warn("invoke: wait response timeout<{}ms> exception, so close the channel[{}]", timeoutMillis, address);
                throw e;
            } finally {
                /*
                if (this.clientConfig.isClientShortConnectionEnable()) {
                    this.closeChannel(addr, channel);
                }
                */
            }
        } else {
            this.closeChannel(address, channel);
            throw new RemoteConnectFailureException(address);
        }

    }

    @Override
    public void invokeAsync(final String address, final RemotingCommand request, final AsyncHandler asyncHandler,
        final long timeoutMillis) {

        final Channel channel = this.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            // We support Netty's channel-level backpressure thereby respecting slow receivers on the other side.
            if (!channel.isWritable()) {
                // Note: It's up to the layer above a transport to decide whether or not to requeue a canceled write.
                LOG.warn("Channel statistics - bytesBeforeUnwritable:{},bytesBeforeWritable:{}", channel.bytesBeforeUnwritable(), channel.bytesBeforeWritable());
            }
            this.invokeAsyncWithInterceptor(channel, request, asyncHandler, timeoutMillis);
        } else {
            this.closeChannel(address, channel);
        }
    }

    @Override
    public void invokeOneWay(final String address, final RemotingCommand request, final long timeoutMillis) {
        final Channel channel = this.createIfAbsent(address);
        if (channel != null && channel.isActive()) {
            if (!channel.isWritable()) {
                //if (this.clientConfig.isSocketFlowControl()) {
                LOG.warn("Channel statistics - bytesBeforeUnwritable:{},bytesBeforeWritable:{}", channel.bytesBeforeUnwritable(), channel.bytesBeforeWritable());
                //throw new ServiceInvocationFailureException(String.format("Channel[%s] is not writable now", channel.toString()));
            }
            this.invokeOnewayWithInterceptor(channel, request, timeoutMillis);
        } else {
            this.closeChannel(address, channel);
        }
    }

    private class ChannelWrapper {
        private final ChannelFuture channelFuture;

        ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        boolean isActive() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        boolean isWriteable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        ChannelFuture getChannelFuture() {
            return channelFuture;
        }
    }

    private class ClientConnectionHandler extends ChannelDuplexHandler {

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            LOG.warn("Channel {} channelWritabilityChanged event triggered - bytesBeforeUnwritable:{},bytesBeforeWritable:{}", ctx.channel(),
                ctx.channel().bytesBeforeUnwritable(), ctx.channel().bytesBeforeWritable());
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise)
            throws Exception {
            LOG.info("Connected from {} to {}.", localAddress, remoteAddress);
            super.connect(ctx, remoteAddress, localAddress, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.ACTIVE, ctx.channel()));
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            LOG.info("Remote address {} disconnect channel {}.", ctx.channel().remoteAddress(), ctx.channel());

            closeChannel(ctx.channel());

            super.disconnect(ctx, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.INACTIVE, ctx.channel()));
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            LOG.info("Remote address {} close channel {}.", ctx.channel().remoteAddress(), ctx.channel());

            closeChannel(ctx.channel());

            super.close(ctx, promise);

            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.INACTIVE, ctx.channel()));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    LOG.info("Close channel {} because of idle event {} ", ctx.channel(), event);
                    closeChannel(ctx.channel());
                    putNettyEvent(new NettyChannelEvent(NettyChannelEventType.IDLE, ctx.channel()));
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.info("Close channel {} because of error {} ", ctx.channel(), cause);

            closeChannel(ctx.channel());
            putNettyEvent(new NettyChannelEvent(NettyChannelEventType.EXCEPTION, ctx.channel()));
        }
    }
}
