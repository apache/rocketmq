/**
 * $Id: NettyRemotingServer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    private ServerBootstrap serverBootstrap;
    private NettyServerConfig nettyServerConfig;

    // ´¦ÀíCallbackÓ¦´ðÆ÷
    private final ExecutorService callbackExecutor;

    private final ChannelEventListener channelEventListener;

    class NettyServerHandler extends ChannelInboundMessageHandlerAdapter<Object> {

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, Object msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnetManageHandler extends ChannelDuplexHandler {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY PIPELINE: channelRegistered, the channel[{}]", remoteAddr);
            super.channelRegistered(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.channelEventListener.onChannelConnect(ctx.channel());
            }
        }


        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY PIPELINE: channelActive, the channel[{}]", remoteAddr);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.channelEventListener.onChannelConnect(ctx.channel());
            }
        }


        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY PIPELINE: channelInactive, the channel[{}]", remoteAddr);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.channelEventListener.onChannelClose(ctx.channel());
            }
        }


        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY PIPELINE: channelUnregistered, the channel[{}]", remoteAddr);
            super.channelUnregistered(ctx);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.channelEventListener.onChannelClose(ctx.channel());
            }
        }


        @Override
        public void flush(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.flush(promise);
        }


        @Override
        public void inboundBufferUpdated(ChannelHandlerContext ctx) throws Exception {
            ctx.fireInboundBufferUpdated();
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY PIPELINE: Unexpected exception from the channel[{}], Exception: {}", remoteAddr,
                cause.getMessage());
            ctx.channel().close();

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.channelEventListener.onChannelException(ctx.channel());
            }
        }
    }


    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }


    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
            final ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        if (nettyServerConfig.getServerCallbackExecutorThreads() > 0) {
            this.callbackExecutor =
                    Executors.newFixedThreadPool(nettyServerConfig.getServerCallbackExecutorThreads(),
                        new ThreadFactory() {

                            private AtomicInteger threadIndex = new AtomicInteger(0);


                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "NettyServerCallbackExecutor_"
                                        + this.threadIndex.incrementAndGet());
                            }
                        });
        }
        else {
            this.callbackExecutor = null;
        }
    }


    @Override
    public void start() throws InterruptedException {
        this.serverBootstrap
            .group(new NioEventLoopGroup(this.nettyServerConfig.getServerSelectorThreads()),
                new NioEventLoopGroup()).channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 65536).childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(//
                        new DefaultEventExecutorGroup(nettyServerConfig.getServerWorkerThreads()), //
                        new NettyEncoder(), //
                        new NettyDecoder(), //
                        new NettyConnetManageHandler(), new NettyServerHandler());
                }
            });

        this.serverBootstrap.bind().sync();
    }


    @Override
    public void shutdown() {
        try {
            this.serverBootstrap.shutdown();
        }
        catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.callbackExecutor != null) {
            try {
                this.callbackExecutor.shutdown();
            }
            catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }


    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, Executor executor) {
        Pair<NettyRequestProcessor, Executor> pair =
                new Pair<NettyRequestProcessor, Executor>(processor, executor);
        this.processorTable.put(requestCode, pair);
    }


    @Override
    public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request,
            final long timeoutMillis) throws InterruptedException, RemotingSendRequestException,
            RemotingTimeoutException {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }


    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis,
            InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }


    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException,
            RemotingSendRequestException {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }


    @Override
    public Executor getCallbackExecutor() {
        return this.callbackExecutor;
    }


    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, Executor executor) {
        this.defaultRequestProcessor = new Pair<NettyRequestProcessor, Executor>(processor, executor);
    }
}
