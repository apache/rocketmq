/**
 * $Id: NettyRemotingClient.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting.netty;

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
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);

    private static final long LockTimeoutMillis = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroup;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentHashMap<String /* addr */, ChannelWrapper> channelTables =
            new ConcurrentHashMap<String, ChannelWrapper>();

    // 定时器
    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    // Name server相关
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock lockNamesrvChannel = new ReentrantLock();

    // 处理Callback应答器
    private final ExecutorService publicExecutor;

    private final ChannelEventListener channelEventListener;

    class ChannelWrapper {
        private final Channel channel;
        private volatile long lastActiveTimestamp = System.currentTimeMillis();


        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }


        public long getLastActiveTimestamp() {
            return lastActiveTimestamp;
        }


        public void setLastActiveTimestamp(long lastActiveTimestamp) {
            this.lastActiveTimestamp = lastActiveTimestamp;
        }


        public Channel getChannel() {
            return channel;
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<Object> {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, Object msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnetManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOW" : localAddress.toString();
            final String remote = remoteAddress == null ? "UNKNOW" : remoteAddress.toString();
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress
                    .toString(), ctx.channel()));
            }
        }


        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress
                    .toString(), ctx.channel()));
            }
        }


        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress
                    .toString(), ctx.channel()));
            }
        }


        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress
                    .toString(), ctx.channel()));
            }
        }
    }


    private static int initValueIndex() {
        Random r = new Random();

        return Math.abs(r.nextInt()) % 999;
    }


    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }


    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
            final ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig
            .getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroup = new NioEventLoopGroup(nettyClientConfig.getClientSelectorThreads());
    }


    private void scanNotActiveChannel() throws InterruptedException {
        // 加锁，尝试扫描过期连接
        if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                for (String addr : this.channelTables.keySet()) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null) {
                        long diff = System.currentTimeMillis() - cw.getLastActiveTimestamp();
                        if (diff > this.nettyClientConfig.getChannelNotActiveInterval()) {
                            log.warn("the channel[{}] not active for a while[{}ms], close it forcibly", addr,
                                diff);
                            this.closeChannel(addr, cw.getChannel());
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("scanNotActiveChannel: close channel exception", e);
            }
            finally {
                this.lockChannelTables.unlock();
            }
        }
        else {
            log.warn("scanNotActiveChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
        }
    }


    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
            nettyClientConfig.getClientWorkerThreads(), //
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);


                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        this.bootstrap.group(this.eventLoopGroup).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(//
                        defaultEventExecutorGroup, //
                        new NettyEncoder(), //
                        new NettyDecoder(), //
                        new NettyConnetManageHandler(), new NettyClientHandler());
                }
            });

        // 每隔1秒扫描下异步调用超时情况
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                }
                catch (Exception e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

        // 每隔10秒扫描下不活动的连接
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanNotActiveChannel();
                }
                catch (Exception e) {
                    log.error("scanNotActiveChannel exception", e);
                }
            }
        }, 1000 * 10, 1000 * 10);

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }
    }


    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (ChannelWrapper cw : this.channelTables.values()) {
                this.closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroup.shutdownGracefully();

            if (this.nettyEventExecuter != null) {
                this.nettyEventExecuter.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        }
        catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            }
            catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }


    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (null == addr)
            return getAndCreateNameserverChannel();

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null) {
            cw.setLastActiveTimestamp(System.currentTimeMillis());
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }


    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null) {
                cw.setLastActiveTimestamp(System.currentTimeMillis());
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        // 加锁，尝试创建连接
        if (this.lockNamesrvChannel.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null) {
                        cw.setLastActiveTimestamp(System.currentTimeMillis());
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null)
                            return channelNew;
                    }
                }
            }
            catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            }
            finally {
                this.lockNamesrvChannel.unlock();
            }
        }
        else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms",
                LockTimeoutMillis);
        }

        return null;
    }


    private Channel createChannel(final String addr) throws InterruptedException {
        // 加锁，尝试创建连接
        if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
            try {
                ChannelWrapper cw = this.channelTables.get(addr);
                if (cw != null) {
                    cw.setLastActiveTimestamp(System.currentTimeMillis());
                    return cw.getChannel();
                }

                ChannelFuture channelFuture =
                        this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                Channel channel = null;
                if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                    channel = channelFuture.channel();
                    if (!channel.isActive()) {
                        log.warn("connect {} in {}ms ok, but channel not active", addr,
                            this.nettyClientConfig.getConnectTimeoutMillis());
                        channel.close();
                        return null;
                    }
                }
                else {
                    log.error("connect {} in {}ms timeout", addr,
                        this.nettyClientConfig.getConnectTimeoutMillis());
                    channelFuture.channel().close();
                    return null;
                }

                log.info("connect {} success, and add to the channel table", addr);
                this.channelTables.put(addr, new ChannelWrapper(channel));
                return channel;
            }
            catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            }
            finally {
                this.lockChannelTables.unlock();
            }
        }
        else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
        }

        return null;
    }


    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel)
            return;

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote,
                        (prevCW != null));

                    if (null == prevCW) {
                        log.info(
                            "closeChannel: the channel[{}] has been removed from the channel table before",
                            addrRemote);
                        removeItemFromTable = false;
                    }
                    else if (prevCW.getChannel() != channel) {
                        log.info(
                            "closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    channel.close().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            log.info("closeChannel: close the connection to remote address[{}] result: {}",
                                addrRemote, future.isSuccess());
                        }
                    });
                }
                catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                }
                finally {
                    this.lockChannelTables.unlock();
                }
            }
            else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
            }
        }
        catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }


    public void closeChannel(final Channel channel) {
        if (null == channel)
            return;

        try {
            if (this.lockChannelTables.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (String key : channelTables.keySet()) {
                        ChannelWrapper prev = this.channelTables.get(key);
                        if (prev.channel.equals(channel)) {
                            prevCW = prev;
                            addrRemote = key;
                            break;
                        }
                    }

                    if (null == prevCW) {
                        log.info(
                            "eventCloseChannel: the channel[{}] has been removed from the channel table before",
                            addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        // channel.close().sync();
                    }
                }
                catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                }
                finally {
                    this.lockChannelTables.unlock();
                }
            }
            else {
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms", LockTimeoutMillis);
            }
        }
        catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }


    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, Executor executor) {
        Executor executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, Executor> pair =
                new Pair<NettyRequestProcessor, Executor>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }


    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                return this.invokeSyncImpl(channel, request, timeoutMillis);
            }
            catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
            catch (RemotingTimeoutException e) {
                log.warn("invokeSync: wait response timeout exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        }
        else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis,
            InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
            RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            }
            catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        }
        else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
            RemotingTimeoutException, RemotingSendRequestException {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            }
            catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        }
        else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }


    @Override
    public Executor getCallbackExecutor() {
        return this.publicExecutor;
    }


    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        // 相等情况下，不需要更新
        if (!addrs.isEmpty() && old != null) {
            if (old.size() == addrs.size()) {
                boolean equal = true;
                for (int i = 0; i < addrs.size(); i++) {
                    if (!old.get(i).equals(addrs.get(i))) {
                        equal = false;
                        break;
                    }
                }

                if (equal)
                    return;
            }
        }

        this.namesrvAddrList.set(addrs);
    }


    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }


    public List<String> getNamesrvAddrList() {
        return namesrvAddrList.get();
    }


    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }
}
