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
package org.apache.rocketmq.remoting.netty;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Stopwatch;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.proxy.SocksProxyConfig;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final long MIN_CLOSE_TIMEOUT_MILLIS = 100;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final Map<String /* cidr */, SocksProxyConfig /* proxy */> proxyMap = new HashMap<>();
    private final ConcurrentHashMap<String /* cidr */, Bootstrap> bootstrapMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<>();
    private final ConcurrentMap<Channel, ChannelWrapper> channelWrapperTables = new ConcurrentHashMap<>();

    private final HashedWheelTimer timer = new HashedWheelTimer(r -> new Thread(r, "ClientHouseKeepingService"));

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final ConcurrentMap<String, Boolean> availableNamesrvAddrMap = new ConcurrentHashMap<>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock namesrvChannelLock = new ReentrantLock();

    private final ExecutorService publicExecutor;
    private final ExecutorService scanExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private EventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        this(nettyClientConfig, channelEventListener, null, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener,
        final EventLoopGroup eventLoopGroup,
        final EventExecutorGroup eventExecutorGroup) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        this.loadSocksProxyJson();

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactoryImpl("NettyClientPublicExecutor_"));

        this.scanExecutor = ThreadUtils.newThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(32), new ThreadFactoryImpl("NettyClientScan_thread_"));

        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactoryImpl("NettyClientSelector_"));
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;

        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                LOGGER.info("SSL enabled for client");
            } catch (IOException e) {
                LOGGER.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                LOGGER.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();
        return r.nextInt(999);
    }

    private void loadSocksProxyJson() {
        Map<String, SocksProxyConfig> sockProxyMap = JSON.parseObject(
            nettyClientConfig.getSocksProxyConfig(), new TypeReference<Map<String, SocksProxyConfig>>() {
            });
        if (sockProxyMap != null) {
            proxyMap.putAll(sockProxyMap);
        }
    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactoryImpl("NettyClientWorkerThread_"));
        }
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    ch.pipeline().addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });
        if (nettyClientConfig.getClientSocketSndBufSize() > 0) {
            LOGGER.info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
            handler.option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize());
        }
        if (nettyClientConfig.getClientSocketRcvBufSize() > 0) {
            LOGGER.info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
            handler.option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize());
        }
        if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0) {
            LOGGER.info("client set netty WRITE_BUFFER_WATER_MARK to {},{}",
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
            handler.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
        }
        if (nettyClientConfig.isClientPooledByteBufAllocatorEnable()) {
            handler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        nettyEventExecutor.start();

        TimerTask timerTaskScanResponseTable = new TimerTask() {
            @Override
            public void run(Timeout timeout) {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    LOGGER.error("scanResponseTable exception", e);
                } finally {
                    timer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
                }
            }
        };
        this.timer.newTimeout(timerTaskScanResponseTable, 1000 * 3, TimeUnit.MILLISECONDS);

        if (nettyClientConfig.isScanAvailableNameSrv()) {
            int connectTimeoutMillis = this.nettyClientConfig.getConnectTimeoutMillis();
            TimerTask timerTaskScanAvailableNameSrv = new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                    try {
                        NettyRemotingClient.this.scanAvailableNameSrv();
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv exception", e);
                    } finally {
                        timer.newTimeout(this, connectTimeoutMillis, TimeUnit.MILLISECONDS);
                    }
                }
            };
            this.timer.newTimeout(timerTaskScanAvailableNameSrv, 0, TimeUnit.MILLISECONDS);
        }
        
        
    }

    private Map.Entry<String, SocksProxyConfig> getProxy(String addr) {
        if (StringUtils.isBlank(addr) || !addr.contains(":")) {
            return null;
        }
        String[] hostAndPort = this.getHostAndPort(addr);
        for (Map.Entry<String, SocksProxyConfig> entry : proxyMap.entrySet()) {
            String cidr = entry.getKey();
            if (RemotingHelper.DEFAULT_CIDR_ALL.equals(cidr) || RemotingHelper.ipInCIDR(hostAndPort[0], cidr)) {
                return entry;
            }
        }
        return null;
    }

    private Bootstrap fetchBootstrap(String addr) {
        Map.Entry<String, SocksProxyConfig> proxyEntry = getProxy(addr);
        if (proxyEntry == null) {
            return bootstrap;
        }

        String cidr = proxyEntry.getKey();
        SocksProxyConfig socksProxyConfig = proxyEntry.getValue();

        LOGGER.info("Netty fetch bootstrap, addr: {}, cidr: {}, proxy: {}",
            addr, cidr, socksProxyConfig != null ? socksProxyConfig.getAddr() : "");

        Bootstrap bootstrapWithProxy = bootstrapMap.get(cidr);
        if (bootstrapWithProxy == null) {
            bootstrapWithProxy = createBootstrap(socksProxyConfig);
            Bootstrap old = bootstrapMap.putIfAbsent(cidr, bootstrapWithProxy);
            if (old != null) {
                bootstrapWithProxy = old;
            }
        }
        return bootstrapWithProxy;
    }

    private Bootstrap createBootstrap(final SocksProxyConfig proxy) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
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
                            pipeline.addFirst(defaultEventExecutorGroup,
                                "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }

                    // Netty Socks5 Proxy
                    if (proxy != null) {
                        String[] hostAndPort = getHostAndPort(proxy.getAddr());
                        pipeline.addFirst(new Socks5ProxyHandler(
                            new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])),
                            proxy.getUsername(), proxy.getPassword()));
                    }

                    pipeline.addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });

        // Support Netty Socks5 Proxy
        if (proxy != null) {
            bootstrap.resolver(NoopAddressResolverGroup.INSTANCE);
        }
        return bootstrap;
    }

    // Do not use RemotingHelper.string2SocketAddress(), it will directly resolve the domain
    private String[] getHostAndPort(String address) {
        int split = address.lastIndexOf(":");
        return split < 0 ? new String[]{address} : new String[]{address.substring(0, split), address.substring(split + 1)};
    }

    @Override
    public void shutdown() {
        try {
            this.timer.stop();

            for (Map.Entry<String, ChannelWrapper> channel : this.channelTables.entrySet()) {
                channel.getValue().close();
            }

            this.channelWrapperTables.clear();
            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            LOGGER.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }

        if (this.scanExecutor != null) {
            try {
                this.scanExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    LOGGER.info("closeChannel: begin close the channel[addr={}, id={}] Found: {}", addrRemote, channel.id(), prevCW != null);

                    if (null == prevCW) {
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] has been removed from the channel table before", addrRemote, channel.id());
                        removeItemFromTable = false;
                    } else if (prevCW.isWrapperOf(channel)) {
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] has been closed before, and has been created again, nothing to do.",
                            addrRemote, channel.id());
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        ChannelWrapper channelWrapper = this.channelWrapperTables.remove(channel);
                        if (channelWrapper != null && channelWrapper.tryClose(channel)) {
                            this.channelTables.remove(addrRemote);
                        }
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] was removed from channel table", addrRemote, channel.id());
                    }

                    RemotingHelper.closeChannel(channel);
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.isWrapperOf(channel)) {
                            prevCW = prev;
                            addrRemote = key;
                            break;
                        }
                    }

                    if (null == prevCW) {
                        LOGGER.info("eventCloseChannel: the channel[addr={}, id={}] has been removed from the channel table before", RemotingHelper.parseChannelRemoteAddr(channel), channel.id());
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        ChannelWrapper channelWrapper = this.channelWrapperTables.remove(channel);
                        if (channelWrapper != null && channelWrapper.tryClose(channel)) {
                            this.channelTables.remove(addrRemote);
                        }
                        LOGGER.info("closeChannel: the channel[addr={}, id={}] was removed from channel table", addrRemote, channel.id());
                        RemotingHelper.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel[id={}] exception", channel.id(), e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (String addr : addrs) {
                    if (!old.contains(addr)) {
                        update = true;
                        break;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                LOGGER.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                // should close the channel if choosed addr is not exist.
                if (this.namesrvAddrChoosed.get() != null && !addrs.contains(this.namesrvAddrChoosed.get())) {
                    String namesrvAddr = this.namesrvAddrChoosed.getAndSet(null);
                    for (String addr : this.channelTables.keySet()) {
                        if (addr.contains(namesrvAddr)) {
                            ChannelWrapper channelWrapper = this.channelTables.get(addr);
                            if (channelWrapper != null) {
                                channelWrapper.close();
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        if (channel != null && channel.isActive()) {
            long left = timeoutMillis;
            try {
                long costTime = System.currentTimeMillis() - beginStartTime;
                left -= costTime;
                if (left <= 0) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + channelRemoteAddr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, left);
                updateChannelLastResponseTime(addr);
                return response;
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeSync: send request exception, so close the channel[{}]", channelRemoteAddr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                // avoid close the success channel if left timeout is small, since it may cost too much time in get the success channel, the left timeout for read is small
                boolean shouldClose = left > MIN_CLOSE_TIMEOUT_MILLIS || left > timeoutMillis / 4;
                if (nettyClientConfig.isClientCloseSocketIfTimeout() && shouldClose) {
                    this.closeChannel(addr, channel);
                    LOGGER.warn("invokeSync: close socket because of timeout, {}ms, channel[addr={}, id={}]", timeoutMillis, channelRemoteAddr, channel.id());
                }
                LOGGER.warn("invokeSync: wait response timeout exception, the channel[addr={}, id={}]", channelRemoteAddr, channel.id());
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw == null) {
                continue;
            }
            this.closeChannel(addr, cw.getChannel());
        }
        interruptPullRequests(new HashSet<>(addrList));
    }

    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand cmd = responseFuture.getRequestCommand();
            if (cmd == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            // interrupt only pull message request
            if (brokerAddrSet.contains(remoteAddr) && (cmd.getCode() == 11 || cmd.getCode() == 361)) {
                LOGGER.info("interrupt {}", cmd);
                responseFuture.interrupt();
            }
        }
    }

    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.namesrvAddrChoosed.get();
        }
        if (address == null) {
            LOGGER.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    private ChannelFuture getAndCreateChannelAsync(final String addr) throws InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannelAsync();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannelFuture();
        }

        return this.createChannelAsync(addr);
    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        ChannelFuture channelFuture = getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            return null;
        }
        return channelFuture.awaitUninterruptibly().channel();
    }

    private ChannelFuture getAndCreateNameserverChannelAsync() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannelFuture();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannelFuture();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    int index = this.namesrvIndex.incrementAndGet();
                    index = Math.abs(index);
                    index = index % addrList.size();
                    String newAddr = addrList.get(index);

                    this.namesrvAddrChoosed.set(newAddr);
                    LOGGER.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                    return this.createChannelAsync(newAddr);
                }
            } catch (Exception e) {
                LOGGER.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.namesrvChannelLock.unlock();
            }
        } else {
            LOGGER.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private ChannelFuture createChannelAsync(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannelFuture();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                cw = this.channelTables.get(addr);
                if (cw != null) {
                    if (cw.isOK() || !cw.getChannelFuture().isDone()) {
                        return cw.getChannelFuture();
                    } else {
                        this.channelTables.remove(addr);
                    }
                }
                return createChannel(addr).getChannelFuture();
            } catch (Exception e) {
                LOGGER.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            LOGGER.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private ChannelWrapper createChannel(String addr) {
        String[] hostAndPort = getHostAndPort(addr);
        ChannelFuture channelFuture = fetchBootstrap(addr)
            .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        LOGGER.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
        ChannelWrapper cw = new ChannelWrapper(addr, channelFuture);
        this.channelTables.put(addr, cw);
        this.channelWrapperTables.put(channelFuture.channel(), cw);
        return cw;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            invokeCallback.operationFail(new RemotingConnectException(addr));
            return;
        }
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = channelFuture.channel();
                String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                if (channel != null && channel.isActive()) {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeoutMillis < costTime) {
                        invokeCallback.operationFail(new RemotingTooMuchRequestException("invokeAsync call the addr[" + channelRemoteAddr + "] timeout"));
                    }
                    this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
                } else {
                    this.closeChannel(addr, channel);
                    invokeCallback.operationFail(new RemotingConnectException(addr));
                }
            } else {
                invokeCallback.operationFail(new RemotingConnectException(addr));
            }
        });
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
        if (channelFuture == null) {
            throw new RemotingConnectException(addr);
        }
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = channelFuture.channel();
                String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
                if (channel != null && channel.isActive()) {
                    doBeforeRpcHooks(channelRemoteAddr, request);
                    this.invokeOnewayImpl(channel, request, timeoutMillis);
                } else {
                    this.closeChannel(addr, channel);
                }
            }
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> invoke(String addr, RemotingCommand request,
        long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            final ChannelFuture channelFuture = this.getAndCreateChannelAsync(addr);
            if (channelFuture == null) {
                future.completeExceptionally(new RemotingConnectException(addr));
                return future;
            }
            channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    Channel channel = channelFuture.channel();
                    if (channel != null && channel.isActive()) {
                        invokeImpl(channel, request, timeoutMillis).whenComplete((v, t) -> {
                            if (t == null) {
                                updateChannelLastResponseTime(addr);
                            }
                        }).thenApply(ResponseFuture::getResponseCommand).whenComplete((v, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(v);
                            }
                        });
                    } else {
                        this.closeChannel(addr, channel);
                        future.completeExceptionally(new RemotingConnectException(addr));
                    }
                } else {
                    future.completeExceptionally(new RemotingConnectException(addr));
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public CompletableFuture<ResponseFuture> invokeImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        doBeforeRpcHooks(channelRemoteAddr, request);

        return super.invokeImpl(channel, request, timeoutMillis).thenCompose(responseFuture -> {
            RemotingCommand response = responseFuture.getResponseCommand();
            if (response.getCode() == ResponseCode.GO_AWAY) {
                if (nettyClientConfig.isEnableReconnectForGoAway()) {
                    LOGGER.info("Receive go away from channelId={}, channel={}", channel.id(), channel);
                    ChannelWrapper channelWrapper = channelWrapperTables.computeIfPresent(channel, (channel0, channelWrapper0) -> {
                        try {
                            if (channelWrapper0.reconnect(channel0)) {
                                LOGGER.info("Receive go away from channelId={}, channel={}, recreate the channelId={}", channel0.id(), channel0, channelWrapper0.getChannel().id());
                                channelWrapperTables.put(channelWrapper0.getChannel(), channelWrapper0);
                            }
                        } catch (Throwable t) {
                            LOGGER.error("Channel {} reconnect error", channelWrapper0, t);
                        }
                        return channelWrapper0;
                    });
                    if (channelWrapper != null && !channelWrapper.isWrapperOf(channel)) {
                        if (nettyClientConfig.isEnableTransparentRetry()) {
                            RemotingCommand retryRequest = RemotingCommand.createRequestCommand(request.getCode(), request.readCustomHeader());
                            retryRequest.setBody(request.getBody());
                            retryRequest.setExtFields(request.getExtFields());
                            if (channelWrapper.isOK()) {
                                long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                stopwatch.stop();
                                Channel retryChannel = channelWrapper.getChannel();
                                if (retryChannel != null && channel != retryChannel) {
                                    return super.invokeImpl(retryChannel, retryRequest, timeoutMillis - duration);
                                }
                            } else {
                                CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
                                ChannelFuture channelFuture = channelWrapper.getChannelFuture();
                                channelFuture.addListener(f -> {
                                    long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                    stopwatch.stop();
                                    if (f.isSuccess()) {
                                        Channel retryChannel0 = channelFuture.channel();
                                        if (retryChannel0 != null && channel != retryChannel0) {
                                            super.invokeImpl(retryChannel0, retryRequest, timeoutMillis - duration).whenComplete((v, t) -> {
                                                if (t != null) {
                                                    future.completeExceptionally(t);
                                                } else {
                                                    future.complete(v);
                                                }
                                            });
                                        }
                                    } else {
                                        future.completeExceptionally(new RemotingConnectException(channelWrapper.channelAddress));
                                    }
                                });
                                return future;
                            }
                        }
                    } else {
                        LOGGER.warn("invokeImpl receive GO_AWAY, channelWrapper is null or channel is the same in wrapper, channelId={}", channel.id());
                    }
                }
            }
            return CompletableFuture.completedFuture(responseFuture);
        }).whenComplete((v, t) -> {
            if (t == null) {
                doAfterRpcHooks(channelRemoteAddr, request, v.getResponseCommand());
            }
        });
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public boolean isAddressReachable(String addr) {
        if (addr == null || addr.isEmpty()) {
            return false;
        }
        try {
            Channel channel = getAndCreateChannel(addr);
            return channel != null && channel.isActive();
        } catch (Exception e) {
            LOGGER.warn("Get and create channel of {} failed", addr, e);
            return false;
        }
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public List<String> getAvailableNameSrvList() {
        return new ArrayList<>(this.availableNamesrvAddrMap.keySet());
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        if (nettyClientConfig.isDisableCallbackExecutor()) {
            return null;
        }
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    protected void scanChannelTablesOfNameServer() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.warn("[SCAN] Addresses of name server is empty!");
            return;
        }

        for (Map.Entry<String, ChannelWrapper> entry : this.channelTables.entrySet()) {
            String addr = entry.getKey();
            ChannelWrapper channelWrapper = entry.getValue();
            if (channelWrapper == null) {
                continue;
            }

            if ((System.currentTimeMillis() - channelWrapper.getLastResponseTime()) > this.nettyClientConfig.getChannelNotActiveInterval()) {
                LOGGER.warn("[SCAN] No response after {} from name server {}, so close it!", channelWrapper.getLastResponseTime(),
                    addr);
                closeChannel(addr, channelWrapper.getChannel());
            }
        }
    }

    private void scanAvailableNameSrv() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.debug("scanAvailableNameSrv addresses of name server is null!");
            return;
        }

        for (String address : NettyRemotingClient.this.availableNamesrvAddrMap.keySet()) {
            if (!nameServerList.contains(address)) {
                LOGGER.warn("scanAvailableNameSrv remove invalid address {}", address);
                NettyRemotingClient.this.availableNamesrvAddrMap.remove(address);
            }
        }

        for (final String namesrvAddr : nameServerList) {
            scanExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Channel channel = NettyRemotingClient.this.getAndCreateChannel(namesrvAddr);
                        if (channel != null) {
                            NettyRemotingClient.this.availableNamesrvAddrMap.putIfAbsent(namesrvAddr, true);
                        } else {
                            Boolean value = NettyRemotingClient.this.availableNamesrvAddrMap.remove(namesrvAddr);
                            if (value != null) {
                                LOGGER.warn("scanAvailableNameSrv remove unconnected address {}", namesrvAddr);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv get channel of {} failed, ", namesrvAddr, e);
                    }
                }
            });
        }
    }

    class ChannelWrapper {
        private final ReentrantReadWriteLock lock;
        private ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        private ChannelFuture channelToClose;
        private long lastResponseTime;
        private final String channelAddress;

        public ChannelWrapper(String address, ChannelFuture channelFuture) {
            this.lock = new ReentrantReadWriteLock();
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
            this.channelAddress = address;
        }

        public boolean isOK() {
            return getChannel() != null && getChannel().isActive();
        }

        public boolean isWritable() {
            return getChannel().isWritable();
        }

        public boolean isWrapperOf(Channel channel) {
            return this.channelFuture.channel() != null && this.channelFuture.channel() == channel;
        }

        private Channel getChannel() {
            return getChannelFuture().channel();
        }

        public ChannelFuture getChannelFuture() {
            lock.readLock().lock();
            try {
                return this.channelFuture;
            } finally {
                lock.readLock().unlock();
            }
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }

        public String getChannelAddress() {
            return channelAddress;
        }

        public boolean reconnect(Channel channel) {
            if (!isWrapperOf(channel)) {
                LOGGER.warn("channelWrapper has reconnect, so do nothing, now channelId={}, input channelId={}",getChannel().id(), channel.id());
                return false;
            }
            if (lock.writeLock().tryLock()) {
                try {
                    if (isWrapperOf(channel)) {
                        channelToClose = channelFuture;
                        String[] hostAndPort = getHostAndPort(channelAddress);
                        channelFuture = fetchBootstrap(channelAddress)
                            .connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                        return true;
                    } else {
                        LOGGER.warn("channelWrapper has reconnect, so do nothing, now channelId={}, input channelId={}",getChannel().id(), channel.id());
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } else {
                LOGGER.warn("channelWrapper reconnect try lock fail, now channelId={}", getChannel().id());
            }
            return false;
        }

        public boolean tryClose(Channel channel) {
            try {
                lock.readLock().lock();
                if (channelFuture != null) {
                    if (channelFuture.channel().equals(channel)) {
                        return true;
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            return false;
        }

        public void close() {
            try {
                lock.writeLock().lock();
                if (channelFuture != null) {
                    closeChannel(channelFuture.channel());
                }
                if (channelToClose != null) {
                    closeChannel(channelToClose.channel());
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final String addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            this.invokeCallback.operationComplete(responseFuture);
        }

        @Override
        public void operationSucceed(RemotingCommand response) {
            updateChannelLastResponseTime(addr);
            this.invokeCallback.operationSucceed(response);
        }

        @Override
        public void operationFail(final Throwable throwable) {
            this.invokeCallback.operationFail(throwable);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            LOGGER.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: ACTIVE, {}, channelId={}", remoteAddress, ctx.channel().id());
            super.channelActive(ctx);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.ACTIVE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: CLOSE channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: channelInactive, the channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
            closeChannel(ctx.channel());
            super.channelInactive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception channel[addr={}, id={}]", remoteAddress, ctx.channel().id());
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught channel[addr={}, id={}]", remoteAddress, ctx.channel().id(), cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
