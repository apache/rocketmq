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
package org.apache.rocketmq.proxy.grpc;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;

public class GrpcServerBuilder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected NettyServerBuilder serverBuilder;
    private final int bossLoopNum;
    private final int workerLoopNum;
    private final boolean enableEpoll;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    protected long time = 30;

    protected TimeUnit unit = TimeUnit.SECONDS;

    protected TlsCertificateManager tlsCertificateManager;

    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port,
        TlsCertificateManager tlsCertificateManager) {
        return new GrpcServerBuilder(executor, port, tlsCertificateManager);
    }

    /**
     * Creates a server with internally owned event loops. Zero selects Netty's default thread count.
     */
    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port,
        TlsCertificateManager tlsCertificateManager, int bossLoopNum, int workerLoopNum) {
        return new GrpcServerBuilder(executor, port, tlsCertificateManager, bossLoopNum, workerLoopNum);
    }

    /**
     * Creates a server using caller-owned event loops. Both groups must match enableGrpcEpoll.
     * The caller must close them after all servers using them have terminated.
     */
    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port,
        TlsCertificateManager tlsCertificateManager, EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        return new GrpcServerBuilder(executor, port, tlsCertificateManager, bossGroup, workerGroup);
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port, TlsCertificateManager tlsCertificateManager) {
        this(executor, port, tlsCertificateManager, ConfigurationManager.getProxyConfig().getGrpcBossLoopNum(),
            ConfigurationManager.getProxyConfig().getGrpcWorkerLoopNum());
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port, TlsCertificateManager tlsCertificateManager,
        int bossLoopNum, int workerLoopNum) {
        this(executor, port, tlsCertificateManager, bossLoopNum, workerLoopNum, null, null);
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port, TlsCertificateManager tlsCertificateManager,
        EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        this(executor, port, tlsCertificateManager, 0, 0,
            Objects.requireNonNull(bossGroup, "bossGroup"), Objects.requireNonNull(workerGroup, "workerGroup"));
    }

    private GrpcServerBuilder(ThreadPoolExecutor executor, int port, TlsCertificateManager tlsCertificateManager,
        int bossLoopNum, int workerLoopNum, EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        if (bossLoopNum < 0 || workerLoopNum < 0) {
            throw new IllegalArgumentException("Event loop thread counts must not be negative");
        }
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        this.bossLoopNum = bossLoopNum;
        this.workerLoopNum = workerLoopNum;
        this.enableEpoll = config.isEnableGrpcEpoll();
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.tlsCertificateManager = tlsCertificateManager;
        serverBuilder = NettyServerBuilder.forPort(port)
            .maxConcurrentCallsPerConnection(config.getGrpcMaxConcurrentCallsPerConnection());

        serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());

        // build server
        int maxInboundMessageSize = config.getGrpcMaxInboundMessageSize();
        long idleTimeMills = config.getGrpcClientIdleTimeMills();

        serverBuilder.channelType(enableEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .executor(executor);

        serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
            .maxConnectionIdle(idleTimeMills, TimeUnit.MILLISECONDS)
            .permitKeepAliveTime(config.getGrpcServerPermitKeepAliveTimeMillis(), TimeUnit.MILLISECONDS)
            .permitKeepAliveWithoutCalls(config.isGrpcServerPermitKeepAliveWithoutCalls());

        log.info("grpc server builder initialized. port: {}, bossLoopNum: {}, workerLoopNum: {}, "
                + "callerOwnedEventLoops: {}, maxInboundMessageSize: {}",
            port, bossLoopNum, workerLoopNum, bossGroup != null, maxInboundMessageSize);
    }

    public GrpcServerBuilder shutdownTime(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
        return this;
    }

    public GrpcServerBuilder addService(BindableService service) {
        this.serverBuilder.addService(service);
        return this;
    }

    public GrpcServerBuilder addService(ServerServiceDefinition service) {
        this.serverBuilder.addService(service);
        return this;
    }

    public GrpcServerBuilder appendInterceptor(ServerInterceptor interceptor) {
        this.serverBuilder.intercept(interceptor);
        return this;
    }

    public GrpcServer build() throws Exception {
        EventLoopGroup boss = bossGroup;
        EventLoopGroup worker = workerGroup;
        boolean ownsEventLoops = boss == null;
        try {
            if (ownsEventLoops) {
                boss = newEventLoopGroup(bossLoopNum);
                worker = newEventLoopGroup(workerLoopNum);
            }
            return new GrpcServer(serverBuilder.bossEventLoopGroup(boss).workerEventLoopGroup(worker).build(),
                time, unit, tlsCertificateManager, ownsEventLoops ? boss : null, ownsEventLoops ? worker : null);
        } catch (Exception | Error e) {
            if (ownsEventLoops) {
                if (boss != null) {
                    boss.shutdownGracefully();
                }
                if (worker != null) {
                    worker.shutdownGracefully();
                }
            }
            throw e;
        }
    }

    private EventLoopGroup newEventLoopGroup(int threads) {
        return enableEpoll ? new EpollEventLoopGroup(threads) : new NioEventLoopGroup(threads);
    }

    public GrpcServerBuilder configInterceptor() {
        this.serverBuilder
            .intercept(new GlobalExceptionInterceptor())
            .intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());
        return this;
    }
}
