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
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.interceptor.AuthenticationInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;

public class GrpcServerBuilder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected NettyServerBuilder serverBuilder;

    protected long time = 30;

    protected TimeUnit unit = TimeUnit.SECONDS;

    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port) {
        return new GrpcServerBuilder(executor, port);
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port) {
        serverBuilder = NettyServerBuilder.forPort(port);

        serverBuilder.protocolNegotiator(new ProxyAndTlsProtocolNegotiator());

        // build server
        int bossLoopNum = ConfigurationManager.getProxyConfig().getGrpcBossLoopNum();
        int workerLoopNum = ConfigurationManager.getProxyConfig().getGrpcWorkerLoopNum();
        int maxInboundMessageSize = ConfigurationManager.getProxyConfig().getGrpcMaxInboundMessageSize();
        long idleTimeMills = ConfigurationManager.getProxyConfig().getGrpcClientIdleTimeMills();

        if (ConfigurationManager.getProxyConfig().isEnableGrpcEpoll()) {
            serverBuilder.bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
                .channelType(EpollServerSocketChannel.class)
                .executor(executor);
        } else {
            serverBuilder.bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
                .channelType(NioServerSocketChannel.class)
                .executor(executor);
        }

        serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
                .maxConnectionIdle(idleTimeMills, TimeUnit.MILLISECONDS);

        log.info("grpc server has built. port: {}, bossLoopNum: {}, workerLoopNum: {}, maxInboundMessageSize: {}",
            port, bossLoopNum, workerLoopNum, maxInboundMessageSize);
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

    public GrpcServer build() {
        return new GrpcServer(this.serverBuilder.build(), time, unit);
    }

    public GrpcServerBuilder configInterceptor(List<AccessValidator> accessValidators) {
        // grpc interceptors, including acl, logging etc.
        this.serverBuilder
            .intercept(new AuthenticationInterceptor(accessValidators));

        this.serverBuilder
            .intercept(new GlobalExceptionInterceptor())
            .intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());

        return this;
    }
}
