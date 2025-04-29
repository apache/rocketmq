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

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.AuthenticationInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.spi.ProxyServerFactoryBase;

public class GrpcServerBuilder extends ProxyServerFactoryBase {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    @Override
    public GrpcServer build() {
        int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
        //
        ThreadPoolExecutor executor = createServerExecutor();
        appendStartAndShutdown(new StartAndShutdown() {
            @Override
            public void shutdown() throws Exception {
                executor.shutdown();
            }

            @Override
            public void start() throws Exception {

            }
        });
        //
        GrpcMessagingApplication application = GrpcMessagingApplication.create(initializer.getMessagingProcessor());
        appendStartAndShutdown(application);
        //
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port);
        serverBuilder.addService(application)
            .addService(ChannelzService.newInstance(100))
            .addService(ProtoReflectionService.newInstance())
            .intercept(new AuthenticationInterceptor(validators))
            .intercept(new GlobalExceptionInterceptor())
            .intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());

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

        return new GrpcServer(serverBuilder.build(), ConfigurationManager.getProxyConfig().getGrpcShutdownTimeSeconds(), TimeUnit.SECONDS);
    }

    private ThreadPoolExecutor createServerExecutor() {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        int threadPoolNums = config.getGrpcThreadPoolNums();
        int threadPoolQueueCapacity = config.getGrpcThreadPoolQueueCapacity();
        return ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );
    }
}
