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

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.AuthenticationInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingProcessor;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcForwardService;

public class GrpcServer implements StartAndShutdown {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final io.grpc.Server server;
    private final ThreadPoolExecutor executor;
    private final GrpcForwardService grpcForwardService;

    public GrpcServer(GrpcForwardService grpcForwardService) {
        this.grpcForwardService = grpcForwardService;
        int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port);

        try {
            configSslContext(serverBuilder);
        } catch (Exception e) {
            log.error("grpc tls set failed. msg: {}, e:", e.getMessage(), e);
            throw new RuntimeException("grpc tls set failed: " + e.getMessage());
        }

        // create executor
        int threadPoolNums = ConfigurationManager.getProxyConfig().getGrpcThreadPoolNums();
        int threadPoolQueueCapacity = ConfigurationManager.getProxyConfig().getGrpcThreadPoolQueueCapacity();
        this.executor = ThreadPoolMonitor.createAndMonitor(
            threadPoolNums,
            threadPoolNums,
            1, TimeUnit.MINUTES,
            "GrpcRequestExecutorThread",
            threadPoolQueueCapacity
        );

        GrpcMessagingProcessor messagingProcessor = createProcessor();

        // build server
        int bossLoopNum = ConfigurationManager.getProxyConfig().getGrpcBossLoopNum();
        int workerLoopNum = ConfigurationManager.getProxyConfig().getGrpcWorkerLoopNum();
        int maxInboundMessageSize = ConfigurationManager.getProxyConfig().getGrpcMaxInboundMessageSize();

        if (ConfigurationManager.getProxyConfig().isEnableGrpcEpoll()) {
            serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
                .bossEventLoopGroup(new EpollEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new EpollEventLoopGroup(workerLoopNum))
                .channelType(EpollServerSocketChannel.class)
                .addService(messagingProcessor)
                .executor(this.executor);
        } else {
            serverBuilder.maxInboundMessageSize(maxInboundMessageSize)
                .bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
                .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
                .channelType(NioServerSocketChannel.class)
                .addService(messagingProcessor)
                .executor(this.executor);
        }

        configInterceptor(serverBuilder);
        this.server = serverBuilder.build();

        log.info(
            "grpc server has built. port: {}, tlsKeyPath: {}, tlsCertPath: {}, threadPool: {}, queueCapacity: {}, "
                + "boosLoop: {}, workerLoop: {}, maxInboundMessageSize: {}",
            port, threadPoolNums, threadPoolQueueCapacity,
            bossLoopNum, workerLoopNum, maxInboundMessageSize);
    }

    protected GrpcMessagingProcessor createProcessor() {
        return new GrpcMessagingProcessor(grpcForwardService);
    }

    protected void configSslContext(NettyServerBuilder serverBuilder) throws SSLException, CertificateException {
        if (null == serverBuilder) {
            return;
        }
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        boolean tlsTestModeEnable = proxyConfig.isGrpcTlsTestModeEnable();
        if (tlsTestModeEnable) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            serverBuilder.sslContext(GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build());
            return;
        }

        String tlsKeyPath = ConfigurationManager.getProxyConfig().getGrpcTlsKeyPath();
        String tlsCertPath = ConfigurationManager.getProxyConfig().getGrpcTlsCertPath();
        try (InputStream serverKeyInputStream = new FileInputStream(tlsKeyPath);
             InputStream serverCertificateStream = new FileInputStream(tlsCertPath)) {
            serverBuilder.sslContext(GrpcSslContexts.forServer(serverCertificateStream, serverKeyInputStream)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build());
            log.info("TLS configured OK");
        } catch (IOException e) {
            log.error("Failed to load Server key/certificate", e);
        }
    }

    protected void configInterceptor(NettyServerBuilder serverBuilder) {
        // grpc interceptors, including acl, logging etc.
        if (ConfigurationManager.getProxyConfig().isEnableACL()) {
            List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
            if (accessValidators.isEmpty()) {
                throw new IllegalArgumentException("Load AccessValidator failed");
            }
            serverBuilder.intercept(new AuthenticationInterceptor(accessValidators));
        }

        serverBuilder.intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());
    }

    public void start() throws Exception {
        // first to start grpc service.
        this.grpcForwardService.start();

        this.server.start();
        log.info("grpc server start successfully.");
    }

    public void shutdown() {
        try {
            this.server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            this.executor.shutdown();

            this.grpcForwardService.shutdown();

            log.info("grpc server shutdown successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}