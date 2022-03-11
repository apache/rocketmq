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
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final io.grpc.Server server;

    private final ThreadPoolExecutor executor;
    private final GrpcService grpcService;

    public GrpcServer(GrpcService grpcService) {
        this.grpcService = grpcService;
        int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(port);

        // add tls files
        String tlsKeyPath = ConfigurationManager.getProxyConfig().getGrpcTlsKeyPath();
        String tlsCertPath = ConfigurationManager.getProxyConfig().getGrpcTlsCertPath();
        try {
            InputStream serverKeyInputStream = new FileInputStream(tlsKeyPath);
            InputStream serverCertificateStream = new FileInputStream(tlsCertPath);

            SslContext sslContext = GrpcSslContexts.forServer(serverCertificateStream, serverKeyInputStream)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build();
            serverBuilder.sslContext(sslContext);
        } catch (IOException e) {
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

        GrpcMessagingProcessor messagingProcessor = new GrpcMessagingProcessor(grpcService);

        // build server
        int bossLoopNum = ConfigurationManager.getProxyConfig().getGrpcBossLoopNum();
        int workerLoopNum = ConfigurationManager.getProxyConfig().getGrpcWorkerLoopNum();
        int maxInboundMessageSize = ConfigurationManager.getProxyConfig().getGrpcMaxInboundMessageSize();

        this.server = serverBuilder
            .maxInboundMessageSize(maxInboundMessageSize)
            .bossEventLoopGroup(new NioEventLoopGroup(bossLoopNum))
            .workerEventLoopGroup(new NioEventLoopGroup(workerLoopNum))
            .channelType(NioServerSocketChannel.class)
            .addService(messagingProcessor)
            .executor(this.executor)
            .build();

        log.info(
            "grpc server has built. port: {}, tlsKeyPath: {}, tlsCertPath: {}, threadPool: {}, queueCapacity: {}, "
                + "boosLoop: {}, workerLoop: {}, maxInboundMessageSize: {}",
            port, tlsKeyPath, tlsCertPath, threadPoolNums, threadPoolQueueCapacity,
            bossLoopNum, workerLoopNum, maxInboundMessageSize);
    }


    public void start() throws Exception {
        // first to start grpc service.
        this.grpcService.start();

        this.server.start();
        log.info("grpc server has started");
    }

    public void shutdown() {
        try {
            this.server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            this.executor.shutdown();

            this.grpcService.shutdown();

            log.info("grpc server has stopped");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}