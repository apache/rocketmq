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

package org.apache.rocketmq.grpc.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.protobuf.services.ChannelzService;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class GrpcServer {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final GrpcServerConfig grpcServerConfig;

    private final Server server;

    private final ThreadPoolExecutor executor;

    public GrpcServer(GrpcServerConfig grpcServerConfig, BindableService service,
        boolean useTls) throws CertificateException, SSLException {
        this.grpcServerConfig = grpcServerConfig;
        int processorNumber = Runtime.getRuntime()
            .availableProcessors();
        executor = new ThreadPoolExecutor(grpcServerConfig.getExecutorCoreThreadNumber(),
            grpcServerConfig.getExecutorMaxThreadNumber(), 1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(grpcServerConfig.getExecutorQueueLength()),
            new ThreadFactoryBuilder().setNameFormat("grpc-server-thread-%d")
                .build(),
            new ThreadPoolExecutor.DiscardOldestPolicy());
        NettyServerBuilder serverBuilder = NettyServerBuilder
            .forPort(grpcServerConfig.getPort());
        if (useTls) {
            configSslContext(serverBuilder);
        }
        server = serverBuilder
            .bossEventLoopGroup(new NioEventLoopGroup(Math.max(1, processorNumber / 2)))
            .workerEventLoopGroup(new NioEventLoopGroup(processorNumber))
            .executor(executor)
            .channelType(NioServerSocketChannel.class)
            .addService(service)
            .addService(ChannelzService.newInstance(100))
            .build();
    }

    private InputStream loadCert(String name) {
        return getClass().getClassLoader()
            .getResourceAsStream("certs/" + name);
    }

    private void configSslContext(NettyServerBuilder serverBuilder) throws CertificateException, SSLException {
        if (null == serverBuilder) {
            return;
        }
        try (InputStream serverKeyInputStream = loadCert("gRPC.key.pem");
             InputStream serverCertificateStream = loadCert("gRPC.chain.cert.pem")) {
            serverBuilder.sslContext(GrpcSslContexts.forServer(serverCertificateStream, serverKeyInputStream)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build());
            LOGGER.info("TLS configured OK");
        } catch (IOException e) {
            LOGGER.error("Failed to load Server key/certificate", e);
        }
    }

    public GrpcServer(GrpcServerConfig grpcServerConfig, ServerServiceDefinition serverServiceDefinition,
        boolean useTls) throws CertificateException, SSLException {
        this.grpcServerConfig = grpcServerConfig;
        int processorNumber = Runtime.getRuntime()
            .availableProcessors();
        executor = new ThreadPoolExecutor(grpcServerConfig.getExecutorCoreThreadNumber(),
            grpcServerConfig.getExecutorMaxThreadNumber(), 1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(grpcServerConfig.getExecutorQueueLength()),
            new ThreadFactoryBuilder().setNameFormat("grpc-server-thread-%d")
                .build(),
            new ThreadPoolExecutor.DiscardOldestPolicy());
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(grpcServerConfig.getPort());
        if (useTls) {
            configSslContext(serverBuilder);
        }
        server = serverBuilder
            .bossEventLoopGroup(new NioEventLoopGroup(Math.max(1, processorNumber / 2)))
            .workerEventLoopGroup(new NioEventLoopGroup(processorNumber))
            .executor(executor)
            .channelType(NioServerSocketChannel.class)
            .addService(serverServiceDefinition)
            .build();
    }

    /**
     * Expose the actual port gRPC listens to, as is useful for unit tests.
     *
     * @return Actual listening port.
     */
    public int getPort() {
        return server.getPort();
    }

    public void addShutdownHook() {
        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> {
                System.err.println("Shutting down gRPC server since JVM is shutting down");
                try {
                    GrpcServer.this.stop();
                    System.err.println("gRPC server has shut down");
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }));
    }

    public void start() throws IOException {
        server.start();
        addShutdownHook();
        LOGGER.info("Server started OK. Listen: {}", getPort());
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (null != server) {
            server.awaitTermination();
        }
        executor.shutdown();
    }

    public void stop() throws InterruptedException {
        if (null != server) {
            server.shutdown()
                .awaitTermination(grpcServerConfig.getTerminationAwaitTimeout(), TimeUnit.SECONDS);
        }
        executor.shutdown();
    }
}
