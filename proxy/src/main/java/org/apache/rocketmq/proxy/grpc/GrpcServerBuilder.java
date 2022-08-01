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
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.AuthenticationInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;

public class GrpcServerBuilder {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    protected NettyServerBuilder serverBuilder;

    public static GrpcServerBuilder newBuilder(ThreadPoolExecutor executor, int port) {
        return new GrpcServerBuilder(executor, port);
    }

    protected GrpcServerBuilder(ThreadPoolExecutor executor, int port) {
        serverBuilder = NettyServerBuilder.forPort(port);

        try {
            configSslContext(serverBuilder);
        } catch (Exception e) {
            log.error("grpc tls set failed. msg: {}, e:", e.getMessage(), e);
            throw new RuntimeException("grpc tls set failed: " + e.getMessage());
        }

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

        log.info(
            "grpc server has built. port: {}, tlsKeyPath: {}, tlsCertPath: {}, threadPool: {}, queueCapacity: {}, "
                + "boosLoop: {}, workerLoop: {}, maxInboundMessageSize: {}",
            port, bossLoopNum, workerLoopNum, maxInboundMessageSize);
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
        return new GrpcServer(this.serverBuilder.build());
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
        try (InputStream serverKeyInputStream = Files.newInputStream(Paths.get(tlsKeyPath));
             InputStream serverCertificateStream = Files.newInputStream(Paths.get(tlsCertPath))) {
            serverBuilder.sslContext(GrpcSslContexts.forServer(serverCertificateStream, serverKeyInputStream)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build());
            log.info("TLS configured OK");
        } catch (IOException e) {
            log.error("Failed to load Server key/certificate", e);
        }
    }

    public GrpcServerBuilder configInterceptor() {
        // grpc interceptors, including acl, logging etc.
        List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
        if (!accessValidators.isEmpty()) {
            this.serverBuilder.intercept(new AuthenticationInterceptor(accessValidators));
        }

        this.serverBuilder
            .intercept(new GlobalExceptionInterceptor())
            .intercept(new ContextInterceptor())
            .intercept(new HeaderInterceptor());

        return this;
    }
}
