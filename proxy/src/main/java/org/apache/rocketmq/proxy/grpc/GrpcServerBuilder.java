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
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
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
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.acl.plain.PlainAccessValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.interceptor.AuthenticationInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.ContextInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.GlobalExceptionInterceptor;
import org.apache.rocketmq.proxy.grpc.interceptor.HeaderInterceptor;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

public class GrpcServerBuilder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
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

    protected void configSslContext(NettyServerBuilder serverBuilder) throws IOException, CertificateException {
        if (null == serverBuilder) {
            return;
        }

        TlsMode tlsMode = TlsSystemConfig.tlsMode;
        if (!TlsMode.DISABLED.equals(tlsMode)) {
            SslContext sslContext = loadSslContext();
            if (TlsMode.PERMISSIVE.equals(tlsMode)) {
                serverBuilder.protocolNegotiator(new OptionalSSLProtocolNegotiator(sslContext));
            } else {
                serverBuilder.sslContext(sslContext);
            }
        }
    }

    protected SslContext loadSslContext() throws CertificateException, IOException {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        if (proxyConfig.isTlsTestModeEnable()) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
                .build();
        } else {
            String tlsKeyPath = ConfigurationManager.getProxyConfig().getTlsKeyPath();
            String tlsCertPath = ConfigurationManager.getProxyConfig().getTlsCertPath();
            try (InputStream serverKeyInputStream = Files.newInputStream(Paths.get(tlsKeyPath));
                 InputStream serverCertificateStream = Files.newInputStream(Paths.get(tlsCertPath))) {
                SslContext res = GrpcSslContexts.forServer(serverCertificateStream, serverKeyInputStream)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(ClientAuth.NONE)
                    .build();
                log.info("load TLS configured OK");
                return res;
            }
        }
    }

    public GrpcServerBuilder configInterceptor() {
        // grpc interceptors, including acl, logging etc.
        List<AccessValidator> accessValidators = ServiceProvider.load(AccessValidator.class);
        if (accessValidators.isEmpty()) {
            log.info("ServiceProvider loaded no AccessValidator, using default org.apache.rocketmq.acl.plain.PlainAccessValidator");
            accessValidators.add(new PlainAccessValidator());
        }
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
