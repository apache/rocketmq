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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

public class GrpcServer implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final Server server;
    private final EventLoopGroup ownedBossGroup;
    private final EventLoopGroup ownedWorkerGroup;

    private final long timeout;

    private final TimeUnit unit;

    private final TlsCertificateManager tlsCertificateManager;
    @VisibleForTesting final GrpcTlsReloadHandler tlsReloadHandler;
    @VisibleForTesting final GrpcDomainTlsReloadHandler domainTlsReloadHandler;

    protected GrpcServer(Server server, long timeout, TimeUnit unit,
        TlsCertificateManager tlsCertificateManager) throws Exception {
        this(server, timeout, unit, tlsCertificateManager, null, null);
    }

    GrpcServer(Server server, long timeout, TimeUnit unit, TlsCertificateManager tlsCertificateManager,
        EventLoopGroup ownedBossGroup, EventLoopGroup ownedWorkerGroup) throws Exception {
        this.server = server;
        this.ownedBossGroup = ownedBossGroup;
        this.ownedWorkerGroup = ownedWorkerGroup;
        this.timeout = timeout;
        this.unit = unit;
        this.tlsCertificateManager = tlsCertificateManager;
        this.tlsReloadHandler = new GrpcTlsReloadHandler();
        this.domainTlsReloadHandler = new GrpcDomainTlsReloadHandler();
    }

    public void start() throws Exception {
        try {
            // Register the TLS context reload handler
            tlsCertificateManager.registerReloadListener(this.tlsReloadHandler);
            tlsCertificateManager.registerDomainReloadListener(this.domainTlsReloadHandler);
            this.server.start();
            log.info("grpc server start successfully.");
        } catch (Exception | Error e) {
            shutdown();
            throw e;
        }
    }

    public void shutdown() {
        try {
            tlsCertificateManager.unregisterReloadListener(this.tlsReloadHandler);
            tlsCertificateManager.unregisterDomainReloadListener(this.domainTlsReloadHandler);

            if (!this.server.shutdown().awaitTermination(timeout, unit)) {
                this.server.shutdownNow().awaitTermination(timeout, unit);
            }

            log.info("grpc server shutdown successfully.");
        } catch (InterruptedException e) {
            this.server.shutdownNow();
            Thread.currentThread().interrupt();
            log.error("Interrupted while shutting down grpc server", e);
        } catch (Exception e) {
            this.server.shutdownNow();
            log.error("Failed to shutdown grpc server", e);
        } finally {
            if (ownedBossGroup != null) {
                ownedBossGroup.shutdownGracefully();
            }
            if (ownedWorkerGroup != null) {
                ownedWorkerGroup.shutdownGracefully();
            }
        }
    }

    @VisibleForTesting
    class GrpcTlsReloadHandler implements TlsCertificateManager.TlsContextReloadListener {
        @Override
        public void onTlsContextReload() {
            try {
                ProxyAndTlsProtocolNegotiator.loadSslContext();
                log.info("SslContext reloaded for grpc server");
            } catch (CertificateException | IOException e) {
                log.error("Failed to reload SslContext for server", e);
            }
        }
    }

    @VisibleForTesting
    class GrpcDomainTlsReloadHandler implements TlsCertificateManager.DomainTlsContextReloadListener {
        @Override
        public void onDomainTlsContextReload(String domainPattern) {
            ProxyAndTlsProtocolNegotiator.reloadDomainSslContext(domainPattern);
            log.info("Domain SslContext reloaded for grpc server, pattern: {}", domainPattern);
        }
    }
}
