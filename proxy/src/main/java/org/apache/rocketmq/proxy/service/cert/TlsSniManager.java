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
package org.apache.rocketmq.proxy.service.cert;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.config.TlsDomainConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages multiple SslContext instances for SNI-based certificate selection.
 * Maintains both standard Netty SslContext (for remoting server) and
 * gRPC-shaded SslContext (for gRPC server) from the same certificate configs.
 */
public class TlsSniManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    // Standard Netty SslContext (for remoting server)
    private volatile io.netty.handler.ssl.SslContext defaultStdContext;
    private volatile Map<String, io.netty.handler.ssl.SslContext> stdDomainContexts = new ConcurrentHashMap<>();

    // gRPC-shaded Netty SslContext (for gRPC server)
    private volatile SslContext defaultShadedContext;
    private volatile Map<String, SslContext> shadedDomainContexts = new ConcurrentHashMap<>();

    private Map<String, TlsDomainConfig> domainConfigs;
    private boolean tlsTestModeEnable;
    private String tlsKeyPassword;
    private boolean wildcardMatchMultiLevel;

    // --- Standard Netty SslContext accessors ---

    public io.netty.handler.ssl.SslContext getStdSslContext(String sniHostname) {
        return lookupContext(sniHostname, stdDomainContexts, defaultStdContext);
    }

    public io.netty.handler.ssl.SslContext getStdDefaultContext() {
        return defaultStdContext;
    }

    // --- gRPC-shaded SslContext accessors ---

    public SslContext getSslContext(String sniHostname) {
        return lookupContext(sniHostname, shadedDomainContexts, defaultShadedContext);
    }

    public SslContext getDefaultContext() {
        return defaultShadedContext;
    }

    // --- Common ---

    public Map<String, TlsDomainConfig> getDomainConfigs() {
        return domainConfigs;
    }

    private <T> T lookupContext(String sniHostname, Map<String, T> domainContexts, T defaultCtx) {
        if (StringUtils.isBlank(sniHostname)) {
            return defaultCtx;
        }

        // Exact match first
        T ctx = domainContexts.get(sniHostname);
        if (ctx != null) {
            return ctx;
        }

        // Wildcard match: foo.example.com matches *.example.com
        for (Map.Entry<String, T> entry : domainContexts.entrySet()) {
            String domainPattern = entry.getKey();
            if (domainPattern.startsWith("*.")) {
                String suffix = domainPattern.substring(1);
                if (sniHostname.endsWith(suffix) && sniHostname.length() > suffix.length()) {
                    String remaining = sniHostname.substring(0, sniHostname.length() - suffix.length());
                    if (wildcardMatchMultiLevel || !remaining.contains(".")) {
                        return entry.getValue();
                    }
                }
            }
        }

        // Bare domain matches wildcard: rocketmq.com matches *.rocketmq.com
        for (Map.Entry<String, T> entry : domainContexts.entrySet()) {
            String domainPattern = entry.getKey();
            if (domainPattern.startsWith("*.")) {
                String bareDomain = domainPattern.substring(2);
                if (sniHostname.equals(bareDomain)) {
                    return entry.getValue();
                }
            }
        }

        return defaultCtx;
    }

    /**
     * Rebuild SslContext for a specific domain (used for hot reload).
     */
    public void reloadDomainContext(String domainPattern) {
        TlsDomainConfig config = domainConfigs.get(domainPattern);
        if (config == null) {
            log.warn("Cannot reload domain context, config not found: {}", domainPattern);
            return;
        }
        try {
            io.netty.handler.ssl.SslContext stdCtx = buildStdSslContext(config, tlsTestModeEnable);
            stdDomainContexts.put(domainPattern, stdCtx);
            SslContext shadedCtx = buildShadedSslContext(config, tlsTestModeEnable);
            shadedDomainContexts.put(domainPattern, shadedCtx);
            log.info("Reloaded SslContext for domain: {}", domainPattern);
        } catch (Exception e) {
            log.error("Failed to reload SslContext for domain: {}", domainPattern, e);
            throw new RuntimeException("Failed to reload SslContext for domain: " + domainPattern, e);
        }
    }

    /**
     * Reload the default context.
     */
    public void reloadDefaultContext() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        try {
            defaultStdContext = buildStdDefaultSslContext(proxyConfig);
            defaultShadedContext = buildShadedDefaultSslContext(proxyConfig);
            log.info("Reloaded default SslContext");
        } catch (Exception e) {
            log.error("Failed to reload default SslContext", e);
            throw new RuntimeException("Failed to reload default SslContext", e);
        }
    }

    /**
     * Initialize all domain SslContexts from ProxyConfig.
     */
    public void initialize(ProxyConfig config) {
        this.tlsTestModeEnable = config.isTlsTestModeEnable();
        this.tlsKeyPassword = config.getTlsKeyPassword();
        this.domainConfigs = config.getTlsDomainConfigs();
        this.wildcardMatchMultiLevel = config.isTlsWildcardMatchMultiLevel();

        try {
            defaultStdContext = buildStdDefaultSslContext(config);
            defaultShadedContext = buildShadedDefaultSslContext(config);
            log.info("Initialized default SslContext (standard + gRPC-shaded)");
        } catch (Exception e) {
            log.error("Failed to initialize default SslContext", e);
            throw new RuntimeException("Failed to initialize TlsSniManager", e);
        }

        if (domainConfigs != null && !domainConfigs.isEmpty()) {
            for (Map.Entry<String, TlsDomainConfig> entry : domainConfigs.entrySet()) {
                String domainPattern = entry.getKey();
                TlsDomainConfig domainConfig = entry.getValue();
                try {
                    io.netty.handler.ssl.SslContext stdCtx = buildStdSslContext(domainConfig, tlsTestModeEnable);
                    stdDomainContexts.put(domainPattern, stdCtx);
                    SslContext shadedCtx = buildShadedSslContext(domainConfig, tlsTestModeEnable);
                    shadedDomainContexts.put(domainPattern, shadedCtx);
                    log.info("Initialized SslContext for domain: {}", domainPattern);
                } catch (Exception e) {
                    log.error("Failed to initialize SslContext for domain: {}", domainPattern, e);
                    throw new RuntimeException("Failed to initialize TlsSniManager for domain: " + domainPattern, e);
                }
            }
        }
    }

    // --- Standard Netty SslContext builders ---

    private io.netty.handler.ssl.SslContext buildStdDefaultSslContext(ProxyConfig config) throws CertificateException, IOException {
        io.netty.handler.ssl.SslProvider provider = io.netty.handler.ssl.OpenSsl.isAvailable()
            ? io.netty.handler.ssl.SslProvider.OPENSSL : io.netty.handler.ssl.SslProvider.JDK;
        if (config.isTlsTestModeEnable()) {
            io.netty.handler.ssl.util.SelfSignedCertificate ssc = new io.netty.handler.ssl.util.SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .sslProvider(provider)
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(io.netty.handler.ssl.ClientAuth.NONE)
                .build();
        } else {
            String tlsCertPath = config.getTlsCertPath();
            String tlsKeyPath = config.getTlsKeyPath();
            String tlsKeyPassword = config.getTlsKeyPassword();
            try (InputStream keyIn = Files.newInputStream(Paths.get(tlsKeyPath));
                 InputStream certIn = Files.newInputStream(Paths.get(tlsCertPath))) {
                return SslContextBuilder.forServer(certIn, keyIn,
                        StringUtils.isNotBlank(tlsKeyPassword) ? tlsKeyPassword : null)
                    .sslProvider(provider)
                    .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }

    private io.netty.handler.ssl.SslContext buildStdSslContext(TlsDomainConfig config, boolean testMode) throws CertificateException, IOException {
        io.netty.handler.ssl.SslProvider provider = io.netty.handler.ssl.OpenSsl.isAvailable()
            ? io.netty.handler.ssl.SslProvider.OPENSSL : io.netty.handler.ssl.SslProvider.JDK;
        if (testMode) {
            io.netty.handler.ssl.util.SelfSignedCertificate ssc = new io.netty.handler.ssl.util.SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                .sslProvider(provider)
                .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(io.netty.handler.ssl.ClientAuth.NONE)
                .build();
        } else {
            String tlsCertPath = config.getCertPath();
            String tlsKeyPath = config.getKeyPath();
            String tlsKeyPassword = StringUtils.isNotBlank(config.getKeyPassword()) ? config.getKeyPassword() : this.tlsKeyPassword;
            try (InputStream keyIn = Files.newInputStream(Paths.get(tlsKeyPath));
                 InputStream certIn = Files.newInputStream(Paths.get(tlsCertPath))) {
                return SslContextBuilder.forServer(certIn, keyIn,
                        StringUtils.isNotBlank(tlsKeyPassword) ? tlsKeyPassword : null)
                    .sslProvider(provider)
                    .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }

    // --- gRPC-shaded Netty SslContext builders ---

    private SslContext buildShadedDefaultSslContext(ProxyConfig config) throws CertificateException, IOException {
        SslProvider provider = io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        if (config.isTlsTestModeEnable()) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                .build();
        } else {
            String tlsCertPath = config.getTlsCertPath();
            String tlsKeyPath = config.getTlsKeyPath();
            String tlsKeyPassword = config.getTlsKeyPassword();
            try (InputStream serverKeyInputStream = Files.newInputStream(Paths.get(tlsKeyPath));
                 InputStream serverCertificateStream = Files.newInputStream(Paths.get(tlsCertPath))) {
                return GrpcSslContexts.forServer(serverCertificateStream,
                        serverKeyInputStream,
                        StringUtils.isNotBlank(tlsKeyPassword) ? tlsKeyPassword : null)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }

    private SslContext buildShadedSslContext(TlsDomainConfig config, boolean testMode) throws CertificateException, IOException {
        SslProvider provider = io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        if (testMode) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                .build();
        } else {
            String tlsCertPath = config.getCertPath();
            String tlsKeyPath = config.getKeyPath();
            String tlsKeyPassword = StringUtils.isNotBlank(config.getKeyPassword()) ? config.getKeyPassword() : this.tlsKeyPassword;
            try (InputStream serverKeyInputStream = Files.newInputStream(Paths.get(tlsKeyPath));
                 InputStream serverCertificateStream = Files.newInputStream(Paths.get(tlsCertPath))) {
                return GrpcSslContexts.forServer(serverCertificateStream,
                        serverKeyInputStream,
                        StringUtils.isNotBlank(tlsKeyPassword) ? tlsKeyPassword : null)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }
}
