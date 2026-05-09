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
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
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

public class TlsSniManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private volatile SslContext defaultContext;
    private volatile Map<String, SslContext> domainContexts = new ConcurrentHashMap<>();
    private Map<String, TlsDomainConfig> domainConfigs;
    private boolean tlsTestModeEnable;
    private String tlsKeyPassword;
    private boolean wildcardMatchMultiLevel;

    /**
     * Get the matching SslContext for the given SNI hostname.
     * Supports wildcard matching (e.g. *.example.com matches foo.example.com).
     * When wildcardMatchMultiLevel is true, a.b.example.com also matches *.example.com.
     * Returns defaultContext when no match is found.
     */
    public SslContext getSslContext(String sniHostname) {
        if (StringUtils.isBlank(sniHostname)) {
            return defaultContext;
        }

        // Exact match first
        SslContext ctx = domainContexts.get(sniHostname);
        if (ctx != null) {
            return ctx;
        }

        // Wildcard match: foo.example.com matches *.example.com
        for (Map.Entry<String, SslContext> entry : domainContexts.entrySet()) {
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
        for (Map.Entry<String, SslContext> entry : domainContexts.entrySet()) {
            String domainPattern = entry.getKey();
            if (domainPattern.startsWith("*.")) {
                String bareDomain = domainPattern.substring(2);
                if (sniHostname.equals(bareDomain)) {
                    return entry.getValue();
                }
            }
        }

        return defaultContext;
    }

    public SslContext getDefaultContext() {
        return defaultContext;
    }

    public Map<String, TlsDomainConfig> getDomainConfigs() {
        return domainConfigs;
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
            SslContext newCtx = buildSslContext(config, tlsTestModeEnable);
            domainContexts.put(domainPattern, newCtx);
            log.info("Reloaded SslContext for domain: {}", domainPattern);
        } catch (Exception e) {
            log.error("Failed to reload SslContext for domain: {}", domainPattern, e);
        }
    }

    /**
     * Reload the default context.
     */
    public void reloadDefaultContext() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        try {
            defaultContext = buildDefaultSslContext(proxyConfig);
            log.info("Reloaded default SslContext");
        } catch (Exception e) {
            log.error("Failed to reload default SslContext", e);
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
            defaultContext = buildDefaultSslContext(config);
            log.info("Initialized default SslContext");
        } catch (Exception e) {
            log.error("Failed to initialize default SslContext", e);
            throw new RuntimeException("Failed to initialize TlsSniManager", e);
        }

        if (domainConfigs != null && !domainConfigs.isEmpty()) {
            for (Map.Entry<String, TlsDomainConfig> entry : domainConfigs.entrySet()) {
                String domainPattern = entry.getKey();
                TlsDomainConfig domainConfig = entry.getValue();
                try {
                    SslContext ctx = buildSslContext(domainConfig, tlsTestModeEnable);
                    domainContexts.put(domainPattern, ctx);
                    log.info("Initialized SslContext for domain: {}", domainPattern);
                } catch (Exception e) {
                    log.error("Failed to initialize SslContext for domain: {}", domainPattern, e);
                    throw new RuntimeException("Failed to initialize TlsSniManager for domain: " + domainPattern, e);
                }
            }
        }
    }

    private SslContext buildDefaultSslContext(ProxyConfig config) throws CertificateException, IOException {
        SslProvider provider = OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        if (config.isTlsTestModeEnable()) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
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
                    .clientAuth(ClientAuth.NONE)
                    .build();
            }
        }
    }

    private SslContext buildSslContext(TlsDomainConfig config, boolean testMode) throws CertificateException, IOException {
        SslProvider provider = OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        if (testMode) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(ClientAuth.NONE)
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
                    .clientAuth(ClientAuth.NONE)
                    .build();
            }
        }
    }
}
