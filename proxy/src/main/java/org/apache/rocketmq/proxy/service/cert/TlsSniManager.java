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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Manages multiple SslContext instances for SNI-based certificate selection.
 * Maintains both standard Netty SslContext (for remoting server) and
 * gRPC-shaded SslContext (for gRPC server) from the same certificate configs.
 *
 * This class is a singleton — all components should access it via {@link #getInstance()}.
 * Wildcard domains are matched in deterministic longest-pattern-first order.
 */
public class TlsSniManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static volatile TlsSniManager instance;

    // Standard Netty SslContext (for remoting server)
    private volatile io.netty.handler.ssl.SslContext defaultStdContext;
    private final Map<String, io.netty.handler.ssl.SslContext> stdExactContexts = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<WildcardSslContext<io.netty.handler.ssl.SslContext>> stdWildcardContexts = new CopyOnWriteArrayList<>();

    // gRPC-shaded Netty SslContext (for gRPC server)
    private volatile SslContext defaultShadedContext;
    private final Map<String, SslContext> shadedExactContexts = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<WildcardSslContext<SslContext>> shadedWildcardContexts = new CopyOnWriteArrayList<>();

    private Map<String, TlsDomainConfig> domainConfigs;
    private volatile boolean tlsTestModeEnable;
    private volatile String tlsKeyPassword;
    private volatile boolean wildcardMatchMultiLevel;

    public static TlsSniManager getInstance() {
        TlsSniManager local = instance;
        if (local == null) {
            synchronized (TlsSniManager.class) {
                local = instance;
                if (local == null) {
                    local = new TlsSniManager();
                    local.initialize(ConfigurationManager.getProxyConfig());
                    instance = local;
                }
            }
        }
        return local;
    }

    // For testing — reset singleton state
    public static void resetInstance() {
        instance = null;
    }

    // --- Standard Netty SslContext accessors ---

    public io.netty.handler.ssl.SslContext getStdSslContext(String sniHostname) {
        return lookupContext(sniHostname, stdExactContexts, stdWildcardContexts, defaultStdContext);
    }

    public io.netty.handler.ssl.SslContext getStdDefaultContext() {
        return defaultStdContext;
    }

    // --- gRPC-shaded SslContext accessors ---

    public SslContext getSslContext(String sniHostname) {
        return lookupContext(sniHostname, shadedExactContexts, shadedWildcardContexts, defaultShadedContext);
    }

    public SslContext getDefaultContext() {
        return defaultShadedContext;
    }

    // --- Common ---

    public Map<String, TlsDomainConfig> getDomainConfigs() {
        return domainConfigs;
    }

    private <T> T lookupContext(String sniHostname,
            Map<String, T> exactContexts,
            List<WildcardSslContext<T>> wildcardContexts,
            T defaultCtx) {
        if (StringUtils.isBlank(sniHostname)) {
            return defaultCtx;
        }

        // Normalize to lowercase — DNS is case-insensitive per RFC 4343
        String hostname = sniHostname.toLowerCase(java.util.Locale.ROOT);

        // Exact match first
        T ctx = exactContexts.get(hostname);
        if (ctx != null) {
            return ctx;
        }

        // Wildcard match: patterns are sorted longest-first for deterministic matching
        for (WildcardSslContext<T> wc : wildcardContexts) {
            if (wc.matches(hostname, wildcardMatchMultiLevel)) {
                return wc.context;
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
            SslContext shadedCtx = buildShadedSslContext(config, tlsTestModeEnable);
            // Release old context to avoid native SSL resource leaks
            if (!domainPattern.startsWith("*.")) {
                releaseQuietly(stdExactContexts.get(domainPattern));
                releaseQuietly(shadedExactContexts.get(domainPattern));
            } else {
                // For wildcard patterns, find and release old contexts before replacement
                for (WildcardSslContext<io.netty.handler.ssl.SslContext> wc : stdWildcardContexts) {
                    if (wc.domainPattern.equals(domainPattern)) {
                        releaseQuietly(wc.context);
                        break;
                    }
                }
                for (WildcardSslContext<SslContext> wc : shadedWildcardContexts) {
                    if (wc.domainPattern.equals(domainPattern)) {
                        releaseQuietly(wc.context);
                        break;
                    }
                }
            }
            addDomainContexts(domainPattern, stdCtx, shadedCtx);
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
            io.netty.handler.ssl.SslContext oldStd = defaultStdContext;
            SslContext oldShaded = defaultShadedContext;
            defaultStdContext = buildStdDefaultSslContext(proxyConfig);
            defaultShadedContext = buildShadedDefaultSslContext(proxyConfig);
            releaseQuietly(oldStd);
            releaseQuietly(oldShaded);
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
            // Sort domain patterns: exact match first (no wildcard), then wildcard patterns longest-first
            List<String> sortedPatterns = new ArrayList<>(domainConfigs.keySet());
            sortedPatterns.sort((a, b) -> {
                boolean aWildcard = a.startsWith("*.");
                boolean bWildcard = b.startsWith("*.");
                if (aWildcard != bWildcard) {
                    return aWildcard ? 1 : -1; // exact first
                }
                if (aWildcard) {
                    return b.length() - a.length(); // longer wildcard first
                }
                return 0;
            });

            for (String domainPattern : sortedPatterns) {
                TlsDomainConfig domainConfig = domainConfigs.get(domainPattern);
                try {
                    io.netty.handler.ssl.SslContext stdCtx = buildStdSslContext(domainConfig, tlsTestModeEnable);
                    SslContext shadedCtx = buildShadedSslContext(domainConfig, tlsTestModeEnable);
                    addDomainContexts(domainPattern, stdCtx, shadedCtx);
                    log.info("Initialized SslContext for domain: {}", domainPattern);
                } catch (Exception e) {
                    log.error("Failed to initialize SslContext for domain: {}", domainPattern, e);
                    throw new RuntimeException("Failed to initialize TlsSniManager for domain: " + domainPattern, e);
                }
            }
        }
    }

    private void addDomainContexts(String domainPattern,
            io.netty.handler.ssl.SslContext stdCtx,
            SslContext shadedCtx) {
        if (domainPattern.startsWith("*.")) {
            // Remove existing entries for this pattern before adding to ensure reload replaces old context
            stdWildcardContexts.removeIf(wc -> wc.domainPattern.equals(domainPattern));
            shadedWildcardContexts.removeIf(wc -> wc.domainPattern.equals(domainPattern));
            stdWildcardContexts.add(new WildcardSslContext<>(domainPattern, stdCtx));
            shadedWildcardContexts.add(new WildcardSslContext<>(domainPattern, shadedCtx));
        } else {
            stdExactContexts.put(domainPattern, stdCtx);
            shadedExactContexts.put(domainPattern, shadedCtx);
        }
    }

    // --- Standard Netty SslContext builders ---

    private io.netty.handler.ssl.SslContext buildStdDefaultSslContext(ProxyConfig config) throws CertificateException, IOException {
        return buildStdSslContextInternal(
            config.isTlsTestModeEnable() ? null : config.getTlsCertPath(),
            config.isTlsTestModeEnable() ? null : config.getTlsKeyPath(),
            config.isTlsTestModeEnable() ? null : config.getTlsKeyPassword(),
            config.isTlsTestModeEnable()
        );
    }

    private io.netty.handler.ssl.SslContext buildStdSslContext(TlsDomainConfig config, boolean testMode) throws CertificateException, IOException {
        String keyPassword = StringUtils.isNotBlank(config.getKeyPassword()) ? config.getKeyPassword() : this.tlsKeyPassword;
        return buildStdSslContextInternal(
            testMode ? null : config.getCertPath(),
            testMode ? null : config.getKeyPath(),
            testMode ? null : keyPassword,
            testMode
        );
    }

    private io.netty.handler.ssl.SslContext buildStdSslContextInternal(
            String certPath, String keyPath, String keyPassword, boolean testMode)
            throws CertificateException, IOException {
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
            try (InputStream keyIn = Files.newInputStream(Paths.get(keyPath));
                 InputStream certIn = Files.newInputStream(Paths.get(certPath))) {
                return SslContextBuilder.forServer(certIn, keyIn,
                        StringUtils.isNotBlank(keyPassword) ? keyPassword : null)
                    .sslProvider(provider)
                    .trustManager(io.netty.handler.ssl.util.InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }

    // --- gRPC-shaded Netty SslContext builders ---

    private SslContext buildShadedDefaultSslContext(ProxyConfig config) throws CertificateException, IOException {
        return buildShadedSslContextInternal(
            config.isTlsTestModeEnable() ? null : config.getTlsCertPath(),
            config.isTlsTestModeEnable() ? null : config.getTlsKeyPath(),
            config.isTlsTestModeEnable() ? null : config.getTlsKeyPassword(),
            config.isTlsTestModeEnable()
        );
    }

    private SslContext buildShadedSslContext(TlsDomainConfig config, boolean testMode) throws CertificateException, IOException {
        String keyPassword = StringUtils.isNotBlank(config.getKeyPassword()) ? config.getKeyPassword() : this.tlsKeyPassword;
        return buildShadedSslContextInternal(
            testMode ? null : config.getCertPath(),
            testMode ? null : config.getKeyPath(),
            testMode ? null : keyPassword,
            testMode
        );
    }

    private SslContext buildShadedSslContextInternal(
            String certPath, String keyPath, String keyPassword, boolean testMode)
            throws CertificateException, IOException {
        SslProvider provider = io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
        if (testMode) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            return GrpcSslContexts.forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                .build();
        } else {
            try (InputStream serverKeyInputStream = Files.newInputStream(Paths.get(keyPath));
                 InputStream serverCertificateStream = Files.newInputStream(Paths.get(certPath))) {
                return GrpcSslContexts.forServer(serverCertificateStream,
                        serverKeyInputStream,
                        StringUtils.isNotBlank(keyPassword) ? keyPassword : null)
                    .sslProvider(provider)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .clientAuth(io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth.NONE)
                    .build();
            }
        }
    }

    private static void releaseQuietly(Object ctx) {
        if (ctx != null) {
            try {
                io.netty.util.ReferenceCountUtil.release(ctx);
            } catch (Exception e) {
                log.warn("Failed to release SslContext", e);
            }
        }
    }

    /**
     * Holds a wildcard domain pattern and its associated SslContext.
     */
    private static class WildcardSslContext<T> {
        final String domainPattern;
        final T context;

        WildcardSslContext(String domainPattern, T context) {
            this.domainPattern = domainPattern;
            this.context = context;
        }

        /**
         * Check if a hostname matches this wildcard pattern.
         * RFC 6125: *.example.com matches foo.example.com but not a.b.example.com
         * (unless multiLevel matching is enabled).
         */
        boolean matches(String hostname, boolean multiLevel) {
            if (!domainPattern.startsWith("*.")) {
                return false;
            }
            String suffix = domainPattern.substring(1); // ".example.com"
            if (!hostname.endsWith(suffix) || hostname.length() <= suffix.length()) {
                return false;
            }
            String remaining = hostname.substring(0, hostname.length() - suffix.length());
            // For single-level wildcard matching, the remaining part must not contain "."
            return multiLevel || !remaining.contains(".");
        }
    }
}
