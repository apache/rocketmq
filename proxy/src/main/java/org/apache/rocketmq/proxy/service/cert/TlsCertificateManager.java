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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.config.TlsDomainConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TlsCertificateManager implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final List<TlsContextReloadListener> reloadListeners = new ArrayList<>();
    private final List<DomainReloadListener> domainReloadListeners = new ArrayList<>();
    private final List<FileWatchService> fileWatchServices = new ArrayList<>();
    private final TlsSniManager tlsSniManager;

    public TlsCertificateManager(TlsSniManager tlsSniManager) {
        this.tlsSniManager = tlsSniManager;
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        int watchInterval = config.getTlsCertWatchIntervalMs();

        // Watch default cert/key pair
        try {
            String defaultCertPath = config.getTlsCertPath();
            String defaultKeyPath = config.getTlsKeyPath();
            FileWatchService defaultWatchService = new FileWatchService(
                new String[] {defaultCertPath, defaultKeyPath},
                new DefaultCertKeyFileWatchListener(),
                watchInterval
            );
            fileWatchServices.add(defaultWatchService);
            log.info("Watching default TLS cert/key: {}, {}", defaultCertPath, defaultKeyPath);
        } catch (Exception e) {
            log.error("Failed to initialize default TLS certificate watch service", e);
            throw new RuntimeException("Failed to initialize TLS certificate manager", e);
        }

        // Watch domain-specific cert/key pairs
        Map<String, TlsDomainConfig> domainConfigs = config.getTlsDomainConfigs();
        if (domainConfigs != null && !domainConfigs.isEmpty()) {
            for (Map.Entry<String, TlsDomainConfig> entry : domainConfigs.entrySet()) {
                String domainPattern = entry.getKey();
                TlsDomainConfig domainConfig = entry.getValue();
                try {
                    FileWatchService domainWatchService = new FileWatchService(
                        new String[] {domainConfig.getCertPath(), domainConfig.getKeyPath()},
                        new DomainCertKeyFileWatchListener(domainPattern),
                        watchInterval
                    );
                    fileWatchServices.add(domainWatchService);
                    log.info("Watching domain TLS cert/key: {}, {} for pattern: {}",
                        domainConfig.getCertPath(), domainConfig.getKeyPath(), domainPattern);
                } catch (Exception e) {
                    log.error("Failed to initialize domain TLS certificate watch service for: {}", domainPattern, e);
                }
            }
        }
    }

    public List<FileWatchService> getFileWatchServices() {
        return this.fileWatchServices;
    }

    public void registerReloadListener(TlsContextReloadListener listener) {
        if (listener != null) {
            this.reloadListeners.add(listener);
        }
    }

    public void unregisterReloadListener(TlsContextReloadListener listener) {
        if (listener != null) {
            this.reloadListeners.remove(listener);
        }
    }

    public void registerDomainReloadListener(DomainReloadListener listener) {
        if (listener != null) {
            this.domainReloadListeners.add(listener);
        }
    }

    public void unregisterDomainReloadListener(DomainReloadListener listener) {
        if (listener != null) {
            this.domainReloadListeners.remove(listener);
        }
    }

    public List<TlsContextReloadListener> getReloadListeners() {
        return this.reloadListeners;
    }

    @Override
    public void start() throws Exception {
        for (FileWatchService service : fileWatchServices) {
            service.start();
        }
        log.info("TLS certificate manager started successfully, watching {} file groups", fileWatchServices.size());
    }

    @Override
    public void shutdown() throws Exception {
        for (FileWatchService service : fileWatchServices) {
            service.shutdown();
        }
        log.info("TLS certificate manager shutdown successfully");
    }

    private class DefaultCertKeyFileWatchListener implements FileWatchService.Listener {
        private boolean certChanged = false;
        private boolean keyChanged = false;

        @Override
        public void onChanged(String path) {
            log.info("Default TLS file changed: {}", path);
            if (path.equals(TlsSystemConfig.tlsServerCertPath) || path.equals(ConfigurationManager.getProxyConfig().getTlsCertPath())) {
                certChanged = true;
            } else if (path.equals(TlsSystemConfig.tlsServerKeyPath) || path.equals(ConfigurationManager.getProxyConfig().getTlsKeyPath())) {
                keyChanged = true;
            }

            if (certChanged && keyChanged) {
                log.info("The default certificate and private key changed, reload the default ssl context");
                tlsSniManager.reloadDefaultContext();
                notifyContextReload();
                certChanged = false;
                keyChanged = false;
            }
        }

        private void notifyContextReload() {
            for (TlsContextReloadListener listener : reloadListeners) {
                try {
                    listener.onTlsContextReload();
                } catch (Throwable e) {
                    log.error("Failed to notify TLS context reload to listener: " + listener, e);
                }
            }
        }
    }

    private class DomainCertKeyFileWatchListener implements FileWatchService.Listener {
        private final String domainPattern;
        private boolean certChanged = false;
        private boolean keyChanged = false;

        DomainCertKeyFileWatchListener(String domainPattern) {
            this.domainPattern = domainPattern;
        }

        @Override
        public void onChanged(String path) {
            log.info("Domain TLS file changed: {} for pattern: {}", path, domainPattern);
            TlsDomainConfig config = ConfigurationManager.getProxyConfig().getTlsDomainConfigs().get(domainPattern);
            if (config == null) {
                return;
            }
            if (path.equals(config.getCertPath())) {
                certChanged = true;
            } else if (path.equals(config.getKeyPath())) {
                keyChanged = true;
            }

            if (certChanged && keyChanged) {
                log.info("The certificate and private key changed for domain: {}, reload the ssl context", domainPattern);
                tlsSniManager.reloadDomainContext(domainPattern);
                notifyDomainReload(domainPattern);
                certChanged = false;
                keyChanged = false;
            }
        }

        private void notifyDomainReload(String domainPattern) {
            for (DomainReloadListener listener : domainReloadListeners) {
                try {
                    listener.onDomainTlsContextReload(domainPattern);
                } catch (Throwable e) {
                    log.error("Failed to notify domain TLS context reload to listener: " + listener, e);
                }
            }
        }
    }

    // Interface for listeners interested in TLS context reload events
    public interface TlsContextReloadListener {
        void onTlsContextReload();
    }

    // Interface for listeners interested in domain-specific TLS context reload events
    public interface DomainReloadListener {
        void onDomainTlsContextReload(String domainPattern);
    }
}
