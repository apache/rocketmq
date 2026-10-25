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
import org.apache.rocketmq.proxy.config.TlsDomainConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TlsCertificateManager implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final FileWatchService fileWatchService;
    private final List<FileWatchService> domainWatchServices = new ArrayList<>();
    private final List<TlsContextReloadListener> reloadListeners = new ArrayList<>();
    private final List<DomainTlsContextReloadListener> domainReloadListeners = new ArrayList<>();

    public TlsCertificateManager() {
        try {
            this.fileWatchService = new FileWatchService(
                new String[] {
                    ConfigurationManager.getProxyConfig().getTlsCertPath(),
                    ConfigurationManager.getProxyConfig().getTlsKeyPath()
                },
                new CertKeyFileWatchListener(),
                ConfigurationManager.getProxyConfig().getTlsCertWatchIntervalMs()
            );

            Map<String, TlsDomainConfig> domainConfigs = ConfigurationManager.getProxyConfig().getTlsDomainConfigs();
            if (domainConfigs != null) {
                for (Map.Entry<String, TlsDomainConfig> entry : domainConfigs.entrySet()) {
                    String pattern = entry.getKey();
                    TlsDomainConfig config = entry.getValue();
                    FileWatchService domainWatch = new FileWatchService(
                        new String[] { config.getCertPath(), config.getKeyPath() },
                        new DomainCertKeyFileWatchListener(pattern, config.getCertPath(), config.getKeyPath()),
                        ConfigurationManager.getProxyConfig().getTlsCertWatchIntervalMs()
                    );
                    domainWatchServices.add(domainWatch);
                }
            }
        } catch (Exception e) {
            log.error("Failed to initialize TLS certificate watch service", e);
            throw new RuntimeException("Failed to initialize TLS certificate manager", e);
        }
    }

    public FileWatchService getFileWatchService() {
        return this.fileWatchService;
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

    public void registerDomainReloadListener(DomainTlsContextReloadListener listener) {
        if (listener != null) {
            this.domainReloadListeners.add(listener);
        }
    }

    public void unregisterDomainReloadListener(DomainTlsContextReloadListener listener) {
        if (listener != null) {
            this.domainReloadListeners.remove(listener);
        }
    }

    public List<TlsContextReloadListener> getReloadListeners() {
        return this.reloadListeners;
    }

    @Override
    public void start() throws Exception {
        this.fileWatchService.start();
        log.info("TLS certificate manager started successfully, start watching: {} {}",
            ConfigurationManager.getProxyConfig().getTlsCertPath(),
            ConfigurationManager.getProxyConfig().getTlsKeyPath()
        );
        for (FileWatchService domainWatch : domainWatchServices) {
            domainWatch.start();
        }
        if (!domainWatchServices.isEmpty()) {
            log.info("Started {} domain certificate watch services", domainWatchServices.size());
        }
    }

    @Override
    public void shutdown() throws Exception {
        this.fileWatchService.shutdown();
        for (FileWatchService domainWatch : domainWatchServices) {
            domainWatch.shutdown();
        }
        log.info("TLS certificate manager shutdown successfully");
    }

    private class CertKeyFileWatchListener implements FileWatchService.Listener {
        private boolean certChanged = false;
        private boolean keyChanged = false;

        @Override
        public void onChanged(String path) {
            log.info("File changed: {}", path);
            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                certChanged = true;
            } else if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                keyChanged = true;
            }

            if (certChanged && keyChanged) {
                log.info("The certificate and private key changed, reload the ssl context");
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
        private final String pattern;
        private final String certPath;
        private final String keyPath;
        private boolean certChanged = false;
        private boolean keyChanged = false;

        DomainCertKeyFileWatchListener(String pattern, String certPath, String keyPath) {
            this.pattern = pattern;
            this.certPath = certPath;
            this.keyPath = keyPath;
        }

        @Override
        public void onChanged(String path) {
            log.info("Domain cert file changed: {} for pattern: {}", path, pattern);
            if (path.equals(certPath)) {
                certChanged = true;
            } else if (path.equals(keyPath)) {
                keyChanged = true;
            }

            if (certChanged && keyChanged) {
                log.info("Domain cert and key both changed for pattern: {}, triggering reload", pattern);
                notifyDomainContextReload();
                certChanged = false;
                keyChanged = false;
            }
        }

        private void notifyDomainContextReload() {
            for (DomainTlsContextReloadListener listener : domainReloadListeners) {
                try {
                    listener.onDomainTlsContextReload(pattern);
                } catch (Throwable e) {
                    log.error("Failed to notify domain TLS context reload for pattern: " + pattern, e);
                }
            }
        }
    }

    public interface TlsContextReloadListener {
        void onTlsContextReload();
    }

    public interface DomainTlsContextReloadListener {
        void onDomainTlsContextReload(String domainPattern);
    }
}
