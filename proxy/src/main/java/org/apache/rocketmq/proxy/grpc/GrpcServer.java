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

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import io.grpc.Server;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

public class GrpcServer implements StartAndShutdown {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final Server server;

    private final long timeout;

    private final TimeUnit unit;

    private final FileWatchService fileWatchService;

    protected GrpcServer(Server server, long timeout, TimeUnit unit) throws Exception {
        this.server = server;
        this.timeout = timeout;
        this.unit = unit;

        this.fileWatchService = initGrpcCertKeyWatchService();
    }

    public void start() throws Exception {
        this.server.start();

        if (fileWatchService != null) {
            fileWatchService.start();
        }

        log.info("grpc server start successfully.");
    }

    public void shutdown() {
        try {
            this.server.shutdown().awaitTermination(timeout, unit);

            if (fileWatchService != null) {
                fileWatchService.shutdown();
            }

            log.info("grpc server shutdown successfully.");
        } catch (Exception e) {
            log.error("Failed to shutdown grpc server", e);
        }
    }

    protected FileWatchService initGrpcCertKeyWatchService() throws Exception {
        return new FileWatchService(
                new String[]{
                    TlsSystemConfig.tlsServerCertPath,
                    TlsSystemConfig.tlsServerKeyPath,
                    TlsSystemConfig.tlsServerTrustCertPath
                },
                new GrpcCertKeyFileWatchListener()
        );
    }

    protected static class GrpcCertKeyFileWatchListener implements FileWatchService.Listener {
        private boolean certChanged = false;
        private boolean keyChanged = false;

        @Override
        public void onChanged(String path) {
            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                log.info("The trust certificate changed, reload the ssl context");
                reloadServerSslContext();
            } else if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                certChanged = true;
            } else if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                keyChanged = true;
            }

            if (certChanged && keyChanged) {
                log.info("The certificate and private key changed, reload the ssl context");
                reloadServerSslContext();

                certChanged = false;
                keyChanged = false;
            }
        }

        private void reloadServerSslContext() {
            try {
                ProxyAndTlsProtocolNegotiator.loadSslContext();
                log.info("SSLContext reloaded for grpc server");
            } catch (CertificateException | IOException e) {
                log.error("Failed to reloaded SSLContext for server", e);
            }
        }
    }
}
