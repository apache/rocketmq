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

import io.grpc.Server;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
class GrpcSslContextUpdateTest {

    // Mock gRPC Server instance
    @Mock
    private Server mockServer;

    // GrpcServer instance for testing
    private GrpcServer grpcServer;

    private static final Field SSL_CTX_FIELD;

    static {
        try {
            SSL_CTX_FIELD = ProxyAndTlsProtocolNegotiator.class
                .getDeclaredField("sslContext");
            SSL_CTX_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final String CERT = "/tmp/server.pem";
    private static final String KEY = "/tmp/server.key";

    @BeforeAll
    static void beforeAll() throws Exception {
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();
        ConfigurationManager.getProxyConfig().setTlsTestModeEnable(true);
        ConfigurationManager.getProxyConfig().setTlsCertPath(CERT);
        ConfigurationManager.getProxyConfig().setTlsKeyPath(KEY);
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        TlsSystemConfig.tlsTestModeEnable = config.isTlsTestModeEnable();
        System.setProperty(TlsSystemConfig.TLS_TEST_MODE_ENABLE, Boolean.toString(config.isTlsTestModeEnable()));
        TlsSystemConfig.tlsServerCertPath = config.getTlsCertPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_CERTPATH, config.getTlsCertPath());
        TlsSystemConfig.tlsServerKeyPath = config.getTlsKeyPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_KEYPATH, config.getTlsKeyPath());
    }

    @BeforeEach
    void before() throws Exception {
        // Create GrpcServer instance for testing
        grpcServer = new GrpcServer(mockServer, 1, TimeUnit.SECONDS);
        ProxyAndTlsProtocolNegotiator.loadSslContext();
    }

    @Test
    void tlsContextReload_shouldReplaceStaticSslContext() throws Exception {
        // Get the reference to the old SSL context
        SslContext oldCtx = (SslContext) SSL_CTX_FIELD.get(null);

        // Create a TLS reload listener instance directly
        GrpcServer.GrpcTlsReloadHandler reloadHandler = grpcServer.new GrpcTlsReloadHandler();

        // Trigger the reload event
        reloadHandler.onTlsContextReload();

        // Get the reference to the new SSL context after reload
        SslContext newCtx = (SslContext) SSL_CTX_FIELD.get(null);

        // Verify the SSL context was replaced
        Assertions.assertNotSame(oldCtx, newCtx,
            "sslContext should be replaced after reload");
    }
}
