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

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;

@ExtendWith(MockitoExtension.class)
class GrpcSslContextUpdateTest {

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

    private static final String CERT  = "/tmp/server.pem";
    private static final String KEY   = "/tmp/server.key";

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
        ProxyAndTlsProtocolNegotiator.loadSslContext();
    }

    @Test
    void onChanged_shouldReplaceStaticSslContext() throws Exception {
        SslContext oldCtx = (SslContext) SSL_CTX_FIELD.get(null);

        GrpcServer.GrpcCertKeyFileWatchListener listener =
                new GrpcServer.GrpcCertKeyFileWatchListener();

        listener.onChanged(CERT);
        listener.onChanged(KEY);

        SslContext newCtx = (SslContext) SSL_CTX_FIELD.get(null);

        Assertions.assertNotSame(oldCtx, newCtx,
                "sslContext should be replaced after reload");
    }
}