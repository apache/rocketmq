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
package org.apache.rocketmq.proxy.remoting;

import io.netty.handler.ssl.SslContext;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;

@ExtendWith(MockitoExtension.class)
public class RemotingServerTlsContextUpdateTest {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final String CERT = "/tmp/server.pem";
    private static final String KEY  = "/tmp/server.key";

    @Mock
    private MessagingProcessor messagingProcessor;

    @BeforeAll
    static void beforeAll() throws Exception {
        // Initialize configuration files/environment
        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();

        // Populate proxy configuration with TLS-related information
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        config.setTlsTestModeEnable(true);
        config.setTlsCertPath(CERT);
        config.setTlsKeyPath(KEY);

        // Fill in static TLS system properties so that Netty can pick them up
        TlsSystemConfig.tlsTestModeEnable = config.isTlsTestModeEnable();
        System.setProperty(TlsSystemConfig.TLS_TEST_MODE_ENABLE, Boolean.toString(config.isTlsTestModeEnable()));

        TlsSystemConfig.tlsServerCertPath = config.getTlsCertPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_CERTPATH, config.getTlsCertPath());

        TlsSystemConfig.tlsServerKeyPath  = config.getTlsKeyPath();
        System.setProperty(TlsSystemConfig.TLS_SERVER_KEYPATH,  config.getTlsKeyPath());
    }

    @Test
    public void testRemotingServerTlsContextReload() throws Exception {
        // Create a RemotingProtocolServer instance
        RemotingProtocolServer remotingProtocolServer = new RemotingProtocolServer(messagingProcessor);

        // Default server must be a NettyRemotingServer
        Assertions.assertTrue(remotingProtocolServer.defaultRemotingServer instanceof NettyRemotingServer,
            "Default remoting server should be instance of NettyRemotingServer");
        NettyRemotingServer nettyRemotingServer = (NettyRemotingServer) remotingProtocolServer.defaultRemotingServer;

        // Obtain the sslContext field from NettyRemotingAbstract
        Field sslContextField = NettyRemotingAbstract.class.getDeclaredField("sslContext");
        sslContextField.setAccessible(true);

        // Retrieve the initial SSL context (create it if necessary)
        SslContext originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        if (originalSslContext == null) {
            nettyRemotingServer.loadSslContext();
            originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        }
        log.info("Original SSL context: {}", originalSslContext);

        // Trigger TLS context reload through the handler
        RemotingProtocolServer.RemotingTlsReloadHandler reloadHandler = remotingProtocolServer.tlsReloadHandler;
        reloadHandler.onTlsContextReload();

        // Retrieve the new SSL context
        SslContext reloadedSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        log.info("Reloaded SSL context: {}", reloadedSslContext);

        // Verify the context has been replaced
        Assertions.assertNotSame(originalSslContext, reloadedSslContext,
            "SSL context should be replaced after reload");
        // Ensure both contexts are non-null
        if (originalSslContext != null) {
            Assertions.assertNotNull(reloadedSslContext, "Reloaded SSL context should not be null");
        }

        // Shutdown the server to free resources
        remotingProtocolServer.shutdown();
    }

    @Test
    public void testTlsCertificateManagerTriggersCorrectUpdate() throws Exception {
        // Create a RemotingProtocolServer instance
        RemotingProtocolServer remotingProtocolServer = new RemotingProtocolServer(messagingProcessor);
        NettyRemotingServer nettyRemotingServer = (NettyRemotingServer) remotingProtocolServer.defaultRemotingServer;

        // Access the sslContext field
        Field sslContextField = NettyRemotingAbstract.class.getDeclaredField("sslContext");
        sslContextField.setAccessible(true);

        // Retrieve the initial SSL context (create it if necessary)
        SslContext originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        if (originalSslContext == null) {
            nettyRemotingServer.loadSslContext();
            originalSslContext = (SslContext) sslContextField.get(nettyRemotingServer);
        }

        // Trigger reload via TlsCertificateManager
        TlsCertificateManager tlsCertificateManager = TlsCertificateManager.getInstance();

        // Read the private 'reloadListeners' field, then invoke every listener directly
        Field listenersField = TlsCertificateManager.class.getDeclaredField("reloadListeners");
        listenersField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.List<TlsCertificateManager.TlsContextReloadListener> listeners =
            (java.util.List<TlsCertificateManager.TlsContextReloadListener>) listenersField.get(tlsCertificateManager);

        // Notify each listener
        for (TlsCertificateManager.TlsContextReloadListener listener : new java.util.ArrayList<>(listeners)) {
            listener.onTlsContextReload();
        }

        // Retrieve the reloaded SSL context
        SslContext reloadedSslContext = (SslContext) sslContextField.get(nettyRemotingServer);

        // Verify the context has been replaced
        Assertions.assertNotSame(originalSslContext, reloadedSslContext,
            "SSL context should be replaced after TlsCertificateManager triggered reload");

        // Shutdown the server to free resources
        remotingProtocolServer.shutdown();
    }
}
