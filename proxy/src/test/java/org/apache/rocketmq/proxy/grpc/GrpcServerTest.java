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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GrpcServerTest {
    // Mock gRPC Server instance
    @Mock
    private Server mockServer;

    // MockedStatic for static method calls
    private MockedStatic<ProxyAndTlsProtocolNegotiator> mockedNegotiator;
    private static MockedStatic<LoggerFactory> mockedLoggerFactory;
    private static Logger mockLogger;

    // GrpcServer instance for testing
    private GrpcServer grpcServer;

    @BeforeAll
    public static void setUpAll() {
        mockLogger = mock(Logger.class);
        mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class);
        mockedLoggerFactory.when(() -> LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME)).thenReturn(mockLogger);
    }

    @AfterAll
    public static void tearDownAll() {
        if (mockedLoggerFactory != null) {
            mockedLoggerFactory.close();
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        Mockito.clearInvocations(mockLogger);

        // Mock static method loadSslContext
        mockedNegotiator = Mockito.mockStatic(ProxyAndTlsProtocolNegotiator.class);

        // Set static paths for TlsSystemConfig
        TlsSystemConfig.tlsServerCertPath = "/tmp/server.pem";
        TlsSystemConfig.tlsServerKeyPath = "/tmp/server.key";
        TlsSystemConfig.tlsServerTrustCertPath = "/tmp/trust.pem";

        // Create GrpcServer instance for testing
        grpcServer = new GrpcServer(mockServer, 1, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockedNegotiator.close();

        // Unregister TLS reload handler and shutdown server
        grpcServer.shutdown();

        // Clear TlsSystemConfig paths
        TlsSystemConfig.tlsServerCertPath = null;
        TlsSystemConfig.tlsServerKeyPath = null;
        TlsSystemConfig.tlsServerTrustCertPath = null;
    }

    @Test
    public void testTlsReloadHandlerSuccess() throws Exception {
        // Create a GrpcTlsReloadHandler instance from the GrpcServer

        TlsCertificateManager.TlsContextReloadListener reloadHandler = grpcServer.new GrpcTlsReloadHandler();

        // Mock successful SSL context loading
        mockedNegotiator.when(ProxyAndTlsProtocolNegotiator::loadSslContext)
            .thenAnswer(invocation -> null); // Method returns void

        // Trigger reload
        reloadHandler.onTlsContextReload();

        // Verify loadSslContext was called
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));

        // Verify success log was printed
        verify(mockLogger).info("SSLContext reloaded for grpc server");

        // Verify no error log was printed
        verify(mockLogger, never()).error(anyString(), any(Throwable.class));
    }

    @Test
    public void testTlsReloadHandlerCertificateException() throws Exception {
        // Create a GrpcTlsReloadHandler instance from the GrpcServer
        TlsCertificateManager.TlsContextReloadListener reloadHandler = grpcServer.new GrpcTlsReloadHandler();

        // Mock CertificateException when loading SSL context
        CertificateException expectedException = new CertificateException("Test certificate exception");
        mockedNegotiator.when(ProxyAndTlsProtocolNegotiator::loadSslContext)
            .thenThrow(expectedException);

        // Trigger reload
        reloadHandler.onTlsContextReload();

        // Verify loadSslContext was called
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));

        // Verify no success log was printed
        verify(mockLogger, never()).info("SSLContext reloaded for grpc server");

        // Verify error log was printed with the correct exception
        verify(mockLogger).error(eq("Failed to reload SSLContext for server"), eq(expectedException));
    }

    @Test
    public void testTlsReloadHandlerIOException() throws Exception {
        // Create a GrpcTlsReloadHandler instance from the GrpcServer
        TlsCertificateManager.TlsContextReloadListener reloadHandler = grpcServer.new GrpcTlsReloadHandler();

        // Mock IOException when loading SSL context
        IOException expectedException = new IOException("Test IO exception");
        mockedNegotiator.when(ProxyAndTlsProtocolNegotiator::loadSslContext)
            .thenThrow(expectedException);

        // Trigger reload
        reloadHandler.onTlsContextReload();

        // Verify loadSslContext was called
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));

        // Verify no success log was printed
        verify(mockLogger, never()).info("SSLContext reloaded for grpc server");

        // Verify error log was printed with the correct exception
        verify(mockLogger).error(eq("Failed to reload SSLContext for server"), eq(expectedException));
    }

    @Test
    public void testShutdownUnregistersReloadHandler() throws Exception {
        // Mock TlsCertificateManager instance
        TlsCertificateManager tlsCertificateManager = mock(TlsCertificateManager.class);
        MockedStatic<TlsCertificateManager> mockedCertManager = Mockito.mockStatic(TlsCertificateManager.class);
        mockedCertManager.when(TlsCertificateManager::getInstance).thenReturn(tlsCertificateManager);

        try {
            when(mockServer.shutdown()).thenReturn(mockServer);

            // Create a new GrpcServer
            GrpcServer localGrpcServer = new GrpcServer(mockServer, 1, TimeUnit.SECONDS);

            // Keep a reference to the reload handler
            GrpcServer.GrpcTlsReloadHandler reloadHandler = localGrpcServer.tlsReloadHandler;

            // Verify handler was registered
            verify(tlsCertificateManager).registerReloadListener(eq(reloadHandler));

            // Shutdown the server
            localGrpcServer.shutdown();

            // Verify handler was unregistered
            verify(tlsCertificateManager).unregisterReloadListener(eq(reloadHandler));

            // Verify log output
            verify(mockLogger).info("grpc server shutdown successfully.");
        } finally {
            mockedCertManager.close();
        }
    }
}
