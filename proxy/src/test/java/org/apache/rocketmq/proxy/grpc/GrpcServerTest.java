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
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Extend with MockitoExtension for JUnit 5
@ExtendWith(MockitoExtension.class)
public class GrpcServerTest {
    // Mock gRPC Server instance
    @Mock
    private Server mockServer;

    // MockedStatic for static method calls
    private MockedStatic<ProxyAndTlsProtocolNegotiator> mockedNegotiator;
    private static MockedStatic<LoggerFactory> mockedLoggerFactory;
    private static Logger mockLogger;

    // Mock for FileWatchService to test GrpcServer's start/shutdown impact
    @Mock
    private FileWatchService mockFileWatchService;

    // A testable GrpcServer subclass to return our mock FileWatchService
    private class TestableGrpcServer extends GrpcServer {
        public TestableGrpcServer(Server server, long timeout, TimeUnit unit) throws Exception {
            super(server, timeout, unit);
        }

        @Override
        protected FileWatchService initGrpcCertKeyWatchService() {
            // Return our mocked FileWatchService
            return mockFileWatchService;
        }
    }

    @BeforeAll
    public static void setUpAll() {
        mockLogger = mock(Logger.class);
        mockedLoggerFactory = Mockito.mockStatic(LoggerFactory.class);
        mockedLoggerFactory.when(() -> LoggerFactory.getLogger(any(String.class))).thenReturn(mockLogger);
    }

    @AfterAll
    public static void tearDownAll() {
        mockedLoggerFactory.close();
    }

    @BeforeEach
    public void setUp() {
        Mockito.clearInvocations(mockLogger);

        // Mock static method loadSslContext
        mockedNegotiator = Mockito.mockStatic(ProxyAndTlsProtocolNegotiator.class);

        // Set static paths for TlsSystemConfig, as FileWatchService constructor uses them.
        // For testing, we don't rely on real files, so any non-null path is fine.
        TlsSystemConfig.tlsServerCertPath = "/tmp/server.pem";
        TlsSystemConfig.tlsServerKeyPath = "/tmp/server.key";
        TlsSystemConfig.tlsServerTrustCertPath = "/tmp/trust.pem";
    }

    @AfterEach
    public void tearDown() {
        mockedNegotiator.close();

        // Clear TlsSystemConfig paths
        TlsSystemConfig.tlsServerCertPath = null;
        TlsSystemConfig.tlsServerKeyPath = null;
        TlsSystemConfig.tlsServerTrustCertPath = null;
    }

    @Test
    void testGrpcCertKeyFileWatchListener_trustCertChange() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate trust certificate file change
        listener.onChanged(TlsSystemConfig.tlsServerTrustCertPath);

        // Verify loadSslContext method was called once
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));
        // Verify log output
        verify(mockLogger).info("The trust certificate changed, reload the ssl context");
        verify(mockLogger).info("SSLContext reloaded for grpc server");
    }

    @Test
    void testGrpcCertKeyFileWatchListener_certOnlyChange() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate certificate file change
        listener.onChanged(TlsSystemConfig.tlsServerCertPath);

        // Verify loadSslContext method was not called
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, never());
        verify(mockLogger, never()).info("The certificate and private key changed, reload the ssl context");
    }

    @Test
    void testGrpcCertKeyFileWatchListener_keyOnlyChange() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate private key file change
        listener.onChanged(TlsSystemConfig.tlsServerKeyPath);

        // Verify loadSslContext method was not called
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, never());
        verify(mockLogger, never()).info("The certificate and private key changed, reload the ssl context");
    }

    @Test
    void testGrpcCertKeyFileWatchListener_certThenKeyChange() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate certificate file change
        listener.onChanged(TlsSystemConfig.tlsServerCertPath);
        // Simulate private key file change
        listener.onChanged(TlsSystemConfig.tlsServerKeyPath);

        // Verify loadSslContext method was called once
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));
        verify(mockLogger).info("The certificate and private key changed, reload the ssl context");
        verify(mockLogger).info("SSLContext reloaded for grpc server");
    }

    @Test
    void testGrpcCertKeyFileWatchListener_keyThenCertChange() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate private key file change
        listener.onChanged(TlsSystemConfig.tlsServerKeyPath);
        // Simulate certificate file change
        listener.onChanged(TlsSystemConfig.tlsServerCertPath);

        // Verify loadSslContext method was called once
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));
        verify(mockLogger).info("The certificate and private key changed, reload the ssl context");
        verify(mockLogger).info("SSLContext reloaded for grpc server");
    }

    @Test
    void testGrpcCertKeyFileWatchListener_reloadError() {
        // Create listener instance
        GrpcServer.GrpcCertKeyFileWatchListener listener = new GrpcServer.GrpcCertKeyFileWatchListener();

        // Simulate loadSslContext throwing an exception
        mockedNegotiator.when(ProxyAndTlsProtocolNegotiator::loadSslContext)
                .thenThrow(new IOException("Test IO Exception"));

        // Simulate both certificate and private key changes
        listener.onChanged(TlsSystemConfig.tlsServerCertPath);
        listener.onChanged(TlsSystemConfig.tlsServerKeyPath);

        // Verify loadSslContext method was called once
        mockedNegotiator.verify(ProxyAndTlsProtocolNegotiator::loadSslContext, times(1));
        // Verify error log output
        verify(mockLogger).error(eq("Failed to reloaded SSLContext for server"), any(IOException.class));
    }


    @Test
    void testGrpcServerStart() throws Exception {
        // Use our special GrpcServer which returns the mockFileWatchService
        GrpcServer grpcServer = new TestableGrpcServer(mockServer, 1, TimeUnit.SECONDS);

        // Start the server
        grpcServer.start();

        // Verify that gRPC Server's start method was called
        verify(mockServer, times(1)).start();
        // Verify that FileWatchService's start method was called
        verify(mockFileWatchService, times(1)).start();
        // Verify log output
        verify(mockLogger).info("grpc server start successfully.");
    }

    @Test
    void testGrpcServerShutdown() throws Exception {
        when(mockServer.shutdown()).thenReturn(mockServer);
        when(mockServer.awaitTermination(any(Long.class), any(TimeUnit.class))).thenReturn(true);

        GrpcServer grpcServer = new TestableGrpcServer(mockServer, 1, TimeUnit.SECONDS);

        grpcServer.shutdown();

        // Verify that gRPC Server's shutdown and awaitTermination methods were called
        verify(mockServer, times(1)).shutdown();
        verify(mockServer, times(1)).awaitTermination(1, TimeUnit.SECONDS);
        // Verify that FileWatchService's shutdown method was called
        verify(mockFileWatchService, times(1)).shutdown();
        // Verify log output
        verify(mockLogger).info("grpc server shutdown successfully.");
    }
}
