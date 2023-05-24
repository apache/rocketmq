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

package org.apache.rocketmq.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_AUTHSERVER;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPASSWORD;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_AUTHCLIENT;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPASSWORD;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_NEED_CLIENT_AUTH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientAuthServer;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsClientTrustCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsConfigFile;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsMode;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerAuthClient;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerNeedClientAuth;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerTrustCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsTestModeEnable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class TlsTest {
    private RemotingServer remotingServer;
    private RemotingClient remotingClient;

    @Rule
    public TestName name = new TestName();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws InterruptedException {
        tlsMode = TlsMode.ENFORCING;
        tlsTestModeEnable = false;
        tlsServerNeedClientAuth = "require";
        tlsServerKeyPath = getCertsPath("server.key");
        tlsServerCertPath = getCertsPath("server.pem");
        tlsServerAuthClient = true;
        tlsServerTrustCertPath = getCertsPath("ca.pem");
        tlsClientKeyPath = getCertsPath("client.key");
        tlsClientCertPath = getCertsPath("client.pem");
        tlsClientAuthServer = true;
        tlsClientTrustCertPath = getCertsPath("ca.pem");
        tlsClientKeyPassword = "1234";
        tlsServerKeyPassword = "";

        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setUseTLS(true);

        if ("serverRejectsUntrustedClientCert".equals(name.getMethodName())) {
            // Create a client. Its credentials come from a CA that the server does not trust. The client
            // trusts both test CAs to ensure the handshake failure is due to the server rejecting the client's cert.
            tlsClientKeyPath = getCertsPath("badClient.key");
            tlsClientCertPath = getCertsPath("badClient.pem");
        } else if ("serverAcceptsUntrustedClientCert".equals(name.getMethodName())) {
            tlsClientKeyPath = getCertsPath("badClient.key");
            tlsClientCertPath = getCertsPath("badClient.pem");
            tlsServerAuthClient = false;
        }
        else if ("noClientAuthFailure".equals(name.getMethodName())) {
            //Clear the client cert config to ensure produce the handshake error
            tlsClientKeyPath = "";
            tlsClientCertPath = "";
        } else if ("clientRejectsUntrustedServerCert".equals(name.getMethodName())) {
            tlsServerKeyPath = getCertsPath("badServer.key");
            tlsServerCertPath = getCertsPath("badServer.pem");
        } else if ("clientAcceptsUntrustedServerCert".equals(name.getMethodName())) {
            tlsServerKeyPath = getCertsPath("badServer.key");
            tlsServerCertPath = getCertsPath("badServer.pem");
            tlsClientAuthServer = false;
        } else if ("serverNotNeedClientAuth".equals(name.getMethodName())) {
            tlsServerNeedClientAuth = "none";
            tlsClientKeyPath = "";
            tlsClientCertPath = "";
        } else if ("serverWantClientAuth".equals(name.getMethodName())) {
            tlsServerNeedClientAuth = "optional";
        } else if ("serverWantClientAuth_ButClientNoCert".equals(name.getMethodName())) {
            tlsServerNeedClientAuth = "optional";
            tlsClientKeyPath = "";
            tlsClientCertPath = "";
        } else if ("serverAcceptsUnAuthClient".equals(name.getMethodName())) {
            tlsMode = TlsMode.PERMISSIVE;
            tlsClientKeyPath = "";
            tlsClientCertPath = "";
            clientConfig.setUseTLS(false);
        } else if ("serverRejectsSSLClient".equals(name.getMethodName())) {
            tlsMode = TlsMode.DISABLED;
        } else if ("reloadSslContextForServer".equals(name.getMethodName())) {
            tlsClientAuthServer = false;
            tlsServerNeedClientAuth = "none";
        }

        remotingServer = RemotingServerTest.createRemotingServer();
        remotingClient = RemotingServerTest.createRemotingClient(clientConfig);

        await().pollDelay(Duration.ofMillis(10))
                .pollInterval(Duration.ofMillis(10))
                .atMost(20, TimeUnit.SECONDS).until(() -> isHostConnectable(getServerAddress()));
    }

    @After
    public void tearDown() {
        remotingClient.shutdown();
        remotingServer.shutdown();
        tlsMode = TlsMode.PERMISSIVE;
    }

    /**
     * Tests that a client and a server configured using two-way SSL auth can successfully
     * communicate with each other.
     */
    @Test
    public void basicClientServerIntegrationTest() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void reloadSslContextForServer() throws Exception {
        requestThenAssertResponse();

        //Use new cert and private key
        tlsClientKeyPath = getCertsPath("badClient.key");
        tlsClientCertPath = getCertsPath("badClient.pem");

        ((NettyRemotingServer) remotingServer).loadSslContext();

        //Request Again
        requestThenAssertResponse();

        //Start another client
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setUseTLS(true);
        RemotingClient remotingClient = RemotingServerTest.createRemotingClient(clientConfig);
        requestThenAssertResponse(remotingClient);
    }

    @Test
    public void serverNotNeedClientAuth() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void serverWantClientAuth_ButClientNoCert() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void serverAcceptsUnAuthClient() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void serverRejectsSSLClient() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 1000 * 5);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException ignore) {
        }
    }

    /**
     * Tests that a server configured to require client authentication refuses to accept connections
     * from a client that has an untrusted certificate.
     */
    @Test
    public void serverRejectsUntrustedClientCert() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 1000 * 5);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException ignore) {
        }
    }

    @Test
    public void serverAcceptsUntrustedClientCert() throws Exception {
        requestThenAssertResponse();
    }

    /**
     * Tests that a server configured to require client authentication actually does require client
     * authentication.
     */
    @Test
    public void noClientAuthFailure() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 1000 * 3);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException ignore) {
        }
    }

    /**
     * Tests that a client configured using GrpcSslContexts refuses to talk to a server that has an
     * an untrusted certificate.
     */
    @Test
    public void clientRejectsUntrustedServerCert() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 1000 * 3);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException ignore) {
        }
    }

    @Test
    public void clientAcceptsUntrustedServerCert() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void testTlsConfigThroughFile() throws Exception {
        File file = tempFolder.newFile("tls.config");
        tlsTestModeEnable = true;

        tlsConfigFile = file.getAbsolutePath();

        StringBuilder sb = new StringBuilder();
        sb.append(TLS_SERVER_NEED_CLIENT_AUTH + "=require\n");
        sb.append(TLS_SERVER_KEYPATH + "=/server.key\n");
        sb.append(TLS_SERVER_CERTPATH + "=/server.pem\n");
        sb.append(TLS_SERVER_KEYPASSWORD + "=2345\n");
        sb.append(TLS_SERVER_AUTHCLIENT + "=true\n");
        sb.append(TLS_SERVER_TRUSTCERTPATH + "=/ca.pem\n");
        sb.append(TLS_CLIENT_KEYPATH + "=/client.key\n");
        sb.append(TLS_CLIENT_KEYPASSWORD + "=1234\n");
        sb.append(TLS_CLIENT_CERTPATH + "=/client.pem\n");
        sb.append(TLS_CLIENT_AUTHSERVER + "=false\n");
        sb.append(TLS_CLIENT_TRUSTCERTPATH + "=/ca.pem\n");

        writeStringToFile(file.getAbsolutePath(), sb.toString());
        TlsHelper.buildSslContext(false);

        assertThat(tlsServerNeedClientAuth).isEqualTo("require");
        assertThat(tlsServerKeyPath).isEqualTo("/server.key");
        assertThat(tlsServerCertPath).isEqualTo("/server.pem");
        assertThat(tlsServerKeyPassword).isEqualTo("2345");
        assertThat(tlsServerAuthClient).isEqualTo(true);
        assertThat(tlsServerTrustCertPath).isEqualTo("/ca.pem");
        assertThat(tlsClientKeyPath).isEqualTo("/client.key");
        assertThat(tlsClientKeyPassword).isEqualTo("1234");
        assertThat(tlsClientCertPath).isEqualTo("/client.pem");
        assertThat(tlsClientAuthServer).isEqualTo(false);
        assertThat(tlsClientTrustCertPath).isEqualTo("/ca.pem");

        tlsConfigFile = "/notFound";
    }

    private static void writeStringToFile(String path, String content) {
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(path, true)));
            out.println(content);
            out.close();
        } catch (IOException ignore) {
        }
    }

    private static String getCertsPath(String fileName) {
        ClassLoader loader = TlsTest.class.getClassLoader();
        InputStream stream = loader.getResourceAsStream("certs/" + fileName);
        if (null == stream) {
            throw new RuntimeException("File: " + fileName + " is not found");
        }

        try {
            String[] segments = fileName.split("\\.");
            File f = File.createTempFile(UUID.randomUUID().toString(), segments[1]);
            f.deleteOnExit();

            try (BufferedInputStream bis = new BufferedInputStream(stream);
                 BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f))) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = bis.read(buffer)) > 0) {
                    bos.write(buffer, 0, len);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return f.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getServerAddress() {
        return "localhost:" + remotingServer.localListenPort();
    }

    private static RemotingCommand createRequest() {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        return RemotingCommand.createRequestCommand(0, requestHeader);
    }

    private void requestThenAssertResponse() throws Exception {
        requestThenAssertResponse(remotingClient);
    }

    private void requestThenAssertResponse(RemotingClient remotingClient) throws Exception {
        RemotingCommand response = remotingClient.invokeSync(getServerAddress(), createRequest(), 1000 * 3);
        assertNotNull(response);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);
        assertThat(response.getExtFields().get("messageTitle")).isEqualTo("Welcome");
    }

    private boolean isHostConnectable(String addr) {
        try (Socket socket = new Socket()) {
            socket.connect(NetworkUtil.string2SocketAddress(addr));
            return true;
        } catch (IOException ignored) {
        }
        return false;
    }
}
