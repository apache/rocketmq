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

import java.io.File;
import java.security.SignatureException;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.assertj.core.util.Throwables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_MODE;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_NEED_CLIENT_AUTH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_SERVER_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_TEST_MODE_ENABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class TlsTest {
    private RemotingServer remotingServer;
    private RemotingClient remotingClient;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp() throws InterruptedException {
        System.setProperty(TLS_SERVER_MODE, "enforcing");
        System.setProperty(TLS_TEST_MODE_ENABLE, "false");

        System.setProperty(TLS_SERVER_NEED_CLIENT_AUTH, "require");
        System.setProperty(TLS_SERVER_KEYPATH, getCertsPath("server.key"));
        System.setProperty(TLS_SERVER_CERTPATH, getCertsPath("server.pem"));
        System.setProperty(TLS_SERVER_AUTHCLIENT, "true");
        System.setProperty(TLS_SERVER_TRUSTCERTPATH, getCertsPath("ca.pem"));

        System.setProperty(TLS_CLIENT_KEYPATH, getCertsPath("client.key"));
        System.setProperty(TLS_CLIENT_CERTPATH, getCertsPath("client.pem"));
        System.setProperty(TLS_CLIENT_AUTHSERVER, "true");
        System.setProperty(TLS_CLIENT_TRUSTCERTPATH, getCertsPath("ca.pem"));
        System.setProperty(TLS_CLIENT_KEYPASSWORD, "1234");

        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setUseTLS(true);

        if ("serverRejectsUntrustedClientCert".equals(name.getMethodName())) {
            // Create a client. Its credentials come from a CA that the server does not trust. The client
            // trusts both test CAs to ensure the handshake failure is due to the server rejecting the client's cert.
            System.setProperty(TLS_CLIENT_KEYPATH, getCertsPath("badClient.key"));
            System.setProperty(TLS_CLIENT_CERTPATH, getCertsPath("badClient.pem"));
        } else if ("serverAcceptsUntrustedClientCert".equals(name.getMethodName())) {
            System.setProperty(TLS_CLIENT_KEYPATH, getCertsPath("badClient.key"));
            System.setProperty(TLS_CLIENT_CERTPATH, getCertsPath("badClient.pem"));
            System.setProperty(TLS_SERVER_AUTHCLIENT, "false");
        }
        else if ("noClientAuthFailure".equals(name.getMethodName())) {
            //Clear the client cert config to ensure produce the handshake error
            System.setProperty(TLS_CLIENT_KEYPATH, "");
            System.setProperty(TLS_CLIENT_CERTPATH, "");
        } else if ("clientRejectsUntrustedServerCert".equals(name.getMethodName())) {
            System.setProperty(TLS_SERVER_KEYPATH, getCertsPath("badServer.key"));
            System.setProperty(TLS_SERVER_CERTPATH, getCertsPath("badServer.pem"));
        } else if ("clientAcceptsUntrustedServerCert".equals(name.getMethodName())) {
            System.setProperty(TLS_SERVER_KEYPATH, getCertsPath("badServer.key"));
            System.setProperty(TLS_SERVER_CERTPATH, getCertsPath("badServer.pem"));
            System.setProperty(TLS_CLIENT_AUTHSERVER, "false");
        } else if ("serverNotNeedClientAuth".equals(name.getMethodName())) {
            System.setProperty(TLS_SERVER_NEED_CLIENT_AUTH, "none");
            System.clearProperty(TLS_CLIENT_KEYPATH);
            System.clearProperty(TLS_CLIENT_CERTPATH);
        } else if ("serverWantClientAuth".equals(name.getMethodName())) {
            System.setProperty(TLS_SERVER_NEED_CLIENT_AUTH, "optional");
        } else if ("serverWantClientAuth_ButClientNoCert".equals(name.getMethodName())) {
            System.setProperty(TLS_SERVER_NEED_CLIENT_AUTH, "optional");
            System.clearProperty(TLS_CLIENT_KEYPATH);
            System.clearProperty(TLS_CLIENT_CERTPATH);
        }

        remotingServer = RemotingServerTest.createRemotingServer();
        remotingClient = RemotingServerTest.createRemotingClient(clientConfig);
    }

    @After
    public void tearDown() {
        remotingClient.shutdown();
        remotingServer.shutdown();
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
    public void serverNotNeedClientAuth() throws Exception {
        requestThenAssertResponse();
    }

    @Test
    public void serverWantClientAuth_ButClientNoCert() throws Exception {
        requestThenAssertResponse();
    }

    /**
     * Tests that a server configured to require client authentication refuses to accept connections
     * from a client that has an untrusted certificate.
     */
    @Test
    public void serverRejectsUntrustedClientCert() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", createRequest(), 1000 * 5);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException exception) {
            assertThat(Throwables.getRootCause(exception)).isInstanceOf(SSLException.class);
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
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", createRequest(), 1000 * 3);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException exception) {
            assertThat(Throwables.getRootCause(exception)).isInstanceOf(SSLException.class);
        }
    }

    /**
     * Tests that a client configured using GrpcSslContexts refuses to talk to a server that has an
     * an untrusted certificate.
     */
    @Test
    public void clientRejectsUntrustedServerCert() throws Exception {
        try {
            RemotingCommand response = remotingClient.invokeSync("localhost:8888", createRequest(), 1000 * 3);
            failBecauseExceptionWasNotThrown(RemotingSendRequestException.class);
        } catch (RemotingSendRequestException exception) {
            assertThat(Throwables.getRootCause(exception)).isInstanceOf(SignatureException.class);
        }
    }

    @Test
    public void clientAcceptsUntrustedServerCert() throws Exception {
        requestThenAssertResponse();
    }

    private static String getCertsPath(String fileName) {
        File resourcesDirectory = new File("src/test/resources/certs");
        return resourcesDirectory.getAbsolutePath() + "/" + fileName;
    }

    private static RemotingCommand createRequest() {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        return RemotingCommand.createRequestCommand(0, requestHeader);
    }

    private void requestThenAssertResponse() throws Exception {
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", createRequest(), 1000 * 3);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);
        assertThat(response.getExtFields().get("messageTitle")).isEqualTo("Welcome");
    }
}
