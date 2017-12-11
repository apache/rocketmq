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
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_AUTHSERVER;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_CERTPATH;
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
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class TlsTest {
    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    @BeforeClass
    public static void setUp() throws InterruptedException {
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
        remotingServer = RemotingServerTest.createRemotingServer();
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setUseTLS(true);
        remotingClient = RemotingServerTest.createRemotingClient(clientConfig);
    }

    @AfterClass
    public static void tearDown() {
        remotingClient.shutdown();
        remotingServer.shutdown();
    }

    /**
     * public static final String TLS_SERVER_MODE = "tls.server.mode";
     public static final String TLS_ENABLE = "tls.enable";
     public static final String TLS_CONFIG_FILE = "tls.config.file";
     public static final String TLS_TEST_MODE_ENABLE = "tls.test.mode.enable";

     public static final String TLS_SERVER_NEED_CLIENT_AUTH = "tls.server.need.client.auth";
     public static final String TLS_SERVER_KEYPATH = "tls.server.keyPath";
     public static final String TLS_SERVER_KEYPASSWORD = "tls.server.keyPassword";
     public static final String TLS_SERVER_CERTPATH = "tls.server.certPath";
     public static final String TLS_SERVER_AUTHCLIENT = "tls.server.authClient";
     public static final String TLS_SERVER_TRUSTCERTPATH = "tls.server.trustCertPath";

     public static final String TLS_CLIENT_KEYPATH = "tls.client.keyPath";
     public static final String TLS_CLIENT_KEYPASSWORD = "tls.client.keyPassword";
     public static final String TLS_CLIENT_CERTPATH = "tls.client.certPath";
     public static final String TLS_CLIENT_AUTHSERVER = "tls.client.authServer";
     public static final String TLS_CLIENT_TRUSTCERTPATH = "tls.client.trustCertPath";
     */
    @Test
    public void basicClientServerIntegrationTest() throws InterruptedException, RemotingTimeoutException, RemotingConnectException, RemotingSendRequestException {
        RequestHeader requestHeader = new RequestHeader();
        requestHeader.setCount(1);
        requestHeader.setMessageTitle("Welcome");
        RemotingCommand request = RemotingCommand.createRequestCommand(0, requestHeader);
        RemotingCommand response = remotingClient.invokeSync("localhost:8888", request, 1000 * 3);
        System.out.println(response);
        assertTrue(response != null);
        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
        assertThat(response.getExtFields()).hasSize(2);

        remotingClient.shutdown();
        remotingServer.shutdown();
    }

    static String getCertsPath(String fileName) {
        //File resourcesDirectory = new File("src/test/resources/certs");
        File resourcesDirectory = new File("/Users/zhouxinyu/apache-space/incubator-rocketmq/remoting/src/test/resources/certs/");
        return resourcesDirectory.getAbsolutePath() + "/" + fileName;
    }
}
