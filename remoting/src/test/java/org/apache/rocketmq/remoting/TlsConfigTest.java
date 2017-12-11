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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_AUTHSERVER;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_CERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPASSWORD;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_KEYPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CLIENT_TRUSTCERTPATH;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.TLS_CONFIG_FILE;
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
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerAuthClient;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerNeedClientAuth;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerTrustCertPath;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TlsConfigTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testTlsConfigThroughFile() throws Exception {
        File file = tempFolder.newFile("tls.config");

        System.setProperty(TLS_CONFIG_FILE, file.getAbsolutePath());

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
        sb.append(TLS_CLIENT_KEYPASSWORD + "=1234\n");
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
    }

    private static void writeStringToFile(String path, String content) {
        try {
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(path, true)));
            out.println(content);
            out.close();
        } catch (IOException ignore) {
        }
    }
}
