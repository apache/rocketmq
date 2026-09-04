/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.proxy.grpc;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.handler.codec.haproxy.HAProxyTLV;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAndTlsProtocolNegotiatorTest {

    private ProxyAndTlsProtocolNegotiator negotiator;

    @Before
    public void setUp() throws Exception {
        ConfigurationManager.initConfig();
        ConfigurationManager.getProxyConfig().setTlsTestModeEnable(true);
        negotiator = new ProxyAndTlsProtocolNegotiator();
    }

    @After
    public void tearDown() {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        proxyConfig.setTlsTestModeEnable(true);
        proxyConfig.setTlsKeyPath("");
        proxyConfig.setTlsCertPath("");
        proxyConfig.setTlsKeyPassword("");
    }

    @Test
    public void handleHAProxyTLV() {
        ByteBuf content = Unpooled.buffer();
        content.writeBytes("xxxx".getBytes(StandardCharsets.UTF_8));
        HAProxyTLV haProxyTLV = new HAProxyTLV((byte) 0xE1, content);
        negotiator.handleHAProxyTLV(haProxyTLV, Attributes.newBuilder());
    }

    @Test
    public void testLoadSslContextWithUnencryptedKey() throws Exception {
        configureTls("server.key", "server.pem", "");
        ProxyAndTlsProtocolNegotiator.loadSslContext();
    }

    @Test
    public void testLoadSslContextWithEncryptedKey() throws Exception {
        // "1234" is the password of certs/client.key, inherited from remoting module test resources
        configureTls("client.key", "client.pem", "1234");
        ProxyAndTlsProtocolNegotiator.loadSslContext();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadSslContextWithWrongPassword() throws Exception {
        configureTls("client.key", "client.pem", "wrong_password");
        ProxyAndTlsProtocolNegotiator.loadSslContext();
    }

    private void configureTls(String keyFile, String certFile, String password) throws IOException {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        proxyConfig.setTlsTestModeEnable(false);
        proxyConfig.setTlsKeyPath(getCertsPath(keyFile));
        proxyConfig.setTlsCertPath(getCertsPath(certFile));
        proxyConfig.setTlsKeyPassword(password);
    }

    private static String getCertsPath(String fileName) throws IOException {
        File tempFile = File.createTempFile(fileName, null);
        tempFile.deleteOnExit();
        try (InputStream is = ProxyAndTlsProtocolNegotiatorTest.class
            .getClassLoader().getResourceAsStream("certs/" + fileName)) {
            Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        return tempFile.getAbsolutePath();
    }
}
