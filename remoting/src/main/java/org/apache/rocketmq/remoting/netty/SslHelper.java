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

package org.apache.rocketmq.remoting.netty;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.util.Properties;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SslHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    public static SslContext buildSslContext(boolean forClient) throws SSLException, CertificateException {

        File configFile = new File(NettySystemConfig.sslConfigFile);
        boolean testMode = !(configFile.exists() && configFile.isFile() && configFile.canRead());
        Properties properties = null;

        if (!testMode) {
            properties = new Properties();
            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(configFile);
                properties.load(inputStream);
            } catch (FileNotFoundException ignore) {
            } catch (IOException ignore) {
            } finally {
                if (null != inputStream) {
                    try {
                        inputStream.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        }

        SslProvider provider = null;
        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
            LOGGER.info("Using OpenSSL provider");
        } else {
            provider = SslProvider.JDK;
            LOGGER.info("Using JDK SSL provider");
        }

        if (forClient) {
            if (testMode) {
                return SslContextBuilder
                    .forClient()
                    .sslProvider(SslProvider.JDK)
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            } else {
                SslContextBuilder sslContextBuilder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);

                if ("false".equals(properties.getProperty("client.auth.server"))) {
                    sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (properties.containsKey("client.trustManager")) {
                        sslContextBuilder.trustManager(new File(properties.getProperty("client.trustManager")));
                    }
                }

                return sslContextBuilder.keyManager(
                    properties.containsKey("client.keyCertChainFile") ? new File(properties.getProperty("client.keyCertChainFile")) : null,
                    properties.containsKey("client.keyFile") ? new File(properties.getProperty("client.keyFile")) : null,
                    properties.containsKey("client.password") ? properties.getProperty("client.password") : null)
                    .build();
            }
        } else {

            if (testMode) {
                SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
                return SslContextBuilder
                    .forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                    .sslProvider(SslProvider.JDK)
                    .clientAuth(ClientAuth.OPTIONAL)
                    .build();
            } else {
                return SslContextBuilder.forServer(
                    properties.containsKey("server.keyCertChainFile") ? new File(properties.getProperty("server.keyCertChainFile")) : null,
                    properties.containsKey("server.keyFile") ? new File(properties.getProperty("server.keyFile")) : null,
                    properties.containsKey("server.password") ? properties.getProperty("server.password") : null)
                    .sslProvider(provider)
                    .trustManager(new File(properties.getProperty("server.trustManager")))
                    .clientAuth(parseClientAuthMode(properties.getProperty("server.auth.client")))
                    .build();
            }
        }
    }

    private static ClientAuth parseClientAuthMode(String authMode) {
        if (null == authMode || authMode.trim().isEmpty()) {
            return ClientAuth.NONE;
        }

        if ("optional".equalsIgnoreCase(authMode)) {
            return ClientAuth.OPTIONAL;
        }

        return ClientAuth.REQUIRE;
    }
}
