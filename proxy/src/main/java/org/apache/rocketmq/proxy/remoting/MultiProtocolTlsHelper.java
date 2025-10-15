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

package org.apache.rocketmq.proxy.remoting;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.TlsHelper;

import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerAuthClient;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPassword;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerKeyPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerNeedClientAuth;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsServerTrustCertPath;
import static org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsTestModeEnable;

public class MultiProtocolTlsHelper extends TlsHelper {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final DecryptionStrategy DECRYPTION_STRATEGY = (privateKeyEncryptPath, forClient) -> new FileInputStream(privateKeyEncryptPath);

    public static SslContext buildSslContext() throws IOException, CertificateException {
        TlsHelper.buildSslContext(false);
        SslProvider provider;
        if (OpenSsl.isAvailable()) {
            provider = SslProvider.OPENSSL;
            log.info("Using OpenSSL provider");
        } else {
            provider = SslProvider.JDK;
            log.info("Using JDK SSL provider");
        }

        SslContextBuilder sslContextBuilder;
        if (tlsTestModeEnable) {
            SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
            sslContextBuilder = SslContextBuilder
                .forServer(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
                .sslProvider(provider)
                .clientAuth(ClientAuth.OPTIONAL);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(
                !StringUtils.isBlank(tlsServerCertPath) ? Files.newInputStream(Paths.get(tlsServerCertPath)) : null,
                !StringUtils.isBlank(tlsServerKeyPath) ? DECRYPTION_STRATEGY.decryptPrivateKey(tlsServerKeyPath, false) : null,
                !StringUtils.isBlank(tlsServerKeyPassword) ? tlsServerKeyPassword : null)
                .sslProvider(provider);

            if (!tlsServerAuthClient) {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                if (!StringUtils.isBlank(tlsServerTrustCertPath)) {
                    sslContextBuilder.trustManager(new File(tlsServerTrustCertPath));
                }
            }

            sslContextBuilder.clientAuth(parseClientAuthMode(tlsServerNeedClientAuth));
        }

        sslContextBuilder.applicationProtocolConfig(new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            ApplicationProtocolNames.HTTP_2));

        return sslContextBuilder.build();
    }

    private static ClientAuth parseClientAuthMode(String authMode) {
        if (null == authMode || authMode.trim().isEmpty()) {
            return ClientAuth.NONE;
        }

        String authModeUpper = authMode.toUpperCase();
        for (ClientAuth clientAuth : ClientAuth.values()) {
            if (clientAuth.name().equals(authModeUpper)) {
                return clientAuth;
            }
        }

        return ClientAuth.NONE;
    }
}
