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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.base;

import io.grpc.Channel;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.testing.GrpcCleanupRule;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.security.cert.CertificateException;
import org.apache.rocketmq.grpc.interceptor.HeaderServerInterceptor;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GrpcBaseTest extends BaseConf {
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    protected Channel setUpServer(apache.rocketmq.v1.MessagingServiceGrpc.MessagingServiceImplBase serverImpl,
        int grpcPort, int remotingPort, boolean enableInterceptor) throws IOException, CertificateException {
        SelfSignedCertificate selfSignedCertificate = new SelfSignedCertificate();
        ServerServiceDefinition serviceDefinition = ServerInterceptors.intercept(serverImpl);
        if (enableInterceptor) {
            serviceDefinition = ServerInterceptors.intercept(serverImpl, new HeaderServerInterceptor());
        }
        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(NettyServerBuilder.forPort(grpcPort)
            .directExecutor()
            .addService(serviceDefinition)
            .useTransportSecurity(selfSignedCertificate.certificate(), selfSignedCertificate.privateKey())
            .build()
            .start());
        // Create a client channel and register for automatic graceful shutdown.
        return grpcCleanup.register(NettyChannelBuilder.forAddress("127.0.0.1", remotingPort)
            .directExecutor()
            .sslContext(SslContextBuilder
                .forClient()
                .sslProvider(SslProvider.OPENSSL)
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocolConfig(new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                    ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                    ApplicationProtocolNames.HTTP_2))
                .build()
            )
            .build());
    }
}
