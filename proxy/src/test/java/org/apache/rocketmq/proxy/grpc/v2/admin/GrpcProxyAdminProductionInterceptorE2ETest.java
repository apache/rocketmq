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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.admin;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.MessagingServiceGrpc;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Status;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.strategy.AuthenticationStrategy;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.ProxyStartup;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.GrpcServer;
import org.apache.rocketmq.proxy.grpc.GrpcServerBuilder;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationSubjectPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.cert.TlsCertificateManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class GrpcProxyAdminProductionInterceptorE2ETest extends InitConfigTest {
    private static final Metadata.Key<String> ADMIN_TOKEN =
        Metadata.Key.of("x-rip2-admin-token", Metadata.ASCII_STRING_MARSHALLER);

    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private TlsCertificateManager tlsCertificateManager;

    @Before
    public void setUp() throws Throwable {
        super.before();
    }

    @Test
    public void productionInterceptorsAuthenticateAdminAndKeepServicesOnSeparatePorts() throws Exception {
        ProxyClientReadService readService = new ProxyClientReadService();
        readService.upsertClient(new ProxyClientInfo(
            "client-production-e2e",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-production-e2e"),
            "JAVA",
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "V5_0_0",
            100L,
            200L
        ));
        AtomicReference<String> authorizedSourceIp = new AtomicReference<>();
        AtomicInteger authorizationCalls = new AtomicInteger();
        AtomicBoolean denyAuthorization = new AtomicBoolean();
        AuthorizingClientAdminService authorizingService = new AuthorizingClientAdminService(
            new DefaultClientAdminService(readService),
            (subject, operation, sourceIp) -> {
                authorizationCalls.incrementAndGet();
                if (!(subject instanceof User)
                    || !"authenticated-admin".equals(((User) subject).getUsername())) {
                    throw new AuthorizationException("authenticated admin is required");
                }
                authorizedSourceIp.set(sourceIp);
                if (denyAuthorization.get()) {
                    throw new AuthorizationException("admin ACL denied");
                }
            }
        );
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingService);
        ProxyClientAdminEndpointHandler endpointHandler = new ProxyClientAdminEndpointHandler(activity);
        ProxyClientAdminEndpointExecutor endpointExecutor = new ProxyClientAdminEndpointExecutor(
            new ProxyClientAdminContextFactory(authenticatedAdminPipeline()),
            endpointHandler
        );
        MessagingServiceGrpc.MessagingServiceImplBase messagingService = messagingService();
        ThreadPoolExecutor messagingExecutor = ProxyStartup.createServerExecutor();
        ThreadPoolExecutor adminExecutor = ProxyStartup.createProxyAdminServerExecutor();
        GrpcServer messagingServer = null;
        GrpcServer adminServer = null;
        ManagedChannel messagingChannel = null;
        ManagedChannel adminChannel = null;
        try {
            messagingServer = GrpcServerBuilder.newBuilder(messagingExecutor, 0, this.tlsCertificateManager)
                .addService(messagingService)
                .configInterceptor()
                .shutdownTime(5, TimeUnit.SECONDS)
                .build();
            adminServer = GrpcServerBuilder.newBuilder(adminExecutor, 0, this.tlsCertificateManager)
                .addService(new GrpcProxyAdminApplication(endpointExecutor))
                .configInterceptor()
                .shutdownTime(5, TimeUnit.SECONDS)
                .build();
            messagingServer.start();
            adminServer.start();

            assertThat(messagingServer.getPort()).isNotEqualTo(adminServer.getPort());
            messagingChannel = channel(messagingServer.getPort());
            adminChannel = channel(adminServer.getPort());

            QueryRouteResponse messagingResponse = MessagingServiceGrpc.newBlockingStub(messagingChannel)
                .queryRoute(QueryRouteRequest.getDefaultInstance());
            assertThat(messagingResponse.getStatus().getCode()).isEqualTo(Code.OK);

            ListClientsResponse authorizedResponse = authorizedAdminStub(adminChannel)
                .listClients(ListClientsRequest.newBuilder().setPageSize(100).build());
            assertThat(authorizedResponse.getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(authorizedResponse.getClientsList())
                .extracting(apache.rocketmq.v2.ProxyClient::getClientId)
                .containsExactly("client-production-e2e");
            assertThat(authorizedSourceIp.get()).isEqualTo("127.0.0.1");
            assertThat(authorizationCalls.get()).isEqualTo(1);

            ListClientsResponse deniedResponse = forgedAdminStub(adminChannel)
                .listClients(ListClientsRequest.newBuilder().setPageSize(100).build());
            assertThat(deniedResponse.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
            assertThat(deniedResponse.getClientsCount()).isZero();
            assertThat(authorizationCalls.get()).isEqualTo(1);

            denyAuthorization.set(true);
            ListClientsResponse aclDeniedResponse = authorizedAdminStub(adminChannel)
                .listClients(ListClientsRequest.newBuilder().setPageSize(100).build());
            assertThat(aclDeniedResponse.getStatus().getCode()).isEqualTo(Code.UNAUTHORIZED);
            assertThat(aclDeniedResponse.getClientsCount()).isZero();
            assertThat(authorizationCalls.get()).isEqualTo(2);

            assertProxyAdminUnimplemented(messagingChannel);
            assertMessagingUnimplemented(adminChannel);
        } finally {
            if (adminChannel != null) {
                adminChannel.shutdownNow();
                adminChannel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (messagingChannel != null) {
                messagingChannel.shutdownNow();
                messagingChannel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (adminServer != null) {
                adminServer.shutdown();
            }
            if (messagingServer != null) {
                messagingServer.shutdown();
            }
            endpointExecutor.shutdown();
            adminExecutor.shutdownNow();
            messagingExecutor.shutdownNow();
        }
    }

    private RequestPipeline authenticatedAdminPipeline() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("proxy-admin-production-e2e-" + System.nanoTime());
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationStrategy(TokenAuthenticationStrategy.class.getName());
        RequestPipeline pipeline = (context, headers, request) -> {
        };
        return pipeline
            .pipe(new AuthenticationSubjectPipeline())
            .pipe(new TokenAuthenticationPipeline(authConfig, this.messagingProcessor))
            .pipe(new ContextInitPipeline());
    }

    private static MessagingServiceGrpc.MessagingServiceImplBase messagingService() {
        return new MessagingServiceGrpc.MessagingServiceImplBase() {
            @Override
            public void queryRoute(QueryRouteRequest request, StreamObserver<QueryRouteResponse> responseObserver) {
                responseObserver.onNext(QueryRouteResponse.newBuilder()
                    .setStatus(Status.newBuilder().setCode(Code.OK).build())
                    .build());
                responseObserver.onCompleted();
            }
        };
    }

    private static ManagedChannel channel(int port) {
        return ManagedChannelBuilder.forAddress("127.0.0.1", port)
            .usePlaintext()
            .build();
    }

    private static ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub authorizedAdminStub(
        ManagedChannel channel) {
        Metadata headers = new Metadata();
        headers.put(ADMIN_TOKEN, "valid");
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");
        headers.put(GrpcConstants.REMOTE_ADDRESS, "203.0.113.10:9999");
        return ProxyAdminServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub forgedAdminStub(ManagedChannel channel) {
        Metadata headers = new Metadata();
        headers.put(ADMIN_TOKEN, "invalid");
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");
        return ProxyAdminServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static void assertUnimplemented(Runnable call) {
        try {
            call.run();
            throw new AssertionError("expected UNIMPLEMENTED");
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNIMPLEMENTED);
        }
    }

    private static void assertProxyAdminUnimplemented(ManagedChannel channel) {
        assertUnimplemented(() -> ProxyAdminServiceGrpc.newBlockingStub(channel)
            .listClients(ListClientsRequest.getDefaultInstance()));
    }

    private static void assertMessagingUnimplemented(ManagedChannel channel) {
        assertUnimplemented(() -> MessagingServiceGrpc.newBlockingStub(channel)
            .queryRoute(QueryRouteRequest.getDefaultInstance()));
    }

    private static class TokenAuthenticationPipeline extends AuthenticationPipeline {
        private TokenAuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
            super(authConfig, messagingProcessor, true);
        }

        @Override
        protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
            GeneratedMessageV3 request) {
            DefaultAuthenticationContext authenticationContext = new DefaultAuthenticationContext();
            authenticationContext.setUsername("valid".equals(headers.get(ADMIN_TOKEN))
                ? "authenticated-admin" : "unauthenticated");
            authenticationContext.setRpcCode("apache.rocketmq.v2.ProxyAdminService/ListClients");
            return authenticationContext;
        }
    }

    public static class TokenAuthenticationStrategy implements AuthenticationStrategy {
        public TokenAuthenticationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        }

        @Override
        public void evaluate(AuthenticationContext context) {
            if (!(context instanceof DefaultAuthenticationContext)
                || !"authenticated-admin".equals(((DefaultAuthenticationContext) context).getUsername())) {
                throw new AuthenticationException("invalid admin token");
            }
        }
    }
}
