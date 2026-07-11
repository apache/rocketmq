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
package org.apache.rocketmq.proxy.grpc.pipeline;

import apache.rocketmq.v2.QueryRouteRequest;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Context;
import io.grpc.Metadata;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.strategy.AuthenticationStrategy;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(MockitoJUnitRunner.class)
public class AuthenticationPipelineTest {
    private AuthConfig authConfig;

    @Mock
    private MessagingProcessor messagingProcessor;

    @Before
    public void setUp() {
        this.authConfig = new AuthConfig();
        this.authConfig.setConfigName("authentication-pipeline-" + System.nanoTime());
        this.authConfig.setAuthenticationEnabled(true);
    }

    @Test
    public void executeUsesSuppliedHeadersWhenCreatingAuthenticationContext() {
        CapturingAuthenticationPipeline pipeline =
            new CapturingAuthenticationPipeline(this.authConfig, this.messagingProcessor);
        Metadata suppliedHeaders = new Metadata();
        suppliedHeaders.put(GrpcConstants.AUTHORIZATION_AK, "supplied-user");
        Metadata currentHeaders = new Metadata();
        currentHeaders.put(GrpcConstants.AUTHORIZATION_AK, "current-user");

        Context.current().withValue(GrpcConstants.METADATA, currentHeaders).run(() ->
            pipeline.execute(ProxyContext.create(), suppliedHeaders, QueryRouteRequest.getDefaultInstance())
        );

        assertThat(pipeline.getCapturedHeaders()).isSameAs(suppliedHeaders);
    }

    @Test
    public void executeReplacesForgedSubjectHeaderAfterAuthenticationSucceeds() {
        this.authConfig.setAuthenticationStrategy(PassAuthenticationStrategy.class.getName());
        DefaultAuthenticationContext authenticationContext = defaultContext("authenticated-user", "test.rpc");
        StubAuthenticationPipeline pipeline =
            new StubAuthenticationPipeline(this.authConfig, this.messagingProcessor, authenticationContext);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");

        pipeline.execute(ProxyContext.create(), headers, QueryRouteRequest.getDefaultInstance());

        assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isEqualTo("authenticated-user");
    }

    @Test
    public void executeClearsForgedSubjectHeaderWhenAuthenticationIsWhitelisted() {
        this.authConfig.setAuthenticationWhitelist("test.rpc");
        DefaultAuthenticationContext authenticationContext = defaultContext("whitelisted-user", "test.rpc");
        StubAuthenticationPipeline pipeline =
            new StubAuthenticationPipeline(this.authConfig, this.messagingProcessor, authenticationContext);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");

        pipeline.execute(ProxyContext.create(), headers, QueryRouteRequest.getDefaultInstance());

        assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isNull();
    }

    @Test
    public void executeClearsForgedSubjectHeaderWhenAuthenticationFails() {
        this.authConfig.setAuthenticationStrategy(FailAuthenticationStrategy.class.getName());
        DefaultAuthenticationContext authenticationContext = defaultContext("unverified-user", "test.rpc");
        StubAuthenticationPipeline pipeline =
            new StubAuthenticationPipeline(this.authConfig, this.messagingProcessor, authenticationContext);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");

        assertThatThrownBy(() -> pipeline.execute(
            ProxyContext.create(), headers, QueryRouteRequest.getDefaultInstance()))
            .isInstanceOf(AuthenticationException.class);
        assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isNull();
    }

    @Test
    public void executeClearsForgedSubjectHeaderForCustomAuthenticationContext() {
        this.authConfig.setAuthenticationStrategy(PassAuthenticationStrategy.class.getName());
        AuthenticationContext authenticationContext = new AuthenticationContext() {
        };
        authenticationContext.setRpcCode("test.rpc");
        StubAuthenticationPipeline pipeline =
            new StubAuthenticationPipeline(this.authConfig, this.messagingProcessor, authenticationContext);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.AUTHORIZATION_AK, "forged-admin");

        pipeline.execute(ProxyContext.create(), headers, QueryRouteRequest.getDefaultInstance());

        assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isNull();
    }

    @Test
    public void messagingPipelinePreservesWhitelistedAuthenticatedSubject() {
        this.authConfig.setAuthenticationWhitelist("test.rpc");
        DefaultAuthenticationContext authenticationContext = defaultContext("messaging-user", "test.rpc");
        MessagingAuthenticationPipeline pipeline =
            new MessagingAuthenticationPipeline(this.authConfig, this.messagingProcessor, authenticationContext);
        Metadata headers = new Metadata();

        pipeline.execute(ProxyContext.create(), headers, QueryRouteRequest.getDefaultInstance());

        assertThat(headers.get(GrpcConstants.AUTHORIZATION_AK)).isEqualTo("messaging-user");
    }

    private static DefaultAuthenticationContext defaultContext(String username, String rpcCode) {
        DefaultAuthenticationContext authenticationContext = new DefaultAuthenticationContext();
        authenticationContext.setUsername(username);
        authenticationContext.setRpcCode(rpcCode);
        return authenticationContext;
    }

    private static class CapturingAuthenticationPipeline extends AuthenticationPipeline {
        private Metadata capturedHeaders;

        private CapturingAuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor) {
            super(authConfig, messagingProcessor);
        }

        @Override
        protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
            GeneratedMessageV3 request) {
            this.capturedHeaders = headers;
            return null;
        }

        private Metadata getCapturedHeaders() {
            return this.capturedHeaders;
        }

    }

    private static class StubAuthenticationPipeline extends AuthenticationPipeline {
        private final AuthenticationContext authenticationContext;

        private StubAuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor,
            AuthenticationContext authenticationContext) {
            super(authConfig, messagingProcessor, true);
            this.authenticationContext = authenticationContext;
        }

        @Override
        protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
            GeneratedMessageV3 request) {
            return this.authenticationContext;
        }
    }

    private static class MessagingAuthenticationPipeline extends AuthenticationPipeline {
        private final AuthenticationContext authenticationContext;

        private MessagingAuthenticationPipeline(AuthConfig authConfig, MessagingProcessor messagingProcessor,
            AuthenticationContext authenticationContext) {
            super(authConfig, messagingProcessor);
            this.authenticationContext = authenticationContext;
        }

        @Override
        protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
            GeneratedMessageV3 request) {
            return this.authenticationContext;
        }
    }

    public static class PassAuthenticationStrategy implements AuthenticationStrategy {
        public PassAuthenticationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        }

        @Override
        public void evaluate(AuthenticationContext context) {
        }
    }

    public static class FailAuthenticationStrategy implements AuthenticationStrategy {
        public FailAuthenticationStrategy(AuthConfig authConfig, Supplier<?> metadataService) {
        }

        @Override
        public void evaluate(AuthenticationContext context) {
            throw new AuthenticationException("authentication failed");
        }
    }
}
