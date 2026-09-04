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

import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Metadata;
import java.lang.reflect.Field;
import org.apache.rocketmq.auth.authentication.AuthenticationEvaluator;
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
import org.apache.rocketmq.auth.authentication.context.DefaultAuthenticationContext;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AuthenticationPipelineTest {

    @Test
    public void removesAuthorizationSubjectWhenAuthenticationIsDisabled() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("grpc-authentication-disabled-test");
        AuthenticationPipeline pipeline = new AuthenticationPipeline(
            authConfig, mock(MessagingProcessor.class));
        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "forged-user");

        pipeline.execute(ProxyContext.create(), metadata, TelemetryCommand.getDefaultInstance());

        assertThat(metadata.containsKey(GrpcConstants.AUTHORIZATION_AK)).isFalse();
    }

    @Test
    public void replacesAuthorizationSubjectAfterAuthentication() throws Exception {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("grpc-authentication-pipeline-test");
        authConfig.setAuthenticationEnabled(true);
        DefaultAuthenticationContext authenticationContext = new DefaultAuthenticationContext();
        authenticationContext.setRpcCode("authenticated-rpc");
        authenticationContext.setUsername("verified-user");
        AuthenticationPipeline pipeline = new AuthenticationPipeline(
            authConfig, mock(MessagingProcessor.class)) {
            @Override
            protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
                com.google.protobuf.GeneratedMessageV3 request) {
                return authenticationContext;
            }
        };
        AuthenticationEvaluator authenticationEvaluator = mock(AuthenticationEvaluator.class);
        Field evaluatorField = AuthenticationPipeline.class.getDeclaredField("authenticationEvaluator");
        evaluatorField.setAccessible(true);
        evaluatorField.set(pipeline, authenticationEvaluator);

        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "forged-user");
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "another-forged-user");
        pipeline.execute(ProxyContext.create(), metadata, TelemetryCommand.getDefaultInstance());

        verify(authenticationEvaluator).evaluate(authenticationContext);
        assertThat(metadata.getAll(GrpcConstants.AUTHORIZATION_AK)).containsExactly("verified-user");
    }

    @Test
    public void doesNotPublishAuthorizationSubjectForWhitelistedRequest() throws Exception {
        String rpcCode = TelemetryCommand.getDescriptor().getFullName();
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("grpc-authentication-whitelist-test");
        authConfig.setAuthenticationEnabled(true);
        authConfig.setAuthenticationWhitelist("other-rpc, " + rpcCode);
        DefaultAuthenticationContext authenticationContext = new DefaultAuthenticationContext();
        authenticationContext.setRpcCode(rpcCode);
        authenticationContext.setUsername("unverified-user");
        AuthenticationPipeline pipeline = new AuthenticationPipeline(
            authConfig, mock(MessagingProcessor.class)) {
            @Override
            protected AuthenticationContext newContext(ProxyContext context, Metadata headers,
                com.google.protobuf.GeneratedMessageV3 request) {
                return authenticationContext;
            }
        };
        AuthenticationEvaluator authenticationEvaluator = mock(AuthenticationEvaluator.class);
        Field evaluatorField = AuthenticationPipeline.class.getDeclaredField("authenticationEvaluator");
        evaluatorField.setAccessible(true);
        evaluatorField.set(pipeline, authenticationEvaluator);

        Metadata metadata = new Metadata();
        metadata.put(GrpcConstants.AUTHORIZATION_AK, "forged-user");
        pipeline.execute(ProxyContext.create(), metadata, TelemetryCommand.getDefaultInstance());

        verify(authenticationEvaluator).evaluate(authenticationContext);
        assertThat(metadata.containsKey(GrpcConstants.AUTHORIZATION_AK)).isFalse();
    }
}
