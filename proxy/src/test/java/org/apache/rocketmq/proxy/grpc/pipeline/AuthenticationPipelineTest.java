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
import org.apache.rocketmq.auth.authentication.context.AuthenticationContext;
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
}
