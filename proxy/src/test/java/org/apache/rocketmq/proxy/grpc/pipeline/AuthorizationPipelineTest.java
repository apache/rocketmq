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

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.auth.authorization.context.AuthorizationContext;
import org.apache.rocketmq.auth.authorization.exception.AuthorizationException;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

public class AuthorizationPipelineTest {

    @Test
    public void allowsCompatibleRequestWithEmptyContexts() {
        AuthorizationPipeline pipeline = createPipeline();
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();

        assertThatCode(() -> pipeline.execute(ProxyContext.create(), new Metadata(), request))
            .doesNotThrowAnyException();
    }

    @Test
    public void rejectsUnsupportedRequestWithEmptyContexts() {
        AuthorizationPipeline pipeline = createPipeline();

        Assert.assertThrows(AuthorizationException.class,
            () -> pipeline.execute(ProxyContext.create(), new Metadata(),
                TelemetryCommand.getDefaultInstance()));
    }

    private AuthorizationPipeline createPipeline() {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setConfigName("grpc-authorization-pipeline-test");
        authConfig.setAuthorizationEnabled(true);
        return new AuthorizationPipeline(authConfig, mock(MessagingProcessor.class)) {
            @Override
            protected List<AuthorizationContext> newContexts(ProxyContext context, Metadata headers,
                GeneratedMessageV3 request) {
                return Collections.emptyList();
            }
        };
    }
}
