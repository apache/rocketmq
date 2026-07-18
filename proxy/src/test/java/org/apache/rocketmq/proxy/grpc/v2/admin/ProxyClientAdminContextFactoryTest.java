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

import apache.rocketmq.v2.QueryRouteRequest;
import io.grpc.Metadata;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.pipeline.AuthenticationSubjectPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminContextFactoryTest {

    @Test
    public void createRunsSharedPipelineWithoutRequiringClientId() {
        RequestPipeline pipeline = ((RequestPipeline) (context, headers, request) -> {
        })
            .pipe(new AuthenticationSubjectPipeline())
            .pipe(new ContextInitPipeline());
        ProxyClientAdminContextFactory factory = new ProxyClientAdminContextFactory(pipeline);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.REMOTE_ADDRESS, "192.168.0.1:8080");
        headers.put(GrpcConstants.LOCAL_ADDRESS, "127.0.0.1:8080");
        headers.put(GrpcConstants.LANGUAGE, "JAVA");
        headers.put(GrpcConstants.CLIENT_VERSION, "V5_0_0");
        headers.put(GrpcConstants.AUTHORIZATION_AK, "admin");

        ProxyContext context = factory.create(headers, QueryRouteRequest.getDefaultInstance());

        assertThat(context.getClientID()).isEmpty();
        assertThat(context.getRemoteAddress()).isEqualTo("192.168.0.1:8080");
        assertThat(context.getLocalAddress()).isEqualTo("127.0.0.1:8080");
        assertThat(context.getLanguage()).isEqualTo("JAVA");
        assertThat(context.getClientVersion()).isEqualTo("V5_0_0");
        assertThat(context.getSubject()).isNotNull();
        assertThat(context.getSubject().getSubjectKey()).isEqualTo("User:admin");
    }

    @Test
    public void constructorRejectsMissingRequestPipeline() {
        assertThatThrownBy(() -> new ProxyClientAdminContextFactory(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requestPipeline is required");
    }

    @Test
    public void createRejectsMissingRequest() {
        ProxyClientAdminContextFactory factory = new ProxyClientAdminContextFactory((context, headers, request) -> {
        });

        assertThatThrownBy(() -> factory.create(new Metadata(), null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("request is required");
    }

    @Test
    public void createTreatsMissingHeadersAsEmptyMetadata() {
        RequestPipeline pipeline = ((RequestPipeline) (context, headers, request) -> {
        })
            .pipe(new ContextInitPipeline());
        ProxyClientAdminContextFactory factory = new ProxyClientAdminContextFactory(pipeline);

        ProxyContext context = factory.create(null, QueryRouteRequest.getDefaultInstance());

        assertThat(context.getClientID()).isEmpty();
        assertThat(context.getRemoteAddress()).isEmpty();
        assertThat(context.getLocalAddress()).isEmpty();
    }
}
