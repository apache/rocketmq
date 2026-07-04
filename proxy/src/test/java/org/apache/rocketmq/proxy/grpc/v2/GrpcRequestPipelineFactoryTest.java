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

package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.QueryRouteRequest;
import io.grpc.Metadata;
import org.apache.rocketmq.common.constant.GrpcConstants;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.admin.ProxyClientAdminContextFactory;
import org.apache.rocketmq.proxy.grpc.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class GrpcRequestPipelineFactoryTest extends InitConfigTest {
    private static final String CLIENT_ID = "client-a";
    private static final String REMOTE_ADDRESS = "192.168.0.1:8080";
    private static final String LOCAL_ADDRESS = "127.0.0.1:8080";
    private static final String LANGUAGE = "JAVA";
    private static final String CLIENT_VERSION = "V5_0_0";
    private static final String NAMESPACE = "namespace-a";
    private static final String ACCESS_KEY = "admin";

    @Mock
    private MessagingProcessor messagingProcessor;

    @Before
    public void setUp() throws Throwable {
        super.before();
    }

    @Test
    public void createBuildsSharedGrpcRequestPipeline() {
        RequestPipeline pipeline = GrpcRequestPipelineFactory.create(this.messagingProcessor);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.CLIENT_ID, CLIENT_ID);
        headers.put(GrpcConstants.REMOTE_ADDRESS, REMOTE_ADDRESS);
        headers.put(GrpcConstants.LOCAL_ADDRESS, LOCAL_ADDRESS);
        headers.put(GrpcConstants.LANGUAGE, LANGUAGE);
        headers.put(GrpcConstants.CLIENT_VERSION, CLIENT_VERSION);
        headers.put(GrpcConstants.NAMESPACE_ID, NAMESPACE);
        headers.put(GrpcConstants.AUTHORIZATION_AK, ACCESS_KEY);
        ProxyContext context = ProxyContext.create();

        pipeline.execute(context, headers, QueryRouteRequest.getDefaultInstance());

        assertThat(context.getClientID()).isEqualTo(CLIENT_ID);
        assertThat(context.getRemoteAddress()).isEqualTo(REMOTE_ADDRESS);
        assertThat(context.getLocalAddress()).isEqualTo(LOCAL_ADDRESS);
        assertThat(context.getLanguage()).isEqualTo(LANGUAGE);
        assertThat(context.getClientVersion()).isEqualTo(CLIENT_VERSION);
        assertThat(context.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(context.getSubject()).isNotNull();
        assertThat(context.getSubject().getSubjectKey()).isEqualTo("User:admin");
    }

    @Test
    public void createProxyClientAdminContextFactoryBuildsAdminContextWithoutClientId() {
        ProxyClientAdminContextFactory factory =
            GrpcRequestPipelineFactory.createProxyClientAdminContextFactory(this.messagingProcessor);
        Metadata headers = new Metadata();
        headers.put(GrpcConstants.REMOTE_ADDRESS, REMOTE_ADDRESS);
        headers.put(GrpcConstants.LOCAL_ADDRESS, LOCAL_ADDRESS);
        headers.put(GrpcConstants.LANGUAGE, LANGUAGE);
        headers.put(GrpcConstants.CLIENT_VERSION, CLIENT_VERSION);
        headers.put(GrpcConstants.NAMESPACE_ID, NAMESPACE);
        headers.put(GrpcConstants.AUTHORIZATION_AK, ACCESS_KEY);

        ProxyContext context = factory.create(headers, QueryRouteRequest.getDefaultInstance());

        assertThat(context.getClientID()).isEmpty();
        assertThat(context.getRemoteAddress()).isEqualTo(REMOTE_ADDRESS);
        assertThat(context.getLocalAddress()).isEqualTo(LOCAL_ADDRESS);
        assertThat(context.getLanguage()).isEqualTo(LANGUAGE);
        assertThat(context.getClientVersion()).isEqualTo(CLIENT_VERSION);
        assertThat(context.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(context.getSubject()).isNotNull();
        assertThat(context.getSubject().getSubjectKey()).isEqualTo("User:admin");
    }
}
