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

package org.apache.rocketmq.proxy.grpc.v2.admin;

import java.lang.reflect.Field;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcMessagingApplication;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GrpcProxyAdminWiringTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;
    @Mock
    private GrpcMessagingActivity grpcMessagingActivity;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
    }

    @Test
    public void createDefaultActivityExposesAdminActivityAcrossGrpcPackages() {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);

        ProxyClientAdminActivity adminActivity = activity.getProxyClientAdminActivity();

        assertThat(adminActivity).isNotNull();
    }

    @Test
    public void createDefaultActivityExposesAdminScopeRouterAcrossGrpcPackages() {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);

        ProxyClientAdminScopeRouter scopeRouter = activity.getProxyClientAdminScopeRouter();

        assertThat(scopeRouter).isNotNull();
    }

    @Test
    public void createDefaultActivityExposesAdminEndpointHandlerUsingSharedScopeRouter() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);

        ProxyClientAdminEndpointHandler endpointHandler = activity.getProxyClientAdminEndpointHandler();

        Field scopeRouterField = ProxyClientAdminEndpointHandler.class.getDeclaredField("proxyClientAdminScopeRouter");
        scopeRouterField.setAccessible(true);
        assertThat(endpointHandler).isNotNull();
        assertThat(scopeRouterField.get(endpointHandler)).isSameAs(activity.getProxyClientAdminScopeRouter());
    }

    @Test
    public void createDefaultActivityExposesAdminEndpointExecutorUsingSharedHandler() throws Exception {
        DefaultGrpcMessagingActivity activity = GrpcMessagingApplication.createDefaultActivity(this.messagingProcessor);

        ProxyClientAdminEndpointExecutor endpointExecutor = activity.getProxyClientAdminEndpointExecutor();

        Field endpointHandlerField = ProxyClientAdminEndpointExecutor.class.getDeclaredField("endpointHandler");
        endpointHandlerField.setAccessible(true);
        assertThat(endpointExecutor).isNotNull();
        assertThat(endpointHandlerField.get(endpointExecutor)).isSameAs(activity.getProxyClientAdminEndpointHandler());
    }

    @Test
    public void createMessagingApplicationUsesSuppliedSharedActivity() throws Exception {
        GrpcMessagingApplication application = GrpcMessagingApplication.create(
            this.messagingProcessor,
            this.grpcMessagingActivity
        );

        Field activityField = GrpcMessagingApplication.class.getDeclaredField("grpcMessagingActivity");
        activityField.setAccessible(true);

        assertThat(activityField.get(application)).isSameAs(this.grpcMessagingActivity);
    }
}
