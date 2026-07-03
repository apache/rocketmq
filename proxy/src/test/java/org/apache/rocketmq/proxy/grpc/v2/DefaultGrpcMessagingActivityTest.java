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

import apache.rocketmq.v2.ClientType;
import java.util.Collections;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultGrpcMessagingActivityTest extends InitConfigTest {
    @Mock
    private MessagingProcessor messagingProcessor;
    @Mock
    private ProxyRelayService proxyRelayService;

    @Before
    public void setUp() throws Throwable {
        super.before();
        when(this.messagingProcessor.getProxyRelayService()).thenReturn(this.proxyRelayService);
    }

    @Test
    public void initCreatesClientAdminServiceWithSharedReadModel() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        activity.proxyClientReadService.upsertClient(clientInfo);

        ClientAdminService clientAdminService = activity.getClientAdminService();
        assertThat(clientAdminService).isNotNull();
        assertThat(clientAdminService.describeClient("client-a")).isSameAs(clientInfo);
    }

    @Test
    public void initCreatesAuthorizingClientAdminServiceWithSharedReadModel() {
        DefaultGrpcMessagingActivity activity = new DefaultGrpcMessagingActivity(this.messagingProcessor);
        ProxyClientInfo clientInfo = new ProxyClientInfo(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        activity.proxyClientReadService.upsertClient(clientInfo);

        AuthorizingClientAdminService clientAdminService = activity.getAuthorizingClientAdminService();
        assertThat(clientAdminService).isNotNull();
        assertThat(clientAdminService.describeClient(
            ClientAdminRequestContext.of(User.of("admin"), "127.0.0.1"),
            "client-a"
        )).isSameAs(clientInfo);
    }
}
