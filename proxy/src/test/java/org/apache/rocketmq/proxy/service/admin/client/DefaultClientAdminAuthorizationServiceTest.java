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
package org.apache.rocketmq.proxy.service.admin.client;

import java.util.List;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.AuthorizationEvaluator;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.action.Action;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultClientAdminAuthorizationServiceTest {

    @Test
    public void authorizeSkipsWhenAuthorizationDisabled() {
        AuthConfig authConfig = authConfig(false);
        ClientAdminAuthPolicy authPolicy = mock(ClientAdminAuthPolicy.class);
        AuthorizationEvaluator authorizationEvaluator = mock(AuthorizationEvaluator.class);
        ClientAdminAuthorizationService authorizationService = new DefaultClientAdminAuthorizationService(
            authConfig,
            authPolicy,
            authorizationEvaluator
        );

        authorizationService.authorize(
            User.of("admin"),
            ClientAdminOperation.LIST_CLIENTS,
            "127.0.0.1"
        );

        verify(authPolicy, never()).newContext(any(), any(), any(), any());
        verify(authorizationEvaluator, never()).evaluate(anyList());
    }

    @Test
    public void authorizeEvaluatesClientAdminRequirementWhenEnabled() {
        AuthConfig authConfig = authConfig(true);
        ClientAdminAuthPolicy authPolicy = mock(ClientAdminAuthPolicy.class);
        AuthorizationEvaluator authorizationEvaluator = mock(AuthorizationEvaluator.class);
        ClientAdminAuthorizationService authorizationService = new DefaultClientAdminAuthorizationService(
            authConfig,
            authPolicy,
            authorizationEvaluator
        );
        User admin = User.of("admin");
        DefaultAuthorizationContext context = DefaultAuthorizationContext.of(
            admin,
            Resource.ofCluster("DefaultCluster"),
            Action.LIST,
            "127.0.0.1"
        );
        when(authPolicy.newContext(admin, ClientAdminOperation.LIST_CLIENTS_BY_TOPIC, "DefaultCluster",
            "127.0.0.1")).thenReturn(context);

        authorizationService.authorize(
            admin,
            ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
            "127.0.0.1"
        );

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<List> contextCaptor = ArgumentCaptor.forClass(List.class);
        verify(authorizationEvaluator).evaluate(contextCaptor.capture());
        assertThat(contextCaptor.getValue()).containsExactly(context);
    }

    private static AuthConfig authConfig(boolean authorizationEnabled) {
        AuthConfig authConfig = new AuthConfig();
        authConfig.setClusterName("DefaultCluster");
        authConfig.setAuthorizationEnabled(authorizationEnabled);
        return authConfig;
    }
}
