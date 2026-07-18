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

import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.context.DefaultAuthorizationContext;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.resource.ResourceType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClientAdminAuthPolicyTest {

    private final ClientAdminAuthPolicy authPolicy = new ClientAdminAuthPolicy();

    @Test
    public void listClientOperationsRequireProxyAdminClientList() {
        assertRequirement(ClientAdminOperation.LIST_CLIENTS, Action.LIST);
        assertRequirement(ClientAdminOperation.LIST_CLIENTS_BY_GROUP, Action.LIST);
        assertRequirement(ClientAdminOperation.LIST_CLIENTS_BY_TOPIC, Action.LIST);
    }

    @Test
    public void describeClientRequiresProxyAdminClientGet() {
        assertRequirement(ClientAdminOperation.DESCRIBE_CLIENT, Action.GET);
    }

    @Test
    public void rejectsMissingOperation() {
        assertThatThrownBy(() -> authPolicy.newContext(User.of("admin"), null, "DefaultCluster", "127.0.0.1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("operation is required");
    }

    private void assertRequirement(ClientAdminOperation operation, Action expectedAction) {
        DefaultAuthorizationContext context = authPolicy.newContext(
            User.of("admin"),
            operation,
            "DefaultCluster",
            "127.0.0.1"
        );

        assertThat(context.getSubject().getSubjectKey()).isEqualTo("User:admin");
        assertThat(context.getResource().getResourceType()).isEqualTo(ResourceType.ADMIN);
        assertThat(context.getResource().getResourceName()).isEqualTo(ClientAdminAuthPolicy.PROXY_ADMIN_CLIENT_RESOURCE);
        assertThat(context.getResource().getResourceKey()).isEqualTo("Admin:proxy.admin.client");
        assertThat(context.getActions()).containsExactly(expectedAction);
        assertThat(context.getSourceIp()).isEqualTo("127.0.0.1");
    }
}
