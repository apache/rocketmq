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
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClientAdminRequestContextTest {

    @Test
    public void fromProxyContextUsesSubjectAndRemoteAddress() {
        User admin = User.of("admin");
        ProxyContext proxyContext = ProxyContext.create()
            .setSubject(admin)
            .setRemoteAddress("127.0.0.1:8080");

        ClientAdminRequestContext requestContext = ClientAdminRequestContext.from(proxyContext);

        assertThat(requestContext.getSubject()).isSameAs(admin);
        assertThat(requestContext.getSourceIp()).isEqualTo("127.0.0.1:8080");
    }

    @Test
    public void fromProxyContextRejectsNullContext() {
        assertThatThrownBy(() -> ClientAdminRequestContext.from(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyContext is required");
    }
}
