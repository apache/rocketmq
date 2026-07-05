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

import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminScopeMapperTest {

    @Test
    public void mapsMissingAndUnspecifiedPublicScopeToLocalProxy() {
        ProxyClientAdminScopeMapper mapper = ProxyClientAdminScopeMapper.getInstance();

        assertThat(mapper.decode(null)).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(mapper.decode("")).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(mapper.decode(" ")).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(mapper.decode("PROXY_SCOPE_UNSPECIFIED")).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void mapsKnownPublicScopeNamesToInternalScopes() {
        ProxyClientAdminScopeMapper mapper = ProxyClientAdminScopeMapper.getInstance();

        assertThat(mapper.decode("LOCAL_PROXY")).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(mapper.decode("ALL_PROXIES")).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(mapper.decode("PROXY_ID")).isEqualTo(ProxyClientScope.PROXY_ID);
    }

    @Test
    public void rejectsUnknownPublicScopeName() {
        ProxyClientAdminScopeMapper mapper = ProxyClientAdminScopeMapper.getInstance();

        assertThatThrownBy(() -> mapper.decode("UNRECOGNIZED"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported proxy scope")
            .hasMessageContaining("UNRECOGNIZED");
    }
}
