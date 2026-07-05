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

package org.apache.rocketmq.proxy.service.admin.client;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyClientQueryTest {

    @Test
    public void queryDefaultsToLocalProxyScope() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder().build();

        assertThat(query.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
    }

    @Test
    public void queryPreservesExplicitScope() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .build();

        assertThat(query.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
    }

    @Test
    public void queryPreservesProxyId() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        assertThat(query.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void queryTrimsStringFiltersAndNormalizesBlankValues() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setGroup(" group-a ")
            .setTopic("\ttopic-a\t")
            .setPageToken(" client-a ")
            .setProxyId(" ")
            .build();

        assertThat(query.getGroup()).isEqualTo("group-a");
        assertThat(query.getTopic()).isEqualTo("topic-a");
        assertThat(query.getPageToken()).isEqualTo("client-a");
        assertThat(query.getProxyId()).isNull();
    }
}
