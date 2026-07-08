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

import apache.rocketmq.v2.ClientType;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void queryRejectsOverlongProxyId() {
        assertThatThrownBy(() -> ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(StringUtils.repeat("p", 256))
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");
    }

    @Test
    public void queryTreatsUnspecifiedClientTypeAsNoFilter() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
            .build();

        assertThat(query.getClientType()).isNull();
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

    @Test
    public void toBuilderCopiesAllQueryFields() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .setTopic("topic-a")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setPageSize(10)
            .setPageToken("client-a")
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId("proxy-a")
            .build();

        ProxyClientQuery copiedQuery = query.toBuilder().build();

        assertThat(copiedQuery.getGroup()).isEqualTo("group-a");
        assertThat(copiedQuery.getTopic()).isEqualTo("topic-a");
        assertThat(copiedQuery.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(copiedQuery.getPageSize()).isEqualTo(10);
        assertThat(copiedQuery.getPageToken()).isEqualTo("client-a");
        assertThat(copiedQuery.getScope()).isEqualTo(ProxyClientScope.PROXY_ID);
        assertThat(copiedQuery.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void toBuilderAllowsClearingProxyId() {
        ProxyClientQuery query = ProxyClientQuery.newBuilder()
            .setGroup("group-a")
            .setScope(ProxyClientScope.LOCAL_PROXY)
            .setProxyId("proxy-a")
            .build();

        ProxyClientQuery copiedQuery = query.toBuilder()
            .setProxyId(null)
            .build();

        assertThat(copiedQuery.getGroup()).isEqualTo("group-a");
        assertThat(copiedQuery.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(copiedQuery.getProxyId()).isNull();
    }
}
