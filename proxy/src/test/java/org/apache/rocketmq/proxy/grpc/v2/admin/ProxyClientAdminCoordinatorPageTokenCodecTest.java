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

import apache.rocketmq.v2.ClientType;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminCoordinatorPageTokenCodecTest {

    @Test
    public void codecRoundTripsCoordinatorOwnedCrossProxyToken() {
        ProxyClientAdminCoordinatorPageTokenCodec codec =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance();
        Map<String, String> peerPageTokens = new LinkedHashMap<>();
        peerPageTokens.put("proxy-b", "client-20");
        peerPageTokens.put("proxy-a", "client-10");
        ProxyClientAdminCoordinatorPageToken token = ProxyClientAdminCoordinatorPageToken.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setGroup(" group-a ")
            .setTopic(" topic-a ")
            .setClientType(ClientType.PUSH_CONSUMER)
            .setLastClientId(" client-20 ")
            .setCreateTimeMillis(1000L)
            .setPeerPageTokens(peerPageTokens)
            .build();

        String encoded = codec.encode(token);

        assertThat(encoded).startsWith("cp1:");
        ProxyClientAdminCoordinatorPageToken decoded = codec.decode(encoded);
        assertThat(decoded.getScope()).isEqualTo(ProxyClientScope.ALL_PROXIES);
        assertThat(decoded.getGroup()).isEqualTo("group-a");
        assertThat(decoded.getTopic()).isEqualTo("topic-a");
        assertThat(decoded.getClientType()).isEqualTo(ClientType.PUSH_CONSUMER);
        assertThat(decoded.getLastClientId()).isEqualTo("client-20");
        assertThat(decoded.getCreateTimeMillis()).isEqualTo(1000L);
        assertThat(decoded.getPeerPageTokens())
            .containsExactly(
                entry("proxy-a", "client-10"),
                entry("proxy-b", "client-20")
            );
    }

    @Test
    public void codecRejectsMalformedCoordinatorTokens() {
        ProxyClientAdminCoordinatorPageTokenCodec codec =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance();

        assertThatThrownBy(() -> codec.decode("v1:Y2xpZW50LWE"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token");
        assertThatThrownBy(() -> codec.decode("cp1:*"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token");
    }

    @Test
    public void codecRejectsNonCanonicalCoordinatorTokens() {
        ProxyClientAdminCoordinatorPageTokenCodec codec =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance();
        String tokenWithNonCanonicalJson = "cp1:" + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("{ \"scope\":\"ALL_PROXIES\", \"lastClientId\":\"client-a\" }"
                .getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> codec.decode(tokenWithNonCanonicalJson))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token");
    }

    @Test
    public void codecRejectsOverlongCoordinatorTokens() {
        ProxyClientAdminCoordinatorPageTokenCodec codec =
            ProxyClientAdminCoordinatorPageTokenCodec.getInstance();

        assertThatThrownBy(() -> codec.decode("cp1:" + StringUtils.repeat("a", 5000)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid coordinator page token");
    }

    @Test
    public void tokenNormalizesBlankFiltersAndRejectsMissingScope() {
        ProxyClientAdminCoordinatorPageToken token = ProxyClientAdminCoordinatorPageToken.newBuilder()
            .setScope(ProxyClientScope.PROXY_ID)
            .setProxyId(" proxy-a ")
            .setGroup(" ")
            .setTopic("")
            .setLastClientId(null)
            .setCreateTimeMillis(1000L)
            .build();

        assertThat(token.getProxyId()).isEqualTo("proxy-a");
        assertThat(token.getGroup()).isNull();
        assertThat(token.getTopic()).isNull();
        assertThat(token.getLastClientId()).isNull();
        assertThat(token.getPeerPageTokens()).isEmpty();
        assertThatThrownBy(() -> ProxyClientAdminCoordinatorPageToken.newBuilder()
            .setScope(null)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("scope is required");
    }

    private static Map.Entry<String, String> entry(String key, String value) {
        return new java.util.AbstractMap.SimpleImmutableEntry<>(key, value);
    }
}
