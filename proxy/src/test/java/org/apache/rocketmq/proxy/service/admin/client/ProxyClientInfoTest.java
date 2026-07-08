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
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientInfoTest {

    @Test
    public void constructorNormalizesClientId() {
        ProxyClientInfo clientInfo = client(" client-a ");

        assertThat(clientInfo.getClientId()).isEqualTo("client-a");
    }

    @Test
    public void constructorRejectsMissingClientId() {
        assertThatThrownBy(() -> client(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");
        assertThatThrownBy(() -> client(" "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");
    }

    @Test
    public void constructorTreatsUnspecifiedClientTypeAsMissing() {
        ProxyClientInfo clientInfo = client("client-a", ClientType.CLIENT_TYPE_UNSPECIFIED);

        assertThat(clientInfo.getClientType()).isNull();
    }

    @Test
    public void constructorTreatsUnrecognizedClientTypeAsMissing() {
        ProxyClientInfo clientInfo = client("client-a", ClientType.UNRECOGNIZED);

        assertThat(clientInfo.getClientType()).isNull();
    }

    @Test
    public void constructorRejectsOverlongProxyId() {
        assertThatThrownBy(() -> client("client-a", ClientType.PRODUCER, StringUtils.repeat("p", 256)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyId length exceeds 255");
    }

    private static ProxyClientInfo client(String clientId) {
        return client(clientId, ClientType.PRODUCER);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType) {
        return client(clientId, clientType, null);
    }

    private static ProxyClientInfo client(String clientId, ClientType clientType, String proxyId) {
        return new ProxyClientInfo(
            clientId,
            clientType,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            proxyId,
            100L,
            200L
        );
    }
}
