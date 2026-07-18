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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientPageTest {

    @Test
    public void constructorRejectsMissingClients() {
        assertThatThrownBy(() -> new ProxyClientPage(null, ""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clients is required");
    }

    @Test
    public void constructorRejectsNullClientEntry() {
        assertThatThrownBy(() -> new ProxyClientPage(Collections.singletonList(null), ""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("client is required");
    }

    @Test
    public void constructorCreatesImmutableClientSnapshotAndNormalizesNullToken() {
        List<ProxyClientInfo> clients = new ArrayList<>();
        clients.add(client("client-a"));

        ProxyClientPage page = new ProxyClientPage(clients, null);
        clients.add(client("client-b"));

        assertThat(page.getClients())
            .extracting(ProxyClientInfo::getClientId)
            .containsExactly("client-a");
        assertThatThrownBy(() -> page.getClients().add(client("client-c")))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThat(page.getNextPageToken()).isEmpty();
    }

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.emptySet(),
            Collections.emptySet(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }
}
