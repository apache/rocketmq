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
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminViewTest {

    @Test
    public void clientViewRequiresClientId() {
        assertThatThrownBy(() -> clientView(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");

        assertThatThrownBy(() -> clientView(" "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId is required");
    }

    @Test
    public void clientViewTrimsClientId() {
        ProxyClientAdminClientView view = clientView(" client-a ");

        assertThat(view.getClientId()).isEqualTo("client-a");
    }

    @Test
    public void clientViewRejectsOverlongClientId() {
        assertThatThrownBy(() -> clientView(StringUtils.repeat("c", 256)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientId length exceeds 255");
    }

    @Test
    public void clientViewTreatsUnrecognizedClientTypeAsUnspecified() {
        ProxyClientAdminClientView view = new ProxyClientAdminClientView(
            "client-a",
            ClientType.UNRECOGNIZED,
            Collections.emptyList(),
            Collections.emptyList(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        assertThat(view.getClientType()).isEqualTo(ClientType.CLIENT_TYPE_UNSPECIFIED);
    }

    @Test
    public void clientViewNormalizesGroupAndTopicEntries() {
        ProxyClientAdminClientView view = new ProxyClientAdminClientView(
            "client-a",
            ClientType.PUSH_CONSUMER,
            Arrays.asList(" group-b ", null, "", " ", "group-a", "group-b"),
            Arrays.asList(" topic-b ", null, "", " ", "topic-a", "topic-b"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );

        assertThat(view.getGroups()).containsExactly("group-b", "group-a");
        assertThat(view.getTopics()).containsExactly("topic-b", "topic-a");
    }

    @Test
    public void clientViewNormalizesStringMetadataFields() {
        ProxyClientAdminClientView view = new ProxyClientAdminClientView(
            "client-a",
            ClientType.PRODUCER,
            Collections.emptyList(),
            Collections.emptyList(),
            " JAVA ",
            " 127.0.0.1:8080 ",
            " 192.168.0.1:8080 ",
            " V5_0_0 ",
            " proxy-a ",
            100L,
            200L
        );

        assertThat(view.getLanguage()).isEqualTo("JAVA");
        assertThat(view.getRemoteAddress()).isEqualTo("127.0.0.1:8080");
        assertThat(view.getLocalAddress()).isEqualTo("192.168.0.1:8080");
        assertThat(view.getClientVersion()).isEqualTo("V5_0_0");
        assertThat(view.getProxyId()).isEqualTo("proxy-a");
    }

    @Test
    public void pageViewRejectsNullClientEntries() {
        assertThatThrownBy(() -> new ProxyClientAdminPageView(
            Arrays.asList(clientView("client-a"), null),
            ""
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("client is required");
    }

    @Test
    public void pageViewNormalizesBlankNextPageToken() {
        ProxyClientAdminPageView view = new ProxyClientAdminPageView(
            Collections.singletonList(clientView("client-a")),
            " "
        );

        assertThat(view.getNextPageToken()).isEmpty();
    }

    private static ProxyClientAdminClientView clientView(String clientId) {
        return new ProxyClientAdminClientView(
            clientId,
            ClientType.PRODUCER,
            Collections.emptyList(),
            Collections.emptyList(),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }
}
