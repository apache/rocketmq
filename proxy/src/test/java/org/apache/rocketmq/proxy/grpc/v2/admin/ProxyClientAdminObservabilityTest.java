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

import apache.rocketmq.v2.Code;
import java.util.Arrays;
import java.util.Collections;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsContext;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProxyClientAdminObservabilityTest {

    @Test
    public void listClientsContextUsesFilterPresenceAndResultSize() {
        ProxyClientAdminListClientsRequest request = ProxyClientAdminListClientsRequest.newBuilder()
            .setClientIdPrefix("client-")
            .setClientLanguage("JAVA")
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(200L)
            .setPageSize(42)
            .build();
        ProxyClientAdminPageView pageView = new ProxyClientAdminPageView(
            Arrays.asList(clientView("client-a"), clientView("client-b")),
            ""
        );
        ProxyClientAdminResult<ProxyClientAdminPageView> result = new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
            pageView
        );

        ClientAdminMetricsContext context = ProxyClientAdminObservability.contextOf(
            ClientAdminOperation.LIST_CLIENTS,
            request,
            result
        );

        assertThat(context.getOperation()).isEqualTo(ClientAdminOperation.LIST_CLIENTS);
        assertThat(context.getScope()).isEqualTo(ProxyClientScope.LOCAL_PROXY);
        assertThat(context.getStatus()).isEqualTo("ok");
        assertThat(context.getPageSize()).isEqualTo(42);
        assertThat(context.getFilters()).isEqualTo("client_id_prefix,client_language,connect_time_range");
        assertThat(context.getResultSize()).isEqualTo(2);
    }

    @Test
    public void describeClientFailureContextDoesNotExposeClientIdValue() {
        ProxyClientAdminDescribeClientRequest request = ProxyClientAdminDescribeClientRequest.newBuilder()
            .setClientId("private-client-id")
            .build();
        ProxyClientAdminResult<ProxyClientAdminClientView> result = new ProxyClientAdminResult<>(
            ResponseBuilder.getInstance().buildStatus(Code.NOT_FOUND, "missing client private-client-id"),
            null
        );

        ClientAdminMetricsContext context = ProxyClientAdminObservability.contextOf(
            ClientAdminOperation.DESCRIBE_CLIENT,
            request,
            result
        );

        assertThat(context.getStatus()).isEqualTo("not_found");
        assertThat(context.getFilters()).isEqualTo("client_id");
        assertThat(context.getResultSize()).isZero();
        assertThat(context.getFilters()).doesNotContain("private-client-id");
    }

    @Test
    public void observeIgnoresObservabilityFailureWithoutLogger() {
        ProxyClientAdminObservability.observe(null, null, null, null);
    }

    @Test
    public void observeWarnsWhenObservabilityFails() {
        Logger log = mock(Logger.class);

        ProxyClientAdminObservability.observe(log, null, null, null);

        verify(log).warn(
            eq("record proxy client admin observability failed. operation:{}, error:{}"),
            any(),
            eq(IllegalArgumentException.class.getSimpleName())
        );
    }

    private static ProxyClientAdminClientView clientView(String clientId) {
        return new ProxyClientAdminClientView(
            clientId,
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            "",
            "",
            "",
            "",
            0,
            0
        );
    }
}
