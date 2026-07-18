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

import java.util.Collections;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClientAdminMetricsContextTest {

    @Test
    public void statusOfMapsEveryResultAndNull() {
        assertThat(ClientAdminMetricsContext.statusOf(null)).isEqualTo("unknown");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.OK)).isEqualTo("ok");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.BAD_REQUEST)).isEqualTo("bad_request");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.NOT_FOUND)).isEqualTo("not_found");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.UNAUTHORIZED)).isEqualTo("unauthorized");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.TIMEOUT)).isEqualTo("proxy_timeout");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.TOO_MANY_REQUESTS))
            .isEqualTo("too_many_requests");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.NOT_IMPLEMENTED))
            .isEqualTo("not_implemented");
        assertThat(ClientAdminMetricsContext.statusOf(ClientAdminMetricsResult.INTERNAL_ERROR))
            .isEqualTo("internal_server_error");
    }

    @Test
    public void buildRejectsMissingOperationAndResult() {
        assertThatThrownBy(() -> ClientAdminMetricsContext.newBuilder()
            .setResult(ClientAdminMetricsResult.OK)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("operation is required");

        assertThatThrownBy(() -> ClientAdminMetricsContext.newBuilder()
            .setOperation(ClientAdminOperation.LIST_CLIENTS)
            .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("result is required");
    }

    @Test
    public void buildUsesDefaultsAndBoundsOptionalFields() {
        ClientAdminMetricsContext context = ClientAdminMetricsContext.newBuilder()
            .setOperation(ClientAdminOperation.LIST_CLIENTS)
            .setResult(ClientAdminMetricsResult.OK)
            .setLatencyMillis(-1L)
            .setStatus(" ")
            .setFilters(" ")
            .setResultSize(-2)
            .setQuery(null)
            .addFilter(" ")
            .build();

        assertThat(context.getLatencyMillis()).isZero();
        assertThat(context.getStatus()).isEqualTo("ok");
        assertThat(context.getFilters()).isEqualTo(ClientAdminMetricsContext.FILTER_NONE);
        assertThat(context.getPageSize()).isEqualTo(-1);
        assertThat(context.getResultSize()).isEqualTo(-1);
    }

    @Test
    public void resultSizeOfCountsNullPageAndSingleBody() {
        assertThat(ClientAdminMetricsContext.resultSizeOf(null)).isZero();
        assertThat(ClientAdminMetricsContext.resultSizeOf(new ProxyClientPage(Collections.emptyList(), ""))).isZero();
        assertThat(ClientAdminMetricsContext.resultSizeOf("client-view")).isEqualTo(1);
    }
}
