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
import apache.rocketmq.v2.ProxyClient;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrpcProxyAdminApplicationBenchmarkTest {

    @Test
    public void benchmarkSetupBuildsQueryablePublicGrpcService() throws Exception {
        GrpcProxyAdminApplicationBenchmark benchmark = new GrpcProxyAdminApplicationBenchmark();
        benchmark.clientCount = 120;
        benchmark.groupCount = 6;
        benchmark.topicCount = 8;
        benchmark.proxyCount = 3;
        benchmark.setup();
        try {
            assertThat(benchmark.listClients().getStatus().getCode()).isEqualTo(Code.OK);
            assertThat(benchmark.listClients().getClientsList())
                .extracting(ProxyClient::getClientId)
                .contains("client-0000000");
            assertThat(benchmark.listClientsByGroup().getClientsList()).isNotEmpty();
            assertThat(benchmark.listClientsByTopic().getClientsList()).isNotEmpty();
            assertThat(benchmark.listClientsByClientIdPrefix().getClientsList()).isNotEmpty();
            assertThat(benchmark.listClientsByLanguage().getClientsList()).isNotEmpty();
            assertThat(benchmark.listClientsByConnectTimeRange().getClientsList()).isNotEmpty();
            assertThat(benchmark.describeClient().getClient().getClientId()).isNotEmpty();
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    public void benchmarkSetupRejectsInvalidClientCount() {
        GrpcProxyAdminApplicationBenchmark benchmark = new GrpcProxyAdminApplicationBenchmark();
        benchmark.clientCount = 0;
        benchmark.groupCount = 6;
        benchmark.topicCount = 8;
        benchmark.proxyCount = 3;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientCount must be positive");
    }
}
