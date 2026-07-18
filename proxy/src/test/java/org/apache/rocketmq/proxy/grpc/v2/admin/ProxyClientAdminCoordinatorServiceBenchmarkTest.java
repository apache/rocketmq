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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientAdminCoordinatorServiceBenchmarkTest {

    @Test
    public void benchmarkSetupBuildsQueryableCoordinatorSyntheticClients() {
        ProxyClientAdminCoordinatorServiceBenchmark benchmark = new ProxyClientAdminCoordinatorServiceBenchmark();
        benchmark.clientCount = 120;
        benchmark.proxyCount = 3;
        benchmark.groupCount = 6;
        benchmark.topicCount = 8;
        benchmark.pageSize = 20;
        benchmark.setup();

        assertThat(benchmark.listAllProxiesFirstPage().getClients()).hasSize(20);
        assertThat(benchmark.listAllProxiesFirstPage().getNextPageToken()).isNotEmpty();
        assertThat(benchmark.listAllProxiesNextPage().getClients()).isNotEmpty();
        assertThat(benchmark.listAllProxiesByGroupPage().getClients()).isNotEmpty();
        assertThat(benchmark.listAllProxiesByTopicPage().getClients()).isNotEmpty();
        assertThat(benchmark.listAllProxiesByClientIdPrefixPage().getClients()).isNotEmpty();
        assertThat(benchmark.listAllProxiesByLanguagePage().getClients()).isNotEmpty();
        assertThat(benchmark.listAllProxiesByConnectTimeRangePage().getClients()).isNotEmpty();
        assertThat(benchmark.listProxyIdPage().getClients()).isNotEmpty();
        assertThat(benchmark.describeClientAllProxies()).isNotNull();
    }

    @Test
    public void defaultBenchmarkUsesOfficialPageSizeCap() {
        ProxyClientAdminCoordinatorServiceBenchmark benchmark = new ProxyClientAdminCoordinatorServiceBenchmark();

        assertThat(benchmark.pageSize).isEqualTo(100);
    }

    @Test
    public void benchmarkSetupRejectsInvalidPageSize() {
        ProxyClientAdminCoordinatorServiceBenchmark benchmark = new ProxyClientAdminCoordinatorServiceBenchmark();
        benchmark.clientCount = 100;
        benchmark.proxyCount = 3;
        benchmark.groupCount = 6;
        benchmark.topicCount = 8;
        benchmark.pageSize = 0;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("pageSize must be positive");
    }
}
