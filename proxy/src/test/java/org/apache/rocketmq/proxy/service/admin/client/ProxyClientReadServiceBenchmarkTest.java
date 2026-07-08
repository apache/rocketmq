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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ProxyClientReadServiceBenchmarkTest {

    @Test
    public void benchmarkSetupBuildsQueryableSyntheticClients() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 100;
        benchmark.groupCount = 10;
        benchmark.topicCount = 20;
        benchmark.proxyCount = 5;
        benchmark.setup();

        assertThat(benchmark.listFirstPage().getClients()).hasSize(100);
        assertThat(benchmark.listByGroupPage().getClients()).isNotEmpty();
        assertThat(benchmark.listByTopicPage().getClients()).isNotEmpty();
        assertThat(benchmark.listByProxyIdPage().getClients()).isNotEmpty();
        assertThat(benchmark.listByClientIdPrefixPage().getClients()).isNotEmpty();
        assertThat(benchmark.listByLanguagePage().getClients()).isNotEmpty();
        assertThat(benchmark.listByConnectTimeRangePage().getClients()).isNotEmpty();
        assertThat(benchmark.describeClient()).isNotNull();
    }

    @Test
    public void benchmarkSetupBuildsQueryableNextPageSyntheticClients() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 1500;
        benchmark.groupCount = 10;
        benchmark.topicCount = 20;
        benchmark.proxyCount = 5;
        benchmark.setup();

        ProxyClientPage nextPage = benchmark.listNextPage();

        assertThat(nextPage.getClients()).hasSize(ProxyClientQuery.MAX_PAGE_SIZE);
        assertThat(nextPage.getClients().get(0).getClientId()).isEqualTo("client-0000100");
        assertThat(nextPage.getNextPageToken()).isEqualTo("client-0000199");
    }

    @Test
    public void benchmarkSetupRejectsInvalidClientCount() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 0;
        benchmark.groupCount = 10;
        benchmark.topicCount = 20;
        benchmark.proxyCount = 5;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("clientCount must be positive");
    }

    @Test
    public void benchmarkSetupRejectsInvalidGroupCount() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 100;
        benchmark.groupCount = 0;
        benchmark.topicCount = 20;
        benchmark.proxyCount = 5;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("groupCount must be positive");
    }

    @Test
    public void benchmarkSetupRejectsInvalidTopicCount() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 100;
        benchmark.groupCount = 10;
        benchmark.topicCount = 0;
        benchmark.proxyCount = 5;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("topicCount must be positive");
    }

    @Test
    public void benchmarkSetupRejectsInvalidProxyCount() {
        ProxyClientReadServiceBenchmark benchmark = new ProxyClientReadServiceBenchmark();
        benchmark.clientCount = 100;
        benchmark.groupCount = 10;
        benchmark.topicCount = 20;
        benchmark.proxyCount = 0;

        assertThatThrownBy(benchmark::setup)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("proxyCount must be positive");
    }
}
