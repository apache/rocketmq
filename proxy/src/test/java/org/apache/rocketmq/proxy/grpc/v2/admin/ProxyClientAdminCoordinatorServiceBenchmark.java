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
import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ProxyClientAdminCoordinatorServiceBenchmark {
    @Param({"1000000"})
    public int clientCount;

    @Param({"100"})
    public int proxyCount;

    @Param({"1000"})
    public int groupCount;

    @Param({"10000"})
    public int topicCount;

    @Param({"100"})
    public int pageSize = ProxyClientQuery.MAX_PAGE_SIZE;

    private ProxyClientAdminCoordinatorService coordinatorService;
    private ProxyContext proxyContext;
    private String[] clientIds;
    private String[] proxyIds;
    private String[] groups;
    private String[] topics;
    private ProxyClientQuery allProxiesFirstPageQuery;
    private ProxyClientQuery allProxiesNextPageQuery;
    private final AtomicInteger sequence = new AtomicInteger();

    @Setup
    public void setup() {
        validatePositive("clientCount", this.clientCount);
        validatePositive("proxyCount", this.proxyCount);
        validatePositive("groupCount", this.groupCount);
        validatePositive("topicCount", this.topicCount);
        validatePositive("pageSize", this.pageSize);

        this.proxyContext = ProxyContext.create()
            .setRemoteAddress("127.0.0.1:8080")
            .setLocalAddress("127.0.0.1:8081");
        this.clientIds = new String[this.clientCount];
        this.proxyIds = names("proxy", this.proxyCount);
        this.groups = names("group", this.groupCount);
        this.topics = names("topic", this.topicCount);

        Map<String, ProxyClientReadService> readServices = new LinkedHashMap<>();
        for (String proxyId : this.proxyIds) {
            readServices.put(proxyId, new ProxyClientReadService());
        }
        for (int i = 0; i < this.clientCount; i++) {
            String clientId = clientId(i);
            String proxyId = this.proxyIds[i % this.proxyCount];
            this.clientIds[i] = clientId;
            readServices.get(proxyId).upsertClient(this.newClientInfo(i, clientId, proxyId));
        }

        this.coordinatorService = new ProxyClientAdminCoordinatorService(new BenchmarkPeerClient(readServices));
        this.allProxiesFirstPageQuery = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(this.pageSize)
            .build();
        ProxyClientPage firstPage = this.listAllProxiesFirstPage();
        this.allProxiesNextPageQuery = ProxyClientQuery.newBuilder()
            .setScope(ProxyClientScope.ALL_PROXIES)
            .setPageSize(this.pageSize)
            .setPageToken(firstPage.getNextPageToken())
            .build();
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesFirstPage() {
        return this.requirePage(this.coordinatorService.listClients(this.proxyContext, this.allProxiesFirstPageQuery));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesNextPage() {
        return this.requirePage(this.coordinatorService.listClients(this.proxyContext, this.allProxiesNextPageQuery));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesByGroupPage() {
        String group = this.groups[this.nextIndex(this.groupCount)];
        return this.requirePage(this.coordinatorService.listClientsByGroup(
            this.proxyContext,
            group,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesByTopicPage() {
        String topic = this.topics[this.nextIndex(this.topicCount)];
        return this.requirePage(this.coordinatorService.listClientsByTopic(
            this.proxyContext,
            topic,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesByClientIdPrefixPage() {
        return this.requirePage(this.coordinatorService.listClients(
            this.proxyContext,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setClientIdPrefix("client-000")
                .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesByLanguagePage() {
        return this.requirePage(this.coordinatorService.listClients(
            this.proxyContext,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setClientLanguage("JAVA")
                .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listAllProxiesByConnectTimeRangePage() {
        return this.requirePage(this.coordinatorService.listClients(
            this.proxyContext,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setConnectTimeStartMillis(100L)
                .setConnectTimeEndMillis(100L)
                .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listProxyIdPage() {
        String proxyId = this.proxyIds[this.nextIndex(this.proxyCount)];
        return this.requirePage(this.coordinatorService.listClients(
            this.proxyContext,
            ProxyClientQuery.newBuilder()
                .setScope(ProxyClientScope.PROXY_ID)
                .setProxyId(proxyId)
                .setPageSize(this.pageSize)
                .build()
        ));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientInfo describeClientAllProxies() {
        String clientId = this.clientIds[this.nextIndex(this.clientCount)];
        ProxyClientAdminResult<ProxyClientInfo> result = this.coordinatorService.describeClient(
            this.proxyContext,
            ProxyClientAdminDescribeClientRequest.newBuilder()
                .setScope(ProxyClientScope.ALL_PROXIES)
                .setClientId(clientId)
                .build()
        );
        if (result.getStatus().getCode() != Code.OK || result.getBody() == null) {
            throw new IllegalStateException("coordinator describe result is required: "
                + result.getStatus().getMessage());
        }
        return result.getBody();
    }

    private ProxyClientPage requirePage(ProxyClientAdminResult<ProxyClientPage> result) {
        if (result.getStatus().getCode() != Code.OK || result.getBody() == null) {
            throw new IllegalStateException("coordinator page result is required: "
                + result.getStatus().getMessage());
        }
        return result.getBody();
    }

    private ProxyClientInfo newClientInfo(int index, String clientId, String proxyId) {
        ClientType clientType = index % 2 == 0 ? ClientType.PRODUCER : ClientType.PUSH_CONSUMER;
        Set<String> clientGroups = clientType == ClientType.PRODUCER
            ? Collections.emptySet() : Collections.singleton(this.groups[(index / 2) % this.groupCount]);
        Set<String> clientTopics = Collections.singleton(this.topics[index % this.topicCount]);
        return new ProxyClientInfo(
            clientId,
            clientType,
            clientGroups,
            clientTopics,
            "JAVA",
            "127.0.0.1:" + (10000 + index % 10000),
            "127.0.0.2:8080",
            "V5_0_0",
            proxyId,
            100L,
            200L
        );
    }

    private int nextIndex(int bound) {
        return Math.floorMod(this.sequence.getAndIncrement(), bound);
    }

    private static void validatePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static String[] names(String prefix, int count) {
        String[] names = new String[count];
        for (int i = 0; i < count; i++) {
            names[i] = prefix + "-" + i;
        }
        return names;
    }

    private static String clientId(int index) {
        String number = Integer.toString(index);
        StringBuilder result = new StringBuilder("client-");
        for (int i = number.length(); i < 7; i++) {
            result.append('0');
        }
        return result.append(number).toString();
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    private static class BenchmarkPeerClient implements ProxyClientAdminPeerClient {
        private final Map<String, ProxyClientAdminPeerLocalExecutor> executors;

        private BenchmarkPeerClient(Map<String, ProxyClientReadService> readServices) {
            this.executors = new LinkedHashMap<>();
            for (Map.Entry<String, ProxyClientReadService> entry : readServices.entrySet()) {
                this.executors.put(entry.getKey(), new ProxyClientAdminPeerLocalExecutor(
                    entry.getKey(),
                    new DefaultClientAdminService(entry.getValue())
                ));
            }
        }

        @Override
        public List<String> listProxyIds() {
            return new ArrayList<>(this.executors.keySet());
        }

        @Override
        public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, String proxyId,
            ProxyClientAdminPeerRequest request) {
            ProxyClientAdminPeerLocalExecutor executor = this.executors.get(proxyId);
            if (executor == null) {
                return ProxyClientAdminPeerResponse.error(proxyId, Code.NOT_FOUND.name(),
                    "Proxy not found: " + proxyId);
            }
            return executor.execute(ctx, request);
        }
    }
}
