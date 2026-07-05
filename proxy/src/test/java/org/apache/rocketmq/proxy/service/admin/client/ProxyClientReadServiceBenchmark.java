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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
public class ProxyClientReadServiceBenchmark {
    @Param({"1000000"})
    public int clientCount;

    @Param({"1000"})
    public int groupCount;

    @Param({"10000"})
    public int topicCount;

    @Param({"100"})
    public int proxyCount;

    private ProxyClientReadService readService;
    private String[] clientIds;
    private String[] groups;
    private String[] topics;
    private String[] proxyIds;
    private ProxyClientQuery firstPageQuery;
    private ProxyClientQuery nextPageQuery;
    private final AtomicInteger sequence = new AtomicInteger();

    @Setup
    public void setup() {
        validatePositive("clientCount", this.clientCount);
        validatePositive("groupCount", this.groupCount);
        validatePositive("topicCount", this.topicCount);
        validatePositive("proxyCount", this.proxyCount);

        this.readService = new ProxyClientReadService();
        this.clientIds = new String[this.clientCount];
        this.groups = names("group", this.groupCount);
        this.topics = names("topic", this.topicCount);
        this.proxyIds = names("proxy", this.proxyCount);
        this.firstPageQuery = ProxyClientQuery.newBuilder()
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.nextPageQuery = ProxyClientQuery.newBuilder()
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .setPageToken(clientId(Math.min(ProxyClientQuery.MAX_PAGE_SIZE - 1, this.clientCount - 1)))
            .build();

        for (int i = 0; i < this.clientCount; i++) {
            String clientId = clientId(i);
            this.clientIds[i] = clientId;
            this.readService.upsertClient(newClientInfo(i, clientId));
        }
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listFirstPage() {
        return this.readService.listClients(this.firstPageQuery);
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listNextPage() {
        return this.readService.listClients(this.nextPageQuery);
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listByGroupPage() {
        return this.readService.listClients(ProxyClientQuery.newBuilder()
            .setGroup(this.groups[nextIndex(this.groupCount)])
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build());
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listByTopicPage() {
        return this.readService.listClients(ProxyClientQuery.newBuilder()
            .setTopic(this.topics[nextIndex(this.topicCount)])
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build());
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientPage listByProxyIdPage() {
        return this.readService.listClients(ProxyClientQuery.newBuilder()
            .setProxyId(this.proxyIds[nextIndex(this.proxyCount)])
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build());
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ProxyClientInfo describeClient() {
        return this.readService.getClient(this.clientIds[nextIndex(this.clientCount)]);
    }

    private ProxyClientInfo newClientInfo(int index, String clientId) {
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
            this.proxyIds[index % this.proxyCount],
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
}
