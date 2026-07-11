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
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.Status;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.DefaultClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadService;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GrpcProxyAdminApplicationBenchmark {
    private static final int PRODUCTION_THREAD_POOL_SIZE = 4;
    private static final int PRODUCTION_QUEUE_CAPACITY = 10000;

    @Param({"1000000"})
    public int clientCount;

    @Param({"1000"})
    public int groupCount;

    @Param({"10000"})
    public int topicCount;

    @Param({"100"})
    public int proxyCount;

    private ProxyClientReadService readService;
    private ProxyClientAdminEndpointExecutor endpointExecutor;
    private ThreadPoolExecutor queryExecutor;
    private ThreadPoolExecutor serverExecutor;
    private Server server;
    private ManagedChannel channel;
    private ProxyAdminServiceGrpc.ProxyAdminServiceBlockingStub stub;
    private String[] clientIds;
    private String[] groups;
    private String[] topics;
    private String[] proxyIds;
    private ListClientsRequest firstPageRequest;
    private ListClientsRequest clientIdPrefixRequest;
    private ListClientsRequest combinedFiltersRequest;
    private ListClientsRequest languageRequest;
    private ListClientsRequest connectTimeRangeRequest;
    private ListClientsRequest wideConnectTimeRangeRequest;
    private final AtomicInteger sequence = new AtomicInteger();

    @Setup
    public void setup() throws Exception {
        validatePositive("clientCount", this.clientCount);
        validatePositive("groupCount", this.groupCount);
        validatePositive("topicCount", this.topicCount);
        validatePositive("proxyCount", this.proxyCount);

        this.readService = new ProxyClientReadService();
        this.clientIds = new String[this.clientCount];
        this.groups = names("group", this.groupCount);
        this.topics = names("topic", this.topicCount);
        this.proxyIds = names("proxy", this.proxyCount);
        this.firstPageRequest = ListClientsRequest.newBuilder()
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.clientIdPrefixRequest = ListClientsRequest.newBuilder()
            .setClientIdPrefix("client-000")
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.combinedFiltersRequest = ListClientsRequest.newBuilder()
            .setClientIdPrefix("client-")
            .setClientLanguage("JAVA")
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(199L)
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.languageRequest = ListClientsRequest.newBuilder()
            .setClientLanguage("JAVA")
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.connectTimeRangeRequest = ListClientsRequest.newBuilder()
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(100L)
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        this.wideConnectTimeRangeRequest = ListClientsRequest.newBuilder()
            .setConnectTimeStartMillis(100L)
            .setConnectTimeEndMillis(199L)
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();

        for (int i = 0; i < this.clientCount; i++) {
            String clientId = clientId(i);
            this.clientIds[i] = clientId;
            this.readService.upsertClient(newClientInfo(i, clientId));
        }

        DefaultClientAdminService clientAdminService = new DefaultClientAdminService(this.readService);
        AuthorizingClientAdminService authorizingClientAdminService = new AuthorizingClientAdminService(
            clientAdminService,
            (subject, operation, sourceIp) -> {
            }
        );
        ProxyClientAdminActivity activity = new ProxyClientAdminActivity(authorizingClientAdminService);
        ProxyClientAdminEndpointHandler endpointHandler = new ProxyClientAdminEndpointHandler(activity);
        ProxyClientAdminContextFactory contextFactory = new ProxyClientAdminContextFactory(
            (context, headers, request) -> context
                .setRemoteAddress("127.0.0.1:8080")
                .setLocalAddress("127.0.0.2:8081")
        );
        this.queryExecutor = newBoundedExecutor("ProxyClientAdminQueryThread_", 0L, TimeUnit.MILLISECONDS);
        this.serverExecutor = newBoundedExecutor(
            "ProxyAdminGrpcRequestExecutorThread_",
            1L,
            TimeUnit.MINUTES
        );
        this.endpointExecutor = new ProxyClientAdminEndpointExecutor(
            contextFactory,
            endpointHandler,
            this.queryExecutor
        );
        this.server = ServerBuilder.forPort(0)
            .executor(this.serverExecutor)
            .addService(new GrpcProxyAdminApplication(this.endpointExecutor))
            .build()
            .start();
        this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", this.server.getPort())
            .usePlaintext()
            .directExecutor()
            .build();
        this.stub = ProxyAdminServiceGrpc.newBlockingStub(this.channel);
    }

    @TearDown
    public void tearDown() throws Exception {
        if (this.channel != null) {
            this.channel.shutdownNow();
            this.channel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (this.server != null) {
            this.server.shutdownNow();
            this.server.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (this.endpointExecutor != null) {
            this.endpointExecutor.shutdown();
        }
        if (this.queryExecutor != null) {
            awaitTerminationOrForceShutdown(this.queryExecutor);
        }
        if (this.serverExecutor != null) {
            this.serverExecutor.shutdownNow();
            this.serverExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClients() {
        return requireClients(this.stub.listClients(this.firstPageRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClientsByClientIdPrefix() {
        return requireClients(this.stub.listClients(this.clientIdPrefixRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClientsByCombinedFilters() {
        return requireClients(this.stub.listClients(this.combinedFiltersRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClientsByLanguage() {
        return requireClients(this.stub.listClients(this.languageRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClientsByConnectTimeRange() {
        return requireClients(this.stub.listClients(this.connectTimeRangeRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsResponse listClientsByWideConnectTimeRange() {
        return requireClients(this.stub.listClients(this.wideConnectTimeRangeRequest));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsByGroupResponse listClientsByGroup() {
        ListClientsByGroupRequest request = ListClientsByGroupRequest.newBuilder()
            .setGroup(this.groups[nextIndex(this.groupCount)])
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        return requireClients(this.stub.listClientsByGroup(request));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public ListClientsByTopicResponse listClientsByTopic() {
        ListClientsByTopicRequest request = ListClientsByTopicRequest.newBuilder()
            .setTopic(this.topics[nextIndex(this.topicCount)])
            .setPageNum(ProxyClientQuery.DEFAULT_PAGE_NUM)
            .setPageSize(ProxyClientQuery.MAX_PAGE_SIZE)
            .build();
        return requireClients(this.stub.listClientsByTopic(request));
    }

    @Benchmark
    @Fork(value = 1)
    @Measurement(iterations = 5, time = 5)
    @Warmup(iterations = 3, time = 1)
    @Threads(4)
    public DescribeClientResponse describeClient() {
        DescribeClientRequest request = DescribeClientRequest.newBuilder()
            .setClientId(this.clientIds[nextIndex(this.clientCount)])
            .build();
        return requireClient(this.stub.describeClient(request));
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
            100L + index % 100,
            200L
        );
    }

    private int nextIndex(int bound) {
        return Math.floorMod(this.sequence.getAndIncrement(), bound);
    }

    int getQueryExecutorThreadPoolSize() {
        return this.queryExecutor.getCorePoolSize();
    }

    int getQueryExecutorQueueCapacity() {
        return queueCapacity(this.queryExecutor);
    }

    int getServerExecutorThreadPoolSize() {
        return this.serverExecutor.getCorePoolSize();
    }

    int getServerExecutorQueueCapacity() {
        return queueCapacity(this.serverExecutor);
    }

    static void awaitTerminationOrForceShutdown(ThreadPoolExecutor executor) throws InterruptedException {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ThreadPoolExecutor newBoundedExecutor(String threadNamePrefix, long keepAliveTime,
        TimeUnit timeUnit) {
        return new ThreadPoolExecutor(
            PRODUCTION_THREAD_POOL_SIZE,
            PRODUCTION_THREAD_POOL_SIZE,
            keepAliveTime,
            timeUnit,
            new LinkedBlockingQueue<>(PRODUCTION_QUEUE_CAPACITY),
            new ThreadFactoryImpl(threadNamePrefix),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static int queueCapacity(ThreadPoolExecutor executor) {
        return executor.getQueue().size() + executor.getQueue().remainingCapacity();
    }

    private static ListClientsResponse requireClients(ListClientsResponse response) {
        requireOk(response.getStatus());
        if (response.getClientsCount() == 0) {
            throw new IllegalStateException("listClients response must contain clients");
        }
        return response;
    }

    private static ListClientsByGroupResponse requireClients(ListClientsByGroupResponse response) {
        requireOk(response.getStatus());
        if (response.getClientsCount() == 0) {
            throw new IllegalStateException("listClientsByGroup response must contain clients");
        }
        return response;
    }

    private static ListClientsByTopicResponse requireClients(ListClientsByTopicResponse response) {
        requireOk(response.getStatus());
        if (response.getClientsCount() == 0) {
            throw new IllegalStateException("listClientsByTopic response must contain clients");
        }
        return response;
    }

    private static DescribeClientResponse requireClient(DescribeClientResponse response) {
        requireOk(response.getStatus());
        if (!response.hasClient()) {
            throw new IllegalStateException("describeClient response must contain client");
        }
        return response;
    }

    private static void requireOk(Status status) {
        if (status.getCode() != Code.OK) {
            throw new IllegalStateException(status.toString());
        }
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
