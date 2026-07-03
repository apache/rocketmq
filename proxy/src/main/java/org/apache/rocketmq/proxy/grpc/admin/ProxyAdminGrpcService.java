/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * The License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.admin;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientRequest;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;

/**
 * gRPC service implementation for Proxy Admin Client queries.
 * <p>
 * Implements the 4 core RPCs defined in RIP-2 M1:
 * - ListClients: Paginated listing of all online clients with optional filters
 * - DescribeClient: Detailed info for a specific client
 * - ListClientsByGroup: Paginated listing of clients in a consumer group
 * - ListClientsByTopic: Paginated listing of clients subscribed to a topic
 * <p>
 * All admin RPCs execute on an isolated thread pool to avoid impacting data plane.
 * <p>
 * Sampling and degradation (RIP-2 §8.5):
 * When the system is under high load (concurrent admin requests exceed threshold),
 * diagnostic interfaces (DescribeClient) support sampling to protect system stability.
 * The sampling rate is controlled by the concurrent request count relative to the
 * thread pool capacity.
 */
public class ProxyAdminGrpcService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final int MAX_PAGE_SIZE = 100;

    /**
     * Sampling threshold: when concurrent DescribeClient requests exceed this value,
     * new requests will be rejected with UNAVAILABLE status (RIP-2 §8.5).
     * This protects the system from diagnostic queries overwhelming the admin thread pool.
     */
    private static final int DESCRIBE_CLIENT_CONCURRENCY_LIMIT = 8;

    /**
     * Sampling rate under load: fraction of DescribeClient requests to accept
     * when concurrency is between half the limit and the full limit.
     * Value of 0.5 means accept 50% of requests.
     */
    private static final double SAMPLING_RATE_UNDER_LOAD = 0.5;

    private final ProxyAdminClientService adminClientService;
    private final ExecutorService adminExecutor;

    /**
     * Current concurrent DescribeClient request count for sampling control.
     */
    private final AtomicInteger describeClientConcurrency = new AtomicInteger(0);

    public ProxyAdminGrpcService(ProxyAdminClientService adminClientService, int threadNums) {
        this.adminClientService = adminClientService;
        this.adminExecutor = createAdminExecutor(threadNums);
    }

    private ExecutorService createAdminExecutor(int threadNums) {
        return new ThreadPoolExecutor(
            threadNums,
            threadNums,
            60L, TimeUnit.SECONDS,
            new java.util.concurrent.SynchronousQueue<>(),
            new ThreadFactoryImpl("ProxyAdminThread_"),
            new ThreadPoolExecutor.DiscardOldestPolicy()
        );
    }

    /**
     * List all online clients with optional filters and pagination.
     * RIP-2 M1: ListClients RPC
     */
    public void listClients(ListClientsRequest request, StreamObserver<ListClientsResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                // Parse filter from request
                ListClientsFilter filter = ProxyAdminProtoConverter.toFilter(request);

                // Parse pagination with upper bound enforcement (RIP-2 §8.2)
                int pageNum = request.getPageNum() > 0 ? request.getPageNum() : 1;
                int pageSize = enforcePageSize(request.getPageSize());

                // Call service
                ListClientsResult result = adminClientService.listClients(filter, pageNum, pageSize);

                // Build response
                ListClientsResponse response = ProxyAdminProtoConverter.toListClientsResponse(result);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS,
                    System.currentTimeMillis() - startTime);
                log.debug("listClients completed in {}ms, total={}", System.currentTimeMillis() - startTime, result.getTotal());
            } catch (Exception e) {
                log.error("listClients failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS,
                    System.currentTimeMillis() - startTime);
                ListClientsResponse errorResponse = ProxyAdminProtoConverter.toListClientsError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Describe a specific client by clientId.
     * RIP-2 M1: DescribeClient RPC
     * <p>
     * Implements sampling and degradation (RIP-2 §8.5):
     * When concurrent DescribeClient requests exceed DESCRIBE_CLIENT_CONCURRENCY_LIMIT,
     * the request is rejected with UNAVAILABLE status to protect system stability.
     * When concurrency is between half the limit and the full limit, sampling is applied
     * at SAMPLING_RATE_UNDER_LOAD rate.
     */
    public void describeClient(DescribeClientRequest request, StreamObserver<DescribeClientResponse> responseObserver) {
        // Sampling and degradation check (RIP-2 §8.5)
        int currentConcurrency = describeClientConcurrency.incrementAndGet();
        try {
            if (!shouldAcceptDescribeClient(currentConcurrency)) {
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT, 0);
                DescribeClientResponse errorResponse = ProxyAdminProtoConverter.toDescribeClientError(
                    AdminCode.ADMIN_CODE_TOO_MANY_REQUESTS,
                    "Server is under high load, DescribeClient request rejected due to sampling. " +
                    "Current concurrency: " + currentConcurrency);
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
                return;
            }

            adminExecutor.execute(() -> {
                long startTime = System.currentTimeMillis();
                try {
                    String clientId = request.getClientId();
                    if (clientId == null || clientId.isEmpty()) {
                        ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT,
                            System.currentTimeMillis() - startTime);
                        DescribeClientResponse errorResponse = ProxyAdminProtoConverter.toDescribeClientError(
                            AdminCode.ADMIN_CODE_BAD_REQUEST, "clientId is required");
                        responseObserver.onNext(errorResponse);
                        responseObserver.onCompleted();
                        return;
                    }

                    ClientDetailInfo detail = adminClientService.describeClient(clientId);
                    if (detail == null) {
                        ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT,
                            System.currentTimeMillis() - startTime);
                        DescribeClientResponse errorResponse = ProxyAdminProtoConverter.toDescribeClientError(
                            AdminCode.ADMIN_CODE_NOT_FOUND, "Client not found: " + clientId);
                        responseObserver.onNext(errorResponse);
                        responseObserver.onCompleted();
                        return;
                    }

                    DescribeClientResponse response = ProxyAdminProtoConverter.toDescribeClientResponse(detail);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();

                    ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT,
                        System.currentTimeMillis() - startTime);
                    log.debug("describeClient completed in {}ms, clientId={}", System.currentTimeMillis() - startTime, clientId);
                } catch (Exception e) {
                    log.error("describeClient failed", e);
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT,
                        System.currentTimeMillis() - startTime);
                    DescribeClientResponse errorResponse = ProxyAdminProtoConverter.toDescribeClientError(
                        AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                }
            });
        } finally {
            describeClientConcurrency.decrementAndGet();
        }
    }

    /**
     * Determine whether to accept a DescribeClient request based on current concurrency.
     * RIP-2 §8.5: Sampling and degradation for diagnostic interfaces.
     * <p>
     * Strategy:
     * - Below half the concurrency limit: always accept
     * - Between half and full limit: accept at SAMPLING_RATE_UNDER_LOAD rate
     * - Above the limit: reject (return false)
     */
    private boolean shouldAcceptDescribeClient(int currentConcurrency) {
        if (currentConcurrency <= DESCRIBE_CLIENT_CONCURRENCY_LIMIT / 2) {
            // Low load: always accept
            return true;
        }
        if (currentConcurrency > DESCRIBE_CLIENT_CONCURRENCY_LIMIT) {
            // Over limit: reject
            log.warn("DescribeClient concurrency limit reached: {}/{}", currentConcurrency, DESCRIBE_CLIENT_CONCURRENCY_LIMIT);
            return false;
        }
        // Medium load: sample at SAMPLING_RATE_UNDER_LOAD
        boolean accept = Math.random() < SAMPLING_RATE_UNDER_LOAD;
        if (!accept) {
            log.info("DescribeClient request sampled out under medium load. Concurrency: {}/{}",
                currentConcurrency, DESCRIBE_CLIENT_CONCURRENCY_LIMIT);
        }
        return accept;
    }

    /**
     * List clients by consumer group.
     * RIP-2 M1: ListClientsByGroup RPC
     */
    public void listClientsByGroup(ListClientsByGroupRequest request, StreamObserver<ListClientsByGroupResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                String group = request.getGroup();
                if (group == null || group.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_GROUP,
                        System.currentTimeMillis() - startTime);
                    ListClientsByGroupResponse errorResponse = ProxyAdminProtoConverter.toListClientsByGroupError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "group is required");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                int pageNum = request.getPageNum() > 0 ? request.getPageNum() : 1;
                int pageSize = enforcePageSize(request.getPageSize());

                ListClientsResult result = adminClientService.listClientsByGroup(group, pageNum, pageSize);
                ListClientsByGroupResponse response = ProxyAdminProtoConverter.toListClientsByGroupResponse(result);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_GROUP,
                    System.currentTimeMillis() - startTime);
                log.debug("listClientsByGroup completed in {}ms, group={}, total={}",
                    System.currentTimeMillis() - startTime, group, result.getTotal());
            } catch (Exception e) {
                log.error("listClientsByGroup failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_GROUP,
                    System.currentTimeMillis() - startTime);
                ListClientsByGroupResponse errorResponse = ProxyAdminProtoConverter.toListClientsByGroupError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * List clients by topic subscription.
     * RIP-2 M1: ListClientsByTopic RPC
     */
    public void listClientsByTopic(ListClientsByTopicRequest request, StreamObserver<ListClientsByTopicResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                String topic = request.getTopic();
                if (topic == null || topic.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_TOPIC,
                        System.currentTimeMillis() - startTime);
                    ListClientsByTopicResponse errorResponse = ProxyAdminProtoConverter.toListClientsByTopicError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "topic is required");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                int pageNum = request.getPageNum() > 0 ? request.getPageNum() : 1;
                int pageSize = enforcePageSize(request.getPageSize());

                ListClientsResult result = adminClientService.listClientsByTopic(topic, pageNum, pageSize);
                ListClientsByTopicResponse response = ProxyAdminProtoConverter.toListClientsByTopicResponse(result);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_TOPIC,
                    System.currentTimeMillis() - startTime);
                log.debug("listClientsByTopic completed in {}ms, topic={}, total={}",
                    System.currentTimeMillis() - startTime, topic, result.getTotal());
            } catch (Exception e) {
                log.error("listClientsByTopic failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_TOPIC,
                    System.currentTimeMillis() - startTime);
                ListClientsByTopicResponse errorResponse = ProxyAdminProtoConverter.toListClientsByTopicError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Shutdown the admin executor.
     */
    public void shutdown() {
        adminExecutor.shutdown();
        try {
            if (!adminExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                adminExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            adminExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // ==================== Helper Methods ====================

    /**
     * Enforce page size upper bound as required by RIP-2 §8.2.
     * Maximum page size is 100 to prevent large queries from overwhelming memory.
     */
    int enforcePageSize(int pageSize) {
        if (pageSize <= 0) {
            return 20; // default
        }
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }
}