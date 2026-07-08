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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.BatchConsumeDiagnosticResult;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.PopReceiptHandleDiagnosticResult;
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
import apache.rocketmq.proxy.admin.v1.GetConfigRequest;
import apache.rocketmq.proxy.admin.v1.GetConfigResponse;
import apache.rocketmq.proxy.admin.v1.UpdateConfigRequest;
import apache.rocketmq.proxy.admin.v1.UpdateConfigResponse;
import apache.rocketmq.proxy.admin.v1.DisconnectClientRequest;
import apache.rocketmq.proxy.admin.v1.DisconnectClientResponse;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesRequest;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesResponse;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsRequest;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.proxy.admin.v1.ProxyRuntimeConfig;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsRequest;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsResponse;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;

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
    private final RouteChangeNotifier routeChangeNotifier;

    /**
     * Current concurrent DescribeClient request count for sampling control.
     */
    private final AtomicInteger describeClientConcurrency = new AtomicInteger(0);

    public ProxyAdminGrpcService(ProxyAdminClientService adminClientService, int threadNums) {
        this.adminClientService = adminClientService;
        this.adminExecutor = createAdminExecutor(threadNums);
        this.routeChangeNotifier = null;
    }

    public ProxyAdminGrpcService(ProxyAdminClientService adminClientService, int threadNums,
        RouteChangeNotifier routeChangeNotifier) {
        this.adminClientService = adminClientService;
        this.adminExecutor = createAdminExecutor(threadNums);
        this.routeChangeNotifier = routeChangeNotifier;
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
     * Get current runtime configuration of the proxy.
     * Allows Dashboard to remotely query Proxy's runtime settings including
     * rate limiting thresholds, timeout values, thread pool sizes, etc.
     */
    public void getConfig(GetConfigRequest request, StreamObserver<GetConfigResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                ProxyConfig config = ConfigurationManager.getProxyConfig();
                GetConfigResponse response = ProxyAdminProtoConverter.toGetConfigResponse(config);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_GET_CONFIG,
                    System.currentTimeMillis() - startTime);
                log.debug("getConfig completed in {}ms", System.currentTimeMillis() - startTime);
            } catch (Exception e) {
                log.error("getConfig failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_GET_CONFIG,
                    System.currentTimeMillis() - startTime);
                GetConfigResponse errorResponse = ProxyAdminProtoConverter.toGetConfigError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Hot-update runtime configuration without restarting proxy.
     * <p>
     * Compares each field of the requested config with the current runtime config,
     * and applies only the fields that have changed. Changed fields are returned
     * in the response for audit/verification.
     * <p>
     * Most configuration changes take effect immediately:
     * - Message size limits, user property limits: applied on next message operation
     * - Timeout/polling values: applied on next client operation
     * - Rate limiting thresholds (concurrency limit, sampling rate): applied on next admin request
     * - Cache settings: applied on next cache access
     * <p>
     * Note: Some settings cannot be hot-updated and require a restart:
     * - Network ports (grpc_server_port, proxy_admin_server_port)
     * - Cluster names and proxy identity
     * - TLS configuration
     */
    public void updateConfig(UpdateConfigRequest request, StreamObserver<UpdateConfigResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                ProxyConfig config = ConfigurationManager.getProxyConfig();
                ProxyRuntimeConfig reqConfig = request.getConfig();

                if (reqConfig == null || reqConfig.equals(ProxyRuntimeConfig.getDefaultInstance())) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_UPDATE_CONFIG,
                        System.currentTimeMillis() - startTime);
                    UpdateConfigResponse errorResponse = ProxyAdminProtoConverter.toUpdateConfigError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "Config is required in the request");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                // Apply config changes and collect changed field names
                List<String> changedFields = ProxyAdminProtoConverter.applyConfigChanges(config, reqConfig);

                UpdateConfigResponse response = ProxyAdminProtoConverter.toUpdateConfigResponse(config, changedFields);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_UPDATE_CONFIG,
                    System.currentTimeMillis() - startTime);
                log.info("updateConfig completed in {}ms, changed {} fields: {}",
                    System.currentTimeMillis() - startTime, changedFields.size(), changedFields);
            } catch (Exception e) {
                log.error("updateConfig failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_UPDATE_CONFIG,
                    System.currentTimeMillis() - startTime);
                UpdateConfigResponse errorResponse = ProxyAdminProtoConverter.toUpdateConfigError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Force disconnect a specific client connection.
     * RIP-2 M2: DisconnectClient RPC
     * <p>
     * Closes the gRPC telemetry stream, removes the channel and settings,
     * triggering client reconnection and consumer group rebalance.
     * <p>
     * Use cases:
     * - Malicious client detection and isolation
     * - Stuck consumer triggering rebalance
     * - Zombie connection cleanup
     */
    public void disconnectClient(DisconnectClientRequest request, StreamObserver<DisconnectClientResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                String clientId = request.getClientId();
                String reason = request.getReason();

                if (clientId == null || clientId.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DISCONNECT_CLIENT,
                        System.currentTimeMillis() - startTime);
                    DisconnectClientResponse errorResponse = ProxyAdminProtoConverter.toDisconnectClientError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "clientId is required");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                if (reason == null || reason.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DISCONNECT_CLIENT,
                        System.currentTimeMillis() - startTime);
                    DisconnectClientResponse errorResponse = ProxyAdminProtoConverter.toDisconnectClientError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "reason is required for audit logging");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                boolean disconnected = adminClientService.forceDisconnectClient(clientId, reason);
                if (!disconnected) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DISCONNECT_CLIENT,
                        System.currentTimeMillis() - startTime);
                    DisconnectClientResponse errorResponse = ProxyAdminProtoConverter.toDisconnectClientError(
                        AdminCode.ADMIN_CODE_NOT_FOUND, "Client not found: " + clientId);
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                DisconnectClientResponse response = ProxyAdminProtoConverter.toDisconnectClientResponse(true);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_DISCONNECT_CLIENT,
                    System.currentTimeMillis() - startTime);
                log.info("disconnectClient completed in {}ms, clientId={}, reason={}",
                    System.currentTimeMillis() - startTime, clientId, reason);
            } catch (Exception e) {
                log.error("disconnectClient failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DISCONNECT_CLIENT,
                    System.currentTimeMillis() - startTime);
                DisconnectClientResponse errorResponse = ProxyAdminProtoConverter.toDisconnectClientError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Query POP receipt handles for diagnostics.
     * RIP-2 M3: DescribePopReceiptHandles RPC
     * <p>
     * Provides diagnostic information for POP consumption mode, including:
     * - Unacked message receipt handles with renewal statistics
     * - Messages with expired invisible time (about to be redelivered)
     * - Frequent ChangeInvisibleTime (renewal) patterns
     * - Consumption timeout detection
     */
    public void describePopReceiptHandles(DescribePopReceiptHandlesRequest request,
        StreamObserver<DescribePopReceiptHandlesResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                String group = request.getGroup();
                if (group == null || group.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_POP_RECEIPT_HANDLES,
                        System.currentTimeMillis() - startTime);
                    DescribePopReceiptHandlesResponse errorResponse = ProxyAdminProtoConverter.toDescribePopReceiptHandlesError(
                        AdminCode.ADMIN_CODE_BAD_REQUEST, "group is required");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                String topic = request.getTopic();
                int pageNum = request.getPageNum() > 0 ? request.getPageNum() : 1;
                int pageSize = enforcePageSize(request.getPageSize());

                PopReceiptHandleDiagnosticResult result = adminClientService.describePopReceiptHandles(
                    group, topic, pageNum, pageSize);
                DescribePopReceiptHandlesResponse response = ProxyAdminProtoConverter.toDescribePopReceiptHandlesResponse(result);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_DESCRIBE_POP_RECEIPT_HANDLES,
                    System.currentTimeMillis() - startTime);
                log.debug("describePopReceiptHandles completed in {}ms, group={}, total={}",
                    System.currentTimeMillis() - startTime, group, result.getTotal());
            } catch (Exception e) {
                log.error("describePopReceiptHandles failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_POP_RECEIPT_HANDLES,
                    System.currentTimeMillis() - startTime);
                DescribePopReceiptHandlesResponse errorResponse = ProxyAdminProtoConverter.toDescribePopReceiptHandlesError(
                    AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Query batch consumption diagnostics, aggregated per client.
     * RIP-2 M4: DescribeBatchConsumeDiagnostics RPC
     * <p>
     * Provides diagnostic information for batch consumption mode, including:
     * - Per-client unacked message counts and handle counts
     * - Clients with expired handles (messages about to be redelivered)
     * - Renewal patterns per client (ChangeInvisibleTime frequency)
     * - Topic distribution of unacked messages per client
     * - Client configuration correlation (receiveBatchSize, longPollingTimeout)
     */
    public void describeBatchConsumeDiagnostics(DescribeBatchConsumeDiagnosticsRequest request,
        StreamObserver<DescribeBatchConsumeDiagnosticsResponse> responseObserver) {
        adminExecutor.execute(() -> {
            long startTime = System.currentTimeMillis();
            try {
                String group = request.getGroup();
                if (group == null || group.isEmpty()) {
                    ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_BATCH_CONSUME_DIAGNOSTICS,
                        System.currentTimeMillis() - startTime);
                    DescribeBatchConsumeDiagnosticsResponse errorResponse =
                        ProxyAdminProtoConverter.toDescribeBatchConsumeDiagnosticsError(
                            AdminCode.ADMIN_CODE_BAD_REQUEST, "group is required");
                    responseObserver.onNext(errorResponse);
                    responseObserver.onCompleted();
                    return;
                }

                String topic = request.getTopic();
                String clientId = request.getClientId();
                int pageNum = request.getPageNum() > 0 ? request.getPageNum() : 1;
                int pageSize = enforcePageSize(request.getPageSize());

                BatchConsumeDiagnosticResult result = adminClientService.describeBatchConsumeDiagnostics(
                    group, topic, clientId, pageNum, pageSize);
                DescribeBatchConsumeDiagnosticsResponse response =
                    ProxyAdminProtoConverter.toDescribeBatchConsumeDiagnosticsResponse(result);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

                ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_DESCRIBE_BATCH_CONSUME_DIAGNOSTICS,
                    System.currentTimeMillis() - startTime);
                log.debug("describeBatchConsumeDiagnostics completed in {}ms, group={}, clientId={}, total={}",
                    System.currentTimeMillis() - startTime, group, clientId, result.getTotal());
            } catch (Exception e) {
                log.error("describeBatchConsumeDiagnostics failed", e);
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_DESCRIBE_BATCH_CONSUME_DIAGNOSTICS,
                    System.currentTimeMillis() - startTime);
                DescribeBatchConsumeDiagnosticsResponse errorResponse =
                    ProxyAdminProtoConverter.toDescribeBatchConsumeDiagnosticsError(
                        AdminCode.ADMIN_CODE_INTERNAL_ERROR, "Internal error: " + e.getMessage());
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
            }
        });
    }

    /**
     * Subscribe to route change events (Server Streaming RPC).
     * <p>
     * Dashboard subscribes to receive real-time notifications when:
     * - Brokers go online or offline (BROKER_ONLINE / BROKER_OFFLINE)
     * - Queue counts change (QUEUE_SCALE)
     * - Topics are created or deleted (TOPIC_CREATE / TOPIC_DELETE)
     * <p>
     * An initial ROUTE_SNAPSHOT event is sent for each cached topic upon subscription,
     * providing the client with the current routing state.
     * <p>
     * The stream remains open until the client disconnects or the server shuts down.
     */
    public void subscribeRouteEvents(SubscribeRouteEventsRequest request,
        StreamObserver<SubscribeRouteEventsResponse> responseObserver) {
        long startTime = System.currentTimeMillis();
        try {
            if (routeChangeNotifier == null) {
                ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_SUBSCRIBE_ROUTE_EVENTS,
                    System.currentTimeMillis() - startTime);
                SubscribeRouteEventsResponse errorResponse = SubscribeRouteEventsResponse.newBuilder()
                    .setCode(AdminCode.ADMIN_CODE_INTERNAL_ERROR)
                    .setMessage("Route change notifier is not available")
                    .build();
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
                return;
            }

            // Parse topic filter from request
            List<String> topics = new ArrayList<>(request.getTopicsList());

            // Parse event type filter from request
            List<RouteChangeEventType> eventTypes = new ArrayList<>();
            for (apache.rocketmq.proxy.admin.v1.RouteChangeEventType protoType : request.getEventTypesList()) {
                RouteChangeEventType internalType = fromProtoRouteChangeEventType(protoType);
                if (internalType != null) {
                    eventTypes.add(internalType);
                }
            }

            routeChangeNotifier.subscribe(topics, eventTypes, responseObserver);

            ProxyAdminMetricsManager.recordSuccess(ProxyAdminMetricsManager.METHOD_SUBSCRIBE_ROUTE_EVENTS,
                System.currentTimeMillis() - startTime);
            log.info("subscribeRouteEvents registered, topics filter: {}, event types filter: {}",
                topics, eventTypes);
        } catch (Exception e) {
            log.error("subscribeRouteEvents failed", e);
            ProxyAdminMetricsManager.recordError(ProxyAdminMetricsManager.METHOD_SUBSCRIBE_ROUTE_EVENTS,
                System.currentTimeMillis() - startTime);
            SubscribeRouteEventsResponse errorResponse = SubscribeRouteEventsResponse.newBuilder()
                .setCode(AdminCode.ADMIN_CODE_INTERNAL_ERROR)
                .setMessage("Internal error: " + e.getMessage())
                .build();
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    /**
     * Convert proto RouteChangeEventType to internal RouteChangeEventType.
     */
    private RouteChangeEventType fromProtoRouteChangeEventType(
        apache.rocketmq.proxy.admin.v1.RouteChangeEventType protoType) {
        if (protoType == null || protoType == apache.rocketmq.proxy.admin.v1.RouteChangeEventType.ROUTE_CHANGE_EVENT_TYPE_UNSPECIFIED) {
            return null;
        }
        switch (protoType) {
            case BROKER_ONLINE:
                return RouteChangeEventType.BROKER_ONLINE;
            case BROKER_OFFLINE:
                return RouteChangeEventType.BROKER_OFFLINE;
            case QUEUE_SCALE:
                return RouteChangeEventType.QUEUE_SCALE;
            case TOPIC_CREATE:
                return RouteChangeEventType.TOPIC_CREATE;
            case TOPIC_DELETE:
                return RouteChangeEventType.TOPIC_DELETE;
            case ROUTE_SNAPSHOT:
                return RouteChangeEventType.ROUTE_SNAPSHOT;
            default:
                return null;
        }
    }

    /**
     * Shutdown the admin executor.
     */
    public void shutdown() {
        if (routeChangeNotifier != null) {
            routeChangeNotifier.shutdown();
        }
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
        if (pageSize > MAX_PAGE_SIZE) {
            log.warn("Requested page size {} exceeds maximum {}, capping to {}", pageSize, MAX_PAGE_SIZE, MAX_PAGE_SIZE);
        }
        return Math.min(pageSize, MAX_PAGE_SIZE);
    }
}