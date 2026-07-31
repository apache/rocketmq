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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEvent;
import org.apache.rocketmq.proxy.grpc.admin.model.RouteChangeEventType;
import org.apache.rocketmq.proxy.grpc.admin.model.TopicRouteSnapshot;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.BatchConsumeDiagnosticResult;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.PopReceiptHandleDiagnosticResult;
import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.AuthStatus;
import apache.rocketmq.proxy.admin.v1.BatchConsumeClientDiagnostics;
import apache.rocketmq.proxy.admin.v1.BatchConsumeGroupSummary;
import apache.rocketmq.proxy.admin.v1.BrokerInfo;
import apache.rocketmq.proxy.admin.v1.ClientDetail;
import apache.rocketmq.proxy.admin.v1.ClientInstance;
import apache.rocketmq.proxy.admin.v1.ClientLanguage;
import apache.rocketmq.proxy.admin.v1.ClientProtocol;
import apache.rocketmq.proxy.admin.v1.ClientRole;
import apache.rocketmq.proxy.admin.v1.ClientSettings;
import apache.rocketmq.proxy.admin.v1.ConsumeProgress;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesResponse;
import apache.rocketmq.proxy.admin.v1.HeartbeatRecord;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;
import apache.rocketmq.proxy.admin.v1.GetConfigResponse;
import apache.rocketmq.proxy.admin.v1.UpdateConfigResponse;
import apache.rocketmq.proxy.admin.v1.DisconnectClientResponse;
import apache.rocketmq.proxy.admin.v1.NetworkInfo;
import apache.rocketmq.proxy.admin.v1.Pagination;
import apache.rocketmq.proxy.admin.v1.PopReceiptHandleGroupSummary;
import apache.rocketmq.proxy.admin.v1.PopReceiptHandleInfo;
import apache.rocketmq.proxy.admin.v1.ProxyRuntimeConfig;
import apache.rocketmq.proxy.admin.v1.QueueInfo;
import apache.rocketmq.proxy.admin.v1.SubscribeRouteEventsResponse;
import apache.rocketmq.proxy.admin.v1.TopicConsumeProgress;
import org.apache.rocketmq.proxy.config.ProxyConfig;

/**
 * Converter between internal model classes and protobuf-generated types.
 * <p>
 * Since the service layer uses internal model classes (ClientInstanceInfo, ClientDetailInfo, etc.)
 * and the gRPC layer uses protobuf-generated types, this converter bridges the two representations.
 */
public class ProxyAdminProtoConverter {

    private ProxyAdminProtoConverter() {
        // Utility class
    }

    // ==================== Request Converters ====================

    /**
     * Convert proto ListClientsRequest to internal ListClientsFilter.
     */
    public static ListClientsFilter toFilter(ListClientsRequest request) {
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup(request.getGroup());
        filter.setTopic(request.getTopic());
        filter.setClientIdPrefix(request.getClientIdPrefix());
        if (request.getLanguage() != null && request.getLanguage() != ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED) {
            filter.setLanguage(fromClientLanguage(request.getLanguage()));
        }
        if (request.getConnectTimeStart() > 0) {
            filter.setConnectTimeStart(request.getConnectTimeStart());
        }
        if (request.getConnectTimeEnd() > 0) {
            filter.setConnectTimeEnd(request.getConnectTimeEnd());
        }
        return filter;
    }

    // ==================== Response Converters ====================

    /**
     * Build a ListClientsResponse from service result.
     */
    public static ListClientsResponse toListClientsResponse(ListClientsResult result) {
        ListClientsResponse.Builder builder = ListClientsResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setPagination(Pagination.newBuilder()
                .setTotal(result.getTotal())
                .setPageNum(result.getPageNum())
                .setPageSize(result.getPageSize())
                .build());

        for (ClientInstanceInfo info : result.getList()) {
            builder.addList(toClientInstance(info));
        }
        return builder.build();
    }

    /**
     * Build a ListClientsResponse for error cases.
     */
    public static ListClientsResponse toListClientsError(AdminCode code, String message) {
        return ListClientsResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build a DescribeClientResponse from ClientDetailInfo.
     */
    public static DescribeClientResponse toDescribeClientResponse(ClientDetailInfo detail) {
        return DescribeClientResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setClientDetail(toClientDetail(detail))
            .build();
    }

    /**
     * Build a DescribeClientResponse for error cases.
     */
    public static DescribeClientResponse toDescribeClientError(AdminCode code, String message) {
        return DescribeClientResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build a ListClientsByGroupResponse from service result.
     */
    public static ListClientsByGroupResponse toListClientsByGroupResponse(ListClientsResult result) {
        ListClientsByGroupResponse.Builder builder = ListClientsByGroupResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setPagination(Pagination.newBuilder()
                .setTotal(result.getTotal())
                .setPageNum(result.getPageNum())
                .setPageSize(result.getPageSize())
                .build());

        for (ClientInstanceInfo info : result.getList()) {
            builder.addList(toClientInstance(info));
        }
        return builder.build();
    }

    /**
     * Build a ListClientsByGroupResponse for error cases.
     */
    public static ListClientsByGroupResponse toListClientsByGroupError(AdminCode code, String message) {
        return ListClientsByGroupResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build a ListClientsByTopicResponse from service result.
     */
    public static ListClientsByTopicResponse toListClientsByTopicResponse(ListClientsResult result) {
        ListClientsByTopicResponse.Builder builder = ListClientsByTopicResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setPagination(Pagination.newBuilder()
                .setTotal(result.getTotal())
                .setPageNum(result.getPageNum())
                .setPageSize(result.getPageSize())
                .build());

        for (ClientInstanceInfo info : result.getList()) {
            builder.addList(toClientInstance(info));
        }
        return builder.build();
    }

    /**
     * Build a ListClientsByTopicResponse for error cases.
     */
    public static ListClientsByTopicResponse toListClientsByTopicError(AdminCode code, String message) {
        return ListClientsByTopicResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build a GetConfigResponse from ProxyConfig.
     */
    public static GetConfigResponse toGetConfigResponse(ProxyConfig config) {
        return GetConfigResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setConfig(toProxyRuntimeConfig(config))
            .build();
    }

    /**
     * Build a GetConfigResponse for error cases.
     */
    public static GetConfigResponse toGetConfigError(AdminCode code, String message) {
        return GetConfigResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build an UpdateConfigResponse from updated ProxyConfig and list of changed field names.
     */
    public static UpdateConfigResponse toUpdateConfigResponse(ProxyConfig config, List<String> changedFields) {
        return UpdateConfigResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setConfig(toProxyRuntimeConfig(config))
            .addAllChangedFields(changedFields)
            .build();
    }

    /**
     * Build an UpdateConfigResponse for error cases.
     */
    public static UpdateConfigResponse toUpdateConfigError(AdminCode code, String message) {
        return UpdateConfigResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Build a DisconnectClientResponse for successful disconnection.
     */
    public static DisconnectClientResponse toDisconnectClientResponse(boolean disconnected) {
        return DisconnectClientResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setDisconnected(disconnected)
            .build();
    }

    /**
     * Build a DisconnectClientResponse for error cases.
     */
    public static DisconnectClientResponse toDisconnectClientError(AdminCode code, String message) {
        return DisconnectClientResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    // ==================== POP Diagnostics Converters ====================

    /**
     * Build a DescribePopReceiptHandlesResponse from diagnostic result.
     */
    public static DescribePopReceiptHandlesResponse toDescribePopReceiptHandlesResponse(
        PopReceiptHandleDiagnosticResult result) {
        DescribePopReceiptHandlesResponse.Builder builder = DescribePopReceiptHandlesResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setSummary(toPopReceiptHandleGroupSummary(result.getSummary()))
            .setPagination(Pagination.newBuilder()
                .setTotal(result.getTotal())
                .setPageNum(result.getPageNum())
                .setPageSize(result.getPageSize())
                .build());

        for (org.apache.rocketmq.proxy.common.PopReceiptHandleInfo info : result.getHandles()) {
            builder.addHandles(toPopReceiptHandleInfo(info));
        }
        return builder.build();
    }

    /**
     * Build a DescribePopReceiptHandlesResponse for error cases.
     */
    public static DescribePopReceiptHandlesResponse toDescribePopReceiptHandlesError(
        AdminCode code, String message) {
        return DescribePopReceiptHandlesResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Convert internal PopReceiptHandleGroupSummary to proto.
     */
    public static PopReceiptHandleGroupSummary toPopReceiptHandleGroupSummary(
        org.apache.rocketmq.proxy.common.PopReceiptHandleGroupSummary summary) {
        return PopReceiptHandleGroupSummary.newBuilder()
            .setGroup(summary.getGroup() != null ? summary.getGroup() : "")
            .setTotalHandles(summary.getTotalHandles())
            .setTotalMessages(summary.getTotalMessages())
            .setTotalRenewTimes(summary.getTotalRenewTimes())
            .setTotalRenewRetryTimes(summary.getTotalRenewRetryTimes())
            .setExpiredHandles(summary.getExpiredHandles())
            .build();
    }

    /**
     * Convert internal PopReceiptHandleInfo to proto.
     */
    public static PopReceiptHandleInfo toPopReceiptHandleInfo(
        org.apache.rocketmq.proxy.common.PopReceiptHandleInfo info) {
        return PopReceiptHandleInfo.newBuilder()
            .setGroup(info.getGroup() != null ? info.getGroup() : "")
            .setTopic(info.getTopic() != null ? info.getTopic() : "")
            .setQueueId(info.getQueueId())
            .setMessageId(info.getMessageId() != null ? info.getMessageId() : "")
            .setQueueOffset(info.getQueueOffset())
            .setReconsumeTimes(info.getReconsumeTimes())
            .setRenewTimes(info.getRenewTimes())
            .setRenewRetryTimes(info.getRenewRetryTimes())
            .setConsumeTimestamp(info.getConsumeTimestamp())
            .setReceiptHandle(info.getReceiptHandle() != null ? info.getReceiptHandle() : "")
            .setNextVisibleTime(info.getNextVisibleTime())
            .setInvisibleTime(info.getInvisibleTime())
            .setBrokerName(info.getBrokerName() != null ? info.getBrokerName() : "")
            .setIsExpired(info.isExpired())
            .build();
    }

    // ==================== Batch Consume Diagnostics Converters ====================

    /**
     * Build a DescribeBatchConsumeDiagnosticsResponse from diagnostic result.
     */
    public static DescribeBatchConsumeDiagnosticsResponse toDescribeBatchConsumeDiagnosticsResponse(
        BatchConsumeDiagnosticResult result) {
        DescribeBatchConsumeDiagnosticsResponse.Builder builder = DescribeBatchConsumeDiagnosticsResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name())
            .setSummary(toBatchConsumeGroupSummary(result.getSummary()))
            .setPagination(Pagination.newBuilder()
                .setTotal(result.getTotal())
                .setPageNum(result.getPageNum())
                .setPageSize(result.getPageSize())
                .build());

        for (org.apache.rocketmq.proxy.common.BatchConsumeClientDiagnostics diag : result.getDiagnostics()) {
            builder.addDiagnostics(toBatchConsumeClientDiagnostics(diag));
        }
        return builder.build();
    }

    /**
     * Build a DescribeBatchConsumeDiagnosticsResponse for error cases.
     */
    public static DescribeBatchConsumeDiagnosticsResponse toDescribeBatchConsumeDiagnosticsError(
        AdminCode code, String message) {
        return DescribeBatchConsumeDiagnosticsResponse.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    /**
     * Convert internal BatchConsumeGroupSummary to proto.
     */
    public static BatchConsumeGroupSummary toBatchConsumeGroupSummary(
        org.apache.rocketmq.proxy.common.BatchConsumeGroupSummary summary) {
        return BatchConsumeGroupSummary.newBuilder()
            .setGroup(summary.getGroup() != null ? summary.getGroup() : "")
            .setTotalClients(summary.getTotalClients())
            .setTotalUnackedMessages(summary.getTotalUnackedMessages())
            .setTotalUnackedHandles(summary.getTotalUnackedHandles())
            .setTotalExpiredHandles(summary.getTotalExpiredHandles())
            .setTotalRenewTimes(summary.getTotalRenewTimes())
            .setTotalRenewRetryTimes(summary.getTotalRenewRetryTimes())
            .build();
    }

    /**
     * Convert internal BatchConsumeClientDiagnostics to proto.
     */
    public static BatchConsumeClientDiagnostics toBatchConsumeClientDiagnostics(
        org.apache.rocketmq.proxy.common.BatchConsumeClientDiagnostics diag) {
        BatchConsumeClientDiagnostics.Builder builder = BatchConsumeClientDiagnostics.newBuilder()
            .setClientId(diag.getClientId() != null ? diag.getClientId() : "")
            .setChannelId(diag.getChannelId() != null ? diag.getChannelId() : "")
            .setUnackedMessageCount(diag.getUnackedMessageCount())
            .setUnackedHandleCount(diag.getUnackedHandleCount())
            .setTotalRenewTimes(diag.getTotalRenewTimes())
            .setTotalRenewRetryTimes(diag.getTotalRenewRetryTimes())
            .setExpiredHandleCount(diag.getExpiredHandleCount())
            .setConsumeType(diag.getConsumeType() != null ? diag.getConsumeType() : "")
            .setMessageModel(diag.getMessageModel() != null ? diag.getMessageModel() : "")
            .setReceiveBatchSize(diag.getReceiveBatchSize())
            .setLongPollingTimeoutMs(diag.getLongPollingTimeoutMs())
            .setLastRttMs(diag.getLastRttMs())
            .setConnectTime(diag.getConnectTime());

        if (diag.getTopicDistribution() != null) {
            builder.putAllTopicDistribution(diag.getTopicDistribution());
        }
        return builder.build();
    }

    /**
     * Compare requested config with current config and apply changes for fields that differ.
     * Returns the list of field names (snake_case) that were actually changed.
     * <p>
     * Due to proto3 default value semantics, fields set to their default value (0, "", false, 0.0)
     * in the request will be treated as "not set" and will NOT be updated. This means the API
     * cannot be used to explicitly set a field to its default value; use a config file reset for that.
     * <p>
     * Configuration changes take effect immediately for operations that read config at invocation time
     * (e.g., message size limits, timeout values, rate limiting thresholds). Some settings such as
     * network ports and cluster names cannot be hot-updated and require a restart.
     */
    public static List<String> applyConfigChanges(ProxyConfig config, ProxyRuntimeConfig req) {
        List<String> changed = new ArrayList<>();

        // === gRPC data plane ===
        ifInt(changed, "grpc_server_port", req.getGrpcServerPort(), config.getGrpcServerPort(), v -> config.setGrpcServerPort(v));
        ifInt(changed, "grpc_boss_loop_num", req.getGrpcBossLoopNum(), config.getGrpcBossLoopNum(), v -> config.setGrpcBossLoopNum(v));
        ifInt(changed, "grpc_worker_loop_num", req.getGrpcWorkerLoopNum(), config.getGrpcWorkerLoopNum(), v -> config.setGrpcWorkerLoopNum(v));
        ifInt(changed, "grpc_thread_pool_nums", req.getGrpcThreadPoolNums(), config.getGrpcThreadPoolNums(), v -> config.setGrpcThreadPoolNums(v));
        ifInt(changed, "grpc_thread_pool_queue_capacity", req.getGrpcThreadPoolQueueCapacity(), config.getGrpcThreadPoolQueueCapacity(), v -> config.setGrpcThreadPoolQueueCapacity(v));
        ifInt(changed, "grpc_max_inbound_message_size", req.getGrpcMaxInboundMessageSize(), config.getGrpcMaxInboundMessageSize(), v -> config.setGrpcMaxInboundMessageSize(v));
        ifInt(changed, "grpc_max_concurrent_calls_per_connection", req.getGrpcMaxConcurrentCallsPerConnection(), config.getGrpcMaxConcurrentCallsPerConnection(), v -> config.setGrpcMaxConcurrentCallsPerConnection(v));

        // === Admin interface ===
        ifBool(changed, "proxy_admin_enabled", req.getProxyAdminEnabled(), config.isProxyAdminEnabled(), v -> config.setProxyAdminEnabled(v));
        ifInt(changed, "proxy_admin_server_port", req.getProxyAdminServerPort(), config.getProxyAdminServerPort(), v -> config.setProxyAdminServerPort(v));
        ifInt(changed, "proxy_admin_thread_pool_nums", req.getProxyAdminThreadPoolNums(), config.getProxyAdminThreadPoolNums(), v -> config.setProxyAdminThreadPoolNums(v));
        ifInt(changed, "proxy_admin_max_page_size", req.getProxyAdminMaxPageSize(), config.getProxyAdminMaxPageSize(), v -> config.setProxyAdminMaxPageSize(v));
        ifInt(changed, "proxy_admin_describe_client_concurrency_limit", req.getProxyAdminDescribeClientConcurrencyLimit(), config.getProxyAdminDescribeClientConcurrencyLimit(), v -> config.setProxyAdminDescribeClientConcurrencyLimit(v));
        ifDouble(changed, "proxy_admin_sampling_rate_under_load", req.getProxyAdminSamplingRateUnderLoad(), config.getProxyAdminSamplingRateUnderLoad(), v -> config.setProxyAdminSamplingRateUnderLoad(v));
        ifInt(changed, "proxy_admin_heartbeat_history_size", req.getProxyAdminHeartbeatHistorySize(), config.getProxyAdminHeartbeatHistorySize(), v -> config.setProxyAdminHeartbeatHistorySize(v));
        ifLong(changed, "proxy_admin_sampling_threshold", req.getProxyAdminSamplingThreshold(), config.getProxyAdminSamplingThreshold(), v -> config.setProxyAdminSamplingThreshold(v));

        // === Thread pools - gRPC activities ===
        ifInt(changed, "grpc_producer_thread_pool_nums", req.getGrpcProducerThreadPoolNums(), config.getGrpcProducerThreadPoolNums(), v -> config.setGrpcProducerThreadPoolNums(v));
        ifInt(changed, "grpc_producer_thread_queue_capacity", req.getGrpcProducerThreadQueueCapacity(), config.getGrpcProducerThreadQueueCapacity(), v -> config.setGrpcProducerThreadQueueCapacity(v));
        ifInt(changed, "grpc_consumer_thread_pool_nums", req.getGrpcConsumerThreadPoolNums(), config.getGrpcConsumerThreadPoolNums(), v -> config.setGrpcConsumerThreadPoolNums(v));
        ifInt(changed, "grpc_consumer_thread_queue_capacity", req.getGrpcConsumerThreadQueueCapacity(), config.getGrpcConsumerThreadQueueCapacity(), v -> config.setGrpcConsumerThreadQueueCapacity(v));
        ifInt(changed, "grpc_route_thread_pool_nums", req.getGrpcRouteThreadPoolNums(), config.getGrpcRouteThreadPoolNums(), v -> config.setGrpcRouteThreadPoolNums(v));
        ifInt(changed, "grpc_route_thread_queue_capacity", req.getGrpcRouteThreadQueueCapacity(), config.getGrpcRouteThreadQueueCapacity(), v -> config.setGrpcRouteThreadQueueCapacity(v));
        ifInt(changed, "grpc_client_manager_thread_pool_nums", req.getGrpcClientManagerThreadPoolNums(), config.getGrpcClientManagerThreadPoolNums(), v -> config.setGrpcClientManagerThreadPoolNums(v));
        ifInt(changed, "grpc_client_manager_thread_queue_capacity", req.getGrpcClientManagerThreadQueueCapacity(), config.getGrpcClientManagerThreadQueueCapacity(), v -> config.setGrpcClientManagerThreadQueueCapacity(v));
        ifInt(changed, "grpc_transaction_thread_pool_nums", req.getGrpcTransactionThreadPoolNums(), config.getGrpcTransactionThreadPoolNums(), v -> config.setGrpcTransactionThreadPoolNums(v));
        ifInt(changed, "grpc_transaction_thread_queue_capacity", req.getGrpcTransactionThreadQueueCapacity(), config.getGrpcTransactionThreadQueueCapacity(), v -> config.setGrpcTransactionThreadQueueCapacity(v));

        // === Thread pools - remoting ===
        ifInt(changed, "remoting_heartbeat_thread_pool_nums", req.getRemotingHeartbeatThreadPoolNums(), config.getRemotingHeartbeatThreadPoolNums(), v -> config.setRemotingHeartbeatThreadPoolNums(v));
        ifInt(changed, "remoting_send_message_thread_pool_nums", req.getRemotingSendMessageThreadPoolNums(), config.getRemotingSendMessageThreadPoolNums(), v -> config.setRemotingSendMessageThreadPoolNums(v));
        ifInt(changed, "remoting_pull_message_thread_pool_nums", req.getRemotingPullMessageThreadPoolNums(), config.getRemotingPullMessageThreadPoolNums(), v -> config.setRemotingPullMessageThreadPoolNums(v));

        // === Message limits ===
        ifInt(changed, "max_message_size", req.getMaxMessageSize(), config.getMaxMessageSize(), v -> config.setMaxMessageSize(v));
        ifInt(changed, "max_user_property_size", req.getMaxUserPropertySize(), config.getMaxUserPropertySize(), v -> config.setMaxUserPropertySize(v));
        ifInt(changed, "user_property_max_num", req.getUserPropertyMaxNum(), config.getUserPropertyMaxNum(), v -> config.setUserPropertyMaxNum(v));
        ifInt(changed, "max_message_group_size", req.getMaxMessageGroupSize(), config.getMaxMessageGroupSize(), v -> config.setMaxMessageGroupSize(v));

        // === Timeout and polling ===
        ifLong(changed, "default_invisible_time_mills", req.getDefaultInvisibleTimeMills(), config.getDefaultInvisibleTimeMills(), v -> config.setDefaultInvisibleTimeMills(v));
        ifLong(changed, "max_invisible_time_mills", req.getMaxInvisibleTimeMills(), config.getMaxInvisibleTimeMills(), v -> config.setMaxInvisibleTimeMills(v));
        ifLong(changed, "min_invisible_time_mills_for_recv", req.getMinInvisibleTimeMillsForRecv(), config.getMinInvisibleTimeMillsForRecv(), v -> config.setMinInvisibleTimeMillsForRecv(v));
        ifLong(changed, "max_delay_time_mills", req.getMaxDelayTimeMills(), config.getMaxDelayTimeMills(), v -> config.setMaxDelayTimeMills(v));
        ifLong(changed, "grpc_client_consumer_min_long_polling_timeout_millis", req.getGrpcClientConsumerMinLongPollingTimeoutMillis(), config.getGrpcClientConsumerMinLongPollingTimeoutMillis(), v -> config.setGrpcClientConsumerMinLongPollingTimeoutMillis(v));
        ifLong(changed, "grpc_client_consumer_max_long_polling_timeout_millis", req.getGrpcClientConsumerMaxLongPollingTimeoutMillis(), config.getGrpcClientConsumerMaxLongPollingTimeoutMillis(), v -> config.setGrpcClientConsumerMaxLongPollingTimeoutMillis(v));
        ifInt(changed, "grpc_client_consumer_long_polling_batch_size", req.getGrpcClientConsumerLongPollingBatchSize(), config.getGrpcClientConsumerLongPollingBatchSize(), v -> config.setGrpcClientConsumerLongPollingBatchSize(v));
        ifInt(changed, "grpc_client_producer_max_attempts", req.getGrpcClientProducerMaxAttempts(), config.getGrpcClientProducerMaxAttempts(), v -> config.setGrpcClientProducerMaxAttempts(v));
        ifLong(changed, "grpc_client_producer_backoff_initial_millis", req.getGrpcClientProducerBackoffInitialMillis(), config.getGrpcClientProducerBackoffInitialMillis(), v -> config.setGrpcClientProducerBackoffInitialMillis(v));
        ifLong(changed, "grpc_client_producer_backoff_max_millis", req.getGrpcClientProducerBackoffMaxMillis(), config.getGrpcClientProducerBackoffMaxMillis(), v -> config.setGrpcClientProducerBackoffMaxMillis(v));
        ifInt(changed, "grpc_client_producer_backoff_multiplier", req.getGrpcClientProducerBackoffMultiplier(), config.getGrpcClientProducerBackoffMultiplier(), v -> config.setGrpcClientProducerBackoffMultiplier(v));

        // === Cache ===
        ifInt(changed, "topic_route_service_cache_expired_seconds", req.getTopicRouteServiceCacheExpiredSeconds(), config.getTopicRouteServiceCacheExpiredSeconds(), v -> config.setTopicRouteServiceCacheExpiredSeconds(v));
        ifInt(changed, "topic_route_service_cache_refresh_seconds", req.getTopicRouteServiceCacheRefreshSeconds(), config.getTopicRouteServiceCacheRefreshSeconds(), v -> config.setTopicRouteServiceCacheRefreshSeconds(v));
        ifInt(changed, "topic_route_service_cache_max_num", req.getTopicRouteServiceCacheMaxNum(), config.getTopicRouteServiceCacheMaxNum(), v -> config.setTopicRouteServiceCacheMaxNum(v));

        // === Metrics ===
        ifInt(changed, "metrics_prom_exporter_port", req.getMetricsPromExporterPort(), config.getMetricsPromExporterPort(), v -> config.setMetricsPromExporterPort(v));
        ifBool(changed, "trace_on", req.getTraceOn(), config.isTraceOn(), v -> config.setTraceOn(v));

        return changed;
    }

    // ==================== Config Update Helpers ====================

    private interface IntSetter { void set(int value); }
    private interface LongSetter { void set(long value); }
    private interface BoolSetter { void set(boolean value); }
    private interface DoubleSetter { void set(double value); }

    private static void ifInt(List<String> changed, String name, int reqVal, int curVal, IntSetter setter) {
        if (reqVal != 0 && reqVal != curVal) {
            setter.set(reqVal);
            changed.add(name);
        }
    }

    private static void ifLong(List<String> changed, String name, long reqVal, long curVal, LongSetter setter) {
        if (reqVal != 0 && reqVal != curVal) {
            setter.set(reqVal);
            changed.add(name);
        }
    }

    private static void ifBool(List<String> changed, String name, boolean reqVal, boolean curVal, BoolSetter setter) {
        // For booleans, we always compare (both true and false are meaningful)
        // but only update if different. Since proto3 default for bool is false,
        // we can't distinguish "not set" from "set to false". To avoid accidentally
        // resetting booleans to false, we only update when reqVal is true and differs.
        if (reqVal && reqVal != curVal) {
            setter.set(reqVal);
            changed.add(name);
        }
    }

    private static void ifDouble(List<String> changed, String name, double reqVal, double curVal, DoubleSetter setter) {
        if (reqVal != 0.0 && Double.compare(reqVal, curVal) != 0) {
            setter.set(reqVal);
            changed.add(name);
        }
    }

    /**
     * Convert ProxyConfig to ProxyRuntimeConfig proto message.
     * Maps the most important runtime configuration fields that Dashboard
     * needs to display for monitoring and diagnostics.
     */
    public static ProxyRuntimeConfig toProxyRuntimeConfig(ProxyConfig config) {
        return ProxyRuntimeConfig.newBuilder()
            // Basic info
            .setProxyMode(nullSafe(config.getProxyMode()))
            .setRocketmqClusterName(nullSafe(config.getRocketMQClusterName()))
            .setProxyClusterName(nullSafe(config.getProxyClusterName()))
            .setProxyName(nullSafe(config.getProxyName()))
            .setLocalServeAddr(nullSafe(config.getLocalServeAddr()))
            .setNamesrvAddr(nullSafe(config.getNamesrvAddr()))
            // gRPC data plane
            .setGrpcServerPort(config.getGrpcServerPort())
            .setGrpcBossLoopNum(config.getGrpcBossLoopNum())
            .setGrpcWorkerLoopNum(config.getGrpcWorkerLoopNum())
            .setGrpcThreadPoolNums(config.getGrpcThreadPoolNums())
            .setGrpcThreadPoolQueueCapacity(config.getGrpcThreadPoolQueueCapacity())
            .setGrpcMaxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
            .setGrpcMaxConcurrentCallsPerConnection(config.getGrpcMaxConcurrentCallsPerConnection())
            // Admin interface
            .setProxyAdminEnabled(config.isProxyAdminEnabled())
            .setProxyAdminServerPort(config.getProxyAdminServerPort())
            .setProxyAdminThreadPoolNums(config.getProxyAdminThreadPoolNums())
            .setProxyAdminMaxPageSize(config.getProxyAdminMaxPageSize())
            .setProxyAdminDescribeClientConcurrencyLimit(config.getProxyAdminDescribeClientConcurrencyLimit())
            .setProxyAdminSamplingRateUnderLoad(config.getProxyAdminSamplingRateUnderLoad())
            .setProxyAdminHeartbeatHistorySize(config.getProxyAdminHeartbeatHistorySize())
            .setProxyAdminSamplingThreshold(config.getProxyAdminSamplingThreshold())
            // Thread pools - gRPC activities
            .setGrpcProducerThreadPoolNums(config.getGrpcProducerThreadPoolNums())
            .setGrpcProducerThreadQueueCapacity(config.getGrpcProducerThreadQueueCapacity())
            .setGrpcConsumerThreadPoolNums(config.getGrpcConsumerThreadPoolNums())
            .setGrpcConsumerThreadQueueCapacity(config.getGrpcConsumerThreadQueueCapacity())
            .setGrpcRouteThreadPoolNums(config.getGrpcRouteThreadPoolNums())
            .setGrpcRouteThreadQueueCapacity(config.getGrpcRouteThreadQueueCapacity())
            .setGrpcClientManagerThreadPoolNums(config.getGrpcClientManagerThreadPoolNums())
            .setGrpcClientManagerThreadQueueCapacity(config.getGrpcClientManagerThreadQueueCapacity())
            .setGrpcTransactionThreadPoolNums(config.getGrpcTransactionThreadPoolNums())
            .setGrpcTransactionThreadQueueCapacity(config.getGrpcTransactionThreadQueueCapacity())
            // Thread pools - remoting
            .setRemotingHeartbeatThreadPoolNums(config.getRemotingHeartbeatThreadPoolNums())
            .setRemotingSendMessageThreadPoolNums(config.getRemotingSendMessageThreadPoolNums())
            .setRemotingPullMessageThreadPoolNums(config.getRemotingPullMessageThreadPoolNums())
            // Message limits
            .setMaxMessageSize(config.getMaxMessageSize())
            .setMaxUserPropertySize(config.getMaxUserPropertySize())
            .setUserPropertyMaxNum(config.getUserPropertyMaxNum())
            .setMaxMessageGroupSize(config.getMaxMessageGroupSize())
            // Timeout and polling
            .setDefaultInvisibleTimeMills(config.getDefaultInvisibleTimeMills())
            .setMaxInvisibleTimeMills(config.getMaxInvisibleTimeMills())
            .setMinInvisibleTimeMillsForRecv(config.getMinInvisibleTimeMillsForRecv())
            .setMaxDelayTimeMills(config.getMaxDelayTimeMills())
            .setGrpcClientConsumerMinLongPollingTimeoutMillis(config.getGrpcClientConsumerMinLongPollingTimeoutMillis())
            .setGrpcClientConsumerMaxLongPollingTimeoutMillis(config.getGrpcClientConsumerMaxLongPollingTimeoutMillis())
            .setGrpcClientConsumerLongPollingBatchSize(config.getGrpcClientConsumerLongPollingBatchSize())
            .setGrpcClientProducerMaxAttempts(config.getGrpcClientProducerMaxAttempts())
            .setGrpcClientProducerBackoffInitialMillis(config.getGrpcClientProducerBackoffInitialMillis())
            .setGrpcClientProducerBackoffMaxMillis(config.getGrpcClientProducerBackoffMaxMillis())
            .setGrpcClientProducerBackoffMultiplier(config.getGrpcClientProducerBackoffMultiplier())
            // Cache
            .setTopicRouteServiceCacheExpiredSeconds(config.getTopicRouteServiceCacheExpiredSeconds())
            .setTopicRouteServiceCacheRefreshSeconds(config.getTopicRouteServiceCacheRefreshSeconds())
            .setTopicRouteServiceCacheMaxNum(config.getTopicRouteServiceCacheMaxNum())
            // TLS
            .setTlsTestModeEnable(config.isTlsTestModeEnable())
            .setTlsKeyPath(nullSafe(config.getTlsKeyPath()))
            .setTlsCertPath(nullSafe(config.getTlsCertPath()))
            // Metrics
            .setMetricsExporterType(config.getMetricsExporterType() != null ? config.getMetricsExporterType().name() : "")
            .setMetricsPromExporterPort(config.getMetricsPromExporterPort())
            .setTraceOn(config.isTraceOn())
            .build();
    }

    /**
     * Null-safe string helper for proto string fields.
     */
    private static String nullSafe(String value) {
        return value != null ? value : "";
    }

    // ==================== Model Converters ====================

    /**
     * Convert ClientInstanceInfo model to ClientInstance proto.
     */
    public static ClientInstance toClientInstance(ClientInstanceInfo info) {
        ClientInstance.Builder builder = ClientInstance.newBuilder()
            .setClientId(info.getClientId() != null ? info.getClientId() : "")
            .setClientVersion(info.getClientVersion() != null ? info.getClientVersion() : "")
            .setAccessPoint(info.getAccessPoint() != null ? info.getAccessPoint() : "")
            .setConnectAt(info.getConnectAt())
            .setLastActiveAt(info.getLastActiveAt());

        if (info.getLanguage() != null) {
            builder.setLanguage(toClientLanguage(info.getLanguage()));
        }
        if (info.getProtocol() != null) {
            builder.setProtocol(toClientProtocol(info.getProtocol()));
        }
        if (info.getRole() != null) {
            builder.setRole(toClientRole(info.getRole()));
        }
        if (info.getGroup() != null) {
            builder.setGroup(info.getGroup());
        }
        if (info.getTopics() != null) {
            builder.addAllTopics(info.getTopics());
        }
        return builder.build();
    }

    /**
     * Convert ClientDetailInfo model to ClientDetail proto.
     */
    public static ClientDetail toClientDetail(ClientDetailInfo info) {
        ClientDetail.Builder builder = ClientDetail.newBuilder();

        if (info.getClientInstance() != null) {
            builder.setClientInstance(toClientInstance(info.getClientInstance()));
        }
        if (info.getSettings() != null) {
            builder.setSettings(toClientSettings(info.getSettings()));
        }
        if (info.getHeartbeatHistory() != null) {
            for (ClientDetailInfo.HeartbeatRecordInfo record : info.getHeartbeatHistory()) {
                builder.addHeartbeatHistory(toHeartbeatRecord(record));
            }
        }
        if (info.getAuthStatus() != null) {
            builder.setAuthStatus(toAuthStatus(info.getAuthStatus()));
        }
        if (info.getConsumeProgress() != null) {
            builder.setConsumeProgress(toConsumeProgress(info.getConsumeProgress()));
        }
        if (info.getNetworkInfo() != null) {
            builder.setNetworkInfo(toNetworkInfo(info.getNetworkInfo()));
        }
        return builder.build();
    }

    /**
     * Convert ClientSettingsInfo to ClientSettings proto.
     */
    public static ClientSettings toClientSettings(ClientDetailInfo.ClientSettingsInfo info) {
        ClientSettings.Builder builder = ClientSettings.newBuilder()
            .setReceiveBatchSize(info.getReceiveBatchSize())
            .setLongPollingTimeoutMs(info.getLongPollingTimeoutMs())
            .setFifo(info.isFifo());

        if (info.getSubscriptionMode() != null) {
            builder.setSubscriptionMode(info.getSubscriptionMode());
        }
        if (info.getSubscriptionTopics() != null) {
            builder.addAllSubscriptionTopics(info.getSubscriptionTopics());
        }
        if (info.getPublishingTopics() != null) {
            builder.addAllPublishingTopics(info.getPublishingTopics());
        }
        return builder.build();
    }

    /**
     * Convert HeartbeatRecordInfo to HeartbeatRecord proto.
     */
    public static HeartbeatRecord toHeartbeatRecord(ClientDetailInfo.HeartbeatRecordInfo info) {
        return HeartbeatRecord.newBuilder()
            .setTimestamp(info.getTimestamp())
            .setSuccess(info.isSuccess())
            .setRemark(info.getRemark() != null ? info.getRemark() : "")
            .build();
    }

    /**
     * Convert AuthStatusInfo to AuthStatus proto.
     */
    public static AuthStatus toAuthStatus(ClientDetailInfo.AuthStatusInfo info) {
        return AuthStatus.newBuilder()
            .setAuthenticated(info.isAuthenticated())
            .setUsername(info.getUsername() != null ? info.getUsername() : "")
            .setLastAuthTime(info.getLastAuthTime())
            .setFailureReason(info.getFailureReason() != null ? info.getFailureReason() : "")
            .build();
    }

    /**
     * Convert ConsumeProgressInfo to ConsumeProgress proto.
     */
    public static ConsumeProgress toConsumeProgress(ClientDetailInfo.ConsumeProgressInfo info) {
        ConsumeProgress.Builder builder = ConsumeProgress.newBuilder()
            .setLag(info.getLag())
            .setLatencyMs(info.getLatencyMs());

        if (info.getTopicProgress() != null) {
            for (ClientDetailInfo.TopicConsumeProgressInfo topicProgress : info.getTopicProgress()) {
                builder.addTopicProgress(toTopicConsumeProgress(topicProgress));
            }
        }
        return builder.build();
    }

    /**
     * Convert TopicConsumeProgressInfo to TopicConsumeProgress proto.
     */
    public static TopicConsumeProgress toTopicConsumeProgress(ClientDetailInfo.TopicConsumeProgressInfo info) {
        return TopicConsumeProgress.newBuilder()
            .setTopic(info.getTopic() != null ? info.getTopic() : "")
            .setLag(info.getLag())
            .setLatencyMs(info.getLatencyMs())
            .build();
    }

    /**
     * Convert NetworkInfoInfo to NetworkInfo proto.
     */
    public static NetworkInfo toNetworkInfo(ClientDetailInfo.NetworkInfoInfo info) {
        return NetworkInfo.newBuilder()
            .setLocalAddress(info.getLocalAddress() != null ? info.getLocalAddress() : "")
            .setRemoteAddress(info.getRemoteAddress() != null ? info.getRemoteAddress() : "")
            .setRttMs(info.getRttMs())
            .setSslEnabled(info.isSslEnabled() ? "true" : "false")
            .build();
    }

    // ==================== Enum Converters ====================

    /**
     * Convert ClientLanguage proto enum to internal string format.
     * This is the reverse of toClientLanguage(), converting proto enum values
     * like CLIENT_LANGUAGE_JAVA to the internal format "JAVA" used by
     * FilterContext.matchesLanguage().
     */
    public static String fromClientLanguage(ClientLanguage language) {
        if (language == null || language == ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED) {
            return null;
        }
        switch (language) {
            case CLIENT_LANGUAGE_JAVA:
                return "JAVA";
            case CLIENT_LANGUAGE_GOLANG:
                return "GOLANG";
            case CLIENT_LANGUAGE_CPP:
                return "CPP";
            case CLIENT_LANGUAGE_RUST:
                return "RUST";
            case CLIENT_LANGUAGE_PYTHON:
                return "PYTHON";
            case CLIENT_LANGUAGE_NODEJS:
                return "NODE_JS";
            case CLIENT_LANGUAGE_CSHARP:
                return "DOTNET";
            case CLIENT_LANGUAGE_PHP:
                return "PHP";
            default:
                return language.name().replace("CLIENT_LANGUAGE_", "");
        }
    }

    /**
     * Convert string language to ClientLanguage proto enum.
     */
    public static ClientLanguage toClientLanguage(String language) {
        if (language == null) {
            return ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED;
        }
        switch (language.toUpperCase()) {
            case "JAVA":
                return ClientLanguage.CLIENT_LANGUAGE_JAVA;
            case "GOLANG":
            case "GO":
                return ClientLanguage.CLIENT_LANGUAGE_GOLANG;
            case "CPP":
                return ClientLanguage.CLIENT_LANGUAGE_CPP;
            case "RUST":
                return ClientLanguage.CLIENT_LANGUAGE_RUST;
            case "PYTHON":
                return ClientLanguage.CLIENT_LANGUAGE_PYTHON;
            case "NODEJS":
            case "NODE_JS":
                return ClientLanguage.CLIENT_LANGUAGE_NODEJS;
            case "CSHARP":
            case "DOTNET":
                return ClientLanguage.CLIENT_LANGUAGE_CSHARP;
            case "PHP":
                return ClientLanguage.CLIENT_LANGUAGE_PHP;
            default:
                return ClientLanguage.CLIENT_LANGUAGE_UNSPECIFIED;
        }
    }

    /**
     * Convert string protocol to ClientProtocol proto enum.
     */
    public static ClientProtocol toClientProtocol(String protocol) {
        if (protocol == null) {
            return ClientProtocol.CLIENT_PROTOCOL_UNSPECIFIED;
        }
        switch (protocol.toUpperCase()) {
            case "GRPC":
            case "GRPC_V2":
                return ClientProtocol.CLIENT_PROTOCOL_GRPC;
            case "REMOTING":
                return ClientProtocol.CLIENT_PROTOCOL_REMOTING;
            default:
                return ClientProtocol.CLIENT_PROTOCOL_UNSPECIFIED;
        }
    }

    /**
     * Convert string role to ClientRole proto enum.
     */
    public static ClientRole toClientRole(String role) {
        if (role == null) {
            return ClientRole.CLIENT_ROLE_UNSPECIFIED;
        }
        switch (role.toUpperCase()) {
            case "PRODUCER":
                return ClientRole.CLIENT_ROLE_PRODUCER;
            case "PUSH_CONSUMER":
                return ClientRole.CLIENT_ROLE_PUSH_CONSUMER;
            case "SIMPLE_CONSUMER":
                return ClientRole.CLIENT_ROLE_SIMPLE_CONSUMER;
            default:
                return ClientRole.CLIENT_ROLE_UNSPECIFIED;
        }
    }

    /**
     * Convert AdminCode enum to proto AdminCode.
     */
    public static AdminCode toProtoAdminCode(org.apache.rocketmq.proxy.grpc.admin.AdminCode code) {
        if (code == null) {
            return AdminCode.ADMIN_CODE_UNSPECIFIED;
        }
        switch (code) {
            case OK:
                return AdminCode.ADMIN_CODE_OK;
            case INTERNAL_ERROR:
                return AdminCode.ADMIN_CODE_INTERNAL_ERROR;
            case BAD_REQUEST:
                return AdminCode.ADMIN_CODE_BAD_REQUEST;
            case UNAUTHORIZED:
                return AdminCode.ADMIN_CODE_UNAUTHORIZED;
            case FORBIDDEN:
                return AdminCode.ADMIN_CODE_FORBIDDEN;
            case NOT_FOUND:
                return AdminCode.ADMIN_CODE_NOT_FOUND;
            case TOO_MANY_REQUESTS:
                return AdminCode.ADMIN_CODE_TOO_MANY_REQUESTS;
            default:
                return AdminCode.ADMIN_CODE_UNSPECIFIED;
        }
    }

    // ==================== Route Event Converters ====================

    /**
     * Convert internal RouteChangeEvent model to SubscribeRouteEventsResponse proto.
     */
    public static SubscribeRouteEventsResponse toSubscribeRouteEventsResponse(RouteChangeEvent event) {
        SubscribeRouteEventsResponse.Builder responseBuilder = SubscribeRouteEventsResponse.newBuilder()
            .setCode(AdminCode.ADMIN_CODE_OK)
            .setMessage(AdminCode.ADMIN_CODE_OK.name());

        apache.rocketmq.proxy.admin.v1.RouteChangeEvent.Builder eventBuilder =
            apache.rocketmq.proxy.admin.v1.RouteChangeEvent.newBuilder()
                .setEventType(toProtoRouteChangeEventType(event.getEventType()))
                .setTimestamp(event.getTimestamp())
                .setTopic(event.getTopic() != null ? event.getTopic() : "");

        if (event.getCluster() != null) {
            eventBuilder.setCluster(event.getCluster());
        }
        if (event.getBrokerName() != null) {
            eventBuilder.setBrokerName(event.getBrokerName());
        }
        if (event.getBrokerId() != 0) {
            eventBuilder.setBrokerId(event.getBrokerId());
        }
        if (event.getBrokerAddress() != null) {
            eventBuilder.setBrokerAddress(event.getBrokerAddress());
        }
        if (event.getPreviousReadQueueNums() != 0) {
            eventBuilder.setPreviousReadQueueNums(event.getPreviousReadQueueNums());
        }
        if (event.getCurrentReadQueueNums() != 0) {
            eventBuilder.setCurrentReadQueueNums(event.getCurrentReadQueueNums());
        }
        if (event.getPreviousWriteQueueNums() != 0) {
            eventBuilder.setPreviousWriteQueueNums(event.getPreviousWriteQueueNums());
        }
        if (event.getCurrentWriteQueueNums() != 0) {
            eventBuilder.setCurrentWriteQueueNums(event.getCurrentWriteQueueNums());
        }
        if (event.getRouteSnapshot() != null) {
            eventBuilder.setRouteSnapshot(toProtoTopicRouteSnapshot(event.getRouteSnapshot()));
        }

        responseBuilder.setEvent(eventBuilder.build());
        return responseBuilder.build();
    }

    /**
     * Convert internal RouteChangeEventType enum to proto RouteChangeEventType.
     */
    public static apache.rocketmq.proxy.admin.v1.RouteChangeEventType toProtoRouteChangeEventType(RouteChangeEventType type) {
        if (type == null) {
            return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.ROUTE_CHANGE_EVENT_TYPE_UNSPECIFIED;
        }
        switch (type) {
            case BROKER_ONLINE:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_ONLINE;
            case BROKER_OFFLINE:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.BROKER_OFFLINE;
            case QUEUE_SCALE:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.QUEUE_SCALE;
            case TOPIC_CREATE:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_CREATE;
            case TOPIC_DELETE:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.TOPIC_DELETE;
            case ROUTE_SNAPSHOT:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.ROUTE_SNAPSHOT;
            default:
                return apache.rocketmq.proxy.admin.v1.RouteChangeEventType.ROUTE_CHANGE_EVENT_TYPE_UNSPECIFIED;
        }
    }

    /**
     * Convert internal TopicRouteSnapshot model to proto TopicRouteSnapshot.
     */
    public static apache.rocketmq.proxy.admin.v1.TopicRouteSnapshot toProtoTopicRouteSnapshot(TopicRouteSnapshot snapshot) {
        apache.rocketmq.proxy.admin.v1.TopicRouteSnapshot.Builder builder = apache.rocketmq.proxy.admin.v1.TopicRouteSnapshot.newBuilder()
            .setTopic(snapshot.getTopic() != null ? snapshot.getTopic() : "");

        if (snapshot.getBrokers() != null) {
            for (TopicRouteSnapshot.BrokerInfo broker : snapshot.getBrokers()) {
                builder.addBrokers(toProtoBrokerInfo(broker));
            }
        }
        if (snapshot.getQueues() != null) {
            for (TopicRouteSnapshot.QueueInfo queue : snapshot.getQueues()) {
                builder.addQueues(toProtoQueueInfo(queue));
            }
        }
        return builder.build();
    }

    /**
     * Convert internal BrokerInfo model to proto BrokerInfo.
     */
    public static BrokerInfo toProtoBrokerInfo(TopicRouteSnapshot.BrokerInfo info) {
        BrokerInfo.Builder builder = BrokerInfo.newBuilder()
            .setCluster(info.getCluster() != null ? info.getCluster() : "")
            .setBrokerName(info.getBrokerName() != null ? info.getBrokerName() : "");
        if (info.getBrokerAddrs() != null) {
            builder.putAllBrokerAddrs(info.getBrokerAddrs());
        }
        return builder.build();
    }

    /**
     * Convert internal QueueInfo model to proto QueueInfo.
     */
    public static QueueInfo toProtoQueueInfo(TopicRouteSnapshot.QueueInfo info) {
        return QueueInfo.newBuilder()
            .setBrokerName(info.getBrokerName() != null ? info.getBrokerName() : "")
            .setReadQueueNums(info.getReadQueueNums())
            .setWriteQueueNums(info.getWriteQueueNums())
            .setPerm(info.getPerm())
            .build();
    }
}