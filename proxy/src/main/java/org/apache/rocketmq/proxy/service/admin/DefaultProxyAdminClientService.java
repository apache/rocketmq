/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.admin;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.UA;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.BatchConsumeClientDiagnostics;
import org.apache.rocketmq.proxy.common.BatchConsumeGroupSummary;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.common.PopReceiptHandleGroupSummary;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.ChannelBatchConsumeData;
import org.apache.rocketmq.proxy.service.receipt.ReceiptHandleManager.PopReceiptHandleDiagnosticResult;

/**
 * Default implementation of ProxyAdminClientService.
 * <p>
 * All client data comes from the internal ClientManager module (GrpcChannelManager and GrpcClientSettingsManager),
 * based on gRPC Telemetry long-connection reported information.
 * <p>
 * Performance considerations (RIP-2 §8):
 * - Filter pushdown: filters are applied during iteration, avoiding full collection before filtering
 * - Pagination is enforced with max page size of 100
 * - Data is weakly consistent (near real-time snapshot)
 * - Sampling support for high-concurrency scenarios
 */
public class DefaultProxyAdminClientService implements ProxyAdminClientService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private static final int MAX_PAGE_SIZE = 100;

    /**
     * Maximum heartbeat history records per client (RIP-2 §5.2.2).
     */
    private static final int MAX_HEARTBEAT_HISTORY_SIZE = 10;

    /**
     * Sampling threshold: when total client count exceeds this value,
     * DescribeClient diagnostics may return sampled data (RIP-2 §8.5).
     */
    private static final long SAMPLING_THRESHOLD = 100_000;

    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;
    private volatile ReceiptHandleManager receiptHandleManager;

    /**
     * Heartbeat history tracker: clientId -> deque of heartbeat timestamps.
     * Tracks the most recent heartbeat records for each client.
     */
    private final ConcurrentLinkedDeque<HeartbeatRecord> heartbeatLog = new ConcurrentLinkedDeque<>();
    private final AtomicLong heartbeatLogCounter = new AtomicLong(0);

    public DefaultProxyAdminClientService(GrpcChannelManager grpcChannelManager,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
    }

    /**
     * Set the ReceiptHandleManager for POP diagnostics.
     * Called during startup after the MessagingProcessor is initialized.
     *
     * @param receiptHandleManager the receipt handle manager instance
     */
    public void setReceiptHandleManager(ReceiptHandleManager receiptHandleManager) {
        this.receiptHandleManager = receiptHandleManager;
    }

    @Override
    public ListClientsResult listClients(ListClientsFilter filter, int pageNum, int pageSize) {
        pageNum = Math.max(pageNum, 1);
        pageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);

        // Filter pushdown (RIP-2 §8.1): apply filters during iteration
        // instead of collecting all clients first then filtering
        FilterContext filterCtx = new FilterContext(filter);
        List<ClientInstanceInfo> allClients = collectAndFilterClients(filterCtx);

        // Paginate
        long total = allClients.size();
        int fromIndex = (pageNum - 1) * pageSize;
        int toIndex = Math.min(fromIndex + pageSize, allClients.size());

        List<ClientInstanceInfo> pageList;
        if (fromIndex >= allClients.size()) {
            pageList = Collections.emptyList();
        } else {
            pageList = allClients.subList(fromIndex, toIndex);
        }

        return new ListClientsResult(total, pageNum, pageSize, new ArrayList<>(pageList));
    }

    @Override
    public ClientDetailInfo describeClient(String clientId) {
        if (StringUtils.isBlank(clientId)) {
            return null;
        }

        GrpcClientChannel channel = grpcChannelManager.getChannel(clientId);
        if (channel == null) {
            return null;
        }

        Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
        if (settings == null) {
            return null;
        }

        ClientDetailInfo detail = new ClientDetailInfo();

        // Build basic client instance info
        ClientInstanceInfo instanceInfo = buildClientInstanceInfo(clientId, channel, settings);
        detail.setClientInstance(instanceInfo);

        // Build client settings info
        ClientDetailInfo.ClientSettingsInfo settingsInfo = buildClientSettingsInfo(settings);
        detail.setSettings(settingsInfo);

        // Build heartbeat history from tracked records
        detail.setHeartbeatHistory(buildHeartbeatHistory(clientId, channel));

        // Build auth status with username extraction from settings
        detail.setAuthStatus(buildAuthStatus(clientId, channel, settings));

        // Build consume progress (only for consumers)
        if (isConsumer(settings)) {
            detail.setConsumeProgress(buildConsumeProgress(clientId, settings));
        }

        // Build network info with full details
        detail.setNetworkInfo(buildNetworkInfo(channel));

        return detail;
    }

    @Override
    public ListClientsResult listClientsByGroup(String group, int pageNum, int pageSize) {
        ListClientsFilter filter = new ListClientsFilter();
        filter.setGroup(group);
        return listClients(filter, pageNum, pageSize);
    }

    @Override
    public ListClientsResult listClientsByTopic(String topic, int pageNum, int pageSize) {
        ListClientsFilter filter = new ListClientsFilter();
        filter.setTopic(topic);
        return listClients(filter, pageNum, pageSize);
    }

    /**
     * Record a heartbeat event for a client.
     * Called by the telemetry processing pipeline when a heartbeat is received.
     * This enables real heartbeat history tracking (RIP-2 §5.2.2).
     *
     * @param clientId the client identifier
     */
    public void recordHeartbeat(String clientId) {
        long now = System.currentTimeMillis();
        heartbeatLog.addFirst(new HeartbeatRecord(clientId, now, true));
        heartbeatLogCounter.incrementAndGet();

        // Evict old entries to bound memory usage.
        // removeLast() is safe here because the while-loop condition
        // (size() > MAX_HEARTBEAT_HISTORY_SIZE * 1000) guarantees the deque is non-empty.
        while (heartbeatLog.size() > MAX_HEARTBEAT_HISTORY_SIZE * 1000) {
            heartbeatLog.removeLast();
        }
    }

    @Override
    public boolean forceDisconnectClient(String clientId, String reason) {
        if (StringUtils.isBlank(clientId)) {
            log.warn("forceDisconnectClient: clientId is blank, ignoring");
            return false;
        }

        GrpcClientChannel channel = grpcChannelManager.getChannel(clientId);
        if (channel == null) {
            log.warn("forceDisconnectClient: client {} not found in channel manager", clientId);
            return false;
        }

        // Step 1: Force close the gRPC telemetry stream
        // This sends a UNAVAILABLE error to the client, triggering reconnection
        boolean closed = channel.forceClose(reason);
        if (!closed) {
            log.warn("forceDisconnectClient: client {} stream already closed, proceeding with cleanup", clientId);
        }

        // Step 2: Remove the channel from GrpcChannelManager
        GrpcClientChannel removed = grpcChannelManager.removeChannel(clientId);
        if (removed != null) {
            log.info("forceDisconnectClient: removed channel for client {} from channel manager", clientId);
        }

        // Step 3: Remove client settings from GrpcClientSettingsManager
        Settings removedSettings = grpcClientSettingsManager.removeAndGetRawClientSettings(clientId);
        if (removedSettings != null) {
            log.info("forceDisconnectClient: removed settings for client {}", clientId);
        }

        log.info("forceDisconnectClient: client {} disconnected. reason: {}, streamClosed: {}, channelRemoved: {}, settingsRemoved: {}",
            clientId, reason, closed, removed != null, removedSettings != null);
        return true;
    }

    /**
     * Check if sampling should be applied for the current load level.
     * RIP-2 §8.5: When client count exceeds SAMPLING_THRESHOLD,
     * diagnostic interfaces should sample to protect system stability.
     *
     * @return true if sampling should be applied
     */
    public boolean shouldSample() {
        return grpcChannelManager.getClientIdChannelMap().size() > SAMPLING_THRESHOLD;
    }

    // ==================== Filter Pushdown Implementation (RIP-2 §8.1) ====================

    /**
     * Collect and filter clients in a single pass.
     * Implements filter pushdown by applying filters during iteration
     * rather than collecting all clients first then filtering.
     * <p>
     * This avoids creating a full ClientInstanceInfo list for all clients
     * when only a subset matches the filter criteria, significantly reducing
     * memory pressure under high connection counts.
     */
    private List<ClientInstanceInfo> collectAndFilterClients(FilterContext filterCtx) {
        List<ClientInstanceInfo> clients = new ArrayList<>();
        Map<String, GrpcClientChannel> channelMap = grpcChannelManager.getClientIdChannelMap();

        for (Map.Entry<String, GrpcClientChannel> entry : channelMap.entrySet()) {
            String clientId = entry.getKey();

            // Pushdown filter: clientId prefix (cheapest check first)
            if (!filterCtx.matchesClientIdPrefix(clientId)) {
                continue;
            }

            GrpcClientChannel channel = entry.getValue();

            // Pushdown filter: connect time range (cheap, no Settings needed)
            if (!filterCtx.matchesConnectTimeRange(channel.getCreateTime())) {
                continue;
            }

            Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
            if (settings == null) {
                continue;
            }

            // Pushdown filter: language (requires Settings)
            if (!filterCtx.matchesLanguage(settings)) {
                continue;
            }

            // Build partial info for remaining filter checks
            ClientInstanceInfo info = buildClientInstanceInfo(clientId, channel, settings);

            // Pushdown filter: group (requires built info)
            if (!filterCtx.matchesGroup(info.getGroup())) {
                continue;
            }

            // Pushdown filter: topic (requires built info)
            if (!filterCtx.matchesTopic(info.getTopics())) {
                continue;
            }

            clients.add(info);
        }
        return clients;
    }

    /**
     * Filter context that encapsulates filter criteria and provides
     * pushdown matching methods for each filter dimension.
     * Each method returns true if the item passes the filter (or if the
     * filter criterion is not set), enabling early elimination.
     */
    private static class FilterContext {
        private final ListClientsFilter filter;

        FilterContext(ListClientsFilter filter) {
            this.filter = filter != null ? filter : new ListClientsFilter();
        }

        boolean matchesClientIdPrefix(String clientId) {
            if (StringUtils.isBlank(filter.getClientIdPrefix())) {
                return true;
            }
            return clientId != null && clientId.startsWith(filter.getClientIdPrefix());
        }

        boolean matchesConnectTimeRange(long connectTime) {
            if (filter.getConnectTimeStart() > 0 && connectTime < filter.getConnectTimeStart()) {
                return false;
            }
            if (filter.getConnectTimeEnd() > 0 && connectTime > filter.getConnectTimeEnd()) {
                return false;
            }
            return true;
        }

        boolean matchesLanguage(Settings settings) {
            if (StringUtils.isBlank(filter.getLanguage())) {
                return true;
            }
            if (settings == null || !settings.hasUserAgent()) {
                return false;
            }
            String clientLanguage = convertLanguageToString(settings.getUserAgent().getLanguage());
            return filter.getLanguage().equalsIgnoreCase(clientLanguage);
        }

        boolean matchesGroup(String group) {
            if (StringUtils.isBlank(filter.getGroup())) {
                return true;
            }
            return filter.getGroup().equals(group);
        }

        boolean matchesTopic(List<String> topics) {
            if (StringUtils.isBlank(filter.getTopic())) {
                return true;
            }
            return topics != null && topics.contains(filter.getTopic());
        }

        private String convertLanguageToString(Language language) {
            if (language == null || language == Language.LANGUAGE_UNSPECIFIED) {
                return "UNSPECIFIED";
            }
            switch (language) {
                case JAVA: return "JAVA";
                case CPP: return "CPP";
                case DOT_NET: return "DOTNET";
                case GOLANG: return "GOLANG";
                case RUST: return "RUST";
                case PYTHON: return "PYTHON";
                case PHP: return "PHP";
                case NODE_JS: return "NODE_JS";
                case RUBY: return "RUBY";
                case OBJECTIVE_C: return "OBJECTIVE_C";
                case DART: return "DART";
                case KOTLIN: return "KOTLIN";
                default: return language.name();
            }
        }
    }

    // ==================== Build Methods ====================

    /**
     * Build a ClientInstanceInfo from channel and settings data.
     */
    private ClientInstanceInfo buildClientInstanceInfo(String clientId, GrpcClientChannel channel,
        Settings settings) {
        ClientInstanceInfo info = new ClientInstanceInfo();
        info.setClientId(clientId);
        info.setProtocol("GRPC_V2");
        info.setConnectAt(channel.getCreateTime());
        info.setLastActiveAt(channel.getLastAccessTime());

        // Extract remote address as access point
        info.setAccessPoint(channel.getRemoteAddress());

        // Extract role, group, and topics from settings
        if (settings != null) {
            ClientType clientType = settings.getClientType();
            info.setRole(convertClientType(clientType));

            // Extract language and clientVersion from Settings.user_agent (RIP-2 §5.2.1)
            if (settings.hasUserAgent()) {
                UA userAgent = settings.getUserAgent();
                info.setLanguage(convertLanguage(userAgent.getLanguage()));
                info.setClientVersion(userAgent.getVersion());
            } else {
                info.setLanguage(convertLanguage(Language.LANGUAGE_UNSPECIFIED));
            }

            if (settings.hasPublishing()) {
                // Producer
                List<String> topics = settings.getPublishing().getTopicsList().stream()
                    .map(Resource::getName)
                    .collect(Collectors.toList());
                info.setTopics(topics);
            } else if (settings.hasSubscription()) {
                // Consumer
                String group = settings.getSubscription().getGroup().getName();
                info.setGroup(group);
                List<String> topics = settings.getSubscription().getSubscriptionsList().stream()
                    .map(entry -> entry.getTopic().getName())
                    .collect(Collectors.toList());
                info.setTopics(topics);
            }
        }

        return info;
    }

    /**
     * Convert proto Language enum to display string.
     * Maps the gRPC v2 Language enum values to the string representation
     * required by RIP-2 §5.2.1 ClientInstance.language field.
     */
    private String convertLanguage(Language language) {
        if (language == null || language == Language.LANGUAGE_UNSPECIFIED) {
            return "UNSPECIFIED";
        }
        switch (language) {
            case JAVA:
                return "JAVA";
            case CPP:
                return "CPP";
            case DOT_NET:
                return "DOTNET";
            case GOLANG:
                return "GOLANG";
            case RUST:
                return "RUST";
            case PYTHON:
                return "PYTHON";
            case PHP:
                return "PHP";
            case NODE_JS:
                return "NODE_JS";
            case RUBY:
                return "RUBY";
            case OBJECTIVE_C:
                return "OBJECTIVE_C";
            case DART:
                return "DART";
            case KOTLIN:
                return "KOTLIN";
            default:
                return language.name();
        }
    }

    /**
     * Convert ClientType to role string.
     */
    private String convertClientType(ClientType clientType) {
        if (clientType == null) {
            return "UNSPECIFIED";
        }
        switch (clientType) {
            case PRODUCER:
                return "PRODUCER";
            case PUSH_CONSUMER:
            case LITE_PUSH_CONSUMER:
                return "PUSH_CONSUMER";
            case SIMPLE_CONSUMER:
                return "SIMPLE_CONSUMER";
            default:
                return clientType.name();
        }
    }

    /**
     * Build client settings info from Settings proto.
     */
    private ClientDetailInfo.ClientSettingsInfo buildClientSettingsInfo(Settings settings) {
        ClientDetailInfo.ClientSettingsInfo info = new ClientDetailInfo.ClientSettingsInfo();
        if (settings == null) {
            return info;
        }

        if (settings.hasSubscription()) {
            info.setSubscriptionMode(settings.getSubscription().getFifo() ? "FIFO" : "STANDARD");
            info.setReceiveBatchSize(settings.getSubscription().getReceiveBatchSize());
            info.setLongPollingTimeoutMs(settings.getSubscription().getLongPollingTimeout().getSeconds() * 1000
                + settings.getSubscription().getLongPollingTimeout().getNanos() / 1_000_000L);
            info.setFifo(settings.getSubscription().getFifo());
            info.setSubscriptionTopics(settings.getSubscription().getSubscriptionsList().stream()
                .map(entry -> entry.getTopic().getName())
                .collect(Collectors.toList()));
        }

        if (settings.hasPublishing()) {
            info.setPublishingTopics(settings.getPublishing().getTopicsList().stream()
                .map(Resource::getName)
                .collect(Collectors.toList()));
        }

        return info;
    }

    /**
     * Build heartbeat history from tracked records and channel last access time.
     * RIP-2 §5.2.2: Returns recent heartbeat records with timestamps and status.
     * <p>
     * If tracked heartbeat records are available, returns those.
     * Otherwise, falls back to a synthetic record based on lastAccessTime.
     */
    private List<ClientDetailInfo.HeartbeatRecordInfo> buildHeartbeatHistory(String clientId,
        GrpcClientChannel channel) {
        List<ClientDetailInfo.HeartbeatRecordInfo> history = new ArrayList<>();

        // Collect tracked heartbeat records for this client
        for (HeartbeatRecord record : heartbeatLog) {
            if (record.clientId.equals(clientId)) {
                ClientDetailInfo.HeartbeatRecordInfo info = new ClientDetailInfo.HeartbeatRecordInfo();
                info.setTimestamp(record.timestamp);
                info.setSuccess(record.success);
                info.setRemark(record.success ? "Heartbeat OK" : "Heartbeat timeout");
                history.add(info);
                if (history.size() >= MAX_HEARTBEAT_HISTORY_SIZE) {
                    break;
                }
            }
        }

        // Fallback: if no tracked records, use lastAccessTime as synthetic heartbeat
        if (history.isEmpty()) {
            ClientDetailInfo.HeartbeatRecordInfo record = new ClientDetailInfo.HeartbeatRecordInfo();
            record.setTimestamp(channel.getLastAccessTime());
            record.setSuccess(true);
            record.setRemark("Last activity (synthetic)");
            history.add(record);
        }

        return history;
    }

    /**
     * Build auth status from channel and settings.
     * RIP-2 §5.2.2: Extracts authentication information including username
     * from the auth metadata (AUTHORIZATION_AK) stored in GrpcClientChannel.
     * <p>
     * The username is obtained from per-connection auth metadata captured
     * during the authentication pipeline, stored in GrpcClientChannel.
     * Falls back to user agent platform field or clientId convention.
     */
    private ClientDetailInfo.AuthStatusInfo buildAuthStatus(String clientId,
        GrpcClientChannel channel, Settings settings) {
        ClientDetailInfo.AuthStatusInfo authStatus = new ClientDetailInfo.AuthStatusInfo();

        // Channel is established, so authentication was successful
        authStatus.setAuthenticated(true);
        authStatus.setLastAuthTime(channel.getCreateTime());

        // Priority 1: Use authenticated username from auth metadata (RIP-2 §5.2.2)
        String authUsername = channel.getAuthUsername();
        if (StringUtils.isNotBlank(authUsername)) {
            authStatus.setUsername(authUsername);
        }

        // Priority 2: Extract username from user agent platform field
        if (StringUtils.isBlank(authStatus.getUsername()) && settings != null && settings.hasUserAgent()) {
            UA userAgent = settings.getUserAgent();
            String platform = userAgent.getPlatform();
            if (StringUtils.isNotBlank(platform)) {
                authStatus.setUsername(platform);
            }
        }

        // Priority 3: Try to extract from clientId convention
        if (StringUtils.isBlank(authStatus.getUsername())) {
            authStatus.setUsername(extractUsernameFromClientId(clientId));
        }

        return authStatus;
    }

    /**
     * Extract username from clientId following common naming conventions.
     * ClientId format typically: hostname@processId@timestamp or user@hostname@processId
     */
    private String extractUsernameFromClientId(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return null;
        }
        // ClientId format: typically "hostname-processId-timestamp"
        // or for authenticated clients: "user@hostname-processId-timestamp"
        int atIndex = clientId.indexOf('@');
        if (atIndex > 0) {
            String prefix = clientId.substring(0, atIndex);
            // Check if the prefix looks like a username (not an IP or hostname)
            if (!prefix.matches(".*[.\\d]+.*") && prefix.length() < 64) {
                return prefix;
            }
        }
        return null;
    }

    /**
     * Check if the client type is a consumer.
     */
    private boolean isConsumer(Settings settings) {
        if (settings == null) {
            return false;
        }
        ClientType clientType = settings.getClientType();
        return clientType == ClientType.PUSH_CONSUMER
            || clientType == ClientType.LITE_PUSH_CONSUMER
            || clientType == ClientType.SIMPLE_CONSUMER;
    }

    /**
     * Build consume progress for consumer clients.
     * In local view mode, consume progress requires broker-side data
     * which is not directly available from the proxy's local state.
     * <p>
     * M1 implementation: returns available local data (subscription info).
     * Full implementation with broker-side lag/latency data planned for M2.
     */
    private ClientDetailInfo.ConsumeProgressInfo buildConsumeProgress(String clientId, Settings settings) {
        ClientDetailInfo.ConsumeProgressInfo progress = new ClientDetailInfo.ConsumeProgressInfo();
        if (!settings.hasSubscription()) {
            return progress;
        }

        // Build per-topic progress entries with available local data
        // Lag and latency require broker-side queries (M2 scope)
        List<ClientDetailInfo.TopicConsumeProgressInfo> topicProgressList = new ArrayList<>();
        for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
            ClientDetailInfo.TopicConsumeProgressInfo topicProgress = new ClientDetailInfo.TopicConsumeProgressInfo();
            topicProgress.setTopic(entry.getTopic().getName());
            // Lag and latency are not available from proxy local state
            // These require broker-side queries planned for M2
            topicProgress.setLag(-1);
            topicProgress.setLatencyMs(-1);
            topicProgressList.add(topicProgress);
        }
        progress.setTopicProgress(topicProgressList);

        // Overall lag/latency not available from local state
        progress.setLag(-1);
        progress.setLatencyMs(-1);

        return progress;
    }

    /**
     * Build network info from channel with full details.
     * RIP-2 §5.2.2: Extracts local/remote addresses, RTT, and SSL state.
     */
    private ClientDetailInfo.NetworkInfoInfo buildNetworkInfo(GrpcClientChannel channel) {
        ClientDetailInfo.NetworkInfoInfo networkInfo = new ClientDetailInfo.NetworkInfoInfo();

        // Remote address
        if (channel.getRemoteAddress() != null) {
            networkInfo.setRemoteAddress(channel.getRemoteAddress());
        }

        // Local address - extracted from SimpleChannel.localAddress field
        String localAddress = channel.getLocalAddress();
        if (localAddress != null) {
            networkInfo.setLocalAddress(localAddress);
        }

        // SSL state detection from the underlying Netty channel pipeline
        networkInfo.setSslEnabled(detectSslState(channel));

        // RTT measurement from telemetry command round-trip (RIP-2 §5.2.1)
        networkInfo.setRttMs(channel.getLastRttMs());

        return networkInfo;
    }

    /**
     * Detect SSL state for a specific client connection.
     * <p>
     * First checks the per-connection SSL state stored in GrpcClientChannel,
     * which is detected from gRPC transport security level at channel creation time.
     * Falls back to global TlsSystemConfig.tlsMode if per-connection state is unavailable.
     */
    private boolean detectSslState(GrpcClientChannel channel) {
        try {
            // Prefer per-connection SSL state from GrpcClientChannel (RIP-2 §5.2.1)
            if (channel.isSslEnabled()) {
                return true;
            }
            // Fallback: infer from global TLS configuration
            org.apache.rocketmq.remoting.common.TlsMode tlsMode = org.apache.rocketmq.remoting.netty.TlsSystemConfig.tlsMode;
            return tlsMode == org.apache.rocketmq.remoting.common.TlsMode.PERMISSIVE
                || tlsMode == org.apache.rocketmq.remoting.common.TlsMode.ENFORCING;
        } catch (Exception e) {
            log.debug("Failed to detect SSL state for channel: {}", channel.getClientId(), e);
        }
        return false;
    }

    // ==================== POP Diagnostics ====================

    @Override
    public PopReceiptHandleDiagnosticResult describePopReceiptHandles(String group, String topic,
        int pageNum, int pageSize) {
        if (receiptHandleManager == null) {
            log.warn("ReceiptHandleManager not initialized, POP diagnostics unavailable");
            return new PopReceiptHandleDiagnosticResult(
                new PopReceiptHandleGroupSummary(group, 0, 0, 0, 0, 0),
                Collections.emptyList(), 0, 1, 1);
        }
        pageNum = Math.max(pageNum, 1);
        pageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);
        return receiptHandleManager.describePopReceiptHandles(group, topic, pageNum, pageSize);
    }

    // ==================== Batch Consume Diagnostics ====================

    @Override
    public ProxyAdminClientService.BatchConsumeDiagnosticResult describeBatchConsumeDiagnostics(
        String group, String topic, String clientId, int pageNum, int pageSize) {
        if (receiptHandleManager == null) {
            log.warn("ReceiptHandleManager not initialized, batch consume diagnostics unavailable");
            return new ProxyAdminClientService.BatchConsumeDiagnosticResult(
                new BatchConsumeGroupSummary(group, 0, 0, 0, 0, 0, 0),
                Collections.emptyList(), 0, 1, 1);
        }
        if (StringUtils.isBlank(group)) {
            return new ProxyAdminClientService.BatchConsumeDiagnosticResult(
                new BatchConsumeGroupSummary("", 0, 0, 0, 0, 0, 0),
                Collections.emptyList(), 0, 1, 1);
        }

        pageNum = Math.max(pageNum, 1);
        pageSize = Math.min(Math.max(pageSize, 1), MAX_PAGE_SIZE);

        // Step 1: Get all channel-level raw data from ReceiptHandleManager
        // We fetch all data (no server-side pagination) because we need to:
        //   a) Enrich with clientId from GrpcClientChannel
        //   b) Apply clientId filter if specified
        //   c) Re-paginate after enrichment and filtering
        ReceiptHandleManager.BatchConsumeDiagnosticResult rawResult = receiptHandleManager.describeBatchConsumeDiagnostics(
            group, topic, 1, Integer.MAX_VALUE);

        // Step 2: Enrich each ChannelBatchConsumeData with gRPC channel/settings data
        List<BatchConsumeClientDiagnostics> enriched = new ArrayList<>();
        for (ChannelBatchConsumeData channelData : rawResult.getChannelData()) {
            BatchConsumeClientDiagnostics diagnostic = enrichChannelData(channelData);
            if (diagnostic == null) {
                continue;
            }

            // Step 3: Apply clientId filter if specified
            if (StringUtils.isNotBlank(clientId) && !clientId.equals(diagnostic.getClientId())) {
                continue;
            }

            enriched.add(diagnostic);
        }

        // Step 4: Re-paginate after enrichment and filtering
        long total = enriched.size();
        int fromIndex = (pageNum - 1) * pageSize;
        int toIndex = Math.min(fromIndex + pageSize, enriched.size());

        List<BatchConsumeClientDiagnostics> pageList;
        if (fromIndex >= enriched.size()) {
            pageList = Collections.emptyList();
        } else {
            pageList = enriched.subList(fromIndex, toIndex);
        }

        // Step 5: Recalculate summary for the filtered set
        BatchConsumeGroupSummary summary = recalculateSummary(group, enriched);

        return new ProxyAdminClientService.BatchConsumeDiagnosticResult(
            summary, new ArrayList<>(pageList), total, pageNum, pageSize);
    }

    /**
     * Enrich a ChannelBatchConsumeData with gRPC channel and settings information.
     * <p>
     * The Channel stored in ReceiptHandleGroupKey is the GrpcClientChannel itself
     * (passed from ReceiveMessageActivity), so we can directly cast and extract:
     * - clientId, lastRttMs, createTime from GrpcClientChannel
     * - receiveBatchSize, longPollingTimeout from Settings
     * - consumeType from Settings.clientType
     * - messageModel defaults to CLUSTERING for gRPC v2 proxy clients
     *
     * @param channelData raw per-channel data from ReceiptHandleManager
     * @return enriched diagnostics, or null if channel cannot be identified
     */
    private BatchConsumeClientDiagnostics enrichChannelData(ChannelBatchConsumeData channelData) {
        Channel channel = channelData.getChannel();

        // Extract clientId and channel info from GrpcClientChannel
        String clientId = null;
        long lastRttMs = -1;
        long connectTime = -1;

        // The Channel in ReceiptHandleGroupKey IS the GrpcClientChannel (set in ReceiveMessageActivity)
        if (channel instanceof GrpcClientChannel) {
            GrpcClientChannel grpcChannel = (GrpcClientChannel) channel;
            clientId = grpcChannel.getClientId();
            lastRttMs = grpcChannel.getLastRttMs();
            connectTime = grpcChannel.getCreateTime();
        } else {
            // Fallback: reverse-lookup from GrpcChannelManager's clientIdChannelMap
            // This handles edge cases where the channel type differs
            for (Map.Entry<String, GrpcClientChannel> entry : grpcChannelManager.getClientIdChannelMap().entrySet()) {
                if (entry.getValue().equals(channel)) {
                    clientId = entry.getKey();
                    lastRttMs = entry.getValue().getLastRttMs();
                    connectTime = entry.getValue().getCreateTime();
                    break;
                }
            }
        }

        if (clientId == null) {
            // Channel no longer exists in gRPC channel manager, skip this entry
            log.debug("Batch diagnostics: channel {} not found in GrpcChannelManager, skipping", channel);
            return null;
        }

        // Get Settings for receiveBatchSize, longPollingTimeout, and consumeType
        int receiveBatchSize = -1;
        long longPollingTimeoutMs = -1;
        String consumeType = "UNSPECIFIED";
        String messageModel = "CLUSTERING"; // Default for gRPC v2 proxy clients

        Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
        if (settings != null) {
            consumeType = convertClientType(settings.getClientType());

            if (settings.hasSubscription()) {
                receiveBatchSize = settings.getSubscription().getReceiveBatchSize();
                longPollingTimeoutMs = settings.getSubscription().getLongPollingTimeout().getSeconds() * 1000
                    + settings.getSubscription().getLongPollingTimeout().getNanos() / 1_000_000L;
            }
        }

        String channelId = channel.id().asShortText();

        return new BatchConsumeClientDiagnostics(
            clientId,
            channelId,
            channelData.getUnackedMessageCount(),
            channelData.getUnackedHandleCount(),
            channelData.getTotalRenewTimes(),
            channelData.getTotalRenewRetryTimes(),
            channelData.getExpiredHandleCount(),
            channelData.getTopicDistribution(),
            consumeType,
            messageModel,
            receiveBatchSize,
            longPollingTimeoutMs,
            lastRttMs,
            connectTime
        );
    }

    /**
     * Recalculate the group summary from the enriched (and possibly filtered) diagnostics list.
     * This is necessary because:
     * 1. The raw summary from ReceiptHandleManager covers ALL channels in the group
     * 2. We may have filtered by clientId, reducing the set
     * 3. Some channels may have been removed (not found in GrpcChannelManager)
     *
     * @param group      consumer group name
     * @param diagnostics enriched and filtered diagnostics list
     * @return recalculated group summary
     */
    private BatchConsumeGroupSummary recalculateSummary(String group, List<BatchConsumeClientDiagnostics> diagnostics) {
        int totalClients = diagnostics.size();
        int totalUnackedMessages = 0;
        int totalUnackedHandles = 0;
        int totalExpiredHandles = 0;
        long totalRenewTimes = 0;
        long totalRenewRetryTimes = 0;

        for (BatchConsumeClientDiagnostics diag : diagnostics) {
            totalUnackedMessages += diag.getUnackedMessageCount();
            totalUnackedHandles += diag.getUnackedHandleCount();
            totalExpiredHandles += diag.getExpiredHandleCount();
            totalRenewTimes += diag.getTotalRenewTimes();
            totalRenewRetryTimes += diag.getTotalRenewRetryTimes();
        }

        return new BatchConsumeGroupSummary(group, totalClients, totalUnackedMessages,
            totalUnackedHandles, totalExpiredHandles, totalRenewTimes, totalRenewRetryTimes);
    }

    // ==================== Inner Classes ====================

    /**
     * Heartbeat record for tracking client heartbeat history.
     */
    private static class HeartbeatRecord {
        final String clientId;
        final long timestamp;
        final boolean success;

        HeartbeatRecord(String clientId, long timestamp, boolean success) {
            this.clientId = clientId;
            this.timestamp = timestamp;
            this.success = success;
        }
    }
}