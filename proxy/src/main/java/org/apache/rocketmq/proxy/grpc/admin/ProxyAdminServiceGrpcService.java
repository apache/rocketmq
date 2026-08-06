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
package org.apache.rocketmq.proxy.grpc.admin;

import apache.rocketmq.v2.AuthStatus;
import apache.rocketmq.v2.ClientConsumeProgress;
import apache.rocketmq.v2.ClientDetail;
import apache.rocketmq.v2.ClientFilter;
import apache.rocketmq.v2.ClientInstance;
import apache.rocketmq.v2.ClientProtocol;
import apache.rocketmq.v2.ClientRole;
import apache.rocketmq.v2.ClientTopicProgress;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeBatchConsumeDiagnosticsRequest;
import apache.rocketmq.v2.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.DescribePopReceiptHandlesRequest;
import apache.rocketmq.v2.DescribePopReceiptHandlesResponse;
import apache.rocketmq.v2.DescribeProxyConfigRequest;
import apache.rocketmq.v2.DescribeProxyConfigResponse;
import apache.rocketmq.v2.DescribeQuotaRequest;
import apache.rocketmq.v2.DescribeQuotaResponse;
import apache.rocketmq.v2.DescribeRouteTopologyRequest;
import apache.rocketmq.v2.DescribeRouteTopologyResponse;
import apache.rocketmq.v2.DisconnectChannelRequest;
import apache.rocketmq.v2.DisconnectChannelResponse;
import apache.rocketmq.v2.KickClientRequest;
import apache.rocketmq.v2.KickClientResponse;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.NetworkInfo;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyScope;
import apache.rocketmq.v2.PublishSettings;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SubscribeRouteEventsRequest;
import apache.rocketmq.v2.SubscribeRouteEventsResponse;
import apache.rocketmq.v2.UA;
import apache.rocketmq.v2.UpdateProxyConfigRequest;
import apache.rocketmq.v2.UpdateProxyConfigResponse;
import apache.rocketmq.v2.UpdateQuotaRequest;
import apache.rocketmq.v2.UpdateQuotaResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.DefaultMessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;

/**
 * RIP-2 Proxy Admin gRPC service — the complete {@code ProxyAdminService} surface:
 *
 * <ul>
 *   <li>M1 online client query: ListClients / DescribeClient / ListClientsByGroup /
 *       ListClientsByTopic;</li>
 *   <li>M2 runtime config & connection control: DescribeProxyConfig / UpdateProxyConfig /
 *       KickClient / DisconnectChannel;</li>
 *   <li>M2 quota visualization: DescribeQuota / UpdateQuota;</li>
 *   <li>M3/M4 diagnostics: DescribePopReceiptHandles / DescribeBatchConsumeDiagnostics;</li>
 *   <li>route observation: SubscribeRouteEvents (server streaming) / DescribeRouteTopology.</li>
 * </ul>
 *
 * <p>Design notes (RIP-2):
 * <ul>
 *   <li>D3 multi-proxy semantics: every reply is tagged with {@code proxy_endpoint} +
 *       {@code epoch}; when the caller asks for {@code PROXY_SCOPE_ALL_PROXIES} and peer
 *       endpoints are configured, the query fans out via {@link ProxyAdminPeerClient} and
 *       returns the deduplicated cluster-wide view.</li>
 *   <li>D4 pagination: stable cursor-based {@code next_token}. The cursor is the
 *       clientId-sorted position of the last returned element, so pages stay consistent
 *       even while clients connect/disconnect between calls.</li>
 *   <li>Every capability is served from the proxy itself; broker-internal data is only
 *       reached through the proxy's managed broker client (AdminService).</li>
 * </ul>
 */
public class ProxyAdminServiceGrpcService extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ProxyAdminServiceGrpcService.class);

    // Server-enforced maximum page size for cursor pagination (D4).
    private static final int MAX_PAGE_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final String CURSOR_PREFIX = "c1:";
    private static final long CONSUME_PROGRESS_TIMEOUT_MILLIS = 3000L;

    private final ServiceManager serviceManager;
    private final DefaultMessagingProcessor messagingProcessor;
    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;
    private final ProxyAdminPeerClient peerClient;
    private final RouteChangeNotifier routeChangeNotifier;
    private final ProxyAdminConfigSupport configSupport;
    private final ProxyAdminDiagnosticsSupport diagnosticsSupport;

    private final String proxyEndpoint;
    private final long epoch;

    public ProxyAdminServiceGrpcService(ServiceManager serviceManager, DefaultMessagingProcessor messagingProcessor,
        GrpcChannelManager grpcChannelManager, GrpcClientSettingsManager grpcClientSettingsManager,
        ProxyAdminPeerClient peerClient, RouteChangeNotifier routeChangeNotifier) {
        this.serviceManager = serviceManager;
        this.messagingProcessor = messagingProcessor;
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.peerClient = peerClient;
        this.routeChangeNotifier = routeChangeNotifier;
        this.configSupport = new ProxyAdminConfigSupport();
        this.diagnosticsSupport = new ProxyAdminDiagnosticsSupport(messagingProcessor);
        this.proxyEndpoint = resolveProxyEndpoint();
        this.epoch = System.currentTimeMillis();
    }

    // -------------------------------------------------------------------------
    // M1: online client query
    // -------------------------------------------------------------------------

    @Override
    public void listClients(ListClientsRequest request, StreamObserver<ListClientsResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            ClientFilter filter = request.hasFilter() ? request.getFilter() : null;
            List<ClientInstance> local = filterInstances(allClientInstances(), filter);
            List<ClientInstance> view = applyScope(local, request.getScope(),
                peers -> peerClient.listClientsAllProxies(local, request, peers, peerTimeoutMillis()));
            Page page = page(view, request.getPageSize(), request.getNextToken());
            ListClientsResponse.Builder builder = ListClientsResponse.newBuilder()
                .setStatus(success())
                .setProxyEndpoint(proxyEndpoint)
                .setEpoch(epoch)
                .addAllClients(page.items);
            if (!page.nextToken.isEmpty()) {
                builder.setNextToken(page.nextToken);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("ListClients", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("ListClients", System.currentTimeMillis() - start, t);
            log.error("listClients failed", t);
            responseObserver.onNext(ListClientsResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void describeClient(DescribeClientRequest request, StreamObserver<DescribeClientResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String clientId = request.getClientId();
            GrpcClientChannel channel = grpcChannelManager.getChannel(clientId);
            if (channel == null) {
                responseObserver.onNext(DescribeClientResponse.newBuilder()
                    .setStatus(fail(Code.NOT_FOUND, "client not connected to this proxy"))
                    .build());
                responseObserver.onCompleted();
                return;
            }
            Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
            ClientDetail.Builder detail = ClientDetail.newBuilder().setInstance(buildClientInstance(channel));

            detail.addAllRecentHeartbeats(channel.getRecentHeartbeats());
            detail.setAuthStatus(buildAuthStatus(channel));

            if (settings != null) {
                detail.setSettings(settings);
                if (settings.hasSubscription()) {
                    detail.addAllSubscriptions(settings.getSubscription().getSubscriptionsList());
                    detail.setConsumeProgress(buildConsumeProgress(channel, settings));
                }
                if (settings.hasPublishing()) {
                    detail.setPublishSettings(PublishSettings.newBuilder()
                        .addAllTopics(settings.getPublishing().getTopicsList()));
                }
            }
            detail.setNetworkInfo(NetworkInfo.newBuilder()
                .setLocalAddress(str(channel.getLocalAddress()))
                .setRemoteAddress(str(channel.getRemoteAddress()))
                .build());
            responseObserver.onNext(DescribeClientResponse.newBuilder()
                .setStatus(success())
                .setClientDetail(detail)
                .build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribeClient", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribeClient", System.currentTimeMillis() - start, t);
            log.error("describeClient failed", t);
            responseObserver.onNext(DescribeClientResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listClientsByGroup(ListClientsByGroupRequest request, StreamObserver<ListClientsByGroupResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String group = request.hasGroup() ? request.getGroup().getName() : "";
            List<ClientInstance> local = new ArrayList<>();
            for (ClientInstance instance : allClientInstances()) {
                if (group.isEmpty() || instance.getGroupsList().contains(group)) {
                    local.add(instance);
                }
            }
            List<ClientInstance> view = applyScope(local, request.getScope(),
                peers -> peerClient.listClientsByGroupAllProxies(local, request, peers, peerTimeoutMillis()));
            Page page = page(view, request.getPageSize(), request.getNextToken());
            ListClientsByGroupResponse.Builder builder = ListClientsByGroupResponse.newBuilder()
                .setStatus(success())
                .setProxyEndpoint(proxyEndpoint)
                .setEpoch(epoch)
                .addAllClients(page.items);
            if (!page.nextToken.isEmpty()) {
                builder.setNextToken(page.nextToken);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("ListClientsByGroup", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("ListClientsByGroup", System.currentTimeMillis() - start, t);
            log.error("listClientsByGroup failed", t);
            responseObserver.onNext(ListClientsByGroupResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listClientsByTopic(ListClientsByTopicRequest request, StreamObserver<ListClientsByTopicResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String topic = request.hasTopic() ? request.getTopic().getName() : "";
            List<ClientInstance> local = new ArrayList<>();
            for (ClientInstance instance : allClientInstances()) {
                if (topic.isEmpty() || instance.getTopicsList().contains(topic)) {
                    local.add(instance);
                }
            }
            List<ClientInstance> view = applyScope(local, request.getScope(),
                peers -> peerClient.listClientsByTopicAllProxies(local, request, peers, peerTimeoutMillis()));
            Page page = page(view, request.getPageSize(), request.getNextToken());
            ListClientsByTopicResponse.Builder builder = ListClientsByTopicResponse.newBuilder()
                .setStatus(success())
                .setProxyEndpoint(proxyEndpoint)
                .setEpoch(epoch)
                .addAllClients(page.items);
            if (!page.nextToken.isEmpty()) {
                builder.setNextToken(page.nextToken);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("ListClientsByTopic", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("ListClientsByTopic", System.currentTimeMillis() - start, t);
            log.error("listClientsByTopic failed", t);
            responseObserver.onNext(ListClientsByTopicResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // M2: runtime config & connection control
    // -------------------------------------------------------------------------

    @Override
    public void describeProxyConfig(DescribeProxyConfigRequest request,
        StreamObserver<DescribeProxyConfigResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(configSupport.describeProxyConfig(request, success()));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribeProxyConfig", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribeProxyConfig", System.currentTimeMillis() - start, t);
            log.error("describeProxyConfig failed", t);
            responseObserver.onNext(DescribeProxyConfigResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void updateProxyConfig(UpdateProxyConfigRequest request,
        StreamObserver<UpdateProxyConfigResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(configSupport.updateProxyConfig(request, success()));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("UpdateProxyConfig", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("UpdateProxyConfig", System.currentTimeMillis() - start, t);
            log.error("updateProxyConfig failed", t);
            responseObserver.onNext(UpdateProxyConfigResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void kickClient(KickClientRequest request, StreamObserver<KickClientResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String clientId = request.getClientId();
            if (StringUtils.isBlank(clientId)) {
                responseObserver.onNext(KickClientResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "client_id is required")).build());
                responseObserver.onCompleted();
                return;
            }
            if (StringUtils.isBlank(request.getReason())) {
                responseObserver.onNext(KickClientResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "reason is required for audit")).build());
                responseObserver.onCompleted();
                return;
            }
            boolean disconnected = disconnectClient(clientId, "kick by admin, reason: " + request.getReason());
            responseObserver.onNext(KickClientResponse.newBuilder()
                .setStatus(success())
                .setDisconnected(disconnected)
                .build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("KickClient", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("KickClient", System.currentTimeMillis() - start, t);
            log.error("kickClient failed", t);
            responseObserver.onNext(KickClientResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void disconnectChannel(DisconnectChannelRequest request,
        StreamObserver<DisconnectChannelResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String channelId = request.getChannelId();
            if (StringUtils.isBlank(channelId)) {
                responseObserver.onNext(DisconnectChannelResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "channel_id is required")).build());
                responseObserver.onCompleted();
                return;
            }
            if (StringUtils.isBlank(request.getReason())) {
                responseObserver.onNext(DisconnectChannelResponse.newBuilder()
                    .setStatus(fail(Code.BAD_REQUEST, "reason is required for audit")).build());
                responseObserver.onCompleted();
                return;
            }
            boolean disconnected = false;
            for (GrpcClientChannel channel : grpcChannelManager.getClientChannels()) {
                if (channelId.equals(channel.id().asShortText()) || channelId.equals(channel.id().asLongText())
                    || channelId.equals(channel.getClientId())) {
                    disconnected = disconnectClient(channel.getClientId(),
                        "channel disconnected by admin, reason: " + request.getReason());
                    break;
                }
            }
            responseObserver.onNext(DisconnectChannelResponse.newBuilder()
                .setStatus(success())
                .setDisconnected(disconnected)
                .build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DisconnectChannel", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DisconnectChannel", System.currentTimeMillis() - start, t);
            log.error("disconnectChannel failed", t);
            responseObserver.onNext(DisconnectChannelResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // M2: quota visualization & controlled adjustment
    // -------------------------------------------------------------------------

    @Override
    public void describeQuota(DescribeQuotaRequest request, StreamObserver<DescribeQuotaResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(configSupport.describeQuota(request, success()));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribeQuota", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribeQuota", System.currentTimeMillis() - start, t);
            log.error("describeQuota failed", t);
            responseObserver.onNext(DescribeQuotaResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void updateQuota(UpdateQuotaRequest request, StreamObserver<UpdateQuotaResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(configSupport.updateQuota(request, success(), fail(Code.BAD_REQUEST,
                "policy with positive limit and metric is required")));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("UpdateQuota", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("UpdateQuota", System.currentTimeMillis() - start, t);
            log.error("updateQuota failed", t);
            responseObserver.onNext(UpdateQuotaResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // M3/M4: POP & batch consume diagnostics
    // -------------------------------------------------------------------------

    @Override
    public void describePopReceiptHandles(DescribePopReceiptHandlesRequest request,
        StreamObserver<DescribePopReceiptHandlesResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(diagnosticsSupport.describePopReceiptHandles(request, success(),
                fail(Code.BAD_REQUEST, "group is required")));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribePopReceiptHandles", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribePopReceiptHandles", System.currentTimeMillis() - start, t);
            log.error("describePopReceiptHandles failed", t);
            responseObserver.onNext(DescribePopReceiptHandlesResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void describeBatchConsumeDiagnostics(DescribeBatchConsumeDiagnosticsRequest request,
        StreamObserver<DescribeBatchConsumeDiagnosticsResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            responseObserver.onNext(diagnosticsSupport.describeBatchConsumeDiagnostics(request, success(),
                fail(Code.BAD_REQUEST, "group is required")));
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribeBatchConsumeDiagnostics",
                System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribeBatchConsumeDiagnostics",
                System.currentTimeMillis() - start, t);
            log.error("describeBatchConsumeDiagnostics failed", t);
            responseObserver.onNext(DescribeBatchConsumeDiagnosticsResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // route observation
    // -------------------------------------------------------------------------

    @Override
    public void subscribeRouteEvents(SubscribeRouteEventsRequest request,
        StreamObserver<SubscribeRouteEventsResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            RouteChangeNotifier.Subscription subscription = routeChangeNotifier.subscribe(request, responseObserver,
                serviceManager.getTopicRouteService().snapshotTopicRouteCache());
            Context.current().addListener(cancelledContext -> {
                routeChangeNotifier.unsubscribe(subscription);
            }, command -> command.run());
            ProxyAdminMetricsManager.recordSuccess("SubscribeRouteEvents", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("SubscribeRouteEvents", System.currentTimeMillis() - start, t);
            log.error("subscribeRouteEvents failed", t);
            responseObserver.onNext(SubscribeRouteEventsResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void describeRouteTopology(DescribeRouteTopologyRequest request,
        StreamObserver<DescribeRouteTopologyResponse> responseObserver) {
        long start = System.currentTimeMillis();
        try {
            String topicFilter = request.hasTopic() ? request.getTopic().getName() : "";
            DescribeRouteTopologyResponse.Builder builder = DescribeRouteTopologyResponse.newBuilder()
                .setStatus(success());
            Map<String, MessageQueueView> routes = serviceManager.getTopicRouteService().snapshotTopicRouteCache();
            int activeConnections = grpcChannelManager.getClientChannels().size();
            for (Map.Entry<String, MessageQueueView> entry : routes.entrySet()) {
                String topic = entry.getKey();
                MessageQueueView view = entry.getValue();
                if (view == null || view.isEmptyCachedQueue()) {
                    continue;
                }
                if (!topicFilter.isEmpty() && !topicFilter.equals(topic)) {
                    continue;
                }
                AdminModelConverter.addRouteTopology(builder, topic, view.getTopicRouteData(), proxyEndpoint,
                    activeConnections);
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
            ProxyAdminMetricsManager.recordSuccess("DescribeRouteTopology", System.currentTimeMillis() - start);
        } catch (Throwable t) {
            ProxyAdminMetricsManager.recordError("DescribeRouteTopology", System.currentTimeMillis() - start, t);
            log.error("describeRouteTopology failed", t);
            responseObserver.onNext(DescribeRouteTopologyResponse.newBuilder()
                .setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    /**
     * Forcefully disconnect a client: detach its channel from the manager and close the
     * underlying transport. Returns true when the client was found and disconnected.
     */
    private boolean disconnectClient(String clientId, String reason) {
        GrpcClientChannel channel = grpcChannelManager.removeChannel(clientId);
        if (channel == null) {
            return false;
        }
        log.info("RIP-2 admin disconnect. clientId:{}, {}", clientId, reason);
        try {
            channel.close();
        } catch (Throwable t) {
            log.warn("RIP-2 admin disconnect close failed. clientId:{}", clientId, t);
        }
        return true;
    }

    private List<ClientInstance> filterInstances(List<ClientInstance> instances, ClientFilter filter) {
        if (filter == null) {
            return instances;
        }
        List<ClientInstance> filtered = new ArrayList<>();
        for (ClientInstance instance : instances) {
            if (matchFilter(instance, filter)) {
                filtered.add(instance);
            }
        }
        return filtered;
    }

    /**
     * D3: resolve the requested scope. For ALL_PROXIES with configured peers, replace the
     * local view with the merged cluster-wide view; otherwise keep the local view.
     */
    private List<ClientInstance> applyScope(List<ClientInstance> localView, ProxyScope scope,
        java.util.function.Function<List<String>, List<ClientInstance>> aggregator) {
        if (scope != ProxyScope.PROXY_SCOPE_ALL_PROXIES) {
            return localView;
        }
        List<String> peers = ConfigurationManager.getProxyConfig().getProxyAdminPeerEndpoints();
        if (peers == null || peers.isEmpty()) {
            return localView;
        }
        return aggregator.apply(peers);
    }

    private long peerTimeoutMillis() {
        return ConfigurationManager.getProxyConfig().getProxyAdminPeerTimeoutMillis();
    }

    private List<ClientInstance> allClientInstances() {
        List<ClientInstance> list = new ArrayList<>();
        Collection<GrpcClientChannel> channels = grpcChannelManager.getClientChannels();
        for (GrpcClientChannel channel : channels) {
            list.add(buildClientInstance(channel));
        }
        return list;
    }

    private ClientInstance buildClientInstance(GrpcClientChannel channel) {
        String clientId = channel.getClientId();
        ClientInstance.Builder builder = ClientInstance.newBuilder().setClientId(clientId);

        ClientRole role = ClientRole.CLIENT_ROLE_UNSPECIFIED;
        List<String> groups = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        String clientVersion = "";
        Language language = Language.LANGUAGE_UNSPECIFIED;

        Settings settings = grpcClientSettingsManager.getRawClientSettings(clientId);
        if (settings != null) {
            UA ua = settings.getUserAgent();
            if (ua != null) {
                clientVersion = ua.getVersion();
                language = ua.getLanguage();
            }
            role = toClientRole(settings.getClientType());
            if (settings.hasSubscription()) {
                Resource group = settings.getSubscription().getGroup();
                if (group != null && !group.getName().isEmpty()) {
                    groups.add(group.getName());
                }
                for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
                    if (entry.getTopic() != null && !entry.getTopic().getName().isEmpty()) {
                        topics.add(entry.getTopic().getName());
                    }
                }
            }
            if (settings.hasPublishing()) {
                for (Resource topic : settings.getPublishing().getTopicsList()) {
                    if (topic != null && !topic.getName().isEmpty()) {
                        topics.add(topic.getName());
                    }
                }
            }
        }

        builder.setClientVersion(clientVersion);
        builder.setLanguage(language);
        builder.setProtocol(ClientProtocol.CLIENT_PROTOCOL_GRPC);
        builder.setRole(role);
        builder.addAllGroups(groups);
        builder.addAllTopics(topics);
        builder.setAccessPoint(str(channel.getRemoteAddress()));
        builder.setConnectTime(toTimestamp(channel.getConnectTimeMillis()));
        // RIP-2: real liveness — last heartbeat / telemetry observed on this channel.
        builder.setLastActiveTime(toTimestamp(channel.getLastActiveTimeMillis()));
        String authUsername = channel.getAuthUsername();
        if (StringUtils.isNotBlank(authUsername)) {
            builder.setAuthSubject(authUsername);
        }
        builder.setProxyEndpoint(proxyEndpoint);
        builder.setEpoch(epoch);
        return builder.build();
    }

    private AuthStatus buildAuthStatus(GrpcClientChannel channel) {
        String authUsername = channel.getAuthUsername();
        AuthStatus.Builder builder = AuthStatus.newBuilder();
        if (StringUtils.isNotBlank(authUsername)) {
            builder.setAuthenticated(true)
                .setUsername(authUsername)
                .setLastAuthTime(toTimestamp(channel.getLastAuthTimeMillis()));
        } else {
            builder.setAuthenticated(false)
                .setFailureReason("no credentials observed on this connection");
        }
        return builder.build();
    }

    /**
     * Best-effort consume progress: for every topic the client's group subscribes to, query
     * broker-side consume stats through the proxy's own admin gateway and aggregate the lag.
     * Latency is not tracked at the broker offset layer and stays unset.
     */
    private ClientConsumeProgress buildConsumeProgress(GrpcClientChannel channel, Settings settings) {
        ClientConsumeProgress.Builder progress = ClientConsumeProgress.newBuilder();
        try {
            if (!settings.hasSubscription()) {
                return progress.build();
            }
            String group = settings.getSubscription().getGroup().getName();
            if (StringUtils.isBlank(group)) {
                return progress.build();
            }
            long totalLag = 0;
            Set<String> queriedTopics = new HashSet<>();
            for (apache.rocketmq.v2.SubscriptionEntry entry : settings.getSubscription().getSubscriptionsList()) {
                String topic = entry.hasTopic() ? entry.getTopic().getName() : "";
                if (StringUtils.isBlank(topic) || !queriedTopics.add(topic)) {
                    continue;
                }
                long topicLag = queryTopicLag(group, topic);
                if (topicLag >= 0) {
                    totalLag += topicLag;
                    progress.addTopicProgress(ClientTopicProgress.newBuilder()
                        .setTopic(topic)
                        .setLag(topicLag)
                        .build());
                }
            }
            progress.setLag(totalLag);
        } catch (Throwable t) {
            log.warn("buildConsumeProgress failed. clientId:{}", channel.getClientId(), t);
        }
        return progress.build();
    }

    private long queryTopicLag(String group, String topic) {
        try {
            MessageQueueView view = serviceManager.getTopicRouteService()
                .getAllMessageQueueView(ProxyContext.create(), topic);
            return AdminModelConverter.computeTopicLag(serviceManager.getAdminService(), view, group, topic,
                CONSUME_PROGRESS_TIMEOUT_MILLIS);
        } catch (Throwable t) {
            log.warn("queryTopicLag failed. group:{}, topic:{}", group, topic, t);
            return -1;
        }
    }

    private static ClientRole toClientRole(ClientType clientType) {
        if (clientType == null) {
            return ClientRole.CLIENT_ROLE_UNSPECIFIED;
        }
        switch (clientType) {
            case PRODUCER:
                return ClientRole.CLIENT_ROLE_PRODUCER;
            case PUSH_CONSUMER:
            case PULL_CONSUMER:
            case LITE_PUSH_CONSUMER:
                return ClientRole.CLIENT_ROLE_PUSH_CONSUMER;
            case SIMPLE_CONSUMER:
            case LITE_SIMPLE_CONSUMER:
                return ClientRole.CLIENT_ROLE_SIMPLE_CONSUMER;
            default:
                return ClientRole.CLIENT_ROLE_UNSPECIFIED;
        }
    }

    private boolean matchFilter(ClientInstance instance, ClientFilter filter) {
        if (filter.hasGroup() && !instance.getGroupsList().contains(filter.getGroup().getName())) {
            return false;
        }
        if (filter.hasTopic() && !instance.getTopicsList().contains(filter.getTopic().getName())) {
            return false;
        }
        if (filter.hasClientIdPrefix() && !instance.getClientId().startsWith(filter.getClientIdPrefix())) {
            return false;
        }
        if (filter.hasLanguage() && instance.getLanguage() != filter.getLanguage()) {
            return false;
        }
        if (filter.hasRole() && instance.getRole() != filter.getRole()) {
            return false;
        }
        if (filter.hasConnectedAfter() && instance.hasConnectTime()
            && instance.getConnectTime().getSeconds() < filter.getConnectedAfter().getSeconds()) {
            return false;
        }
        if (filter.hasConnectedBefore() && instance.hasConnectTime()
            && instance.getConnectTime().getSeconds() > filter.getConnectedBefore().getSeconds()) {
            return false;
        }
        return true;
    }

    /**
     * D4 stable cursor pagination: instances are sorted by clientId and the cursor is the
     * (opaque, base64-encoded) clientId of the last returned element. Membership churn
     * between calls therefore cannot shift the window.
     */
    private Page page(List<ClientInstance> all, int pageSize, String nextToken) {
        int size = pageSize > 0 ? Math.min(pageSize, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        all.sort(Comparator.comparing(ClientInstance::getClientId));
        String afterClientId = decodeCursor(nextToken);
        int start = 0;
        if (afterClientId != null) {
            for (int i = 0; i < all.size(); i++) {
                if (all.get(i).getClientId().compareTo(afterClientId) > 0) {
                    start = i;
                    break;
                }
                start = i + 1;
            }
        }
        int end = Math.min(start + size, all.size());
        Page page = new Page();
        page.items = new ArrayList<>(all.subList(start, end));
        page.nextToken = end < all.size() && !page.items.isEmpty()
            ? encodeCursor(page.items.get(page.items.size() - 1).getClientId()) : "";
        return page;
    }

    private static String encodeCursor(String clientId) {
        return CURSOR_PREFIX + Base64.getEncoder().encodeToString(clientId.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeCursor(String token) {
        if (token == null || !token.startsWith(CURSOR_PREFIX)) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(token.substring(CURSOR_PREFIX.length())),
                StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return null;
        }
    }

    private static final class Page {
        List<ClientInstance> items;
        String nextToken;
    }

    private static Timestamp toTimestamp(long millis) {
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }

    private static String str(Object address) {
        return address == null ? "" : address.toString();
    }

    private static String resolveProxyEndpoint() {
        try {
            ProxyConfig config = ConfigurationManager.getProxyConfig();
            String addr = config.getLocalServeAddr();
            Integer port = config.getGrpcServerPort();
            String endpoint = (addr == null ? "" : addr) + (port == null ? "" : ":" + port);
            if (endpoint.isEmpty()) {
                endpoint = config.getProxyName();
            }
            return endpoint;
        } catch (Throwable t) {
            return "rocketmq-proxy";
        }
    }

    private Status success() {
        return Status.newBuilder().setCode(Code.OK).build();
    }

    private Status fail(Code code, String message) {
        return Status.newBuilder().setCode(code).setMessage(message == null ? "" : message).build();
    }
}
