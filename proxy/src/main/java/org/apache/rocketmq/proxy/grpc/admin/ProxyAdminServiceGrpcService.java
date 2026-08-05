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

import apache.rocketmq.v2.ClientDetail;
import apache.rocketmq.v2.ClientFilter;
import apache.rocketmq.v2.ClientInstance;
import apache.rocketmq.v2.ClientProtocol;
import apache.rocketmq.v2.ClientRole;
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.Language;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.NetworkInfo;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.PublishSettings;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.UA;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.ServiceManager;

/**
 * RIP-2 Proxy Admin gRPC service — M1 online client query.
 *
 * <p>This service is protocol-pure: it only depends on the RIP-2 gRPC contract
 * ({@code apache.rocketmq.v2.*}, generated from rocketmq-apis). It exposes the
 * {@code ProxyAdminService} RPCs that query the clients currently connected to
 * THIS proxy node, reading from the proxy's own {@link GrpcChannelManager}
 * (the authority on gRPC clients attached to the proxy) and the
 * {@link GrpcClientSettingsManager} (per-client settings / subscriptions /
 * publishing config).
 *
 * <p>Design notes (RIP-2):
 * <ul>
 *   <li>A proxy returns only its LOCAL view, tagged with {@code proxy_endpoint}
 *       + {@code epoch} so a dashboard / CLI can dedup across proxies (D3).</li>
 *   <li>Pagination is cursor-based via {@code next_token} (D4).</li>
 *   <li>Every capability is served from the proxy itself; it never opens a
 *       direct link to the broker.</li>
 * </ul>
 */
public class ProxyAdminServiceGrpcService extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ProxyAdminServiceGrpcService.class);

    // Server-enforced maximum page size for cursor pagination (D4).
    private static final int MAX_PAGE_SIZE = 1000;
    private static final int DEFAULT_PAGE_SIZE = 100;

    private final ServiceManager serviceManager;
    private final MessagingProcessor messagingProcessor;
    private final GrpcChannelManager grpcChannelManager;
    private final GrpcClientSettingsManager grpcClientSettingsManager;

    private final String proxyEndpoint;
    private final long epoch;

    public ProxyAdminServiceGrpcService(ServiceManager serviceManager, MessagingProcessor messagingProcessor,
        GrpcChannelManager grpcChannelManager, GrpcClientSettingsManager grpcClientSettingsManager) {
        this.serviceManager = serviceManager;
        this.messagingProcessor = messagingProcessor;
        this.grpcChannelManager = grpcChannelManager;
        this.grpcClientSettingsManager = grpcClientSettingsManager;
        this.proxyEndpoint = resolveProxyEndpoint();
        this.epoch = System.currentTimeMillis();
    }

    // -------------------------------------------------------------------------
    // M1: online client query
    // -------------------------------------------------------------------------

    @Override
    public void listClients(ListClientsRequest request, StreamObserver<ListClientsResponse> responseObserver) {
        try {
            ClientFilter filter = request.hasFilter() ? request.getFilter() : null;
            List<ClientInstance> all = allClientInstances();
            List<ClientInstance> filtered = new ArrayList<>();
            for (ClientInstance instance : all) {
                if (matchFilter(instance, filter)) {
                    filtered.add(instance);
                }
            }
            Page page = page(filtered, request.getPageSize(), request.getNextToken());
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
        } catch (Throwable t) {
            log.error("listClients failed", t);
            responseObserver.onNext(ListClientsResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void describeClient(DescribeClientRequest request, StreamObserver<DescribeClientResponse> responseObserver) {
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
            if (settings != null) {
                detail.setSettings(settings);
                if (settings.hasSubscription()) {
                    detail.addAllSubscriptions(settings.getSubscription().getSubscriptionsList());
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
        } catch (Throwable t) {
            log.error("describeClient failed", t);
            responseObserver.onNext(DescribeClientResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listClientsByGroup(ListClientsByGroupRequest request, StreamObserver<ListClientsByGroupResponse> responseObserver) {
        try {
            String group = request.hasGroup() ? request.getGroup().getName() : "";
            List<ClientInstance> all = allClientInstances();
            List<ClientInstance> filtered = new ArrayList<>();
            for (ClientInstance instance : all) {
                if (group.isEmpty() || instance.getGroupsList().contains(group)) {
                    filtered.add(instance);
                }
            }
            Page page = page(filtered, request.getPageSize(), request.getNextToken());
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
        } catch (Throwable t) {
            log.error("listClientsByGroup failed", t);
            responseObserver.onNext(ListClientsByGroupResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listClientsByTopic(ListClientsByTopicRequest request, StreamObserver<ListClientsByTopicResponse> responseObserver) {
        try {
            String topic = request.hasTopic() ? request.getTopic().getName() : "";
            List<ClientInstance> all = allClientInstances();
            List<ClientInstance> filtered = new ArrayList<>();
            for (ClientInstance instance : all) {
                if (topic.isEmpty() || instance.getTopicsList().contains(topic)) {
                    filtered.add(instance);
                }
            }
            Page page = page(filtered, request.getPageSize(), request.getNextToken());
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
        } catch (Throwable t) {
            log.error("listClientsByTopic failed", t);
            responseObserver.onNext(ListClientsByTopicResponse.newBuilder().setStatus(fail(Code.INTERNAL_ERROR, t.getMessage())).build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

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
        builder.setLastActiveTime(toTimestamp(System.currentTimeMillis()));
        builder.setProxyEndpoint(proxyEndpoint);
        builder.setEpoch(epoch);
        return builder.build();
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
        if (filter == null) {
            return true;
        }
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

    private Page page(List<ClientInstance> all, int pageSize, String nextToken) {
        int size = pageSize > 0 ? Math.min(pageSize, MAX_PAGE_SIZE) : DEFAULT_PAGE_SIZE;
        int start = 0;
        if (nextToken != null && !nextToken.isEmpty()) {
            try {
                start = Integer.parseInt(nextToken);
            } catch (NumberFormatException e) {
                start = 0;
            }
        }
        if (start < 0 || start > all.size()) {
            start = 0;
        }
        int end = Math.min(start + size, all.size());
        Page page = new Page();
        page.items = all.subList(start, end);
        page.nextToken = end < all.size() ? String.valueOf(end) : "";
        return page;
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
        return Status.newBuilder().setCode(code).setMessage(message).build();
    }
}
