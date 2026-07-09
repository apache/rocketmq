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

import apache.rocketmq.v2.DescribeClientRequest;
import apache.rocketmq.v2.DescribeClientResponse;
import apache.rocketmq.v2.ListClientsByGroupRequest;
import apache.rocketmq.v2.ListClientsByGroupResponse;
import apache.rocketmq.v2.ListClientsByTopicRequest;
import apache.rocketmq.v2.ListClientsByTopicResponse;
import apache.rocketmq.v2.ListClientsRequest;
import apache.rocketmq.v2.ListClientsResponse;
import apache.rocketmq.v2.ProxyAdminServiceGrpc;
import apache.rocketmq.v2.ProxyClient;
import apache.rocketmq.v2.ProxyScope;
import apache.rocketmq.v2.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class GrpcProxyAdminApplication extends ProxyAdminServiceGrpc.ProxyAdminServiceImplBase {
    private final ProxyClientAdminEndpointExecutor endpointExecutor;
    private final ProxyClientAdminRequestConverter requestConverter;

    public GrpcProxyAdminApplication(ProxyClientAdminEndpointExecutor endpointExecutor) {
        if (endpointExecutor == null) {
            throw new IllegalArgumentException("endpointExecutor is required");
        }
        this.endpointExecutor = endpointExecutor;
        this.requestConverter = ProxyClientAdminRequestConverter.getInstance();
    }

    @Override
    public void listClients(ListClientsRequest request, StreamObserver<ListClientsResponse> responseObserver) {
        this.endpointExecutor.listClients(
            request,
            this::toListClientsRequest,
            responseObserver,
            this::toListClientsResponse
        );
    }

    @Override
    public void describeClient(DescribeClientRequest request,
        StreamObserver<DescribeClientResponse> responseObserver) {
        this.endpointExecutor.describeClient(
            request,
            this::toDescribeClientRequest,
            responseObserver,
            this::toDescribeClientResponse
        );
    }

    @Override
    public void listClientsByGroup(ListClientsByGroupRequest request,
        StreamObserver<ListClientsByGroupResponse> responseObserver) {
        this.endpointExecutor.listClientsByGroup(
            request,
            this::toListClientsByGroupRequest,
            responseObserver,
            this::toListClientsByGroupResponse
        );
    }

    @Override
    public void listClientsByTopic(ListClientsByTopicRequest request,
        StreamObserver<ListClientsByTopicResponse> responseObserver) {
        this.endpointExecutor.listClientsByTopic(
            request,
            this::toListClientsByTopicRequest,
            responseObserver,
            this::toListClientsByTopicResponse
        );
    }

    private ProxyClientAdminListClientsRequest toListClientsRequest(ListClientsRequest request) {
        return this.requestConverter.toListClientsRequest(
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getGroup(),
            request.getTopic(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminDescribeClientRequest toDescribeClientRequest(DescribeClientRequest request) {
        return this.requestConverter.toDescribeClientRequest(
            request.getClientId(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminListClientsByGroupRequest toListClientsByGroupRequest(
        ListClientsByGroupRequest request) {
        return this.requestConverter.toListClientsByGroupRequest(
            request.getGroup(),
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ProxyClientAdminListClientsByTopicRequest toListClientsByTopicRequest(
        ListClientsByTopicRequest request) {
        return this.requestConverter.toListClientsByTopicRequest(
            request.getTopic(),
            request.getClientId(),
            request.getClientIdPrefix(),
            request.getClientLanguage(),
            optionalLong(request.hasConnectTimeStartMillis(), request.getConnectTimeStartMillis()),
            optionalLong(request.hasConnectTimeEndMillis(), request.getConnectTimeEndMillis()),
            pageNumOrDefault(request.getPageNum()),
            request.getPageSize(),
            scopeName(request.getScope()),
            request.getProxyId()
        );
    }

    private ListClientsResponse toListClientsResponse(Status status, ProxyClientAdminPageView pageView) {
        ListClientsResponse.Builder builder = ListClientsResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private DescribeClientResponse toDescribeClientResponse(Status status, ProxyClientAdminClientView clientView) {
        DescribeClientResponse.Builder builder = DescribeClientResponse.newBuilder().setStatus(status);
        if (clientView != null) {
            builder.setClient(toProxyClient(clientView));
        }
        return builder.build();
    }

    private ListClientsByGroupResponse toListClientsByGroupResponse(Status status,
        ProxyClientAdminPageView pageView) {
        ListClientsByGroupResponse.Builder builder = ListClientsByGroupResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private ListClientsByTopicResponse toListClientsByTopicResponse(Status status,
        ProxyClientAdminPageView pageView) {
        ListClientsByTopicResponse.Builder builder = ListClientsByTopicResponse.newBuilder().setStatus(status);
        if (pageView != null) {
            builder.addAllClients(toProxyClients(pageView.getClients()));
            builder.setHasMore(StringUtils.isNotBlank(pageView.getNextPageToken()));
        }
        return builder.build();
    }

    private static List<ProxyClient> toProxyClients(List<ProxyClientAdminClientView> clientViews) {
        List<ProxyClient> clients = new ArrayList<>(clientViews.size());
        for (ProxyClientAdminClientView clientView : clientViews) {
            clients.add(toProxyClient(clientView));
        }
        return clients;
    }

    private static ProxyClient toProxyClient(ProxyClientAdminClientView clientView) {
        return ProxyClient.newBuilder()
            .setClientId(clientView.getClientId())
            .setClientType(clientView.getClientType())
            .addAllGroups(clientView.getGroups())
            .addAllTopics(clientView.getTopics())
            .setLanguage(clientView.getLanguage())
            .setRemoteAddress(clientView.getRemoteAddress())
            .setLocalAddress(clientView.getLocalAddress())
            .setVersion(clientView.getClientVersion())
            .setConnectTimeMillis(clientView.getConnectTimeMillis())
            .setLastActiveTimeMillis(clientView.getLastActiveTimeMillis())
            .setProxyId(clientView.getProxyId())
            .build();
    }

    private static Long optionalLong(boolean present, long value) {
        return present ? value : null;
    }

    private static int pageNumOrDefault(int pageNum) {
        if (pageNum == 0) {
            return 1;
        }
        return pageNum;
    }

    private static String scopeName(ProxyScope scope) {
        if (scope == null) {
            return ProxyScope.PROXY_SCOPE_UNSPECIFIED.name();
        }
        if (scope == ProxyScope.PROXY_SCOPE_ALL_PROXIES || scope == ProxyScope.PROXY_SCOPE_PROXY_ID) {
            throw new IllegalArgumentException(
                "public proxy admin endpoint only supports LOCAL_PROXY scope: " + scope.name());
        }
        return scope.name();
    }
}
