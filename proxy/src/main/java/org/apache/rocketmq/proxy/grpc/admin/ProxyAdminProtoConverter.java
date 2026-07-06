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

import org.apache.rocketmq.proxy.grpc.admin.model.ClientDetailInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ClientInstanceInfo;
import org.apache.rocketmq.proxy.grpc.admin.model.ListClientsFilter;
import org.apache.rocketmq.proxy.service.admin.ProxyAdminClientService.ListClientsResult;
import apache.rocketmq.proxy.admin.v1.AdminCode;
import apache.rocketmq.proxy.admin.v1.AuthStatus;
import apache.rocketmq.proxy.admin.v1.ClientDetail;
import apache.rocketmq.proxy.admin.v1.ClientInstance;
import apache.rocketmq.proxy.admin.v1.ClientLanguage;
import apache.rocketmq.proxy.admin.v1.ClientProtocol;
import apache.rocketmq.proxy.admin.v1.ClientRole;
import apache.rocketmq.proxy.admin.v1.ClientSettings;
import apache.rocketmq.proxy.admin.v1.ConsumeProgress;
import apache.rocketmq.proxy.admin.v1.HeartbeatRecord;
import apache.rocketmq.proxy.admin.v1.ListClientsRequest;
import apache.rocketmq.proxy.admin.v1.ListClientsResponse;
import apache.rocketmq.proxy.admin.v1.DescribeClientResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByGroupResponse;
import apache.rocketmq.proxy.admin.v1.ListClientsByTopicResponse;
import apache.rocketmq.proxy.admin.v1.NetworkInfo;
import apache.rocketmq.proxy.admin.v1.Pagination;
import apache.rocketmq.proxy.admin.v1.TopicConsumeProgress;

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
                return ClientLanguage.CLIENT_LANGUAGE_NODEJS;
            case "CSHARP":
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
}