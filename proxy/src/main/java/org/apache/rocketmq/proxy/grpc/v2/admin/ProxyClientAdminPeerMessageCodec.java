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
import com.alibaba.fastjson2.JSON;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public final class ProxyClientAdminPeerMessageCodec {
    private static final ProxyClientAdminPeerMessageCodec INSTANCE = new ProxyClientAdminPeerMessageCodec();
    private static final int MAX_PEER_MESSAGE_LENGTH = 1024 * 1024;

    private ProxyClientAdminPeerMessageCodec() {
    }

    public static ProxyClientAdminPeerMessageCodec getInstance() {
        return INSTANCE;
    }

    public String encodeRequest(ProxyClientAdminPeerRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("peer request is required");
        }
        RequestPayload payload = new RequestPayload();
        payload.operation = request.getOperation().name();
        payload.clientId = request.getClientId();
        payload.group = request.getGroup();
        payload.topic = request.getTopic();
        payload.clientType = request.getClientType() == null ? null : request.getClientType().name();
        payload.pageSize = request.getPageSize();
        payload.pageToken = request.getPageToken();
        payload.scope = request.getScope() == null ? null : request.getScope().name();
        return JSON.toJSONString(payload);
    }

    public ProxyClientAdminPeerRequest decodeRequest(String message) {
        String requiredMessage = requireMessage(message, "peer request message");
        RequestPayload payload = parseRequestPayload(requiredMessage);
        if (payload == null) {
            throw new IllegalArgumentException("peer request message is required");
        }
        return ProxyClientAdminPeerRequest.newBuilder()
            .setOperation(parseOperation(payload.operation))
            .setClientId(payload.clientId)
            .setGroup(payload.group)
            .setTopic(payload.topic)
            .setClientType(parseClientType(payload.clientType))
            .setPageSize(payload.pageSize)
            .setPageToken(payload.pageToken)
            .setScope(parseScope(payload.scope))
            .build();
    }

    private RequestPayload parseRequestPayload(String message) {
        try {
            return JSON.parseObject(message, RequestPayload.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid peer request message", e);
        }
    }

    public String encodePageResponse(ProxyClientAdminPeerResponse<ProxyClientPage> response) {
        return JSON.toJSONString(toPageResponsePayload(response));
    }

    public ProxyClientAdminPeerResponse<ProxyClientPage> decodePageResponse(String message) {
        ResponsePayload payload = parseResponsePayload(message);
        if (!Boolean.TRUE.equals(payload.success)) {
            return ProxyClientAdminPeerResponse.error(payload.proxyId, payload.errorCode, payload.errorMessage);
        }
        if (payload.client != null) {
            throw new IllegalArgumentException("peer page response must not include client body");
        }
        if (payload.page == null) {
            throw new IllegalArgumentException("peer page response body is required");
        }
        return ProxyClientAdminPeerResponse.success(payload.proxyId, toPage(payload.page));
    }

    public String encodeClientResponse(ProxyClientAdminPeerResponse<ProxyClientInfo> response) {
        return JSON.toJSONString(toClientResponsePayload(response));
    }

    public ProxyClientAdminPeerResponse<ProxyClientInfo> decodeClientResponse(String message) {
        ResponsePayload payload = parseResponsePayload(message);
        if (!Boolean.TRUE.equals(payload.success)) {
            return ProxyClientAdminPeerResponse.error(payload.proxyId, payload.errorCode, payload.errorMessage);
        }
        if (payload.page != null) {
            throw new IllegalArgumentException("peer client response must not include page body");
        }
        if (payload.client == null) {
            throw new IllegalArgumentException("peer client response body is required");
        }
        return ProxyClientAdminPeerResponse.success(payload.proxyId, toClientInfo(payload.client));
    }

    private ResponsePayload toPageResponsePayload(ProxyClientAdminPeerResponse<ProxyClientPage> response) {
        ResponsePayload payload = toResponsePayload(response);
        if (payload.success) {
            payload.page = toPagePayload(response.getBody());
        }
        return payload;
    }

    private ResponsePayload toClientResponsePayload(ProxyClientAdminPeerResponse<ProxyClientInfo> response) {
        ResponsePayload payload = toResponsePayload(response);
        if (payload.success) {
            payload.client = toClientPayload(response.getBody());
        }
        return payload;
    }

    private ResponsePayload toResponsePayload(ProxyClientAdminPeerResponse<?> response) {
        if (response == null) {
            throw new IllegalArgumentException("peer response is required");
        }
        ResponsePayload payload = new ResponsePayload();
        payload.proxyId = response.getProxyId();
        payload.success = response.isSuccess();
        payload.errorCode = response.getErrorCode();
        payload.errorMessage = response.getErrorMessage();
        return payload;
    }

    private ResponsePayload parseResponsePayload(String message) {
        String requiredMessage = requireMessage(message, "peer response message");
        ResponsePayload payload = parseResponsePayloadBody(requiredMessage);
        if (payload == null) {
            throw new IllegalArgumentException("peer response message is required");
        }
        if (payload.success == null) {
            throw new IllegalArgumentException("peer response success flag is required");
        }
        if (!Boolean.TRUE.equals(payload.success) && (payload.page != null || payload.client != null)) {
            throw new IllegalArgumentException("peer error response must not include body");
        }
        return payload;
    }

    private ResponsePayload parseResponsePayloadBody(String message) {
        try {
            return JSON.parseObject(message, ResponsePayload.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid peer response message", e);
        }
    }

    private PagePayload toPagePayload(ProxyClientPage page) {
        if (page == null) {
            throw new IllegalArgumentException("peer page response body is required");
        }
        PagePayload payload = new PagePayload();
        payload.clients = new ArrayList<>();
        for (ProxyClientInfo clientInfo : page.getClients()) {
            payload.clients.add(toClientPayload(clientInfo));
        }
        payload.nextPageToken = page.getNextPageToken();
        return payload;
    }

    private ProxyClientPage toPage(PagePayload payload) {
        if (payload.clients == null) {
            throw new IllegalArgumentException("peer page clients are required");
        }
        List<ProxyClientInfo> clients = new ArrayList<>();
        for (ClientPayload clientPayload : payload.clients) {
            clients.add(toClientInfo(clientPayload));
        }
        return new ProxyClientPage(clients, payload.nextPageToken);
    }

    private ClientPayload toClientPayload(ProxyClientInfo clientInfo) {
        if (clientInfo == null) {
            throw new IllegalArgumentException("peer client is required");
        }
        ClientPayload payload = new ClientPayload();
        payload.clientId = clientInfo.getClientId();
        payload.clientType = clientInfo.getClientType() == null ? null : clientInfo.getClientType().name();
        payload.groups = sortedList(clientInfo.getGroups());
        payload.topics = sortedList(clientInfo.getTopics());
        payload.language = clientInfo.getLanguage();
        payload.remoteAddress = clientInfo.getRemoteAddress();
        payload.localAddress = clientInfo.getLocalAddress();
        payload.clientVersion = clientInfo.getClientVersion();
        payload.proxyId = clientInfo.getProxyId();
        payload.connectTimeMillis = clientInfo.getConnectTimeMillis();
        payload.lastActiveTimeMillis = clientInfo.getLastActiveTimeMillis();
        return payload;
    }

    private ProxyClientInfo toClientInfo(ClientPayload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("peer client is required");
        }
        return new ProxyClientInfo(
            payload.clientId,
            parseClientType(payload.clientType),
            toSet(payload.groups),
            toSet(payload.topics),
            payload.language,
            payload.remoteAddress,
            payload.localAddress,
            payload.clientVersion,
            payload.proxyId,
            payload.connectTimeMillis,
            payload.lastActiveTimeMillis
        );
    }

    private static List<String> sortedList(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>(values);
        Collections.sort(result);
        return result;
    }

    private static Set<String> toSet(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(values);
    }

    private static ProxyClientAdminPeerOperation parseOperation(String operation) {
        String normalizedOperation = StringUtils.trimToNull(operation);
        if (normalizedOperation == null) {
            return null;
        }
        try {
            return ProxyClientAdminPeerOperation.valueOf(normalizedOperation);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported peer operation: " + normalizedOperation, e);
        }
    }

    private static ClientType parseClientType(String clientType) {
        String normalizedClientType = StringUtils.trimToNull(clientType);
        if (normalizedClientType == null) {
            return null;
        }
        try {
            return ClientType.valueOf(normalizedClientType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported peer clientType: " + normalizedClientType, e);
        }
    }

    private static ProxyClientScope parseScope(String scope) {
        String normalizedScope = StringUtils.trimToNull(scope);
        if (normalizedScope == null) {
            return ProxyClientScope.LOCAL_PROXY;
        }
        try {
            return ProxyClientScope.valueOf(normalizedScope);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported peer scope: " + normalizedScope, e);
        }
    }

    private static String requireMessage(String message, String messageName) {
        String normalizedMessage = StringUtils.trimToNull(message);
        if (normalizedMessage == null) {
            throw new IllegalArgumentException(messageName + " is required");
        }
        if (normalizedMessage.length() > MAX_PEER_MESSAGE_LENGTH) {
            throw new IllegalArgumentException(
                messageName + " length exceeds " + MAX_PEER_MESSAGE_LENGTH
            );
        }
        return normalizedMessage;
    }

    public static class RequestPayload {
        public String operation;
        public String clientId;
        public String group;
        public String topic;
        public String clientType;
        public int pageSize;
        public String pageToken;
        public String scope;
    }

    public static class ResponsePayload {
        public String proxyId;
        public Boolean success;
        public String errorCode;
        public String errorMessage;
        public PagePayload page;
        public ClientPayload client;
    }

    public static class PagePayload {
        public List<ClientPayload> clients;
        public String nextPageToken;
    }

    public static class ClientPayload {
        public String clientId;
        public String clientType;
        public List<String> groups;
        public List<String> topics;
        public String language;
        public String remoteAddress;
        public String localAddress;
        public String clientVersion;
        public String proxyId;
        public long connectTimeMillis;
        public long lastActiveTimeMillis;
    }
}
