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

import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerLocalExecutor {
    private final String localProxyId;
    private final ProxyClientAdminActivity activity;

    public ProxyClientAdminPeerLocalExecutor(String localProxyId, ProxyClientAdminActivity activity) {
        String normalizedLocalProxyId = StringUtils.trimToNull(localProxyId);
        if (normalizedLocalProxyId == null) {
            throw new IllegalArgumentException("localProxyId is required");
        }
        if (activity == null) {
            throw new IllegalArgumentException("activity is required");
        }
        this.localProxyId = normalizedLocalProxyId;
        this.activity = activity;
    }

    public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, ProxyClientAdminPeerRequest request) {
        ProxyContext requiredContext = this.requireContext(ctx);
        ProxyClientAdminPeerRequest requiredRequest = this.requireRequest(request);
        try {
            switch (requiredRequest.getOperation()) {
                case LIST_CLIENTS:
                    return this.toPeerResponse(this.activity.listClients(requiredContext,
                        requiredRequest.toLocalQuery()));
                case DESCRIBE_CLIENT:
                    return this.toPeerResponse(
                        this.activity.describeClient(requiredContext, requiredRequest.toLocalDescribeClientRequest())
                    );
                case LIST_CLIENTS_BY_GROUP:
                    return this.toPeerResponse(this.activity.listClientsByGroup(
                        requiredContext,
                        requiredRequest.getGroup(),
                        requiredRequest.toLocalQuery()
                    ));
                case LIST_CLIENTS_BY_TOPIC:
                    return this.toPeerResponse(this.activity.listClientsByTopic(
                        requiredContext,
                        requiredRequest.getTopic(),
                        requiredRequest.toLocalQuery()
                    ));
                default:
                    throw new IllegalStateException("Unsupported peer operation: " + requiredRequest.getOperation());
            }
        } catch (Throwable t) {
            return ProxyClientAdminPeerResponse.error(
                localProxyId,
                Code.INTERNAL_SERVER_ERROR.name(),
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
            );
        }
    }

    private <T> ProxyClientAdminPeerResponse<T> toPeerResponse(ProxyClientAdminResult<T> result) {
        if (result == null) {
            return ProxyClientAdminPeerResponse.error(localProxyId, Code.INTERNAL_SERVER_ERROR.name(),
                "peer result is required");
        }
        if (result.getStatus().getCode() == Code.OK) {
            if (result.getBody() == null) {
                return ProxyClientAdminPeerResponse.error(localProxyId, Code.INTERNAL_SERVER_ERROR.name(),
                    "peer result body is required");
            }
            return ProxyClientAdminPeerResponse.success(localProxyId, this.stampLocalProxyId(result.getBody()));
        }
        return ProxyClientAdminPeerResponse.error(
            localProxyId,
            result.getStatus().getCode().name(),
            result.getStatus().getMessage()
        );
    }

    @SuppressWarnings("unchecked")
    private <T> T stampLocalProxyId(T body) {
        if (body instanceof ProxyClientPage) {
            return (T) this.stampPage((ProxyClientPage) body);
        }
        if (body instanceof ProxyClientInfo) {
            return (T) this.stampClient((ProxyClientInfo) body);
        }
        return body;
    }

    private ProxyClientPage stampPage(ProxyClientPage page) {
        List<ProxyClientInfo> stampedClients = new ArrayList<>(page.getClients().size());
        for (ProxyClientInfo clientInfo : page.getClients()) {
            stampedClients.add(this.stampClient(clientInfo));
        }
        return new ProxyClientPage(stampedClients, page.getNextPageToken());
    }

    private ProxyClientInfo stampClient(ProxyClientInfo clientInfo) {
        return new ProxyClientInfo(
            clientInfo.getClientId(),
            clientInfo.getClientType(),
            clientInfo.getGroups(),
            clientInfo.getTopics(),
            clientInfo.getLanguage(),
            clientInfo.getRemoteAddress(),
            clientInfo.getLocalAddress(),
            clientInfo.getClientVersion(),
            localProxyId,
            clientInfo.getConnectTimeMillis(),
            clientInfo.getLastActiveTimeMillis()
        );
    }

    private ProxyContext requireContext(ProxyContext ctx) {
        if (ctx == null) {
            throw new IllegalArgumentException("proxyContext is required");
        }
        return ctx;
    }

    private ProxyClientAdminPeerRequest requireRequest(ProxyClientAdminPeerRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        return request;
    }
}
