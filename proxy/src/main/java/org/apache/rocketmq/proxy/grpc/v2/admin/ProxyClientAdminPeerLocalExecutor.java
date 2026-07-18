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
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;

public class ProxyClientAdminPeerLocalExecutor {
    private final String localProxyId;
    private final ProxyClientAdminActivity activity;
    private final ClientAdminService clientAdminService;

    public ProxyClientAdminPeerLocalExecutor(String localProxyId, ProxyClientAdminActivity activity) {
        String normalizedLocalProxyId = ProxyClientAdminPeerIds.requireLocalProxyId(localProxyId);
        if (activity == null) {
            throw new IllegalArgumentException("activity is required");
        }
        this.localProxyId = normalizedLocalProxyId;
        this.activity = activity;
        this.clientAdminService = null;
    }

    public ProxyClientAdminPeerLocalExecutor(String localProxyId, ClientAdminService clientAdminService) {
        String normalizedLocalProxyId = ProxyClientAdminPeerIds.requireLocalProxyId(localProxyId);
        if (clientAdminService == null) {
            throw new IllegalArgumentException("clientAdminService is required");
        }
        this.localProxyId = normalizedLocalProxyId;
        this.activity = null;
        this.clientAdminService = clientAdminService;
    }

    String getLocalProxyId() {
        return localProxyId;
    }

    public ProxyClientAdminPeerResponse<?> execute(ProxyContext ctx, ProxyClientAdminPeerRequest request) {
        try {
            ProxyContext requiredContext = this.requireContext(ctx);
            ProxyClientAdminPeerRequest requiredRequest = this.requireRequest(request);
            switch (requiredRequest.getOperation()) {
                case LIST_CLIENTS:
                    return this.toPeerResponse(this.listClients(requiredContext, requiredRequest));
                case DESCRIBE_CLIENT:
                    return this.toPeerResponse(this.describeClient(requiredContext, requiredRequest));
                case LIST_CLIENTS_BY_GROUP:
                    return this.toPeerResponse(this.listClientsByGroup(requiredContext, requiredRequest));
                case LIST_CLIENTS_BY_TOPIC:
                    return this.toPeerResponse(this.listClientsByTopic(requiredContext, requiredRequest));
                default:
                    throw new IllegalStateException("Unsupported peer operation: " + requiredRequest.getOperation());
            }
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            return ProxyClientAdminPeerResponse.error(
                localProxyId,
                Code.INTERNAL_SERVER_ERROR.name(),
                StringUtils.defaultIfBlank(t.getMessage(), t.getClass().getSimpleName())
            );
        }
    }

    private ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx,
        ProxyClientAdminPeerRequest request) {
        if (this.clientAdminService != null) {
            return this.executeClientAdminService(() -> this.clientAdminService.listClients(request.toLocalQuery()));
        }
        return this.activity.listClients(ctx, request.toLocalQuery());
    }

    private ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx,
        ProxyClientAdminPeerRequest request) {
        if (this.clientAdminService != null) {
            return this.executeClientAdminService(() -> this.clientAdminService.describeClient(request.getClientId()));
        }
        return this.activity.describeClient(ctx, request.toLocalDescribeClientRequest());
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx,
        ProxyClientAdminPeerRequest request) {
        if (this.clientAdminService != null) {
            return this.executeClientAdminService(() -> this.clientAdminService.listClientsByGroup(
                request.getGroup(),
                request.toLocalQuery()
            ));
        }
        return this.activity.listClientsByGroup(ctx, request.getGroup(), request.toLocalQuery());
    }

    private ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx,
        ProxyClientAdminPeerRequest request) {
        if (this.clientAdminService != null) {
            return this.executeClientAdminService(() -> this.clientAdminService.listClientsByTopic(
                request.getTopic(),
                request.toLocalQuery()
            ));
        }
        return this.activity.listClientsByTopic(ctx, request.getTopic(), request.toLocalQuery());
    }

    private <T> ProxyClientAdminResult<T> executeClientAdminService(Supplier<T> supplier) {
        try {
            return new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                supplier.get()
            );
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
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

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }
}
