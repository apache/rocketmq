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
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.AuthorizingClientAdminService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminActivity {
    private final AuthorizingClientAdminService clientAdminService;

    public ProxyClientAdminActivity(AuthorizingClientAdminService clientAdminService) {
        if (clientAdminService == null) {
            throw new IllegalArgumentException("clientAdminService is required");
        }
        this.clientAdminService = clientAdminService;
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx, ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClients(ClientAdminRequestContext.from(ctx), query));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        return this.execute(() -> this.clientAdminService.listClients(
            ClientAdminRequestContext.from(ctx),
            this.requireRequest(request).toQuery()
        ));
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViews(ProxyContext ctx,
        ProxyClientQuery query) {
        return this.convertResult(this.listClients(ctx, query), ProxyClientAdminResponseConverter::toPageView);
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViews(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        return this.convertResult(this.listClients(ctx, request), ProxyClientAdminResponseConverter::toPageView);
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx, String clientId) {
        return this.describeClient(ctx, clientId, ProxyClientScope.LOCAL_PROXY);
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        return this.execute(() -> {
            ProxyClientAdminDescribeClientRequest requiredRequest = this.requireRequest(request);
            this.validateLocalProxyScope(requiredRequest.getScope());
            return this.clientAdminService.describeClient(
                ClientAdminRequestContext.from(ctx),
                requiredRequest.getClientId()
            );
        });
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx, String clientId,
        ProxyClientScope scope) {
        return this.execute(() -> {
            this.validateLocalProxyScope(scope);
            return this.clientAdminService.describeClient(ClientAdminRequestContext.from(ctx), clientId);
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminClientView> describeClientView(ProxyContext ctx, String clientId) {
        return this.describeClientView(ctx, clientId, ProxyClientScope.LOCAL_PROXY);
    }

    public ProxyClientAdminResult<ProxyClientAdminClientView> describeClientView(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        return this.convertResult(this.describeClient(ctx, request), ProxyClientAdminResponseConverter::toClientView);
    }

    public ProxyClientAdminResult<ProxyClientAdminClientView> describeClientView(ProxyContext ctx, String clientId,
        ProxyClientScope scope) {
        return this.convertResult(
            this.describeClient(ctx, clientId, scope),
            ProxyClientAdminResponseConverter::toClientView
        );
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx, String group,
        ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClientsByGroup(
            ClientAdminRequestContext.from(ctx),
            group,
            query
        ));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx,
        ProxyClientAdminListClientsByGroupRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsByGroupRequest requiredRequest = this.requireRequest(request);
            return this.clientAdminService.listClientsByGroup(
                ClientAdminRequestContext.from(ctx),
                requiredRequest.getGroup(),
                requiredRequest.toQuery()
            );
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByGroup(ProxyContext ctx, String group,
        ProxyClientQuery query) {
        return this.convertResult(
            this.listClientsByGroup(ctx, group, query),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByGroup(ProxyContext ctx,
        ProxyClientAdminListClientsByGroupRequest request) {
        return this.convertResult(
            this.listClientsByGroup(ctx, request),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx, String topic,
        ProxyClientQuery query) {
        return this.execute(() -> this.clientAdminService.listClientsByTopic(
            ClientAdminRequestContext.from(ctx),
            topic,
            query
        ));
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx,
        ProxyClientAdminListClientsByTopicRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsByTopicRequest requiredRequest = this.requireRequest(request);
            return this.clientAdminService.listClientsByTopic(
                ClientAdminRequestContext.from(ctx),
                requiredRequest.getTopic(),
                requiredRequest.toQuery()
            );
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByTopic(ProxyContext ctx, String topic,
        ProxyClientQuery query) {
        return this.convertResult(
            this.listClientsByTopic(ctx, topic, query),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByTopic(ProxyContext ctx,
        ProxyClientAdminListClientsByTopicRequest request) {
        return this.convertResult(
            this.listClientsByTopic(ctx, request),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<T> supplier) {
        try {
            T body = supplier.get();
            if (body == null) {
                throw new IllegalStateException("result body is required");
            }
            return new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()),
                body
            );
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T, R> ProxyClientAdminResult<R> convertResult(ProxyClientAdminResult<T> result,
        Function<T, R> converter) {
        if (result.getStatus().getCode() != Code.OK || result.getBody() == null) {
            return new ProxyClientAdminResult<>(result.getStatus(), null);
        }
        try {
            return new ProxyClientAdminResult<>(result.getStatus(), converter.apply(result.getBody()));
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T> T requireRequest(T request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        return request;
    }

    private void validateLocalProxyScope(ProxyClientScope scope) {
        ProxyClientScope effectiveScope = scope == null ? ProxyClientScope.LOCAL_PROXY : scope;
        if (effectiveScope != ProxyClientScope.LOCAL_PROXY) {
            throw new IllegalArgumentException("Unsupported proxy scope: " + effectiveScope);
        }
    }
}
