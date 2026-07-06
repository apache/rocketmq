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
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminScopeRouter {
    private final ProxyClientAdminActivity localActivity;
    private final ProxyClientAdminCoordinatorService coordinatorService;
    private final boolean coordinatorScopesEnabled;

    public ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService) {
        this(localActivity, coordinatorService, true);
    }

    public ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService, boolean coordinatorScopesEnabled) {
        if (localActivity == null) {
            throw new IllegalArgumentException("localActivity is required");
        }
        if (coordinatorService == null) {
            throw new IllegalArgumentException("coordinatorService is required");
        }
        this.localActivity = localActivity;
        this.coordinatorService = coordinatorService;
        this.coordinatorScopesEnabled = coordinatorScopesEnabled;
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsRequest requiredRequest = this.requireRequest(request);
            switch (this.effectiveScope(requiredRequest.getScope())) {
                case LOCAL_PROXY:
                    return this.localActivity.listClients(ctx, requiredRequest);
                case ALL_PROXIES:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClients(ctx, requiredRequest.toQuery());
                case PROXY_ID:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClients(ctx, requiredRequest.toQuery());
                default:
                    throw this.unsupportedScope("listClients", requiredRequest.getScope());
            }
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViews(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        return this.convertResult(this.listClients(ctx, request), ProxyClientAdminResponseConverter::toPageView);
    }

    public ProxyClientAdminResult<ProxyClientInfo> describeClient(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        return this.execute(() -> {
            ProxyClientAdminDescribeClientRequest requiredRequest = this.requireRequest(request);
            switch (this.effectiveScope(requiredRequest.getScope())) {
                case LOCAL_PROXY:
                    return this.localActivity.describeClient(ctx, requiredRequest);
                case ALL_PROXIES:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.describeClient(ctx, requiredRequest);
                case PROXY_ID:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.describeClient(ctx, requiredRequest);
                default:
                    throw this.unsupportedScope("describeClient", requiredRequest.getScope());
            }
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminClientView> describeClientView(ProxyContext ctx,
        ProxyClientAdminDescribeClientRequest request) {
        return this.convertResult(this.describeClient(ctx, request), ProxyClientAdminResponseConverter::toClientView);
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByGroup(ProxyContext ctx,
        ProxyClientAdminListClientsByGroupRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsByGroupRequest requiredRequest = this.requireRequest(request);
            switch (this.effectiveScope(requiredRequest.getScope())) {
                case LOCAL_PROXY:
                    return this.localActivity.listClientsByGroup(ctx, requiredRequest);
                case ALL_PROXIES:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClientsByGroup(
                        ctx,
                        requiredRequest.getGroup(),
                        requiredRequest.toQuery()
                    );
                case PROXY_ID:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClientsByGroup(
                        ctx,
                        requiredRequest.getGroup(),
                        requiredRequest.toQuery()
                    );
                default:
                    throw this.unsupportedScope("listClientsByGroup", requiredRequest.getScope());
            }
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByGroup(ProxyContext ctx,
        ProxyClientAdminListClientsByGroupRequest request) {
        return this.convertResult(
            this.listClientsByGroup(ctx, request),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    public ProxyClientAdminResult<ProxyClientPage> listClientsByTopic(ProxyContext ctx,
        ProxyClientAdminListClientsByTopicRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsByTopicRequest requiredRequest = this.requireRequest(request);
            switch (this.effectiveScope(requiredRequest.getScope())) {
                case LOCAL_PROXY:
                    return this.localActivity.listClientsByTopic(ctx, requiredRequest);
                case ALL_PROXIES:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClientsByTopic(
                        ctx,
                        requiredRequest.getTopic(),
                        requiredRequest.toQuery()
                    );
                case PROXY_ID:
                    this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                    return this.coordinatorService.listClientsByTopic(
                        ctx,
                        requiredRequest.getTopic(),
                        requiredRequest.toQuery()
                    );
                default:
                    throw this.unsupportedScope("listClientsByTopic", requiredRequest.getScope());
            }
        });
    }

    public ProxyClientAdminResult<ProxyClientAdminPageView> listClientViewsByTopic(ProxyContext ctx,
        ProxyClientAdminListClientsByTopicRequest request) {
        return this.convertResult(
            this.listClientsByTopic(ctx, request),
            ProxyClientAdminResponseConverter::toPageView
        );
    }

    private <T> ProxyClientAdminResult<T> execute(Supplier<ProxyClientAdminResult<T>> supplier) {
        try {
            ProxyClientAdminResult<T> result = this.requireResult(this.requireSupplier(supplier).get());
            if (result.getStatus().getCode() == Code.OK && result.getBody() == null) {
                throw new IllegalStateException("result body is required");
            }
            return result;
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T, R> ProxyClientAdminResult<R> convertResult(ProxyClientAdminResult<T> result,
        Function<T, R> converter) {
        try {
            ProxyClientAdminResult<T> requiredResult = this.requireResult(result);
            if (requiredResult.getStatus().getCode() != Code.OK || requiredResult.getBody() == null) {
                return new ProxyClientAdminResult<>(requiredResult.getStatus(), null);
            }
            return new ProxyClientAdminResult<>(
                requiredResult.getStatus(),
                this.requireConvertedBody(this.requireConverter(converter).apply(requiredResult.getBody()))
            );
        } catch (Throwable t) {
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private ProxyClientScope effectiveScope(ProxyClientScope scope) {
        return scope == null ? ProxyClientScope.LOCAL_PROXY : scope;
    }

    private void requireCoordinatorScopesEnabled(ProxyClientScope scope) {
        if (!this.coordinatorScopesEnabled) {
            throw new IllegalArgumentException(
                "Proxy client admin coordinator scopes are disabled: " + this.effectiveScope(scope)
            );
        }
    }

    private IllegalArgumentException unsupportedScope(String operation, ProxyClientScope scope) {
        return new IllegalArgumentException(
            "Unsupported proxy scope for " + operation + ": " + this.effectiveScope(scope)
        );
    }

    private <T> T requireRequest(T request) {
        if (request == null) {
            throw new IllegalArgumentException("request is required");
        }
        return request;
    }

    private <T> Supplier<ProxyClientAdminResult<T>> requireSupplier(Supplier<ProxyClientAdminResult<T>> supplier) {
        if (supplier == null) {
            throw new IllegalArgumentException("supplier is required");
        }
        return supplier;
    }

    private <T> ProxyClientAdminResult<T> requireResult(ProxyClientAdminResult<T> result) {
        if (result == null) {
            throw new IllegalStateException("result is required");
        }
        return result;
    }

    private <T, R> Function<T, R> requireConverter(Function<T, R> converter) {
        if (converter == null) {
            throw new IllegalArgumentException("converter is required");
        }
        return converter;
    }

    private <T> T requireConvertedBody(T body) {
        if (body == null) {
            throw new IllegalStateException("converted body is required");
        }
        return body;
    }
}
