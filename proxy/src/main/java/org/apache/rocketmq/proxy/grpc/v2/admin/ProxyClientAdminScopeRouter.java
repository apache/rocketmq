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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminAuthorizationService;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsRecorder;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsResult;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminRequestContext;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientPage;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

public class ProxyClientAdminScopeRouter {
    private static final ClientAdminMetricsRecorder NOOP_METRICS_RECORDER = (operation, result, latencyMillis,
        scope) -> {
    };
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ProxyClientAdminActivity localActivity;
    private final ProxyClientAdminCoordinatorService coordinatorService;
    private final boolean coordinatorScopesEnabled;
    private final ClientAdminAuthorizationService coordinatorAuthorizationService;
    private final ClientAdminMetricsRecorder coordinatorMetricsRecorder;
    private final LongSupplier nanoTimeSupplier;

    public ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService) {
        this(localActivity, coordinatorService, true);
    }

    public ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService, boolean coordinatorScopesEnabled) {
        this(localActivity, coordinatorService, coordinatorScopesEnabled, null, null);
    }

    public ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService, boolean coordinatorScopesEnabled,
        ClientAdminAuthorizationService coordinatorAuthorizationService,
        ClientAdminMetricsRecorder coordinatorMetricsRecorder) {
        this(
            localActivity,
            coordinatorService,
            coordinatorScopesEnabled,
            coordinatorAuthorizationService,
            coordinatorMetricsRecorder,
            System::nanoTime
        );
    }

    ProxyClientAdminScopeRouter(ProxyClientAdminActivity localActivity,
        ProxyClientAdminCoordinatorService coordinatorService, boolean coordinatorScopesEnabled,
        ClientAdminAuthorizationService coordinatorAuthorizationService,
        ClientAdminMetricsRecorder coordinatorMetricsRecorder, LongSupplier nanoTimeSupplier) {
        if (localActivity == null) {
            throw new IllegalArgumentException("localActivity is required");
        }
        if (coordinatorScopesEnabled && coordinatorService == null) {
            throw new IllegalArgumentException("coordinatorService is required");
        }
        this.localActivity = localActivity;
        this.coordinatorService = coordinatorService;
        this.coordinatorScopesEnabled = coordinatorScopesEnabled;
        this.coordinatorAuthorizationService = coordinatorAuthorizationService;
        this.coordinatorMetricsRecorder = coordinatorMetricsRecorder == null
            ? NOOP_METRICS_RECORDER : coordinatorMetricsRecorder;
        this.nanoTimeSupplier = nanoTimeSupplier == null ? System::nanoTime : nanoTimeSupplier;
    }

    public ProxyClientAdminResult<ProxyClientPage> listClients(ProxyContext ctx,
        ProxyClientAdminListClientsRequest request) {
        return this.execute(() -> {
            ProxyClientAdminListClientsRequest requiredRequest = this.requireRequest(request);
            ProxyClientScope scope = this.effectiveScope(requiredRequest.getScope());
            switch (scope) {
                case LOCAL_PROXY:
                    return this.localActivity.listClients(ctx, requiredRequest);
                case ALL_PROXIES:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS,
                        scope,
                        ctx,
                        () -> this.requireCoordinatorScopesEnabled(requiredRequest.getScope()),
                        () -> this.requireCoordinatorService().listClients(ctx, requiredRequest.toQuery())
                    );
                case PROXY_ID:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS,
                        scope,
                        ctx,
                        () -> this.requireCoordinatorScopesEnabled(requiredRequest.getScope()),
                        () -> this.requireCoordinatorService().listClients(ctx, requiredRequest.toQuery())
                    );
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
            ProxyClientScope scope = this.effectiveScope(requiredRequest.getScope());
            switch (scope) {
                case LOCAL_PROXY:
                    return this.localActivity.describeClient(ctx, requiredRequest);
                case ALL_PROXIES:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.DESCRIBE_CLIENT,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireClientId(requiredRequest.getClientId());
                        },
                        () -> this.requireCoordinatorService().describeClient(ctx, requiredRequest)
                    );
                case PROXY_ID:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.DESCRIBE_CLIENT,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireClientId(requiredRequest.getClientId());
                        },
                        () -> this.requireCoordinatorService().describeClient(ctx, requiredRequest)
                    );
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
            ProxyClientScope scope = this.effectiveScope(requiredRequest.getScope());
            switch (scope) {
                case LOCAL_PROXY:
                    return this.localActivity.listClientsByGroup(ctx, requiredRequest);
                case ALL_PROXIES:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireGroup(requiredRequest.getGroup());
                        },
                        () -> this.requireCoordinatorService().listClientsByGroup(
                            ctx,
                            requiredRequest.getGroup(),
                            requiredRequest.toQuery()
                        )
                    );
                case PROXY_ID:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS_BY_GROUP,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireGroup(requiredRequest.getGroup());
                        },
                        () -> this.requireCoordinatorService().listClientsByGroup(
                            ctx,
                            requiredRequest.getGroup(),
                            requiredRequest.toQuery()
                        )
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
            ProxyClientScope scope = this.effectiveScope(requiredRequest.getScope());
            switch (scope) {
                case LOCAL_PROXY:
                    return this.localActivity.listClientsByTopic(ctx, requiredRequest);
                case ALL_PROXIES:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireTopic(requiredRequest.getTopic());
                        },
                        () -> this.requireCoordinatorService().listClientsByTopic(
                            ctx,
                            requiredRequest.getTopic(),
                            requiredRequest.toQuery()
                        )
                    );
                case PROXY_ID:
                    return this.executeCoordinatorOperation(
                        ClientAdminOperation.LIST_CLIENTS_BY_TOPIC,
                        scope,
                        ctx,
                        () -> {
                            this.requireCoordinatorScopesEnabled(requiredRequest.getScope());
                            this.requireTopic(requiredRequest.getTopic());
                        },
                        () -> this.requireCoordinatorService().listClientsByTopic(
                            ctx,
                            requiredRequest.getTopic(),
                            requiredRequest.toQuery()
                        )
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
            if (result.getStatus().getCode() != Code.OK) {
                return new ProxyClientAdminResult<>(result.getStatus(), null);
            }
            if (result.getStatus().getCode() == Code.OK && result.getBody() == null) {
                throw new IllegalStateException("result body is required");
            }
            return result;
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            return new ProxyClientAdminResult<>(ResponseBuilder.getInstance().buildStatus(t), null);
        }
    }

    private <T> ProxyClientAdminResult<T> executeCoordinatorOperation(ClientAdminOperation operation,
        ProxyClientScope scope, ProxyContext ctx, Supplier<ProxyClientAdminResult<T>> supplier) {
        return this.executeCoordinatorOperation(operation, scope, ctx, () -> {
        }, supplier);
    }

    private <T> ProxyClientAdminResult<T> executeCoordinatorOperation(ClientAdminOperation operation,
        ProxyClientScope scope, ProxyContext ctx, Runnable preAuthorizationValidation,
        Supplier<ProxyClientAdminResult<T>> supplier) {
        long startNanos = this.nanoTimeSupplier.getAsLong();
        ClientAdminMetricsResult metricsResult = ClientAdminMetricsResult.INTERNAL_ERROR;
        try {
            preAuthorizationValidation.run();
            this.authorizeCoordinatorOperation(ctx, operation);
            ProxyClientAdminResult<T> result = this.execute(supplier);
            metricsResult = this.toMetricsResult(result.getStatus().getCode());
            return result;
        } catch (Throwable t) {
            this.restoreInterruptedStatus(t);
            ProxyClientAdminResult<T> result = new ProxyClientAdminResult<>(
                ResponseBuilder.getInstance().buildStatus(t),
                null
            );
            metricsResult = this.toMetricsResult(result.getStatus().getCode());
            return result;
        } finally {
            this.recordCoordinatorMetrics(operation, metricsResult, this.elapsedMillis(startNanos), scope);
        }
    }

    private void authorizeCoordinatorOperation(ProxyContext ctx, ClientAdminOperation operation) {
        if (this.coordinatorAuthorizationService == null) {
            return;
        }
        ClientAdminRequestContext requestContext = ClientAdminRequestContext.from(ctx);
        this.coordinatorAuthorizationService.authorize(
            requestContext.getSubject(),
            operation,
            requestContext.getSourceIp()
        );
    }

    private void recordCoordinatorMetrics(ClientAdminOperation operation, ClientAdminMetricsResult result,
        long latencyMillis, ProxyClientScope scope) {
        try {
            this.coordinatorMetricsRecorder.record(operation, result, latencyMillis, scope);
        } catch (Throwable e) {
            log.warn("record proxy client admin coordinator metrics failed. operation:{}, result:{}, scope:{}",
                operation, result, scope, e);
        }
    }

    private ClientAdminMetricsResult toMetricsResult(Code code) {
        if (code == Code.OK) {
            return ClientAdminMetricsResult.OK;
        }
        if (code == Code.BAD_REQUEST) {
            return ClientAdminMetricsResult.BAD_REQUEST;
        }
        if (code == Code.NOT_FOUND) {
            return ClientAdminMetricsResult.NOT_FOUND;
        }
        if (code == Code.UNAUTHORIZED) {
            return ClientAdminMetricsResult.UNAUTHORIZED;
        }
        if (code == Code.PROXY_TIMEOUT) {
            return ClientAdminMetricsResult.TIMEOUT;
        }
        if (code == Code.TOO_MANY_REQUESTS) {
            return ClientAdminMetricsResult.TOO_MANY_REQUESTS;
        }
        if (code == Code.NOT_IMPLEMENTED) {
            return ClientAdminMetricsResult.NOT_IMPLEMENTED;
        }
        return ClientAdminMetricsResult.INTERNAL_ERROR;
    }

    private long elapsedMillis(long startNanos) {
        long elapsedNanos = this.nanoTimeSupplier.getAsLong() - startNanos;
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
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
            this.restoreInterruptedStatus(t);
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

    private ProxyClientAdminCoordinatorService requireCoordinatorService() {
        if (this.coordinatorService == null) {
            throw new IllegalStateException("coordinatorService is required");
        }
        return this.coordinatorService;
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

    private void requireClientId(String clientId) {
        if (StringUtils.isBlank(clientId)) {
            throw new IllegalArgumentException("clientId is required");
        }
    }

    private void requireGroup(String group) {
        if (StringUtils.isBlank(group)) {
            throw new IllegalArgumentException("group is required");
        }
    }

    private void requireTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("topic is required");
        }
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

    private void restoreInterruptedStatus(Throwable t) {
        ProxyClientAdminInterrupts.restoreInterruptedStatus(t);
    }
}
