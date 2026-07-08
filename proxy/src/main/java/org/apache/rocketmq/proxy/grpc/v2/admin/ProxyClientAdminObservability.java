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
import apache.rocketmq.v2.Status;
import io.opentelemetry.api.trace.Span;
import java.util.Locale;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsContext;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsResult;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientQuery;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientScope;

final class ProxyClientAdminObservability {
    static final String TRACE_OPERATION = "rocketmq.proxy.client_admin.operation";
    static final String TRACE_SCOPE = "rocketmq.proxy.client_admin.scope";
    static final String TRACE_STATUS = "rocketmq.proxy.client_admin.status";
    static final String TRACE_PAGE_SIZE = "rocketmq.proxy.client_admin.page_size";
    static final String TRACE_FILTERS = "rocketmq.proxy.client_admin.filters";
    static final String TRACE_RESULT_SIZE = "rocketmq.proxy.client_admin.result_size";

    private ProxyClientAdminObservability() {
    }

    static <T> void observe(Logger log, ClientAdminOperation operation, Object request,
        ProxyClientAdminResult<T> result) {
        try {
            ClientAdminMetricsContext context = contextOf(operation, request, result);
            recordTrace(context);
            logFailure(log, context);
        } catch (Throwable t) {
            if (log != null) {
                log.warn("record proxy client admin observability failed. operation:{}, error:{}",
                    operation, t.getClass().getSimpleName());
            }
        }
    }

    static ClientAdminMetricsContext contextOf(ClientAdminOperation operation, Object request,
        ProxyClientAdminResult<?> result) {
        Status status = result == null ? null : result.getStatus();
        Object body = result == null ? null : result.getBody();
        ClientAdminMetricsContext.Builder builder = ClientAdminMetricsContext.newBuilder()
            .setOperation(operation)
            .setResult(toMetricsResult(status == null ? null : status.getCode()))
            .setStatus(statusOf(status))
            .setScope(scopeOf(request))
            .setResultSize(resultSizeOf(body));
        ProxyClientQuery query = queryOf(request);
        if (query != null) {
            builder.setQuery(query);
        }
        if (request instanceof ProxyClientAdminDescribeClientRequest) {
            builder.addFilter("client_id");
        }
        return builder.build();
    }

    private static void recordTrace(ClientAdminMetricsContext context) {
        Span span = Span.current();
        span.setAttribute(TRACE_OPERATION, context.getOperation().name().toLowerCase(Locale.ROOT));
        if (context.getScope() != null) {
            span.setAttribute(TRACE_SCOPE, context.getScope().name().toLowerCase(Locale.ROOT));
        }
        span.setAttribute(TRACE_STATUS, context.getStatus());
        span.setAttribute(TRACE_FILTERS, context.getFilters());
        if (context.getPageSize() >= 0) {
            span.setAttribute(TRACE_PAGE_SIZE, context.getPageSize());
        }
        if (context.getResultSize() >= 0) {
            span.setAttribute(TRACE_RESULT_SIZE, context.getResultSize());
        }
    }

    private static void logFailure(Logger log, ClientAdminMetricsContext context) {
        if (log == null || "ok".equals(context.getStatus())) {
            return;
        }
        log.warn("proxy client admin request failed. operation:{}, status:{}, result:{}, scope:{}, filters:{}, "
                + "pageSize:{}, resultSize:{}",
            context.getOperation(),
            context.getStatus(),
            context.getResult(),
            context.getScope(),
            context.getFilters(),
            context.getPageSize(),
            context.getResultSize());
    }

    private static ProxyClientScope scopeOf(Object request) {
        if (request instanceof ProxyClientAdminListClientsRequest) {
            return ((ProxyClientAdminListClientsRequest) request).getScope();
        }
        if (request instanceof ProxyClientAdminDescribeClientRequest) {
            return ((ProxyClientAdminDescribeClientRequest) request).getScope();
        }
        return ProxyClientScope.LOCAL_PROXY;
    }

    private static ProxyClientQuery queryOf(Object request) {
        if (!(request instanceof ProxyClientAdminListClientsRequest)) {
            return null;
        }
        try {
            return ((ProxyClientAdminListClientsRequest) request).toQuery();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static int resultSizeOf(Object body) {
        if (body == null) {
            return 0;
        }
        if (body instanceof ProxyClientAdminPageView) {
            return ((ProxyClientAdminPageView) body).getClients().size();
        }
        return 1;
    }

    private static String statusOf(Status status) {
        if (status == null) {
            return "internal_server_error";
        }
        return status.getCode().name().toLowerCase(Locale.ROOT);
    }

    private static ClientAdminMetricsResult toMetricsResult(Code code) {
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
}
