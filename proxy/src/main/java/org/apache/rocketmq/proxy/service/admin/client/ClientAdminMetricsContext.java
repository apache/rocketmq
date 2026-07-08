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
package org.apache.rocketmq.proxy.service.admin.client;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class ClientAdminMetricsContext {
    public static final String FILTER_NONE = "none";

    private final ClientAdminOperation operation;
    private final ClientAdminMetricsResult result;
    private final long latencyMillis;
    private final ProxyClientScope scope;
    private final String status;
    private final int pageSize;
    private final String filters;
    private final int resultSize;

    private ClientAdminMetricsContext(Builder builder) {
        if (builder.operation == null) {
            throw new IllegalArgumentException("operation is required");
        }
        if (builder.result == null) {
            throw new IllegalArgumentException("result is required");
        }
        this.operation = builder.operation;
        this.result = builder.result;
        this.latencyMillis = Math.max(0L, builder.latencyMillis);
        this.scope = builder.scope;
        this.status = StringUtils.defaultIfBlank(builder.status, statusOf(builder.result));
        this.pageSize = builder.pageSize;
        this.filters = StringUtils.defaultIfBlank(builder.filters, filtersOf(builder.filterNames));
        this.resultSize = Math.max(-1, builder.resultSize);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ClientAdminOperation getOperation() {
        return operation;
    }

    public ClientAdminMetricsResult getResult() {
        return result;
    }

    public long getLatencyMillis() {
        return latencyMillis;
    }

    public ProxyClientScope getScope() {
        return scope;
    }

    public String getStatus() {
        return status;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getFilters() {
        return filters;
    }

    public int getResultSize() {
        return resultSize;
    }

    public static String statusOf(ClientAdminMetricsResult result) {
        if (result == null) {
            return "unknown";
        }
        switch (result) {
            case OK:
                return "ok";
            case BAD_REQUEST:
                return "bad_request";
            case NOT_FOUND:
                return "not_found";
            case UNAUTHORIZED:
                return "unauthorized";
            case TIMEOUT:
                return "proxy_timeout";
            case TOO_MANY_REQUESTS:
                return "too_many_requests";
            case NOT_IMPLEMENTED:
                return "not_implemented";
            case INTERNAL_ERROR:
            default:
                return "internal_server_error";
        }
    }

    private static String filtersOf(Set<String> filterNames) {
        if (filterNames == null || filterNames.isEmpty()) {
            return FILTER_NONE;
        }
        return StringUtils.join(filterNames, ',');
    }

    static int resultSizeOf(Object body) {
        if (body == null) {
            return 0;
        }
        if (body instanceof ProxyClientPage) {
            return ((ProxyClientPage) body).getClients().size();
        }
        return 1;
    }

    public static class Builder {
        private ClientAdminOperation operation;
        private ClientAdminMetricsResult result;
        private long latencyMillis;
        private ProxyClientScope scope;
        private String status;
        private int pageSize = -1;
        private String filters;
        private int resultSize = -1;
        private final Set<String> filterNames = new LinkedHashSet<>();

        public Builder setOperation(ClientAdminOperation operation) {
            this.operation = operation;
            return this;
        }

        public Builder setResult(ClientAdminMetricsResult result) {
            this.result = result;
            return this;
        }

        public Builder setLatencyMillis(long latencyMillis) {
            this.latencyMillis = latencyMillis;
            return this;
        }

        public Builder setScope(ProxyClientScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder setStatus(String status) {
            this.status = StringUtils.trimToNull(status);
            return this;
        }

        public Builder setPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder setFilters(String filters) {
            this.filters = StringUtils.trimToNull(filters);
            return this;
        }

        public Builder setResultSize(int resultSize) {
            this.resultSize = resultSize;
            return this;
        }

        public Builder setQuery(ProxyClientQuery query) {
            if (query == null) {
                return this;
            }
            this.setPageSize(query.getBoundedPageSize());
            this.addFilterIfPresent("client_id", query.getClientId());
            this.addFilterIfPresent("client_id_prefix", query.getClientIdPrefix());
            this.addFilterIfPresent("group", query.getGroup());
            this.addFilterIfPresent("topic", query.getTopic());
            this.addFilterIfPresent("client_language", query.getClientLanguage());
            if (query.getConnectTimeStartMillis() != null || query.getConnectTimeEndMillis() != null) {
                this.addFilter("connect_time_range");
            }
            if (query.getClientType() != null) {
                this.addFilter("client_type");
            }
            this.addFilterIfPresent("proxy_id", query.getProxyId());
            return this;
        }

        public Builder addFilter(String filterName) {
            String normalizedFilterName = StringUtils.trimToNull(filterName);
            if (normalizedFilterName != null) {
                this.filterNames.add(normalizedFilterName);
            }
            return this;
        }

        public Builder setResultBody(Object body) {
            this.resultSize = resultSizeOf(body);
            return this;
        }

        private void addFilterIfPresent(String filterName, String value) {
            if (StringUtils.isNotBlank(value)) {
                this.addFilter(filterName);
            }
        }

        public ClientAdminMetricsContext build() {
            return new ClientAdminMetricsContext(this);
        }
    }
}
