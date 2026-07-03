/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  may obtain a copy of the License at
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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import java.util.function.Supplier;
import org.apache.rocketmq.proxy.metrics.ProxyMetricsManager;

/**
 * Metrics manager for Proxy Admin gRPC interface (RIP-2).
 * <p>
 * Provides OTel metrics for admin RPC operations:
 * - rocketmq_proxy_admin_rpc_total: Counter for admin RPC invocations (by method, status)
 * - rocketmq_proxy_admin_rpc_latency: Histogram for admin RPC latency (by method)
 * <p>
 * These metrics are enabled by default as required by RIP-2 §5.4.5 and §8.6.
 */
public class ProxyAdminMetricsManager {

    // Metric names following RocketMQ naming convention
    public static final String COUNTER_ADMIN_RPC_TOTAL = "rocketmq_proxy_admin_rpc_total";
    public static final String HISTOGRAM_ADMIN_RPC_LATENCY = "rocketmq_proxy_admin_rpc_latency";

    // Label keys
    public static final String LABEL_RPC_METHOD = "rpc_method";
    public static final String LABEL_STATUS = "status";

    // RPC method names
    public static final String METHOD_LIST_CLIENTS = "ListClients";
    public static final String METHOD_DESCRIBE_CLIENT = "DescribeClient";
    public static final String METHOD_LIST_CLIENTS_BY_GROUP = "ListClientsByGroup";
    public static final String METHOD_LIST_CLIENTS_BY_TOPIC = "ListClientsByTopic";

    // Status values
    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_ERROR = "error";

    private static LongCounter adminRpcCounter;
    private static io.opentelemetry.api.metrics.DoubleHistogram adminRpcLatency;

    private static volatile boolean initialized = false;

    /**
     * Initialize admin metrics with the given OpenTelemetry Meter.
     * Called during ProxyStartup when metrics are enabled.
     */
    public static synchronized void init(Meter meter) {
        if (meter == null || initialized) {
            return;
        }

        adminRpcCounter = meter
            .counterBuilder(COUNTER_ADMIN_RPC_TOTAL)
            .setDescription("Total number of proxy admin RPC invocations")
            .build();

        adminRpcLatency = meter
            .histogramBuilder(HISTOGRAM_ADMIN_RPC_LATENCY)
            .setDescription("Latency of proxy admin RPC invocations in milliseconds")
            .setUnit("ms")
            .build();

        initialized = true;
    }

    /**
     * Check if metrics have been initialized.
     */
    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Record a successful admin RPC invocation.
     *
     * @param method the RPC method name
     * @param latencyMs the latency in milliseconds
     */
    public static void recordSuccess(String method, long latencyMs) {
        if (!initialized) {
            return;
        }
        Attributes attrs = buildAttributes(method, STATUS_SUCCESS);
        adminRpcCounter.add(1, attrs);
        adminRpcLatency.record(latencyMs, attrs);
    }

    /**
     * Record a failed admin RPC invocation.
     *
     * @param method the RPC method name
     * @param latencyMs the latency in milliseconds
     */
    public static void recordError(String method, long latencyMs) {
        if (!initialized) {
            return;
        }
        Attributes attrs = buildAttributes(method, STATUS_ERROR);
        adminRpcCounter.add(1, attrs);
        adminRpcLatency.record(latencyMs, attrs);
    }

    private static Attributes buildAttributes(String method, String status) {
        AttributesBuilder builder = ProxyMetricsManager.newAttributesBuilder();
        builder.put(LABEL_RPC_METHOD, method);
        builder.put(LABEL_STATUS, status);
        return builder.build();
    }
}