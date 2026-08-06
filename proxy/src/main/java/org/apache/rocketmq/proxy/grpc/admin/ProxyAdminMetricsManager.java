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
package org.apache.rocketmq.proxy.grpc.admin;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import com.google.common.base.Splitter;

/**
 * RIP-2 acceptance criteria #4: the admin interface exposes its own call RT and
 * error-rate metrics.
 *
 * <p>Exported instruments (OpenTelemetry, same exporter configuration as
 * {@code ProxyMetricsManager}):
 * <ul>
 *   <li>{@code rocketmq_proxy_admin_rpc_total} — LongCounter labeled by
 *       {@code rpc_method} and {@code status} (success/error, with
 *       {@code error_type} on failure). Error rate = rate of status=error.</li>
 *   <li>{@code rocketmq_proxy_admin_rpc_latency} — DoubleHistogram in
 *       milliseconds labeled by {@code rpc_method} and {@code status};
 *       quantiles give P50/P99 RT per method.</li>
 * </ul>
 */
public class ProxyAdminMetricsManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public static final String METRIC_RPC_TOTAL = "rocketmq_proxy_admin_rpc_total";
    public static final String METRIC_RPC_LATENCY = "rocketmq_proxy_admin_rpc_latency";

    public static final AttributeKey<String> LABEL_METHOD = AttributeKey.stringKey("rpc_method");
    public static final AttributeKey<String> LABEL_STATUS = AttributeKey.stringKey("status");
    public static final AttributeKey<String> LABEL_ERROR_TYPE = AttributeKey.stringKey("error_type");
    public static final AttributeKey<String> LABEL_CLUSTER_NAME = AttributeKey.stringKey("cluster_name");
    public static final AttributeKey<String> LABEL_NODE_ID = AttributeKey.stringKey("node_id");

    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_ERROR = "error";

    private static final String OPEN_TELEMETRY_METER_NAME = "org.apache.rocketmq.proxy.admin";

    private static volatile boolean initialized;
    private static LongCounter rpcTotal;
    private static DoubleHistogram rpcLatency;
    private static String clusterName = "";
    private static String nodeName = "";

    // exporter handles for shutdown
    private static OtlpGrpcMetricExporter otlpExporter;
    private static PeriodicMetricReader periodicReader;
    private static PrometheusHttpServer prometheusHttpServer;
    private static MetricExporter loggingExporter;

    private ProxyAdminMetricsManager() {
    }

    /**
     * Initialize the admin metrics pipeline from the same exporter settings used by the
     * proxy data-plane metrics. Safe to call multiple times; first call wins. When the
     * exporter is disabled the record methods become no-ops.
     */
    public static synchronized void init(ProxyConfig proxyConfig) {
        if (initialized) {
            return;
        }
        try {
            MetricsExporterType exporterType = proxyConfig.getMetricsExporterType();
            if (exporterType == null || !exporterType.isEnable()) {
                log.info("RIP-2 admin metrics disabled, metricsExporterType:{}", exporterType);
                return;
            }
            clusterName = StringUtils.defaultString(proxyConfig.getProxyClusterName());
            nodeName = StringUtils.defaultString(proxyConfig.getProxyName());

            SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder().setResource(Resource.empty());
            if (exporterType == MetricsExporterType.OTLP_GRPC) {
                String endpoint = proxyConfig.getMetricsGrpcExporterTarget();
                if (StringUtils.isBlank(endpoint)) {
                    log.warn("RIP-2 admin metrics: OTLP exporter enabled but no target configured");
                    return;
                }
                if (!endpoint.startsWith("http")) {
                    endpoint = "https://" + endpoint;
                }
                OtlpGrpcMetricExporterBuilder exporterBuilder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(endpoint)
                    .setTimeout(proxyConfig.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                    .setAggregationTemporalitySelector(type -> {
                        if (proxyConfig.isMetricsInDelta()
                            && (type == InstrumentType.COUNTER || type == InstrumentType.OBSERVABLE_COUNTER
                            || type == InstrumentType.HISTOGRAM)) {
                            return AggregationTemporality.DELTA;
                        }
                        return AggregationTemporality.CUMULATIVE;
                    });
                String headers = proxyConfig.getMetricsGrpcExporterHeader();
                if (StringUtils.isNotBlank(headers)) {
                    Map<String, String> headerMap = new HashMap<>();
                    List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                    for (String item : kvPairs) {
                        String[] split = item.split(":");
                        if (split.length != 2) {
                            continue;
                        }
                        headerMap.put(split[0], split[1]);
                    }
                    headerMap.forEach(exporterBuilder::addHeader);
                }
                otlpExporter = exporterBuilder.build();
                periodicReader = PeriodicMetricReader.builder(otlpExporter)
                    .setInterval(proxyConfig.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                    .build();
                providerBuilder.registerMetricReader(periodicReader);
            } else if (exporterType == MetricsExporterType.PROM) {
                String host = proxyConfig.getMetricsPromExporterHost();
                if (StringUtils.isBlank(host)) {
                    host = "0.0.0.0";
                }
                prometheusHttpServer = PrometheusHttpServer.builder()
                    .setHost(host)
                    // +1 avoids binding the same port as the data-plane PrometheusHttpServer
                    // when both run in one process; operators may override via config.
                    .setPort(proxyConfig.getMetricsPromExporterPort() + 1)
                    .build();
                providerBuilder.registerMetricReader(prometheusHttpServer);
            } else if (exporterType == MetricsExporterType.LOG) {
                loggingExporter = OtlpJsonLoggingMetricExporter.create(proxyConfig.isMetricsInDelta()
                    ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
                periodicReader = PeriodicMetricReader.builder(loggingExporter)
                    .setInterval(proxyConfig.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                    .build();
                providerBuilder.registerMetricReader(periodicReader);
            }

            Meter meter = OpenTelemetrySdk.builder()
                .setMeterProvider(providerBuilder.build())
                .build()
                .getMeter(OPEN_TELEMETRY_METER_NAME);

            rpcTotal = meter.counterBuilder(METRIC_RPC_TOTAL)
                .setDescription("total number of RIP-2 proxy admin RPC calls")
                .build();
            rpcLatency = meter.histogramBuilder(METRIC_RPC_LATENCY)
                .setDescription("latency of RIP-2 proxy admin RPC calls")
                .setUnit("ms")
                .build();
            initialized = true;
            log.info("RIP-2 admin metrics initialized, exporterType:{}", exporterType);
        } catch (Throwable t) {
            log.error("RIP-2 admin metrics init failed, metrics disabled", t);
        }
    }

    public static void recordSuccess(String rpcMethod, long latencyMillis) {
        if (!initialized) {
            return;
        }
        Attributes attributes = Attributes.builder()
            .put(LABEL_METHOD, rpcMethod)
            .put(LABEL_STATUS, STATUS_SUCCESS)
            .put(LABEL_CLUSTER_NAME, clusterName)
            .put(LABEL_NODE_ID, nodeName)
            .build();
        rpcTotal.add(1, attributes);
        rpcLatency.record(latencyMillis, attributes);
    }

    public static void recordError(String rpcMethod, long latencyMillis, Throwable error) {
        if (!initialized) {
            return;
        }
        Attributes attributes = Attributes.builder()
            .put(LABEL_METHOD, rpcMethod)
            .put(LABEL_STATUS, STATUS_ERROR)
            .put(LABEL_ERROR_TYPE, error == null ? "unknown" : StringUtils.defaultIfBlank(
                error.getClass().getSimpleName(), "unknown"))
            .put(LABEL_CLUSTER_NAME, clusterName)
            .put(LABEL_NODE_ID, nodeName)
            .build();
        rpcTotal.add(1, attributes);
        rpcLatency.record(latencyMillis, attributes);
    }

    public static synchronized void shutdown() {
        if (!initialized) {
            return;
        }
        try {
            if (periodicReader != null) {
                periodicReader.forceFlush();
                periodicReader.shutdown();
            }
            if (otlpExporter != null) {
                otlpExporter.shutdown();
            }
            if (prometheusHttpServer != null) {
                prometheusHttpServer.forceFlush();
                prometheusHttpServer.shutdown();
            }
            if (loggingExporter != null) {
                loggingExporter.shutdown();
            }
        } catch (Throwable t) {
            log.warn("RIP-2 admin metrics shutdown failed", t);
        } finally {
            initialized = false;
        }
    }

    public static boolean isInitialized() {
        return initialized;
    }
}
