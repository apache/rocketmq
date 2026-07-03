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
package org.apache.rocketmq.proxy.metrics;

import apache.rocketmq.v2.ClientType;
import com.google.common.base.Splitter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminMetricsResult;
import org.apache.rocketmq.proxy.service.admin.client.ClientAdminOperation;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceOperation;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceStats;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.AGGREGATION_DELTA;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_AGGREGATION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_ID;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.OPEN_TELEMETRY_METER_NAME;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.COUNTER_PROXY_CLIENT_ADMIN_REQUESTS_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.COUNTER_PROXY_CLIENT_READ_MODEL_OPERATIONS_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_INDEX_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_TYPE_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_UP;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.HISTOGRAM_PROXY_CLIENT_ADMIN_REQUEST_LATENCY;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.INDEX_TYPE_GROUP;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.INDEX_TYPE_TOPIC;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_CLIENT_TYPE;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_INDEX_TYPE;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_OPERATION;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_PROXY_MODE;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_RESULT;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.NODE_TYPE_PROXY;

public class ProxyMetricsManager implements StartAndShutdown {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private static final Supplier<ProxyClientReadServiceStats> EMPTY_PROXY_CLIENT_STATS_SUPPLIER =
        () -> new ProxyClientReadServiceStats(0L, 0L, 0L, Collections.emptyMap());
    private static ProxyConfig proxyConfig;
    private final static Map<String, String> LABEL_MAP = new HashMap<>();
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    private static Supplier<ProxyClientReadServiceStats> proxyClientReadServiceStatsSupplier =
        EMPTY_PROXY_CLIENT_STATS_SUPPLIER;

    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private MetricExporter loggingMetricExporter;

    public static ObservableLongGauge proxyUp = null;
    public static ObservableLongGauge proxyClientTotal = null;
    public static ObservableLongGauge proxyClientTypeTotal = null;
    public static ObservableLongGauge proxyClientIndexTotal = null;
    public static LongCounter proxyClientAdminRequestsTotal = new NopLongCounter();
    public static LongHistogram proxyClientAdminRequestLatency = new NopLongHistogram();
    public static LongCounter proxyClientReadModelOperationsTotal = new NopLongCounter();

    public static void initLocalMode(BrokerMetricsManager brokerMetricsManager, ProxyConfig proxyConfig) {
        if (proxyConfig.getMetricsExporterType() == MetricsExporterType.DISABLE) {
            return;
        }
        ProxyMetricsManager.proxyConfig = proxyConfig;
        LABEL_MAP.put(LABEL_NODE_TYPE, NODE_TYPE_PROXY);
        LABEL_MAP.put(LABEL_CLUSTER_NAME, proxyConfig.getProxyClusterName());
        LABEL_MAP.put(LABEL_NODE_ID, proxyConfig.getProxyName());
        LABEL_MAP.put(LABEL_PROXY_MODE, proxyConfig.getProxyMode().toLowerCase());
        initMetrics(brokerMetricsManager.getBrokerMeter(), brokerMetricsManager::newAttributesBuilder);
    }

    public static ProxyMetricsManager initClusterMode(ProxyConfig proxyConfig) {
        ProxyMetricsManager.proxyConfig = proxyConfig;
        return new ProxyMetricsManager();
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder attributesBuilder;
        if (attributesBuilderSupplier == null) {
            attributesBuilder = Attributes.builder();
            LABEL_MAP.forEach(attributesBuilder::put);
            return attributesBuilder;
        }
        attributesBuilder = attributesBuilderSupplier.get();
        LABEL_MAP.forEach(attributesBuilder::put);
        return attributesBuilder;
    }

    public static void setProxyClientReadServiceStatsSupplier(
        Supplier<ProxyClientReadServiceStats> proxyClientReadServiceStatsSupplier) {
        if (proxyClientReadServiceStatsSupplier == null) {
            ProxyMetricsManager.proxyClientReadServiceStatsSupplier = EMPTY_PROXY_CLIENT_STATS_SUPPLIER;
            return;
        }
        ProxyMetricsManager.proxyClientReadServiceStatsSupplier = proxyClientReadServiceStatsSupplier;
    }

    static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        initMetrics(meter, attributesBuilderSupplier, proxyClientReadServiceStatsSupplier);
    }

    static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier,
        Supplier<ProxyClientReadServiceStats> proxyClientReadServiceStatsSupplier) {
        ProxyMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
        setProxyClientReadServiceStatsSupplier(proxyClientReadServiceStatsSupplier);

        proxyUp = meter.gaugeBuilder(GAUGE_PROXY_UP)
            .setDescription("proxy status")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(1, newAttributesBuilder().build()));

        proxyClientTotal = meter.gaugeBuilder(GAUGE_PROXY_CLIENT_TOTAL)
            .setDescription("online proxy client count")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(
                snapshotProxyClientStats().getTotalClientCount(),
                newAttributesBuilder().build()));

        proxyClientTypeTotal = meter.gaugeBuilder(GAUGE_PROXY_CLIENT_TYPE_TOTAL)
            .setDescription("online proxy client count by client type")
            .ofLongs()
            .buildWithCallback(ProxyMetricsManager::recordProxyClientTypeTotal);

        proxyClientIndexTotal = meter.gaugeBuilder(GAUGE_PROXY_CLIENT_INDEX_TOTAL)
            .setDescription("proxy client read model index count")
            .ofLongs()
            .buildWithCallback(ProxyMetricsManager::recordProxyClientIndexTotal);

        proxyClientReadModelOperationsTotal = meter.counterBuilder(COUNTER_PROXY_CLIENT_READ_MODEL_OPERATIONS_TOTAL)
            .setDescription("proxy client read model operation count")
            .build();

        proxyClientAdminRequestsTotal = meter.counterBuilder(COUNTER_PROXY_CLIENT_ADMIN_REQUESTS_TOTAL)
            .setDescription("proxy client admin request count")
            .build();

        proxyClientAdminRequestLatency = meter.histogramBuilder(HISTOGRAM_PROXY_CLIENT_ADMIN_REQUEST_LATENCY)
            .setDescription("proxy client admin request latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
    }

    public static void recordProxyClientAdminRequest(ClientAdminOperation operation,
        ClientAdminMetricsResult result, long latencyMillis) {
        if (operation == null || result == null) {
            return;
        }
        Attributes attributes = newAttributesBuilder()
            .put(LABEL_OPERATION, operation.name().toLowerCase())
            .put(LABEL_RESULT, result.name().toLowerCase())
            .build();
        proxyClientAdminRequestsTotal.add(1L, attributes);
        proxyClientAdminRequestLatency.record(Math.max(0L, latencyMillis), attributes);
    }

    public static void recordProxyClientReadModelOperation(ProxyClientReadServiceOperation operation) {
        if (operation == null) {
            return;
        }
        proxyClientReadModelOperationsTotal.add(
            1L,
            newAttributesBuilder()
                .put(LABEL_OPERATION, operation.name().toLowerCase())
                .build()
        );
    }

    private static ProxyClientReadServiceStats snapshotProxyClientStats() {
        ProxyClientReadServiceStats stats = proxyClientReadServiceStatsSupplier.get();
        if (stats == null) {
            return EMPTY_PROXY_CLIENT_STATS_SUPPLIER.get();
        }
        return stats;
    }

    private static void recordProxyClientTypeTotal(ObservableLongMeasurement measurement) {
        ProxyClientReadServiceStats stats = snapshotProxyClientStats();
        for (Map.Entry<ClientType, Long> entry : stats.getClientTypeCounts().entrySet()) {
            ClientType clientType = entry.getKey();
            Long count = entry.getValue();
            if (clientType == null || count == null) {
                continue;
            }
            measurement.record(
                count,
                newAttributesBuilder()
                    .put(LABEL_CLIENT_TYPE, clientType.name().toLowerCase())
                    .build()
            );
        }
    }

    private static void recordProxyClientIndexTotal(ObservableLongMeasurement measurement) {
        ProxyClientReadServiceStats stats = snapshotProxyClientStats();
        measurement.record(
            stats.getGroupIndexCount(),
            newAttributesBuilder().put(LABEL_INDEX_TYPE, INDEX_TYPE_GROUP).build()
        );
        measurement.record(
            stats.getTopicIndexCount(),
            newAttributesBuilder().put(LABEL_INDEX_TYPE, INDEX_TYPE_TOPIC).build()
        );
    }

    public ProxyMetricsManager() {
    }

    private boolean checkConfig() {
        if (proxyConfig == null) {
            return false;
        }
        MetricsExporterType exporterType = proxyConfig.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(proxyConfig.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
            case LOG:
                return true;
        }
        return false;
    }

    @Override
    public void start() throws Exception {
        MetricsExporterType metricsExporterType = proxyConfig.getMetricsExporterType();
        if (metricsExporterType == MetricsExporterType.DISABLE) {
            return;
        }
        if (!checkConfig()) {
            log.error("check metrics config failed, will not export metrics");
            return;
        }

        String labels = proxyConfig.getMetricsLabel();
        if (StringUtils.isNotBlank(labels)) {
            List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(labels);
            for (String item : kvPairs) {
                String[] split = item.split(":");
                if (split.length != 2) {
                    log.warn("metricsLabel is not valid: {}", labels);
                    continue;
                }
                LABEL_MAP.put(split[0], split[1]);
            }
        }
        if (proxyConfig.isMetricsInDelta()) {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        }
        LABEL_MAP.put(LABEL_NODE_TYPE, NODE_TYPE_PROXY);
        LABEL_MAP.put(LABEL_CLUSTER_NAME, proxyConfig.getProxyClusterName());
        LABEL_MAP.put(LABEL_NODE_ID, proxyConfig.getProxyName());
        LABEL_MAP.put(LABEL_PROXY_MODE, proxyConfig.getProxyMode().toLowerCase());

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
            .setResource(Resource.empty());

        if (metricsExporterType == MetricsExporterType.OTLP_GRPC) {
            String endpoint = proxyConfig.getMetricsGrpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(proxyConfig.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .setAggregationTemporalitySelector(type -> {
                    if (proxyConfig.isMetricsInDelta() &&
                        (type == InstrumentType.COUNTER || type == InstrumentType.OBSERVABLE_COUNTER || type == InstrumentType.HISTOGRAM)) {
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
                        log.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(split[0], split[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            metricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(proxyConfig.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        if (metricsExporterType == MetricsExporterType.PROM) {
            String promExporterHost = proxyConfig.getMetricsPromExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                promExporterHost = "0.0.0.0";
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(proxyConfig.getMetricsPromExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        }

        if (metricsExporterType == MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricExporter = OtlpJsonLoggingMetricExporter.create(proxyConfig.isMetricsInDelta() ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(OtlpJsonLoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricExporter)
                .setInterval(proxyConfig.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        Meter proxyMeter = OpenTelemetrySdk.builder()
            .setMeterProvider(providerBuilder.build())
            .build()
            .getMeter(OPEN_TELEMETRY_METER_NAME);

        initMetrics(proxyMeter, null);
    }

    @Override
    public void shutdown() throws Exception {
        if (proxyConfig.getMetricsExporterType() == MetricsExporterType.OTLP_GRPC) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            metricExporter.shutdown();
        }
        if (proxyConfig.getMetricsExporterType() == MetricsExporterType.PROM) {
            prometheusHttpServer.forceFlush();
            prometheusHttpServer.shutdown();
        }
        if (proxyConfig.getMetricsExporterType() == MetricsExporterType.LOG) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            loggingMetricExporter.shutdown();
        }
    }
}
