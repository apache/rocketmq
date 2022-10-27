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
package org.apache.rocketmq.broker.metrics;

import com.google.common.base.Splitter;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageStore;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.AGGREGATION_DELTA;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.NODE_TYPE_BROKER;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_BROKER_PERMISSION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_AGGREGATION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_ID;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.OPEN_TELEMETRY_METER_NAME;

public class BrokerMetricsManager {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerConfig brokerConfig;
    private final MessageStore messageStore;
    private final BrokerController brokerController;
    private final static Map<String, String> LABEL_MAP = new HashMap<>();
    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private Meter brokerMeter;

    public static ObservableLongGauge brokerPermission = null;

    public BrokerMetricsManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        brokerConfig = brokerController.getBrokerConfig();
        this.messageStore = brokerController.getMessageStore();
        init();
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder attributesBuilder = Attributes.builder();
        LABEL_MAP.forEach(attributesBuilder::put);
        return attributesBuilder;
    }

    public Meter getBrokerMeter() {
        return brokerMeter;
    }

    private boolean checkConfig() {
        if (brokerConfig == null) {
            return false;
        }
        BrokerConfig.MetricsExporterType exporterType = brokerConfig.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(brokerConfig.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
        }
        return false;
    }

    private void init() {
        if (!checkConfig()) {
            LOGGER.error("check metrics config failed, will not export metrics");
            return;
        }

        String labels = brokerConfig.getMetricsLabel();
        if (StringUtils.isNotBlank(labels)) {
            List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(labels);
            for (String item : kvPairs) {
                String[] split = item.split(":");
                if (split.length != 2) {
                    LOGGER.warn("metricsLabel is not valid: {}", labels);
                    continue;
                }
                LABEL_MAP.put(split[0], split[1]);
            }
        }
        if (brokerConfig.isMetricsInDelta()) {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        }
        LABEL_MAP.put(LABEL_NODE_TYPE, NODE_TYPE_BROKER);
        LABEL_MAP.put(LABEL_CLUSTER_NAME, brokerConfig.getBrokerClusterName());
        LABEL_MAP.put(LABEL_NODE_ID, brokerConfig.getBrokerName());

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
            .setResource(Resource.empty());

        if (brokerConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.OTLP_GRPC) {
            String endpoint = brokerConfig.getMetricsGrpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(brokerConfig.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .setAggregationTemporalitySelector(type -> {
                    if (brokerConfig.isMetricsInDelta() &&
                        (type == InstrumentType.COUNTER || type == InstrumentType.OBSERVABLE_COUNTER || type == InstrumentType.HISTOGRAM)) {
                        return AggregationTemporality.DELTA;
                    }
                    return AggregationTemporality.CUMULATIVE;
                });

            String headers = brokerConfig.getMetricsGrpcExporterHeader();
            if (StringUtils.isNotBlank(headers)) {
                Map<String, String> headerMap = new HashMap<>();
                List<String> kvPairs = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                for (String item : kvPairs) {
                    String[] split = item.split(":");
                    if (split.length != 2) {
                        LOGGER.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(split[0], split[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            metricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(brokerConfig.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        if (brokerConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.PROM) {
            String promExporterHost = brokerConfig.getMetricsPromExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                promExporterHost = brokerConfig.getBrokerIP1();
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(brokerConfig.getMetricsPromExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        }

        brokerMeter = OpenTelemetrySdk.builder()
            .setMeterProvider(providerBuilder.build())
            .build()
            .getMeter(OPEN_TELEMETRY_METER_NAME);

        initStatsMetrics();
    }

    private void initStatsMetrics() {
        brokerPermission = brokerMeter.gaugeBuilder(GAUGE_BROKER_PERMISSION)
            .setDescription("Broker permission")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(brokerConfig.getBrokerPermission(), newAttributesBuilder().build()));
    }

    public void shutdown() {
        if (brokerConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.OTLP_GRPC) {
            periodicMetricReader.forceFlush();
            periodicMetricReader.shutdown();
            metricExporter.shutdown();
        }
        if (brokerConfig.getMetricsExporterType() == BrokerConfig.MetricsExporterType.PROM) {
            prometheusHttpServer.forceFlush();
            prometheusHttpServer.shutdown();
        }
    }
}
