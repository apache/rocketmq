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

package org.apache.rocketmq.controller.metrics;

import com.google.common.base.Splitter;
import io.openmessaging.storage.dledger.MemberState;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.exporter.logging.otlp.OtlpJsonLoggingMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.common.metrics.NopLongUpDownCounter;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.AGGREGATION_DELTA;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.COUNTER_DLEDGER_OP_TOTAL;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.COUNTER_ELECTION_TOTAL;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.COUNTER_REQUEST_TOTAL;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.GAUGE_ACTIVE_BROKER_NUM;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.GAUGE_DLEDGER_DISK_USAGE;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.GAUGE_ROLE;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.HISTOGRAM_DLEDGER_OP_LATENCY;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.HISTOGRAM_REQUEST_LATENCY;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_ADDRESS;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_AGGREGATION;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_BROKER_SET;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_GROUP;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.LABEL_PEER_ID;
import static org.apache.rocketmq.controller.metrics.ControllerMetricsConstant.OPEN_TELEMETRY_METER_NAME;

public class ControllerMetricsManager {

    private static final Logger logger = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

    private static volatile ControllerMetricsManager instance;

    private static final Map<String, String> LABEL_MAP = new HashMap<>();

    // metrics about node status
    public static LongUpDownCounter role = new NopLongUpDownCounter();

    public static ObservableLongGauge dLedgerDiskUsage = new NopObservableLongGauge();

    public static ObservableLongGauge activeBrokerNum = new NopObservableLongGauge();

    public static LongCounter requestTotal = new NopLongCounter();

    public static LongCounter dLedgerOpTotal = new NopLongCounter();

    public static LongCounter electionTotal = new NopLongCounter();

    // metrics about latency
    public static LongHistogram requestLatency = new NopLongHistogram();

    public static LongHistogram dLedgerOpLatency = new NopLongHistogram();

    private static double us = 1d;

    private static double ms = 1000 * us;

    private static double s = 1000 * ms;

    private final ControllerManager controllerManager;

    private final ControllerConfig config;

    private Meter controllerMeter;

    private OtlpGrpcMetricExporter metricExporter;

    private PeriodicMetricReader periodicMetricReader;

    private PrometheusHttpServer prometheusHttpServer;

    private MetricExporter loggingMetricExporter;

    public static ControllerMetricsManager getInstance(ControllerManager controllerManager) {
        if (instance == null) {
            synchronized (ControllerMetricsManager.class) {
                if (instance == null) {
                    instance = new ControllerMetricsManager(controllerManager);
                }
            }
        }
        return instance;
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder builder = Attributes.builder();
        LABEL_MAP.forEach(builder::put);
        return builder;
    }

    public static void recordRole(MemberState.Role newRole, MemberState.Role oldRole) {
        role.add(getRoleValue(newRole) - getRoleValue(oldRole),
            newAttributesBuilder().build());
    }

    private static int getRoleValue(MemberState.Role role) {
        switch (role) {
            case UNKNOWN:
                return 0;
            case CANDIDATE:
                return 1;
            case FOLLOWER:
                return 2;
            case LEADER:
                return 3;
            default:
                logger.error("Unknown role {}", role);
                return 0;
        }
    }

    private ControllerMetricsManager(ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
        this.config = this.controllerManager.getControllerConfig();
        if (config.getControllerType().equals(ControllerConfig.JRAFT_CONTROLLER)) {
            this.LABEL_MAP.put(LABEL_ADDRESS, this.config.getJRaftAddress());
            this.LABEL_MAP.put(LABEL_GROUP, this.config.getjRaftGroupId());
            this.LABEL_MAP.put(LABEL_PEER_ID, this.config.getjRaftServerId());
        } else {
            this.LABEL_MAP.put(LABEL_ADDRESS, this.config.getDLedgerAddress());
            this.LABEL_MAP.put(LABEL_GROUP, this.config.getControllerDLegerGroup());
            this.LABEL_MAP.put(LABEL_PEER_ID, this.config.getControllerDLegerSelfId());
        }
        this.init();
    }

    private boolean checkConfig() {
        if (config == null) {
            return false;
        }
        MetricsExporterType exporterType = config.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(config.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
            case LOG:
                return true;
        }
        return false;
    }

    private void registerMetricsView(SdkMeterProviderBuilder providerBuilder) {
        // define latency bucket
        List<Double> latencyBuckets = Arrays.asList(
            1 * us, 3 * us, 5 * us,
            10 * us, 30 * us, 50 * us,
            100 * us, 300 * us, 500 * us,
            1 * ms, 3 * ms, 5 * ms,
            10 * ms, 30 * ms, 50 * ms,
            100 * ms, 300 * ms, 500 * ms,
            1 * s, 3 * s, 5 * s,
            10 * s
        );

        View latencyView = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(latencyBuckets))
            .build();

        InstrumentSelector requestLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_REQUEST_LATENCY)
            .build();

        InstrumentSelector dLedgerOpLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_DLEDGER_OP_LATENCY)
            .build();

        providerBuilder.registerView(requestLatencySelector, latencyView);
        providerBuilder.registerView(dLedgerOpLatencySelector, latencyView);
    }

    private void initMetric(Meter meter) {
        role = meter.upDownCounterBuilder(GAUGE_ROLE)
            .setDescription("role of current node")
            .build();

        dLedgerDiskUsage = meter.gaugeBuilder(GAUGE_DLEDGER_DISK_USAGE)
            .setDescription("disk usage of dledger")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {
                String path = config.getControllerStorePath();
                if (!UtilAll.isPathExists(path)) {
                    return;
                }
                File file = new File(path);
                Long diskUsage = UtilAll.calculateFileSizeInPath(file);
                if (diskUsage == -1) {
                    logger.error("calculateFileSizeInPath error, path: {}", path);
                    return;
                }
                measurement.record(diskUsage, newAttributesBuilder().build());
            });

        activeBrokerNum = meter.gaugeBuilder(GAUGE_ACTIVE_BROKER_NUM)
            .setDescription("now active brokers num")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<String, Map<String, Integer>> activeBrokersNum = controllerManager.getHeartbeatManager().getActiveBrokersNum();
                activeBrokersNum.forEach((cluster, brokerSetAndNum) -> {
                    brokerSetAndNum.forEach((brokerSet, num) -> measurement.record(num,
                        newAttributesBuilder().put(LABEL_CLUSTER_NAME, cluster).put(LABEL_BROKER_SET, brokerSet).build()));
                });
            });

        requestTotal = meter.counterBuilder(COUNTER_REQUEST_TOTAL)
            .setDescription("total request num")
            .build();

        dLedgerOpTotal = meter.counterBuilder(COUNTER_DLEDGER_OP_TOTAL)
            .setDescription("total dledger operation num")
            .build();

        electionTotal = meter.counterBuilder(COUNTER_ELECTION_TOTAL)
            .setDescription("total elect num")
            .build();

        requestLatency = meter.histogramBuilder(HISTOGRAM_REQUEST_LATENCY)
            .setDescription("request latency")
            .setUnit("us")
            .ofLongs()
            .build();

        dLedgerOpLatency = meter.histogramBuilder(HISTOGRAM_DLEDGER_OP_LATENCY)
            .setDescription("dledger operation latency")
            .setUnit("us")
            .ofLongs()
            .build();

    }

    public void init() {
        MetricsExporterType type = this.config.getMetricsExporterType();
        if (type == MetricsExporterType.DISABLE) {
            return;
        }
        if (!checkConfig()) {
            logger.error("check metric config failed, will not export metrics");
            return;
        }

        String labels = config.getMetricsLabel();
        if (StringUtils.isNotBlank(labels)) {
            List<String> labelList = Splitter.on(',').omitEmptyStrings().splitToList(labels);
            for (String label : labelList) {
                String[] pair = label.split(":");
                if (pair.length != 2) {
                    logger.warn("metrics label is not valid: {}", label);
                    continue;
                }
                LABEL_MAP.put(pair[0], pair[1]);
            }
        }
        if (config.isMetricsInDelta()) {
            LABEL_MAP.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        }

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder().setResource(Resource.empty());

        if (type == MetricsExporterType.OTLP_GRPC) {
            String endpoint = config.getMetricsGrpcExporterTarget();
            if (!endpoint.startsWith("http")) {
                endpoint = "https://" + endpoint;
            }
            OtlpGrpcMetricExporterBuilder metricExporterBuilder = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(config.getMetricGrpcExporterTimeOutInMills(), TimeUnit.MILLISECONDS)
                .setAggregationTemporalitySelector(x -> {
                    if (config.isMetricsInDelta() &&
                        (x == InstrumentType.COUNTER || x == InstrumentType.OBSERVABLE_COUNTER || x == InstrumentType.HISTOGRAM)) {
                        return AggregationTemporality.DELTA;
                    }
                    return AggregationTemporality.CUMULATIVE;
                });

            String headers = config.getMetricsGrpcExporterHeader();
            if (StringUtils.isNotBlank(headers)) {
                Map<String, String> headerMap = new HashMap<>();
                List<String> headerList = Splitter.on(',').omitEmptyStrings().splitToList(headers);
                for (String header : headerList) {
                    String[] pair = header.split(":");
                    if (pair.length != 2) {
                        logger.warn("metricsGrpcExporterHeader is not valid: {}", headers);
                        continue;
                    }
                    headerMap.put(pair[0], pair[1]);
                }
                headerMap.forEach(metricExporterBuilder::addHeader);
            }

            metricExporter = metricExporterBuilder.build();

            periodicMetricReader = PeriodicMetricReader.builder(metricExporter)
                .setInterval(config.getMetricGrpcExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();

            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        if (type == MetricsExporterType.PROM) {
            String promExporterHost = config.getMetricsPromExporterHost();
            if (StringUtils.isBlank(promExporterHost)) {
                promExporterHost = "0.0.0.0";
            }
            prometheusHttpServer = PrometheusHttpServer.builder()
                .setHost(promExporterHost)
                .setPort(config.getMetricsPromExporterPort())
                .build();
            providerBuilder.registerMetricReader(prometheusHttpServer);
        }

        if (type == MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricExporter = OtlpJsonLoggingMetricExporter.create(config.isMetricsInDelta() ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(OtlpJsonLoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricExporter)
                .setInterval(config.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        registerMetricsView(providerBuilder);

        controllerMeter = OpenTelemetrySdk.builder().setMeterProvider(providerBuilder.build())
            .build().getMeter(OPEN_TELEMETRY_METER_NAME);

        initMetric(controllerMeter);
    }

}
