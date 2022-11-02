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
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
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
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageStore;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.AGGREGATION_DELTA;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_PROCESSOR_WATERMARK;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_MESSAGE_SIZE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_PROCESSOR;
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

    // broker stats metrics
    public static ObservableLongGauge processorWatermark = new NopObservableLongGauge();
    public static ObservableLongGauge brokerPermission = new NopObservableLongGauge();

    // request metrics
    public static LongCounter messagesInTotal = new NopLongCounter();
    public static LongCounter messagesOutTotal = new NopLongCounter();
    public static LongCounter throughputInTotal = new NopLongCounter();
    public static LongCounter throughputOutTotal = new NopLongCounter();
    public static LongHistogram messageSize = new NopLongHistogram();

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

    public static boolean isRetryOrDlqTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        return topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }

    public static TopicMessageType getMessageType(SendMessageRequestHeader requestHeader) {
        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        TopicMessageType topicMessageType = TopicMessageType.NORMAL;
        if (Boolean.parseBoolean(traFlag)) {
            topicMessageType = TopicMessageType.TRANSACTION;
        } else if (properties.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
            topicMessageType = TopicMessageType.FIFO;
        } else if (properties.get("__STARTDELIVERTIME") != null
            || properties.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null
            || properties.get(MessageConst.PROPERTY_TIMER_DELIVER_MS) != null
            || properties.get(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null) {
            topicMessageType = TopicMessageType.DELAY;
        }
        return topicMessageType;
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

        registerMetricsView(providerBuilder);

        brokerMeter = OpenTelemetrySdk.builder()
            .setMeterProvider(providerBuilder.build())
            .build()
            .getMeter(OPEN_TELEMETRY_METER_NAME);

        initStatsMetrics();
        initRequestMetrics();
    }

    private void registerMetricsView(SdkMeterProviderBuilder providerBuilder) {
        // message size buckets, 1k, 4k, 512k, 1M, 2M, 4M
        List<Double> messageSizeBuckets = Arrays.asList(
            1d * 1024, //1KB
            4d * 1024, //4KB
            512d * 1024, //512KB
            1d * 1024 * 1024, //1MB
            2d * 1024 * 1024, //2MB
            4d * 1024 * 1024 //4MB
        );
        InstrumentSelector messageSizeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_MESSAGE_SIZE)
            .build();
        View messageSizeView = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(messageSizeBuckets))
            .build();
        providerBuilder.registerView(messageSizeSelector, messageSizeView);
    }

    private void initStatsMetrics() {
        processorWatermark = brokerMeter.gaugeBuilder(GAUGE_PROCESSOR_WATERMARK)
            .setDescription("Request processor watermark")
            .ofLongs()
            .buildWithCallback(measurement -> {
                measurement.record(brokerController.getSendThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "send").build());
                measurement.record(brokerController.getAsyncPutThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "async_put").build());
                measurement.record(brokerController.getPullThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "pull").build());
                measurement.record(brokerController.getAckThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "ack").build());
                measurement.record(brokerController.getQueryThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "query_message").build());
                measurement.record(brokerController.getClientManagerThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "client_manager").build());
                measurement.record(brokerController.getHeartbeatThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "heartbeat").build());
                measurement.record(brokerController.getLitePullThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "lite_pull").build());
                measurement.record(brokerController.getEndTransactionThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "transaction").build());
                measurement.record(brokerController.getConsumerManagerThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "consumer_manager").build());
                measurement.record(brokerController.getAdminBrokerThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "admin").build());
                measurement.record(brokerController.getReplyThreadPoolQueue().size(), newAttributesBuilder().put(LABEL_PROCESSOR, "reply").build());
            });

        brokerPermission = brokerMeter.gaugeBuilder(GAUGE_BROKER_PERMISSION)
            .setDescription("Broker permission")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(brokerConfig.getBrokerPermission(), newAttributesBuilder().build()));
    }

    private void initRequestMetrics() {
        messagesInTotal = brokerMeter.counterBuilder(COUNTER_MESSAGES_IN_TOTAL)
            .setDescription("Total number of incoming messages")
            .build();

        messagesOutTotal = brokerMeter.counterBuilder(COUNTER_MESSAGES_OUT_TOTAL)
            .setDescription("Total number of outgoing messages")
            .build();

        throughputInTotal = brokerMeter.counterBuilder(COUNTER_THROUGHPUT_IN_TOTAL)
            .setDescription("Total traffic of incoming messages")
            .build();

        throughputOutTotal = brokerMeter.counterBuilder(COUNTER_THROUGHPUT_OUT_TOTAL)
            .setDescription("Total traffic of outgoing messages")
            .build();

        messageSize = brokerMeter.histogramBuilder(HISTOGRAM_MESSAGE_SIZE)
            .setDescription("Incoming messages size")
            .ofLongs()
            .build();
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
