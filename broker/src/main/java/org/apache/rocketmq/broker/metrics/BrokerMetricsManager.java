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
import io.opentelemetry.sdk.metrics.ViewBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.SdkMeterProviderUtil;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.AGGREGATION_DELTA;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_COMMIT_MESSAGES_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_MESSAGES_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_ROLLBACK_MESSAGES_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_IN_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.COUNTER_THROUGHPUT_OUT_TOTAL;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_TOPIC_NUM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_GROUP_NUM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_BROKER_PERMISSION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_CONNECTIONS;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_INFLIGHT_MESSAGES;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_LAG_LATENCY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_LAG_MESSAGES;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_QUEUEING_LATENCY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_CONSUMER_READY_MESSAGES;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_HALF_MESSAGES;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_PROCESSOR_WATERMARK;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.GAUGE_PRODUCER_CONNECTIONS;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_FINISH_MSG_LATENCY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_MESSAGE_SIZE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_AGGREGATION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CLUSTER_NAME;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUME_MODE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_RETRY;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_IS_SYSTEM;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_LANGUAGE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_ID;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_NODE_TYPE;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_PROCESSOR;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_VERSION;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.NODE_TYPE_BROKER;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.OPEN_TELEMETRY_METER_NAME;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_PROTOCOL_TYPE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.PROTOCOL_TYPE_REMOTING;

public class BrokerMetricsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerConfig brokerConfig;
    private final MessageStore messageStore;
    private final BrokerController brokerController;
    private final ConsumerLagCalculator consumerLagCalculator;
    private final Map<String, String> labelMap = new HashMap<>();
    private OtlpGrpcMetricExporter metricExporter;
    private PeriodicMetricReader periodicMetricReader;
    private PrometheusHttpServer prometheusHttpServer;
    private MetricExporter loggingMetricExporter;
    private Meter brokerMeter;

    private Supplier<AttributesBuilder> attributesBuilderSupplier = Attributes::builder;

    // broker stats metrics
    private ObservableLongGauge processorWatermark = new NopObservableLongGauge();
    private ObservableLongGauge brokerPermission = new NopObservableLongGauge();
    private ObservableLongGauge topicNum = new NopObservableLongGauge();
    private ObservableLongGauge consumerGroupNum = new NopObservableLongGauge();

    // request metrics
    private LongCounter messagesInTotal = new NopLongCounter();
    private LongCounter messagesOutTotal = new NopLongCounter();
    private LongCounter throughputInTotal = new NopLongCounter();
    private LongCounter throughputOutTotal = new NopLongCounter();
    private LongHistogram messageSize = new NopLongHistogram();
    private LongHistogram topicCreateExecuteTime = new NopLongHistogram();
    private LongHistogram consumerGroupCreateExecuteTime = new NopLongHistogram();

    // client connection metrics
    private ObservableLongGauge producerConnection = new NopObservableLongGauge();
    private ObservableLongGauge consumerConnection = new NopObservableLongGauge();

    // Lag metrics
    private ObservableLongGauge consumerLagMessages = new NopObservableLongGauge();
    private ObservableLongGauge consumerLagLatency = new NopObservableLongGauge();
    private ObservableLongGauge consumerInflightMessages = new NopObservableLongGauge();
    private ObservableLongGauge consumerQueueingLatency = new NopObservableLongGauge();
    private ObservableLongGauge consumerReadyMessages = new NopObservableLongGauge();
    private LongCounter sendToDlqMessages = new NopLongCounter();
    private ObservableLongGauge halfMessages = new NopObservableLongGauge();
    private LongCounter commitMessagesTotal = new NopLongCounter();
    private LongCounter rollBackMessagesTotal = new NopLongCounter();
    private LongHistogram transactionFinishLatency = new NopLongHistogram();

    @SuppressWarnings("DoubleBraceInitialization")
    public static final List<String> SYSTEM_GROUP_PREFIX_LIST = new ArrayList<String>() {
        {
            add(MixAll.CID_RMQ_SYS_PREFIX.toLowerCase());
        }
    };

    public BrokerMetricsManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        brokerConfig = brokerController.getBrokerConfig();
        this.messageStore = brokerController.getMessageStore();
        this.consumerLagCalculator = new ConsumerLagCalculator(brokerController);
        init();
    }

    public AttributesBuilder newAttributesBuilder() {
        AttributesBuilder attributesBuilder;
        if (attributesBuilderSupplier == null) {
            attributesBuilderSupplier = Attributes::builder;
        }
        attributesBuilder = attributesBuilderSupplier.get();
        labelMap.forEach(attributesBuilder::put);
        return attributesBuilder;
    }

    private Attributes buildLagAttributes(ConsumerLagCalculator.BaseCalculateResult result) {
        AttributesBuilder attributesBuilder = newAttributesBuilder();
        attributesBuilder.put(LABEL_CONSUMER_GROUP, result.group);
        attributesBuilder.put(LABEL_TOPIC, result.topic);
        attributesBuilder.put(LABEL_IS_RETRY, result.isRetry);
        attributesBuilder.put(LABEL_IS_SYSTEM, isSystem(result.topic, result.group));
        return attributesBuilder.build();
    }

    public static boolean isRetryOrDlqTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        return topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX);
    }

    public static boolean isSystemGroup(String group) {
        if (StringUtils.isBlank(group)) {
            return false;
        }
        String groupInLowerCase = group.toLowerCase();
        for (String prefix : SYSTEM_GROUP_PREFIX_LIST) {
            if (groupInLowerCase.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isSystem(String topic, String group) {
        return TopicValidator.isSystemTopic(topic) || isSystemGroup(group);
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
            || properties.get(MessageConst.PROPERTY_TIMER_DELAY_SEC) != null
            || properties.get(MessageConst.PROPERTY_TIMER_DELAY_MS) != null) {
            topicMessageType = TopicMessageType.DELAY;
        }
        return topicMessageType;
    }

    public Meter getBrokerMeter() {
        return brokerMeter;
    }

    // Getter methods for metrics variables
    public LongCounter getMessagesInTotal() {
        return messagesInTotal;
    }

    public LongCounter getMessagesOutTotal() {
        return messagesOutTotal;
    }

    public LongCounter getThroughputInTotal() {
        return throughputInTotal;
    }

    public LongCounter getThroughputOutTotal() {
        return throughputOutTotal;
    }

    public LongHistogram getMessageSize() {
        return messageSize;
    }

    public LongCounter getSendToDlqMessages() {
        return sendToDlqMessages;
    }

    public LongCounter getCommitMessagesTotal() {
        return commitMessagesTotal;
    }

    public LongCounter getRollBackMessagesTotal() {
        return rollBackMessagesTotal;
    }

    public LongHistogram getTransactionFinishLatency() {
        return transactionFinishLatency;
    }

    public LongHistogram getTopicCreateExecuteTime() {
        return topicCreateExecuteTime;
    }

    public LongHistogram getConsumerGroupCreateExecuteTime() {
        return consumerGroupCreateExecuteTime;
    }

    // Setter method for testing purposes
    public void setAttributesBuilderSupplier(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    private boolean checkConfig() {
        if (brokerConfig == null) {
            return false;
        }
        MetricsExporterType exporterType = brokerConfig.getMetricsExporterType();
        if (!exporterType.isEnable()) {
            return false;
        }

        switch (exporterType) {
            case OTLP_GRPC:
                return StringUtils.isNotBlank(brokerConfig.getMetricsGrpcExporterTarget());
            case PROM:
                return true;
            case LOG:
                return true;
        }
        return false;
    }

    private void init() {
        MetricsExporterType metricsExporterType = brokerConfig.getMetricsExporterType();
        if (metricsExporterType == MetricsExporterType.DISABLE) {
            return;
        }

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
                labelMap.put(split[0], split[1]);
            }
        }
        if (brokerConfig.isMetricsInDelta()) {
            labelMap.put(LABEL_AGGREGATION, AGGREGATION_DELTA);
        }
        labelMap.put(LABEL_NODE_TYPE, NODE_TYPE_BROKER);
        labelMap.put(LABEL_CLUSTER_NAME, brokerConfig.getBrokerClusterName());
        labelMap.put(LABEL_NODE_ID, brokerConfig.getBrokerName());

        SdkMeterProviderBuilder providerBuilder = SdkMeterProvider.builder()
            .setResource(Resource.empty());

        if (metricsExporterType == MetricsExporterType.OTLP_GRPC) {
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

        if (metricsExporterType == MetricsExporterType.PROM) {
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

        if (metricsExporterType == MetricsExporterType.LOG) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
            loggingMetricExporter = OtlpJsonLoggingMetricExporter.create(brokerConfig.isMetricsInDelta() ? AggregationTemporality.DELTA : AggregationTemporality.CUMULATIVE);
            java.util.logging.Logger.getLogger(OtlpJsonLoggingMetricExporter.class.getName()).setLevel(java.util.logging.Level.FINEST);
            periodicMetricReader = PeriodicMetricReader.builder(loggingMetricExporter)
                .setInterval(brokerConfig.getMetricLoggingExporterIntervalInMills(), TimeUnit.MILLISECONDS)
                .build();
            providerBuilder.registerMetricReader(periodicMetricReader);
        }

        registerMetricsView(providerBuilder);

        brokerMeter = OpenTelemetrySdk.builder()
            .setMeterProvider(providerBuilder.build())
            .build()
            .getMeter(OPEN_TELEMETRY_METER_NAME);

        initStatsMetrics();
        initRequestMetrics();
        initConnectionMetrics();
        initLagAndDlqMetrics();
        initTransactionMetrics();
        initOtherMetrics();
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

        List<Double> commitLatencyBuckets = Arrays.asList(
                1d * 1 * 1 * 5, //5s
                1d * 1 * 1 * 60, //1min
                1d * 1 * 10 * 60, //10min
                1d * 1 * 60 * 60, //1h
                1d * 12 * 60 * 60, //12h
                1d * 24 * 60 * 60 //24h
        );

        List<Double> createTimeBuckets = Arrays.asList(
                (double) Duration.ofMillis(10).toMillis(), //10ms
                (double) Duration.ofMillis(100).toMillis(), //100ms
                (double) Duration.ofSeconds(1).toMillis(), //1s
                (double) Duration.ofSeconds(3).toMillis(), //3s
                (double) Duration.ofSeconds(5).toMillis() //5s
        );
        InstrumentSelector messageSizeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_MESSAGE_SIZE)
            .build();
        ViewBuilder messageSizeViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(messageSizeBuckets));
        // To config the cardinalityLimit for openTelemetry metrics exporting.
        SdkMeterProviderUtil.setCardinalityLimit(messageSizeViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(messageSizeSelector, messageSizeViewBuilder.build());

        InstrumentSelector commitLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_FINISH_MSG_LATENCY)
            .build();
        ViewBuilder commitLatencyViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(commitLatencyBuckets));
        // To config the cardinalityLimit for openTelemetry metrics exporting.
        SdkMeterProviderUtil.setCardinalityLimit(commitLatencyViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(commitLatencySelector, commitLatencyViewBuilder.build());

        InstrumentSelector createTopicTimeSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM)
                .setName(HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME)
                .build();
        InstrumentSelector createSubGroupTimeSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM)
                .setName(HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME)
                .build();
        ViewBuilder createTopicTimeViewBuilder = View.builder()
                .setAggregation(Aggregation.explicitBucketHistogram(createTimeBuckets));
        ViewBuilder createSubGroupTimeViewBuilder = View.builder()
                .setAggregation(Aggregation.explicitBucketHistogram(createTimeBuckets));
        // To config the cardinalityLimit for openTelemetry metrics exporting.
        SdkMeterProviderUtil.setCardinalityLimit(createTopicTimeViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(createTopicTimeSelector, createTopicTimeViewBuilder.build());
        SdkMeterProviderUtil.setCardinalityLimit(createSubGroupTimeViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(createSubGroupTimeSelector, createSubGroupTimeViewBuilder.build());

        for (Pair<InstrumentSelector, ViewBuilder> selectorViewPair : RemotingMetricsManager.getMetricsView()) {
            ViewBuilder viewBuilder = selectorViewPair.getObject2();
            SdkMeterProviderUtil.setCardinalityLimit(viewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
            providerBuilder.registerView(selectorViewPair.getObject1(), viewBuilder.build());
        }

        for (Pair<InstrumentSelector, ViewBuilder> selectorViewPair : messageStore.getMetricsView()) {
            ViewBuilder viewBuilder = selectorViewPair.getObject2();
            SdkMeterProviderUtil.setCardinalityLimit(viewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
            providerBuilder.registerView(selectorViewPair.getObject1(), viewBuilder.build());
        }

        for (Pair<InstrumentSelector, ViewBuilder> selectorViewPair : PopMetricsManager.getMetricsView()) {
            ViewBuilder viewBuilder = selectorViewPair.getObject2();
            SdkMeterProviderUtil.setCardinalityLimit(viewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
            providerBuilder.registerView(selectorViewPair.getObject1(), viewBuilder.build());
        }

        // default view builder for all counter.
        InstrumentSelector defaultCounterSelector = InstrumentSelector.builder()
            .setType(InstrumentType.COUNTER)
            .build();
        ViewBuilder defaultCounterViewBuilder = View.builder().setDescription("default view for counter.");
        SdkMeterProviderUtil.setCardinalityLimit(defaultCounterViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(defaultCounterSelector, defaultCounterViewBuilder.build());

        //default view builder for all observable gauge.
        InstrumentSelector defaultGaugeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.OBSERVABLE_GAUGE)
            .build();
        ViewBuilder defaultGaugeViewBuilder = View.builder().setDescription("default view for gauge.");
        SdkMeterProviderUtil.setCardinalityLimit(defaultGaugeViewBuilder, brokerConfig.getMetricsOtelCardinalityLimit());
        providerBuilder.registerView(defaultGaugeSelector, defaultGaugeViewBuilder.build());
    }

    private void initStatsMetrics() {
        if (!brokerConfig.isEnableStatsMetrics()) {
            return;
        }

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

        topicNum = brokerMeter.gaugeBuilder(GAUGE_TOPIC_NUM)
            .setDescription("Active topic number")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(brokerController.getTopicConfigManager().getTopicConfigTable().size(), newAttributesBuilder().build()));

        consumerGroupNum = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_GROUP_NUM)
            .setDescription("Active subscription group number")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().size(), newAttributesBuilder().build()));
    }

    private void initRequestMetrics() {
        if (!brokerConfig.isEnableRequestMetrics()) {
            return;
        }

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

        topicCreateExecuteTime = brokerMeter.histogramBuilder(HISTOGRAM_TOPIC_CREATE_EXECUTE_TIME)
                .setDescription("The distribution of create topic time")
                .ofLongs()
                .setUnit("milliseconds")
                .build();

        consumerGroupCreateExecuteTime = brokerMeter.histogramBuilder(HISTOGRAM_CONSUMER_GROUP_CREATE_EXECUTE_TIME)
                .setDescription("The distribution of create subscription time")
                .ofLongs()
                .setUnit("milliseconds")
                .build();
    }

    private void initConnectionMetrics() {
        if (!brokerConfig.isEnableConnectionMetrics()) {
            return;
        }

        producerConnection = brokerMeter.gaugeBuilder(GAUGE_PRODUCER_CONNECTIONS)
            .setDescription("Producer connections")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<ProducerAttr, Integer> metricsMap = new HashMap<>();
                brokerController.getProducerManager()
                    .getGroupChannelTable()
                    .values()
                    .stream()
                    .flatMap(map -> map.values().stream())
                    .forEach(info -> {
                        ProducerAttr attr = new ProducerAttr(info.getLanguage(), info.getVersion());
                        Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                        metricsMap.put(attr, count + 1);
                    });
                metricsMap.forEach((attr, count) -> {
                    Attributes attributes = newAttributesBuilder()
                        .put(LABEL_LANGUAGE, attr.language.name().toLowerCase())
                        .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.version).toLowerCase())
                        .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING)
                        .build();
                    measurement.record(count, attributes);
                });
            });

        consumerConnection = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_CONNECTIONS)
            .setDescription("Consumer connections")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<ConsumerAttr, Integer> metricsMap = new HashMap<>();
                ConsumerManager consumerManager = brokerController.getConsumerManager();
                consumerManager.getConsumerTable()
                    .forEach((group, groupInfo) -> {
                        if (groupInfo != null) {
                            groupInfo.getChannelInfoTable().values().forEach(info -> {
                                ConsumerAttr attr = new ConsumerAttr(group, info.getLanguage(), info.getVersion(), groupInfo.getConsumeType());
                                Integer count = metricsMap.computeIfAbsent(attr, k -> 0);
                                metricsMap.put(attr, count + 1);
                            });
                        }
                    });
                metricsMap.forEach((attr, count) -> {
                    Attributes attributes = newAttributesBuilder()
                        .put(LABEL_CONSUMER_GROUP, attr.group)
                        .put(LABEL_LANGUAGE, attr.language.name().toLowerCase())
                        .put(LABEL_VERSION, MQVersion.getVersionDesc(attr.version).toLowerCase())
                        .put(LABEL_CONSUME_MODE, attr.consumeMode.getTypeCN().toLowerCase())
                        .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING)
                        .put(LABEL_IS_SYSTEM, isSystemGroup(attr.group))
                        .build();
                    measurement.record(count, attributes);
                });
            });
    }

    private void initLagAndDlqMetrics() {
        if (!brokerConfig.isEnableLagAndDlqMetrics()) {
            return;
        }

        consumerLagMessages = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_LAG_MESSAGES)
            .setDescription("Consumer lag messages")
            .ofLongs()
            .buildWithCallback(measurement ->
                consumerLagCalculator.calculateLag(result -> measurement.record(result.lag, buildLagAttributes(result))));

        consumerLagLatency = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_LAG_LATENCY)
            .setDescription("Consumer lag time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> consumerLagCalculator.calculateLag(result -> {
                long latency = 0;
                long curTimeStamp = System.currentTimeMillis();
                if (result.earliestUnconsumedTimestamp != 0) {
                    latency = curTimeStamp - result.earliestUnconsumedTimestamp;
                }
                measurement.record(latency, buildLagAttributes(result));
            }));

        consumerInflightMessages = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_INFLIGHT_MESSAGES)
            .setDescription("Consumer inflight messages")
            .ofLongs()
            .buildWithCallback(measurement ->
                consumerLagCalculator.calculateInflight(result -> measurement.record(result.inFlight, buildLagAttributes(result))));

        consumerQueueingLatency = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_QUEUEING_LATENCY)
            .setDescription("Consumer queueing time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> consumerLagCalculator.calculateInflight(result -> {
                long latency = 0;
                long curTimeStamp = System.currentTimeMillis();
                if (result.earliestUnPulledTimestamp != 0) {
                    latency = curTimeStamp - result.earliestUnPulledTimestamp;
                }
                measurement.record(latency, buildLagAttributes(result));
            }));

        consumerReadyMessages = brokerMeter.gaugeBuilder(GAUGE_CONSUMER_READY_MESSAGES)
            .setDescription("Consumer ready messages")
            .ofLongs()
            .buildWithCallback(measurement ->
                consumerLagCalculator.calculateAvailable(result -> measurement.record(result.available, buildLagAttributes(result))));

        sendToDlqMessages = brokerMeter.counterBuilder(COUNTER_CONSUMER_SEND_TO_DLQ_MESSAGES_TOTAL)
            .setDescription("Consumer send to DLQ messages")
            .build();
    }

    private void initTransactionMetrics() {
        if (!brokerController.getBrokerConfig().isEnableTransactionMetrics()) {
            return;
        }

        commitMessagesTotal = brokerMeter.counterBuilder(COUNTER_COMMIT_MESSAGES_TOTAL)
                .setDescription("Total number of commit messages")
                .build();

        rollBackMessagesTotal = brokerMeter.counterBuilder(COUNTER_ROLLBACK_MESSAGES_TOTAL)
                .setDescription("Total number of rollback messages")
                .build();

        transactionFinishLatency = brokerMeter.histogramBuilder(HISTOGRAM_FINISH_MSG_LATENCY)
                .setDescription("Transaction finish latency")
                .ofLongs()
                .setUnit("ms")
                .build();

        halfMessages = brokerMeter.gaugeBuilder(GAUGE_HALF_MESSAGES)
                .setDescription("Half messages of all topics")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    brokerController.getTransactionalMessageService().getTransactionMetrics().getTransactionCounts()
                            .forEach((topic, metric) -> {
                                measurement.record(
                                        metric.getCount().get(),
                                        newAttributesBuilder().put(DefaultStoreMetricsConstant.LABEL_TOPIC, topic).build()
                                );
                            });
                });
    }
    private void initOtherMetrics() {
        if (brokerConfig.isEnableRemotingMetrics()) {
            RemotingMetricsManager.initMetrics(brokerMeter, this::newAttributesBuilder);
        }
        if (brokerConfig.isEnableMessageStoreMetrics()) {
            messageStore.initMetrics(brokerMeter, this::newAttributesBuilder);
        }
        if (brokerConfig.isEnablePopMetrics()) {
            PopMetricsManager.initMetrics(brokerMeter, brokerController, this::newAttributesBuilder);
        }
    }

    public void shutdown() {
        if (brokerConfig.isInBrokerContainer()) {
            // only rto need
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.OTLP_GRPC) {
                while (!periodicMetricReader.forceFlush().join(60, TimeUnit.SECONDS).isDone()) ;
                while (!periodicMetricReader.shutdown().join(60, TimeUnit.SECONDS).isSuccess()) ;
                while (!metricExporter.shutdown().join(60, TimeUnit.SECONDS).isSuccess()) ;
            }
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.PROM) {
                while (!prometheusHttpServer.forceFlush().join(60, TimeUnit.SECONDS).isDone()) ;
                while (!prometheusHttpServer.shutdown().join(60, TimeUnit.SECONDS).isSuccess()) ;
            }
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.LOG) {
                while (!periodicMetricReader.forceFlush().join(60, TimeUnit.SECONDS).isDone()) ;
                while (!periodicMetricReader.shutdown().join(60, TimeUnit.SECONDS).isSuccess()) ;
                while (!loggingMetricExporter.shutdown().join(60, TimeUnit.SECONDS).isSuccess()) ;
            }
        } else {
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.OTLP_GRPC) {
                periodicMetricReader.forceFlush();
                periodicMetricReader.shutdown();
                metricExporter.shutdown();
            }
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.PROM) {
                prometheusHttpServer.forceFlush();
                prometheusHttpServer.shutdown();
            }
            if (brokerConfig.getMetricsExporterType() == MetricsExporterType.LOG) {
                periodicMetricReader.forceFlush();
                periodicMetricReader.shutdown();
                loggingMetricExporter.shutdown();
            }
        }
    }

}
