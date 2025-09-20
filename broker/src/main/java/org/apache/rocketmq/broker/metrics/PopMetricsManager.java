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

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PopBufferMergeService;
import org.apache.rocketmq.broker.processor.PopReviveService;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_CONSUMER_GROUP;
import static org.apache.rocketmq.broker.metrics.BrokerMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.GAUGE_POP_CHECKPOINT_BUFFER_SIZE;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.GAUGE_POP_OFFSET_BUFFER_SIZE;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.GAUGE_POP_REVIVE_LAG;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.GAUGE_POP_REVIVE_LATENCY;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.LABEL_PUT_STATUS;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.LABEL_QUEUE_ID;
import static org.apache.rocketmq.broker.metrics.PopMetricsConstant.LABEL_REVIVE_MESSAGE_TYPE;

public class PopMetricsManager {
    private static final Logger log = LoggerFactory.getLogger(PopMetricsManager.class);
    
    private Supplier<AttributesBuilder> attributesBuilderSupplier;

    private LongHistogram popBufferScanTimeConsume = new NopLongHistogram();
    private LongCounter popRevivePutTotal = new NopLongCounter();
    private LongCounter popReviveGetTotal = new NopLongCounter();
    private LongCounter popReviveRetryMessageTotal = new NopLongCounter();

    public PopMetricsManager() {
    }

    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofMillis(1).toMillis(),
            (double) Duration.ofMillis(10).toMillis(),
            (double) Duration.ofMillis(100).toMillis(),
            (double) Duration.ofSeconds(1).toMillis(),
            (double) Duration.ofSeconds(2).toMillis(),
            (double) Duration.ofSeconds(3).toMillis()
        );
        InstrumentSelector popBufferScanTimeConsumeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME)
            .build();
        ViewBuilder popBufferScanTimeConsumeViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));

        return Lists.newArrayList(new Pair<>(popBufferScanTimeConsumeSelector, popBufferScanTimeConsumeViewBuilder));
    }

    public void initMetrics(Meter meter, BrokerController brokerController,
        Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;

        this.popBufferScanTimeConsume = meter.histogramBuilder(HISTOGRAM_POP_BUFFER_SCAN_TIME_CONSUME)
            .setDescription("Time consuming of pop buffer scan")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
        this.popRevivePutTotal = meter.counterBuilder(COUNTER_POP_REVIVE_IN_MESSAGE_TOTAL)
            .setDescription("Total number of put message to revive topic")
            .build();
        this.popReviveGetTotal = meter.counterBuilder(COUNTER_POP_REVIVE_OUT_MESSAGE_TOTAL)
            .setDescription("Total number of get message from revive topic")
            .build();
        this.popReviveRetryMessageTotal = meter.counterBuilder(COUNTER_POP_REVIVE_RETRY_MESSAGES_TOTAL)
            .setDescription("Total number of put message to pop retry topic")
            .build();

        meter.gaugeBuilder(GAUGE_POP_OFFSET_BUFFER_SIZE)
            .setDescription("Time number of buffered offset")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopBufferOffsetSize(brokerController, measurement));
        meter.gaugeBuilder(GAUGE_POP_CHECKPOINT_BUFFER_SIZE)
            .setDescription("The number of buffered checkpoint")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopBufferCkSize(brokerController, measurement));
        meter.gaugeBuilder(GAUGE_POP_REVIVE_LAG)
            .setDescription("The processing lag of revive topic")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopReviveLag(brokerController, measurement));
        meter.gaugeBuilder(GAUGE_POP_REVIVE_LATENCY)
            .setDescription("The processing latency of revive topic")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> calculatePopReviveLatency(brokerController, measurement));
    }

    private void calculatePopBufferOffsetSize(BrokerController brokerController,
        ObservableLongMeasurement measurement) {
        PopBufferMergeService popBufferMergeService = brokerController.getPopMessageProcessor().getPopBufferMergeService();
        measurement.record(popBufferMergeService.getOffsetTotalSize(), this.newAttributesBuilder().build());
    }

    private void calculatePopBufferCkSize(BrokerController brokerController,
        ObservableLongMeasurement measurement) {
        PopBufferMergeService popBufferMergeService = brokerController.getPopMessageProcessor().getPopBufferMergeService();
        measurement.record(popBufferMergeService.getBufferedCKSize(), this.newAttributesBuilder().build());
    }

    private void calculatePopReviveLatency(BrokerController brokerController,
        ObservableLongMeasurement measurement) {
        PopReviveService[] popReviveServices = brokerController.getAckMessageProcessor().getPopReviveServices();
        for (PopReviveService popReviveService : popReviveServices) {
            try {
                measurement.record(popReviveService.getReviveBehindMillis(), this.newAttributesBuilder()
                    .put(LABEL_QUEUE_ID, popReviveService.getQueueId())
                    .build());
            } catch (ConsumeQueueException e) {
                log.error("Failed to get revive behind duration", e);
            }
        }
    }

    private void calculatePopReviveLag(BrokerController brokerController,
        ObservableLongMeasurement measurement) {
        PopReviveService[] popReviveServices = brokerController.getAckMessageProcessor().getPopReviveServices();
        for (PopReviveService popReviveService : popReviveServices) {
            try {
                measurement.record(popReviveService.getReviveBehindMessages(), this.newAttributesBuilder()
                    .put(LABEL_QUEUE_ID, popReviveService.getQueueId())
                    .build());
            } catch (ConsumeQueueException e) {
                log.error("Failed to get revive behind message count", e);
            }
        }
    }

    public void incPopReviveAckPutCount(AckMsg ackMsg, PutMessageStatus status) {
        incPopRevivePutCount(ackMsg.getConsumerGroup(), ackMsg.getTopic(), PopReviveMessageType.ACK, status, 1);
    }

    public void incPopReviveCkPutCount(PopCheckPoint checkPoint, PutMessageStatus status) {
        incPopRevivePutCount(checkPoint.getCId(), checkPoint.getTopic(), PopReviveMessageType.CK, status, 1);
    }

    public void incPopRevivePutCount(String group, String topic, PopReviveMessageType messageType,
        PutMessageStatus status, int num) {
        Attributes attributes = this.newAttributesBuilder()
            .put(LABEL_CONSUMER_GROUP, group)
            .put(LABEL_TOPIC, topic)
            .put(LABEL_REVIVE_MESSAGE_TYPE, messageType.name())
            .put(LABEL_PUT_STATUS, status.name())
            .build();
        this.popRevivePutTotal.add(num, attributes);
    }

    public void incPopReviveAckGetCount(AckMsg ackMsg, int queueId) {
        incPopReviveGetCount(ackMsg.getConsumerGroup(), ackMsg.getTopic(), PopReviveMessageType.ACK, queueId, 1);
    }

    public void incPopReviveCkGetCount(PopCheckPoint checkPoint, int queueId) {
        incPopReviveGetCount(checkPoint.getCId(), checkPoint.getTopic(), PopReviveMessageType.CK, queueId, 1);
    }

    public void incPopReviveGetCount(String group, String topic, PopReviveMessageType messageType, int queueId,
        int num) {
        AttributesBuilder builder = this.newAttributesBuilder();
        Attributes attributes = builder
            .put(LABEL_CONSUMER_GROUP, group)
            .put(LABEL_TOPIC, topic)
            .put(LABEL_QUEUE_ID, queueId)
            .put(LABEL_REVIVE_MESSAGE_TYPE, messageType.name())
            .build();
        this.popReviveGetTotal.add(num, attributes);
    }

    public void incPopReviveRetryMessageCount(PopCheckPoint checkPoint, PutMessageStatus status) {
        AttributesBuilder builder = this.newAttributesBuilder();
        Attributes attributes = builder
            .put(LABEL_CONSUMER_GROUP, checkPoint.getCId())
            .put(LABEL_TOPIC, checkPoint.getTopic())
            .put(LABEL_PUT_STATUS, status.name())
            .build();
        this.popReviveRetryMessageTotal.add(1, attributes);
    }

    public void recordPopBufferScanTimeConsume(long time) {
        this.popBufferScanTimeConsume.record(time, this.newAttributesBuilder().build());
    }

    public AttributesBuilder newAttributesBuilder() {
        return this.attributesBuilderSupplier != null ? this.attributesBuilderSupplier.get() : Attributes.builder();
    }

    // Getter methods for external access
    public LongHistogram getPopBufferScanTimeConsume() {
        return popBufferScanTimeConsume;
    }

    public LongCounter getPopRevivePutTotal() {
        return popRevivePutTotal;
    }

    public LongCounter getPopReviveGetTotal() {
        return popReviveGetTotal;
    }

    public LongCounter getPopReviveRetryMessageTotal() {
        return popReviveRetryMessageTotal;
    }

    public Supplier<AttributesBuilder> getAttributesBuilderSupplier() {
        return attributesBuilderSupplier;
    }

    // Setter methods for testing
    public void setAttributesBuilderSupplier(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;
    }

}
