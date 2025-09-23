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
package org.apache.rocketmq.store.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.Slot;
import org.apache.rocketmq.store.timer.TimerMessageStore;
import org.apache.rocketmq.store.timer.TimerMetrics;
import org.apache.rocketmq.store.timer.TimerWheel;

import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.COUNTER_TIMER_DEQUEUE_TOTAL;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.COUNTER_TIMER_ENQUEUE_TOTAL;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_TYPE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_DISPATCH_BEHIND;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_FLUSH_BEHIND;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_MESSAGE_RESERVE_TIME;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_SIZE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMER_DEQUEUE_LAG;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMER_DEQUEUE_LATENCY;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMER_ENQUEUE_LAG;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMER_ENQUEUE_LATENCY;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMER_MESSAGE_SNAPSHOT;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMING_MESSAGES;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.HISTOGRAM_DELAY_MSG_LATENCY;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_TYPE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_TIMING_BOUND;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_TOPIC;

public class DefaultStoreMetricsManager {
    private Supplier<AttributesBuilder> attributesBuilderSupplier;
    private MessageStoreConfig messageStoreConfig;

    private ObservableLongGauge storageSize = new NopObservableLongGauge();
    private ObservableLongGauge flushBehind = new NopObservableLongGauge();
    private ObservableLongGauge dispatchBehind = new NopObservableLongGauge();
    private ObservableLongGauge messageReserveTime = new NopObservableLongGauge();

    private ObservableLongGauge timerEnqueueLag = new NopObservableLongGauge();
    private ObservableLongGauge timerEnqueueLatency = new NopObservableLongGauge();
    private ObservableLongGauge timerDequeueLag = new NopObservableLongGauge();
    private ObservableLongGauge timerDequeueLatency = new NopObservableLongGauge();
    private ObservableLongGauge timingMessages = new NopObservableLongGauge();

    private LongCounter timerDequeueTotal = new NopLongCounter();
    private LongCounter timerEnqueueTotal = new NopLongCounter();
    private ObservableLongGauge timerMessageSnapshot = new NopObservableLongGauge();
    private LongHistogram timerMessageSetLatency = new NopLongHistogram();

    private RocksDBStoreMetricsManager rocksDBStoreMetricsManager;

    public DefaultStoreMetricsManager() {
        this.rocksDBStoreMetricsManager = new RocksDBStoreMetricsManager();
    }

    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Double> rpcCostTimeBuckets = Arrays.asList(
                // day * hour * min * second
                1d * 1 * 1 * 60, // 60 second
                1d * 1 * 10 * 60, // 10 min
                1d * 1 * 60 * 60, // 1 hour
                1d * 12 * 60 * 60, // 12 hour
                1d * 24 * 60 * 60, // 1 day
                3d * 24 * 60 * 60 // 3 day
        );
        InstrumentSelector selector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM)
                .setName(HISTOGRAM_DELAY_MSG_LATENCY)
                .build();
        ViewBuilder viewBuilder = View.builder()
                .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));
        return Lists.newArrayList(new Pair<>(selector, viewBuilder));
    }

    public void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier,
        DefaultMessageStore messageStore) {

        // Also add some metrics for rocksdb's monitoring.
        this.rocksDBStoreMetricsManager.init(meter, attributesBuilderSupplier, messageStore.getQueueStore());

        this.attributesBuilderSupplier = attributesBuilderSupplier;
        this.messageStoreConfig = messageStore.getMessageStoreConfig();

        this.storageSize = meter.gaugeBuilder(GAUGE_STORAGE_SIZE)
            .setDescription("Broker storage size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {
                File storeDir = new File(this.messageStoreConfig.getStorePathRootDir());
                if (storeDir.exists() && storeDir.isDirectory()) {
                    long totalSpace = storeDir.getTotalSpace();
                    if (totalSpace > 0) {
                        measurement.record(totalSpace - storeDir.getFreeSpace(), this.newAttributesBuilder().build());
                    }
                }
            });

        this.flushBehind = meter.gaugeBuilder(GAUGE_STORAGE_FLUSH_BEHIND)
            .setDescription("Broker flush behind bytes")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(messageStore.flushBehindBytes(), this.newAttributesBuilder().build()));

        this.dispatchBehind = meter.gaugeBuilder(GAUGE_STORAGE_DISPATCH_BEHIND)
            .setDescription("Broker dispatch behind bytes")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(messageStore.dispatchBehindBytes(), this.newAttributesBuilder().build()));

        this.messageReserveTime = meter.gaugeBuilder(GAUGE_STORAGE_MESSAGE_RESERVE_TIME)
            .setDescription("Broker message reserve time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                long earliestMessageTime = messageStore.getEarliestMessageTime();
                if (earliestMessageTime <= 0) {
                    return;
                }
                measurement.record(System.currentTimeMillis() - earliestMessageTime, this.newAttributesBuilder().build());
            });

        if (messageStore.getMessageStoreConfig().isTimerWheelEnable()) {
            this.timerEnqueueLag = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LAG)
                .setDescription("Timer enqueue messages lag")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getEnqueueBehindMessages(), this.newAttributesBuilder().build());
                });

            this.timerEnqueueLatency = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LATENCY)
                .setDescription("Timer enqueue latency")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getEnqueueBehindMillis(), this.newAttributesBuilder().build());
                });
            this.timerDequeueLag = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LAG)
                .setDescription("Timer dequeue messages lag")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getDequeueBehindMessages(), this.newAttributesBuilder().build());
                });
            this.timerDequeueLatency = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LATENCY)
                .setDescription("Timer dequeue latency")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getDequeueBehind(), this.newAttributesBuilder().build());
                });
            this.timingMessages = meter.gaugeBuilder(GAUGE_TIMING_MESSAGES)
                .setDescription("Current message number in timing")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    timerMessageStore.getTimerMetrics()
                        .getTimingCount()
                        .forEach((topic, metric) -> {
                            measurement.record(
                                metric.getCount().get(),
                                this.newAttributesBuilder().put(LABEL_TOPIC, topic).build()
                            );
                        });
                });
            this.timerDequeueTotal = meter.counterBuilder(COUNTER_TIMER_DEQUEUE_TOTAL)
                .setDescription("Total number of timer dequeue")
                .build();
            this.timerEnqueueTotal = meter.counterBuilder(COUNTER_TIMER_ENQUEUE_TOTAL)
                .setDescription("Total number of timer enqueue")
                .build();
            this.timerMessageSnapshot = meter.gaugeBuilder(GAUGE_TIMER_MESSAGE_SNAPSHOT)
                .setDescription("Timer message distribution snapshot, only count timing messages in 24h.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMetrics timerMetrics = messageStore.getTimerMessageStore().getTimerMetrics();
                    TimerWheel timerWheel = messageStore.getTimerMessageStore().getTimerWheel();
                    int precisionMs = this.messageStoreConfig.getTimerPrecisionMs();
                    List<Integer> timerDist = timerMetrics.getTimerDistList();
                    long currTime = System.currentTimeMillis() / precisionMs * precisionMs;
                    for (int i = 0; i < timerDist.size(); i++) {
                        int slotBeforeNum = i == 0 ? 0 : timerDist.get(i - 1) * 1000 / precisionMs;
                        int slotTotalNum = timerDist.get(i) * 1000 / precisionMs;
                        int periodTotal = 0;
                        for (int j = slotBeforeNum; j < slotTotalNum; j++) {
                            Slot slotEach = timerWheel.getSlot(currTime + (long) j * precisionMs);
                            periodTotal += slotEach.num;
                        }
                        measurement.record(periodTotal, this.newAttributesBuilder().put(LABEL_TIMING_BOUND, timerDist.get(i).toString()).build());
                    }
                });
            this.timerMessageSetLatency = meter.histogramBuilder(HISTOGRAM_DELAY_MSG_LATENCY)
                    .setDescription("Timer message set latency distribution")
                    .setUnit("seconds")
                    .ofLongs()
                    .build();
        }
    }

    public void incTimerDequeueCount(String topic) {
        this.timerDequeueTotal.add(1, this.newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .build());
    }

    public void incTimerEnqueueCount(String topic) {
        AttributesBuilder attributesBuilder = this.newAttributesBuilder();
        if (topic != null) {
            attributesBuilder.put(LABEL_TOPIC, topic);
        }
        this.timerEnqueueTotal.add(1, attributesBuilder.build());
    }

    public AttributesBuilder newAttributesBuilder() {
        if (this.attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return this.attributesBuilderSupplier.get()
            .put(LABEL_STORAGE_TYPE, DEFAULT_STORAGE_TYPE)
            .put(LABEL_STORAGE_MEDIUM, DEFAULT_STORAGE_MEDIUM);
    }

    // Getter methods for external access
    public Supplier<AttributesBuilder> getAttributesBuilderSupplier() {
        return attributesBuilderSupplier;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ObservableLongGauge getStorageSize() {
        return storageSize;
    }

    public ObservableLongGauge getFlushBehind() {
        return flushBehind;
    }

    public ObservableLongGauge getDispatchBehind() {
        return dispatchBehind;
    }

    public ObservableLongGauge getMessageReserveTime() {
        return messageReserveTime;
    }

    public ObservableLongGauge getTimerEnqueueLag() {
        return timerEnqueueLag;
    }

    public ObservableLongGauge getTimerEnqueueLatency() {
        return timerEnqueueLatency;
    }

    public ObservableLongGauge getTimerDequeueLag() {
        return timerDequeueLag;
    }

    public ObservableLongGauge getTimerDequeueLatency() {
        return timerDequeueLatency;
    }

    public ObservableLongGauge getTimingMessages() {
        return timingMessages;
    }

    public LongCounter getTimerDequeueTotal() {
        return timerDequeueTotal;
    }

    public LongCounter getTimerEnqueueTotal() {
        return timerEnqueueTotal;
    }

    public ObservableLongGauge getTimerMessageSnapshot() {
        return timerMessageSnapshot;
    }

    public LongHistogram getTimerMessageSetLatency() {
        return timerMessageSetLatency;
    }

    // Setter methods for testing
    public void setAttributesBuilderSupplier(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;
    }

    public void setMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    public RocksDBStoreMetricsManager getRocksDBStoreMetricsManager() {
        return rocksDBStoreMetricsManager;
    }
}
