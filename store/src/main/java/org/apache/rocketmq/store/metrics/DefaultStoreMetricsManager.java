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
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;
import java.io.File;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.timer.TimerMessageStore;

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
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_TIMING_MESSAGES;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_TYPE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_TOPIC;

public class DefaultStoreMetricsManager {
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    public static MessageStoreConfig messageStoreConfig;

    public static ObservableLongGauge storageSize = new NopObservableLongGauge();
    public static ObservableLongGauge flushBehind = new NopObservableLongGauge();
    public static ObservableLongGauge dispatchBehind = new NopObservableLongGauge();
    public static ObservableLongGauge messageReserveTime = new NopObservableLongGauge();

    public static ObservableLongGauge timerEnqueueLag = new NopObservableLongGauge();
    public static ObservableLongGauge timerEnqueueLatency = new NopObservableLongGauge();
    public static ObservableLongGauge timerDequeueLag = new NopObservableLongGauge();
    public static ObservableLongGauge timerDequeueLatency = new NopObservableLongGauge();
    public static ObservableLongGauge timingMessages = new NopObservableLongGauge();

    public static LongCounter timerDequeueTotal = new NopLongCounter();
    public static LongCounter timerEnqueueTotal = new NopLongCounter();

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        return Lists.newArrayList();
    }

    public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier,
        DefaultMessageStore messageStore) {
        DefaultStoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
        DefaultStoreMetricsManager.messageStoreConfig = messageStore.getMessageStoreConfig();

        storageSize = meter.gaugeBuilder(GAUGE_STORAGE_SIZE)
            .setDescription("Broker storage size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {
                File storeDir = new File(messageStoreConfig.getStorePathRootDir());
                if (storeDir.exists() && storeDir.isDirectory()) {
                    long totalSpace = storeDir.getTotalSpace();
                    if (totalSpace > 0) {
                        measurement.record(totalSpace - storeDir.getFreeSpace(), newAttributesBuilder().build());
                    }
                }
            });

        flushBehind = meter.gaugeBuilder(GAUGE_STORAGE_FLUSH_BEHIND)
            .setDescription("Broker flush behind bytes")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(messageStore.flushBehindBytes(), newAttributesBuilder().build()));

        dispatchBehind = meter.gaugeBuilder(GAUGE_STORAGE_DISPATCH_BEHIND)
            .setDescription("Broker dispatch behind bytes")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> measurement.record(messageStore.dispatchBehindBytes(), newAttributesBuilder().build()));

        messageReserveTime = meter.gaugeBuilder(GAUGE_STORAGE_MESSAGE_RESERVE_TIME)
            .setDescription("Broker message reserve time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                long earliestMessageTime = messageStore.getEarliestMessageTime();
                if (earliestMessageTime <= 0) {
                    return;
                }
                measurement.record(System.currentTimeMillis() - earliestMessageTime, newAttributesBuilder().build());
            });

        if (messageStore.getMessageStoreConfig().isTimerWheelEnable()) {
            timerEnqueueLag = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LAG)
                .setDescription("Timer enqueue messages lag")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getEnqueueBehindMessages(), newAttributesBuilder().build());
                });

            timerEnqueueLatency = meter.gaugeBuilder(GAUGE_TIMER_ENQUEUE_LATENCY)
                .setDescription("Timer enqueue latency")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getEnqueueBehindMillis(), newAttributesBuilder().build());
                });
            timerDequeueLag = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LAG)
                .setDescription("Timer dequeue messages lag")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getDequeueBehindMessages(), newAttributesBuilder().build());
                });
            timerDequeueLatency = meter.gaugeBuilder(GAUGE_TIMER_DEQUEUE_LATENCY)
                .setDescription("Timer dequeue latency")
                .setUnit("milliseconds")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    measurement.record(timerMessageStore.getDequeueBehind(), newAttributesBuilder().build());
                });
            timingMessages = meter.gaugeBuilder(GAUGE_TIMING_MESSAGES)
                .setDescription("Current message number in timing")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    TimerMessageStore timerMessageStore = messageStore.getTimerMessageStore();
                    timerMessageStore.getTimerMetrics()
                        .getTimingCount()
                        .forEach((topic, metric) -> {
                            measurement.record(
                                metric.getCount().get(),
                                newAttributesBuilder().put(LABEL_TOPIC, topic).build()
                            );
                        });
                });
            timerDequeueTotal = meter.counterBuilder(COUNTER_TIMER_DEQUEUE_TOTAL)
                .setDescription("Total number of timer dequeue")
                .build();
            timerEnqueueTotal = meter.counterBuilder(COUNTER_TIMER_ENQUEUE_TOTAL)
                .setDescription("Total number of timer enqueue")
                .build();
        }
    }

    public static void incTimerDequeueCount(String topic) {
        timerDequeueTotal.add(1, newAttributesBuilder()
            .put(LABEL_TOPIC, topic)
            .build());
    }

    public static void incTimerEnqueueCount(String topic) {
        AttributesBuilder attributesBuilder = newAttributesBuilder();
        if (topic != null) {
            attributesBuilder.put(LABEL_TOPIC, topic);
        }
        timerEnqueueTotal.add(1, attributesBuilder.build());
    }

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get()
            .put(LABEL_STORAGE_TYPE, DEFAULT_STORAGE_TYPE)
            .put(LABEL_STORAGE_MEDIUM, DEFAULT_STORAGE_MEDIUM);
    }
}
