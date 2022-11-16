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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.View;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_TYPE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_DISPATCH_BEHIND;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_FLUSH_BEHIND;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_MESSAGE_RESERVE_TIME;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_SIZE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_TYPE;

public class DefaultStoreMetricsManager {
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    public static MessageStoreConfig messageStoreConfig;

    public static ObservableLongGauge storageSize = new NopObservableLongGauge();
    public static ObservableLongGauge flushBehind = new NopObservableLongGauge();
    public static ObservableLongGauge dispatchBehind = new NopObservableLongGauge();
    public static ObservableLongGauge messageReserveTime = new NopObservableLongGauge();

    public static List<Pair<InstrumentSelector, View>> getMetricsView() {
        return Collections.emptyList();
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
