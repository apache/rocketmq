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
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopObservableDoubleGauge;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.store.RocksDBMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.rocksdb.TickerType;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.DEFAULT_STORAGE_TYPE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_BYTES_ROCKSDB_READ;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_BYTES_ROCKSDB_WRITTEN;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_TYPE;

public class RocksDBStoreMetricsManager {
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    public static MessageStoreConfig messageStoreConfig;

    // The cumulative number of bytes read from the database.
    public static ObservableLongGauge bytesRocksdbRead = new NopObservableLongGauge();

    // The cumulative number of bytes written to the database.
    public static ObservableLongGauge bytesRocksdbWritten = new NopObservableLongGauge();

    // The cumulative number of read operations performed.
    public static ObservableLongGauge timesRocksdbRead = new NopObservableLongGauge();

    // The cumulative number of write operations performed.
    public static ObservableLongGauge timesRocksdbWrittenSelf = new NopObservableLongGauge();
    public static ObservableLongGauge timesRocksdbWrittenOther = new NopObservableLongGauge();

    // The cumulative number of compressions that have occurred.
    public static ObservableLongGauge timesRocksdbCompressed = new NopObservableLongGauge();

    // The ratio of the amount of data actually written to the storage medium to the amount of data written by the application.
    public static ObservableDoubleGauge bytesRocksdbAmplificationRead = new NopObservableDoubleGauge();

    // The rate at which cache lookups were served from the cache rather than needing to be fetched from disk.
    public static ObservableDoubleGauge rocksdbCacheHitRate = new NopObservableDoubleGauge();

    public static volatile long blockCacheHitTimes = 0;
    public static volatile long blockCacheMissTimes = 0;



    public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        return Lists.newArrayList();
    }

    public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier,
        RocksDBMessageStore messageStore) {
        RocksDBStoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
        bytesRocksdbWritten = meter.gaugeBuilder(GAUGE_BYTES_ROCKSDB_WRITTEN)
                .setDescription("The cumulative number of bytes written to the database.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.BYTES_WRITTEN), newAttributesBuilder().put("type", "consume_queue").build());
                });
        bytesRocksdbRead = meter.gaugeBuilder(GAUGE_BYTES_ROCKSDB_READ)
                .setDescription("The cumulative number of bytes read from the database.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.BYTES_READ), newAttributesBuilder().put("type", "consume_queue").build());
                });
        timesRocksdbWrittenSelf = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_TIMES_ROCKSDB_WRITTEN_SELF)
                .setDescription("The cumulative number of write operations performed by self.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.WRITE_DONE_BY_SELF), newAttributesBuilder().put("type", "consume_queue").build());
                });
        timesRocksdbWrittenOther = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_TIMES_ROCKSDB_WRITTEN_OTHER)
                .setDescription("The cumulative number of write operations performed by other.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.WRITE_DONE_BY_OTHER), newAttributesBuilder().put("type", "consume_queue").build());
                });
        timesRocksdbRead = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_TIMES_ROCKSDB_READ)
                .setDescription("The cumulative number of write operations performed by other.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.NUMBER_KEYS_READ), newAttributesBuilder().put("type", "consume_queue").build());
                });
        rocksdbCacheHitRate = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_RATE_ROCKSDB_CACHE_HIT)
                .setDescription("The rate at which cache lookups were served from the cache rather than needing to be fetched from disk.")
                .buildWithCallback(measurement -> {
                    long newHitTimes = ((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.BLOCK_CACHE_HIT);
                    long newMissTimes = ((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.BLOCK_CACHE_MISS);
                    double hitRate = (double)(newHitTimes - blockCacheHitTimes) / (double)(newHitTimes - blockCacheHitTimes + newMissTimes - blockCacheMissTimes);
                    blockCacheHitTimes = newHitTimes;
                    blockCacheMissTimes = newMissTimes;
                    measurement.record(hitRate, newAttributesBuilder().put("type", "consume_queue").build());
                });
        timesRocksdbCompressed = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_TIMES_ROCKSDB_COMPRESSED)
                .setDescription("The cumulative number of compressions that have occurred.")
                .ofLongs()
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.NUMBER_BLOCK_COMPRESSED), newAttributesBuilder().put("type", "consume_queue").build());
                });
        bytesRocksdbAmplificationRead = meter.gaugeBuilder(DefaultStoreMetricsConstant.GAUGE_BYTES_READ_AMPLIFICATION)
                .setDescription("The rate at which cache lookups were served from the cache rather than needing to be fetched from disk.")
                .buildWithCallback(measurement -> {
                    measurement.record(((RocksDBConsumeQueueStore)messageStore.getQueueStore())
                            .getStatistics().getTickerCount(TickerType.READ_AMP_TOTAL_READ_BYTES), newAttributesBuilder().put("type", "consume_queue").build());
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
