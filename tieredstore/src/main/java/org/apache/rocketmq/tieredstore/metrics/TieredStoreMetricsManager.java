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
package org.apache.rocketmq.tieredstore.metrics;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.common.metrics.NopObservableLongGauge;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcher;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.GAUGE_STORAGE_SIZE;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_MEDIUM;
import static org.apache.rocketmq.store.metrics.DefaultStoreMetricsConstant.LABEL_STORAGE_TYPE;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.COUNTER_CACHE_ACCESS;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.COUNTER_CACHE_HIT;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.COUNTER_GET_MESSAGE_FALLBACK_TOTAL;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.COUNTER_MESSAGES_DISPATCH_TOTAL;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.COUNTER_MESSAGES_OUT_TOTAL;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.GAUGE_CACHE_BYTES;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.GAUGE_CACHE_COUNT;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.GAUGE_DISPATCH_BEHIND;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.GAUGE_DISPATCH_LATENCY;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.GAUGE_STORAGE_MESSAGE_RESERVE_TIME;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.HISTOGRAM_API_LATENCY;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.HISTOGRAM_DOWNLOAD_BYTES;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.HISTOGRAM_PROVIDER_RPC_LATENCY;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.HISTOGRAM_UPLOAD_BYTES;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_FILE_TYPE;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_QUEUE_ID;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.LABEL_TOPIC;
import static org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant.STORAGE_MEDIUM_BLOB;

public class TieredStoreMetricsManager {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;
    private static String storageMedium = STORAGE_MEDIUM_BLOB;

    public static LongHistogram apiLatency = new NopLongHistogram();

    // tiered store provider metrics
    public static LongHistogram providerRpcLatency = new NopLongHistogram();
    public static LongHistogram uploadBytes = new NopLongHistogram();
    public static LongHistogram downloadBytes = new NopLongHistogram();

    public static ObservableLongGauge dispatchBehind = new NopObservableLongGauge();
    public static ObservableLongGauge dispatchLatency = new NopObservableLongGauge();
    public static LongCounter messagesDispatchTotal = new NopLongCounter();
    public static LongCounter messagesOutTotal = new NopLongCounter();
    public static LongCounter fallbackTotal = new NopLongCounter();

    public static ObservableLongGauge cacheCount = new NopObservableLongGauge();
    public static ObservableLongGauge cacheBytes = new NopObservableLongGauge();
    public static LongCounter cacheAccess = new NopLongCounter();
    public static LongCounter cacheHit = new NopLongCounter();

    public static ObservableLongGauge storageSize = new NopObservableLongGauge();
    public static ObservableLongGauge storageMessageReserveTime = new NopObservableLongGauge();

    public static List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        ArrayList<Pair<InstrumentSelector, ViewBuilder>> res = new ArrayList<>();

        InstrumentSelector providerRpcLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_PROVIDER_RPC_LATENCY)
            .build();

        InstrumentSelector rpcLatencySelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_API_LATENCY)
            .build();

        ViewBuilder rpcLatencyViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(Arrays.asList(1d, 3d, 5d, 7d, 10d, 100d, 200d, 400d, 600d, 800d, 1d * 1000, 1d * 1500, 1d * 3000)))
            .setDescription("tiered_store_rpc_latency_view");

        InstrumentSelector uploadBufferSizeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_UPLOAD_BYTES)
            .build();

        InstrumentSelector downloadBufferSizeSelector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_DOWNLOAD_BYTES)
            .build();

        ViewBuilder bufferSizeViewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(Arrays.asList(1d * MessageStoreUtil.KB, 10d * MessageStoreUtil.KB, 100d * MessageStoreUtil.KB, 1d * MessageStoreUtil.MB, 10d * MessageStoreUtil.MB, 32d * MessageStoreUtil.MB, 50d * MessageStoreUtil.MB, 100d * MessageStoreUtil.MB)))
            .setDescription("tiered_store_buffer_size_view");

        res.add(new Pair<>(rpcLatencySelector, rpcLatencyViewBuilder));
        res.add(new Pair<>(providerRpcLatencySelector, rpcLatencyViewBuilder));
        res.add(new Pair<>(uploadBufferSizeSelector, bufferSizeViewBuilder));
        res.add(new Pair<>(downloadBufferSizeSelector, bufferSizeViewBuilder));
        return res;
    }

    public static void setStorageMedium(String storageMedium) {
        TieredStoreMetricsManager.storageMedium = storageMedium;
    }

    public static void init(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier,
        MessageStoreConfig storeConfig, MessageStoreFetcher fetcher,
        FlatFileStore flatFileStore, MessageStore next) {

        TieredStoreMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;

        apiLatency = meter.histogramBuilder(HISTOGRAM_API_LATENCY)
            .setDescription("Tiered store rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        providerRpcLatency = meter.histogramBuilder(HISTOGRAM_PROVIDER_RPC_LATENCY)
            .setDescription("Tiered store rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();

        uploadBytes = meter.histogramBuilder(HISTOGRAM_UPLOAD_BYTES)
            .setDescription("Tiered store upload buffer size")
            .setUnit("bytes")
            .ofLongs()
            .build();

        downloadBytes = meter.histogramBuilder(HISTOGRAM_DOWNLOAD_BYTES)
            .setDescription("Tiered store download buffer size")
            .setUnit("bytes")
            .ofLongs()
            .build();

        dispatchBehind = meter.gaugeBuilder(GAUGE_DISPATCH_BEHIND)
            .setDescription("Tiered store dispatch behind message count")
            .ofLongs()
            .buildWithCallback(measurement -> {
                for (FlatMessageFile flatFile : flatFileStore.deepCopyFlatFileToList()) {

                    MessageQueue mq = flatFile.getMessageQueue();
                    long maxOffset = next.getMaxOffsetInQueue(mq.getTopic(), mq.getQueueId());
                    long maxTimestamp = next.getMessageStoreTimeStamp(mq.getTopic(), mq.getQueueId(), maxOffset - 1);
                    if (maxTimestamp > 0 && System.currentTimeMillis() - maxTimestamp > (long) storeConfig.getTieredStoreFileReservedTime() * 60 * 60 * 1000) {
                        continue;
                    }

                    Attributes commitLogAttributes = newAttributesBuilder()
                        .put(LABEL_TOPIC, mq.getTopic())
                        .put(LABEL_QUEUE_ID, mq.getQueueId())
                        .put(LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                        .build();

                    Attributes consumeQueueAttributes = newAttributesBuilder()
                        .put(LABEL_TOPIC, mq.getTopic())
                        .put(LABEL_QUEUE_ID, mq.getQueueId())
                        .put(LABEL_FILE_TYPE, FileSegmentType.CONSUME_QUEUE.name().toLowerCase())
                        .build();
                    measurement.record(Math.max(maxOffset - flatFile.getConsumeQueueMaxOffset(), 0), consumeQueueAttributes);
                }
            });

        dispatchLatency = meter.gaugeBuilder(GAUGE_DISPATCH_LATENCY)
            .setDescription("Tiered store dispatch latency")
            .setUnit("seconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                for (FlatMessageFile flatFile : flatFileStore.deepCopyFlatFileToList()) {

                    MessageQueue mq = flatFile.getMessageQueue();
                    long maxOffset = next.getMaxOffsetInQueue(mq.getTopic(), mq.getQueueId());
                    long maxTimestamp = next.getMessageStoreTimeStamp(mq.getTopic(), mq.getQueueId(), maxOffset - 1);
                    if (maxTimestamp > 0 && System.currentTimeMillis() - maxTimestamp > (long) storeConfig.getTieredStoreFileReservedTime() * 60 * 60 * 1000) {
                        continue;
                    }

                    Attributes commitLogAttributes = newAttributesBuilder()
                        .put(LABEL_TOPIC, mq.getTopic())
                        .put(LABEL_QUEUE_ID, mq.getQueueId())
                        .put(LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
                        .build();

                    Attributes consumeQueueAttributes = newAttributesBuilder()
                        .put(LABEL_TOPIC, mq.getTopic())
                        .put(LABEL_QUEUE_ID, mq.getQueueId())
                        .put(LABEL_FILE_TYPE, FileSegmentType.CONSUME_QUEUE.name().toLowerCase())
                        .build();
                    long consumeQueueDispatchOffset = flatFile.getConsumeQueueMaxOffset();
                    long consumeQueueDispatchLatency = next.getMessageStoreTimeStamp(mq.getTopic(), mq.getQueueId(), consumeQueueDispatchOffset);
                    if (maxOffset <= consumeQueueDispatchOffset || consumeQueueDispatchLatency < 0) {
                        measurement.record(0, consumeQueueAttributes);
                    } else {
                        measurement.record(System.currentTimeMillis() - consumeQueueDispatchLatency, consumeQueueAttributes);
                    }
                }
            });

        messagesDispatchTotal = meter.counterBuilder(COUNTER_MESSAGES_DISPATCH_TOTAL)
            .setDescription("Total number of dispatch messages")
            .build();

        messagesOutTotal = meter.counterBuilder(COUNTER_MESSAGES_OUT_TOTAL)
            .setDescription("Total number of outgoing messages")
            .build();

        fallbackTotal = meter.counterBuilder(COUNTER_GET_MESSAGE_FALLBACK_TOTAL)
            .setDescription("Total times of fallback to next store when getting message")
            .build();

        cacheCount = meter.gaugeBuilder(GAUGE_CACHE_COUNT)
            .setDescription("Tiered store cache message count")
            .ofLongs()
            .buildWithCallback(measurement -> {
                if (fetcher instanceof MessageStoreFetcherImpl) {
                    long count = ((MessageStoreFetcherImpl) fetcher).getFetcherCache().stats().loadCount();
                    measurement.record(count, newAttributesBuilder().build());
                }
            });

        cacheBytes = meter.gaugeBuilder(GAUGE_CACHE_BYTES)
            .setDescription("Tiered store cache message bytes")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {
                if (fetcher instanceof MessageStoreFetcherImpl) {
                    long count = ((MessageStoreFetcherImpl) fetcher).getFetcherCache().estimatedSize();
                    measurement.record(count, newAttributesBuilder().build());
                }
            });

        cacheAccess = meter.counterBuilder(COUNTER_CACHE_ACCESS)
            .setDescription("Tiered store cache access count")
            .build();

        cacheHit = meter.counterBuilder(COUNTER_CACHE_HIT)
            .setDescription("Tiered store cache hit count")
            .build();

        storageSize = meter.gaugeBuilder(GAUGE_STORAGE_SIZE)
            .setDescription("Broker storage size")
            .setUnit("bytes")
            .ofLongs()
            .buildWithCallback(measurement -> {
                Map<String, Map<FileSegmentType, Long>> topicFileSizeMap = new HashMap<>();
                try {
                    MetadataStore metadataStore = flatFileStore.getMetadataStore();
                    metadataStore.iterateFileSegment(fileSegment -> {
                        Map<FileSegmentType, Long> subMap =
                            topicFileSizeMap.computeIfAbsent(fileSegment.getPath(), k -> new HashMap<>());
                        FileSegmentType fileSegmentType =
                            FileSegmentType.valueOf(fileSegment.getType());
                        Long size = subMap.computeIfAbsent(fileSegmentType, k -> 0L);
                        subMap.put(fileSegmentType, size + fileSegment.getSize());
                    });
                } catch (Exception e) {
                    log.error("Failed to get storage size", e);
                }
                topicFileSizeMap.forEach((topic, subMap) -> {
                    subMap.forEach((fileSegmentType, size) -> {
                        Attributes attributes = newAttributesBuilder()
                            .put(LABEL_TOPIC, topic)
                            .put(LABEL_FILE_TYPE, fileSegmentType.name().toLowerCase())
                            .build();
                        measurement.record(size, attributes);
                    });
                });
            });

        storageMessageReserveTime = meter.gaugeBuilder(GAUGE_STORAGE_MESSAGE_RESERVE_TIME)
            .setDescription("Broker message reserve time")
            .setUnit("milliseconds")
            .ofLongs()
            .buildWithCallback(measurement -> {
                for (FlatMessageFile flatFile : flatFileStore.deepCopyFlatFileToList()) {
                    long timestamp = flatFile.getMinStoreTimestamp();
                    if (timestamp > 0) {
                        MessageQueue mq = flatFile.getMessageQueue();
                        Attributes attributes = newAttributesBuilder()
                            .put(LABEL_TOPIC, mq.getTopic())
                            .put(LABEL_QUEUE_ID, mq.getQueueId())
                            .build();
                        measurement.record(System.currentTimeMillis() - timestamp, attributes);
                    }
                }
            });
    }

    public static AttributesBuilder newAttributesBuilder() {
        AttributesBuilder builder = attributesBuilderSupplier != null ? attributesBuilderSupplier.get() : Attributes.builder();
        return builder.put(LABEL_STORAGE_TYPE, "tiered")
            .put(LABEL_STORAGE_MEDIUM, storageMedium);
    }
}
