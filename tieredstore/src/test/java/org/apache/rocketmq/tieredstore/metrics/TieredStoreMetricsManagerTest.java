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

import com.github.benmanes.caffeine.cache.Cache;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.provider.PosixFileSegment;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TieredStoreMetricsManagerTest {

    @Test
    public void getMetricsView() {
        TieredStoreMetricsManager.getMetricsView();
    }

    @Test
    public void init() {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setTieredBackendServiceProvider(PosixFileSegment.class.getName());
        TieredMessageStore messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(Mockito.mock(FlatFileStore.class));
        MessageStoreFetcherImpl fetcher = Mockito.spy(new MessageStoreFetcherImpl(messageStore));

        TieredStoreMetricsManager.init(
            OpenTelemetrySdk.builder().build().getMeter(""),
            null, storeConfig, fetcher,
            Mockito.mock(FlatFileStore.class), Mockito.mock(DefaultMessageStore.class));
    }

    @Test
    public void newAttributesBuilder() {
        TieredStoreMetricsManager.newAttributesBuilder();
    }

    @Test
    public void testCacheCountMetric() {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        TieredMessageStore messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(Mockito.mock(FlatFileStore.class));
        // The fetcher will create real cache
        MessageStoreFetcherImpl fetcher = new MessageStoreFetcherImpl(messageStore);

        AtomicLong capturedCacheCount = new AtomicLong(-1);
        Meter mockMeter = createMockMeter(TieredStoreMetricsConstant.GAUGE_CACHE_COUNT, capturedCacheCount);

        // Prepare cache before init so the gauge callback sees a populated cache instead of an empty one.
        int[] bufferSizes = prepareTestCache(fetcher);

        TieredStoreMetricsManager.init(mockMeter,
                null, storeConfig, fetcher,
            Mockito.mock(FlatFileStore.class), Mockito.mock(DefaultMessageStore.class));

        // CacheCount gauge should report the number of cached entries.
        Assert.assertEquals(bufferSizes.length, capturedCacheCount.get());
    }

    @Test
    public void testCacheBytesMetric() {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        TieredMessageStore messageStore = Mockito.mock(TieredMessageStore.class);
        Mockito.when(messageStore.getStoreConfig()).thenReturn(storeConfig);
        Mockito.when(messageStore.getFlatFileStore()).thenReturn(Mockito.mock(FlatFileStore.class));
        // The fetcher will create real cache
        MessageStoreFetcherImpl fetcher = new MessageStoreFetcherImpl(messageStore);

        AtomicLong capturedCacheBytes = new AtomicLong(-1);
        Meter mockMeter = createMockMeter(TieredStoreMetricsConstant.GAUGE_CACHE_BYTES, capturedCacheBytes);

        // Prepare cache before init so the gauge callback sees a populated cache instead of an empty one.
        int[] bufferSizes = prepareTestCache(fetcher);

        TieredStoreMetricsManager.init(mockMeter,
            null, storeConfig, fetcher,
            Mockito.mock(FlatFileStore.class), Mockito.mock(DefaultMessageStore.class));

        // CacheBytes gauge should report the sum of all cached buffer sizes.
        int expectedSum = Arrays.stream(bufferSizes).sum();
        Assert.assertEquals(expectedSum, capturedCacheBytes.get());
    }

    private Meter createMockMeter(String targetMetricName, AtomicLong capturedValue) {
        Meter mockMeter = Mockito.mock(Meter.class, Mockito.RETURNS_DEEP_STUBS);

        // Setup target gauge builder chain to capture the callback value
        DoubleGaugeBuilder targetGaugeBuilder = Mockito.mock(DoubleGaugeBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(mockMeter.gaugeBuilder(targetMetricName)).thenReturn(targetGaugeBuilder);
        Mockito.when(targetGaugeBuilder.setDescription(Mockito.anyString())).thenReturn(targetGaugeBuilder);
        Mockito.when(targetGaugeBuilder.setUnit(Mockito.anyString())).thenReturn(targetGaugeBuilder);
        Mockito.when(targetGaugeBuilder.ofLongs().buildWithCallback(Mockito.any(Consumer.class)))
            .thenAnswer(invocation -> {
                Consumer<ObservableLongMeasurement> callback = invocation.getArgument(0);
                // Immediately invoke the callback to capture the current cache state
                callback.accept(new ObservableLongMeasurement() {
                    @Override
                    public void record(long value) {
                        capturedValue.set(value);
                    }

                    @Override
                    public void record(long value, Attributes attributes) {
                        capturedValue.set(value);
                    }
                });
                return Mockito.mock(ObservableLongGauge.class);
            });

        return mockMeter;
    }

    private int[] prepareTestCache(MessageStoreFetcherImpl fetcher) {
        Cache<String, SelectBufferResult> cache = fetcher.getFetcherCache();
        String topic = "TestTopic";
        MessageQueue mq1 = new MessageQueue(topic, "broker", 0);
        MessageQueue mq2 = new MessageQueue(topic, "broker", 1);

        int[] bufferSizes = {100, 200, 150, 300};
        for (int i = 0; i < bufferSizes.length; i++) {
            SelectBufferResult result = new SelectBufferResult(
                ByteBuffer.allocate(bufferSizes[i]), 0L, bufferSizes[i], 0L);
            MessageQueue mq = i < 2 ? mq1 : mq2;
            String key = String.format("%s@%d@%d", mq.getTopic(), mq.getQueueId(), (i + 1) * 100L);
            cache.put(key, result);
        }
        return bufferSizes;
    }

}
