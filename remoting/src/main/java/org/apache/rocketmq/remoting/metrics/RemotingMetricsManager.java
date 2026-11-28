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
package org.apache.rocketmq.remoting.metrics;

import com.google.common.collect.Lists;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.metrics.NopLongHistogram;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.HISTOGRAM_RPC_LATENCY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_PROTOCOL_TYPE_KEY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.PROTOCOL_TYPE_REMOTING;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_IS_LONG_POLLING_KEY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE_KEY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE_KEY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT_KEY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_CANCELED;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_ONEWAY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_SUCCESS;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED;

public class RemotingMetricsManager {
    private LongHistogram rpcLatency = new NopLongHistogram();
    private Supplier<AttributesBuilder> attributesBuilderSupplier;
    private final ConcurrentHashMap<Long, Attributes> attributesCache = new ConcurrentHashMap<>();

    public RemotingMetricsManager() {
    }

    public AttributesBuilder newAttributesBuilder() {
        if (this.attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return this.attributesBuilderSupplier.get()
            .put(LABEL_PROTOCOL_TYPE_KEY, PROTOCOL_TYPE_REMOTING);
    }

    public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;
        this.rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
            .setDescription("Rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
    }

    public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
        List<Double> rpcCostTimeBuckets = Arrays.asList(
            (double) Duration.ofMillis(1).toMillis(),
            (double) Duration.ofMillis(3).toMillis(),
            (double) Duration.ofMillis(5).toMillis(),
            (double) Duration.ofMillis(7).toMillis(),
            (double) Duration.ofMillis(10).toMillis(),
            (double) Duration.ofMillis(100).toMillis(),
            (double) Duration.ofSeconds(1).toMillis(),
            (double) Duration.ofSeconds(2).toMillis(),
            (double) Duration.ofSeconds(3).toMillis()
        );
        InstrumentSelector selector = InstrumentSelector.builder()
            .setType(InstrumentType.HISTOGRAM)
            .setName(HISTOGRAM_RPC_LATENCY)
            .build();
        ViewBuilder viewBuilder = View.builder()
            .setAggregation(Aggregation.explicitBucketHistogram(rpcCostTimeBuckets));
        return Lists.newArrayList(new Pair<>(selector, viewBuilder));
    }

    public Attributes getOrBuildAttributes(int requestCode, int responseCode,
        boolean isLongPolling, String result) {
        int resultIdx;
        if (RESULT_SUCCESS.equals(result)) resultIdx = 0;
        else if (RESULT_ONEWAY.equals(result)) resultIdx = 1;
        else if (RESULT_WRITE_CHANNEL_FAILED.equals(result)) resultIdx = 2;
        else if (RESULT_CANCELED.equals(result)) resultIdx = 3;
        else resultIdx = -1;

        if (resultIdx < 0 || requestCode < 0 || requestCode > 0xFFFF || responseCode < 0 || responseCode > 0xFFFF) {
            // Rare shapes (unknown result, or codes outside 16 bits such as negative response
            // codes) fall back to direct building so distinct inputs can never collide in the cache.
            return buildAttributes(requestCode, responseCode, isLongPolling, result);
        }

        long key = ((long) requestCode << 19)
            | ((long) responseCode << 3)
            | (isLongPolling ? 4L : 0L)
            | resultIdx;
        return attributesCache.computeIfAbsent(key,
            k -> buildAttributes(requestCode, responseCode, isLongPolling, result));
    }

    private Attributes buildAttributes(int requestCode, int responseCode,
        boolean isLongPolling, String result) {
        return newAttributesBuilder()
            .put(LABEL_IS_LONG_POLLING_KEY, isLongPolling)
            .put(LABEL_REQUEST_CODE_KEY, RemotingHelper.getRequestCodeDesc(requestCode))
            .put(LABEL_RESPONSE_CODE_KEY, RemotingHelper.getResponseCodeDesc(responseCode))
            .put(LABEL_RESULT_KEY, result)
            .build();
    }

    public String getWriteAndFlushResult(Future<?> future) {
        String result = RESULT_SUCCESS;
        if (future.isCancelled()) {
            result = RESULT_CANCELED;
        } else if (!future.isSuccess()) {
            result = RESULT_WRITE_CHANNEL_FAILED;
        }
        return result;
    }

    // Getter methods for external access
    public LongHistogram getRpcLatency() {
        return rpcLatency;
    }

    public Supplier<AttributesBuilder> getAttributesBuilderSupplier() {
        return attributesBuilderSupplier;
    }

    // Setter methods for testing
    public void setAttributesBuilderSupplier(Supplier<AttributesBuilder> attributesBuilderSupplier) {
        this.attributesBuilderSupplier = attributesBuilderSupplier;
    }


}
