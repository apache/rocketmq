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

import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.function.Supplier;
import org.apache.rocketmq.common.metrics.NopLongHistogram;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.HISTOGRAM_RPC_LATENCY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_PROTOCOL_TYPE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.PROTOCOL_TYPE_REMOTING;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.REQUEST_CODE_MAP;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESPONSE_CODE_MAP;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_CANCELED;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_SUCCESS;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED;

public class RemotingMetricsManager {
    public static LongHistogram rpcLatency = new NopLongHistogram();
    public static Supplier<AttributesBuilder> attributesBuilderSupplier;

    public static AttributesBuilder newAttributesBuilder() {
        if (attributesBuilderSupplier == null) {
            return Attributes.builder();
        }
        return attributesBuilderSupplier.get()
            .put(LABEL_PROTOCOL_TYPE, PROTOCOL_TYPE_REMOTING);
    }

    public static void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
        RemotingMetricsManager.attributesBuilderSupplier = attributesBuilderSupplier;
        rpcLatency = meter.histogramBuilder(HISTOGRAM_RPC_LATENCY)
            .setDescription("Rpc latency")
            .setUnit("milliseconds")
            .ofLongs()
            .build();
    }

    public static String getWriteAndFlushResult(Future<?> future) {
        String result = RESULT_SUCCESS;
        if (future.isCancelled()) {
            result = RESULT_CANCELED;
        } else if (!future.isSuccess()) {
            result = RESULT_WRITE_CHANNEL_FAILED;
        }
        return result;
    }

    public static String getRequestCodeDesc(int code) {
        return REQUEST_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }

    public static String getResponseCodeDesc(int code) {
        return RESPONSE_CODE_MAP.getOrDefault(code, String.valueOf(code));
    }
}
