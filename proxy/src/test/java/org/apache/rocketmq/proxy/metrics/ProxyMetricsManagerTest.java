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
package org.apache.rocketmq.proxy.metrics;

import apache.rocketmq.v2.ClientType;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceStats;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_INDEX_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_CLIENT_TYPE_TOTAL;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.GAUGE_PROXY_UP;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.INDEX_TYPE_GROUP;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.INDEX_TYPE_TOPIC;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_CLIENT_TYPE;
import static org.apache.rocketmq.proxy.metrics.ProxyMetricsConstant.LABEL_INDEX_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProxyMetricsManagerTest {

    @Test
    public void initMetricsRecordsProxyClientReadModelStats() {
        Meter meter = mock(Meter.class);
        mockLongGauge(meter, GAUGE_PROXY_UP);
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> clientTotalCallback =
            mockLongGauge(meter, GAUGE_PROXY_CLIENT_TOTAL);
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> clientTypeCallback =
            mockLongGauge(meter, GAUGE_PROXY_CLIENT_TYPE_TOTAL);
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> clientIndexCallback =
            mockLongGauge(meter, GAUGE_PROXY_CLIENT_INDEX_TOTAL);
        Map<ClientType, Long> clientTypeCounts = new HashMap<>();
        clientTypeCounts.put(ClientType.PRODUCER, 1L);
        clientTypeCounts.put(ClientType.PUSH_CONSUMER, 2L);

        ProxyMetricsManager.initMetrics(
            meter,
            Attributes::builder,
            () -> new ProxyClientReadServiceStats(3L, 4L, 5L, clientTypeCounts)
        );

        ObservableLongMeasurement clientTotalMeasurement = mock(ObservableLongMeasurement.class);
        clientTotalCallback.getValue().accept(clientTotalMeasurement);
        verify(clientTotalMeasurement).record(eq(3L), any(Attributes.class));

        ObservableLongMeasurement clientTypeMeasurement = mock(ObservableLongMeasurement.class);
        clientTypeCallback.getValue().accept(clientTypeMeasurement);
        verify(clientTypeMeasurement).record(eq(1L), argThat(attributes ->
            ClientType.PRODUCER.name().toLowerCase().equals(
                attributes.get(AttributeKey.stringKey(LABEL_CLIENT_TYPE)))));
        verify(clientTypeMeasurement).record(eq(2L), argThat(attributes ->
            ClientType.PUSH_CONSUMER.name().toLowerCase().equals(
                attributes.get(AttributeKey.stringKey(LABEL_CLIENT_TYPE)))));

        ObservableLongMeasurement clientIndexMeasurement = mock(ObservableLongMeasurement.class);
        clientIndexCallback.getValue().accept(clientIndexMeasurement);
        verify(clientIndexMeasurement).record(eq(4L), argThat(attributes ->
            INDEX_TYPE_GROUP.equals(attributes.get(AttributeKey.stringKey(LABEL_INDEX_TYPE)))));
        verify(clientIndexMeasurement).record(eq(5L), argThat(attributes ->
            INDEX_TYPE_TOPIC.equals(attributes.get(AttributeKey.stringKey(LABEL_INDEX_TYPE)))));
    }

    private static ArgumentCaptor<Consumer<ObservableLongMeasurement>> mockLongGauge(Meter meter, String name) {
        DoubleGaugeBuilder doubleGaugeBuilder = mock(DoubleGaugeBuilder.class);
        LongGaugeBuilder longGaugeBuilder = mock(LongGaugeBuilder.class);
        ObservableLongGauge observableLongGauge = mock(ObservableLongGauge.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor =
            ArgumentCaptor.forClass(Consumer.class);

        when(meter.gaugeBuilder(name)).thenReturn(doubleGaugeBuilder);
        when(doubleGaugeBuilder.setDescription(any(String.class))).thenReturn(doubleGaugeBuilder);
        when(doubleGaugeBuilder.ofLongs()).thenReturn(longGaugeBuilder);
        when(longGaugeBuilder.buildWithCallback(callbackCaptor.capture())).thenReturn(observableLongGauge);
        return callbackCaptor;
    }
}
