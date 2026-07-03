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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.DefaultGrpcMessagingActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientInfo;
import org.apache.rocketmq.proxy.service.admin.client.ProxyClientReadServiceStats;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Before;
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

public class ProxyMetricsManagerTest extends InitConfigTest {

    @Before
    public void setUp() {
        ProxyMetricsManager.setProxyClientReadServiceStatsSupplier(null);
    }

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

    @Test
    public void defaultGrpcMessagingActivityRegistersProxyClientStatsSupplier() throws Exception {
        Meter meter = mock(Meter.class);
        mockLongGauge(meter, GAUGE_PROXY_UP);
        ArgumentCaptor<Consumer<ObservableLongMeasurement>> clientTotalCallback =
            mockLongGauge(meter, GAUGE_PROXY_CLIENT_TOTAL);
        mockLongGauge(meter, GAUGE_PROXY_CLIENT_TYPE_TOTAL);
        mockLongGauge(meter, GAUGE_PROXY_CLIENT_INDEX_TOTAL);
        MessagingProcessor messagingProcessor = mock(MessagingProcessor.class);
        ProxyRelayService proxyRelayService = mock(ProxyRelayService.class);
        when(messagingProcessor.getProxyRelayService()).thenReturn(proxyRelayService);
        TestDefaultGrpcMessagingActivity activity = new TestDefaultGrpcMessagingActivity(messagingProcessor);
        try {
            activity.upsertClient(client("client-a"));

            ProxyMetricsManager.initMetrics(meter, Attributes::builder);

            ObservableLongMeasurement clientTotalMeasurement = mock(ObservableLongMeasurement.class);
            clientTotalCallback.getValue().accept(clientTotalMeasurement);
            verify(clientTotalMeasurement).record(eq(1L), any(Attributes.class));
        } finally {
            activity.shutdownForTest();
            ProxyMetricsManager.setProxyClientReadServiceStatsSupplier(null);
        }
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

    private static ProxyClientInfo client(String clientId) {
        return new ProxyClientInfo(
            clientId,
            ClientType.PRODUCER,
            Collections.singleton("group-a"),
            Collections.singleton("topic-a"),
            "JAVA",
            "127.0.0.1:8080",
            "192.168.0.1:8080",
            "V5_0_0",
            100L,
            200L
        );
    }

    private static class TestDefaultGrpcMessagingActivity extends DefaultGrpcMessagingActivity {
        protected TestDefaultGrpcMessagingActivity(MessagingProcessor messagingProcessor) {
            super(messagingProcessor);
        }

        private void upsertClient(ProxyClientInfo clientInfo) {
            this.proxyClientReadService.upsertClient(clientInfo);
        }

        private void shutdownForTest() throws Exception {
            this.grpcChannelManager.shutdown();
        }
    }
}
