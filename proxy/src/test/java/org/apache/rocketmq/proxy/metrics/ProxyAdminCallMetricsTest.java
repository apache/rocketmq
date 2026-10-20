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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.common.metrics.MetricsExporterType;
import org.apache.rocketmq.common.metrics.NopLongCounter;
import org.apache.rocketmq.common.metrics.NopLongHistogram;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminCallMetricsTest {

    @Mock
    private BrokerMetricsManager brokerMetricsManager;

    private LongCounter savedCounter;
    private LongHistogram savedHistogram;
    private MetricsExporterType savedExporterType;
    private Supplier<AttributesBuilder> savedSupplier;

    @BeforeClass
    public static void initConfiguration() throws Exception {
        ConfigurationManager.initConfig();
    }

    @Before
    public void setUp() {
        savedCounter = ProxyMetricsManager.proxyAdminCallTotal;
        savedHistogram = ProxyMetricsManager.proxyAdminRt;
        savedExporterType = ConfigurationManager.getProxyConfig().getMetricsExporterType();
        savedSupplier = ProxyMetricsManager.attributesBuilderSupplier;
    }

    @After
    public void tearDown() {
        ProxyMetricsManager.proxyAdminCallTotal = savedCounter;
        ProxyMetricsManager.proxyAdminRt = savedHistogram;
        ProxyMetricsManager.attributesBuilderSupplier = savedSupplier;
        ConfigurationManager.getProxyConfig().setMetricsExporterType(savedExporterType);
    }

    @Test
    public void recordBeforeInitLandsInNoopInstruments() {
        ProxyMetricsManager.proxyAdminCallTotal = new NopLongCounter();
        ProxyMetricsManager.proxyAdminRt = new NopLongHistogram();

        ProxyMetricsManager.recordAdminCall("GetMetadata", "OK", 3);

        assertTrue(ProxyMetricsManager.proxyAdminCallTotal instanceof NopLongCounter);
        assertTrue(ProxyMetricsManager.proxyAdminRt instanceof NopLongHistogram);
    }

    @Test
    public void initLocalModeReplacesNoopInstruments() {
        ConfigurationManager.getProxyConfig().setMetricsExporterType(MetricsExporterType.LOG);
        Meter meter = OpenTelemetrySdk.builder().build().getMeter("proxy-test");
        when(brokerMetricsManager.getBrokerMeter()).thenReturn(meter);
        doReturn(Attributes.builder()).when(brokerMetricsManager).newAttributesBuilder();

        ProxyMetricsManager.initLocalMode(brokerMetricsManager, ConfigurationManager.getProxyConfig());

        assertFalse("init must install a real counter",
            ProxyMetricsManager.proxyAdminCallTotal instanceof NopLongCounter);
        assertFalse("init must install a real histogram",
            ProxyMetricsManager.proxyAdminRt instanceof NopLongHistogram);
        ProxyMetricsManager.recordAdminCall("GetMetadata", "PERMISSION_DENIED", 7);
    }

    @Test
    public void disabledMetricsKeepsNoopInstruments() {
        ProxyMetricsManager.proxyAdminCallTotal = new NopLongCounter();
        ProxyMetricsManager.proxyAdminRt = new NopLongHistogram();
        ConfigurationManager.getProxyConfig().setMetricsExporterType(MetricsExporterType.DISABLE);

        ProxyMetricsManager.initLocalMode(brokerMetricsManager, ConfigurationManager.getProxyConfig());

        assertTrue(ProxyMetricsManager.proxyAdminCallTotal instanceof NopLongCounter);
        assertTrue(ProxyMetricsManager.proxyAdminRt instanceof NopLongHistogram);
        ProxyMetricsManager.recordAdminCall("GetMetadata", "OK", 1);
    }

    @Test
    public void exportsCounterAndHistogramWithExpectedNamesAndLabels() {
        ConfigurationManager.getProxyConfig().setMetricsExporterType(MetricsExporterType.LOG);
        CollectingReader reader = new CollectingReader();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(SdkMeterProvider.builder()
                .registerMetricReader(reader)
                .build())
            .build();
        Meter meter = sdk.getMeter("proxy-test-e2e");
        when(brokerMetricsManager.getBrokerMeter()).thenReturn(meter);
        doReturn(Attributes.builder()).when(brokerMetricsManager).newAttributesBuilder();

        ProxyMetricsManager.initLocalMode(brokerMetricsManager, ConfigurationManager.getProxyConfig());
        ProxyMetricsManager.recordAdminCall("GetMetadata", "PERMISSION_DENIED", 7);

        AttributeKey<String> methodKey = AttributeKey.stringKey("method");
        AttributeKey<String> statusKey = AttributeKey.stringKey("invocation_status");

        List<MetricData> all = new ArrayList<>(reader.collect());
        MetricData counter = all.stream()
            .filter(m -> "rocketmq_proxy_admin_calls_total".equals(m.getName()))
            .findFirst().orElseThrow(() -> new AssertionError("counter not exported: " + names(all)));
        long counterValue = counter.getLongSumData().getPoints().iterator().next().getValue();
        Attributes counterAttrs = counter.getLongSumData().getPoints().iterator().next().getAttributes();
        assertEquals(1L, counterValue);
        assertEquals("GetMetadata", counterAttrs.get(methodKey));
        assertEquals("PERMISSION_DENIED", counterAttrs.get(statusKey));

        MetricData histogram = all.stream()
            .filter(m -> "rocketmq_proxy_admin_rt".equals(m.getName()))
            .findFirst().orElseThrow(() -> new AssertionError("histogram not exported: " + names(all)));
        HistogramPointData point = histogram.getHistogramData().getPoints().iterator().next();
        assertEquals(1L, point.getCount());
        assertEquals(7.0, point.getSum(), 0.0001);
        assertEquals("GetMetadata", point.getAttributes().get(methodKey));
        assertEquals("PERMISSION_DENIED", point.getAttributes().get(statusKey));

        sdk.close();
    }

    private static String names(List<MetricData> all) {
        List<String> out = new ArrayList<>();
        for (MetricData m : all) {
            out.add(m.getName());
        }
        return out.toString();
    }

    private static final class CollectingReader implements MetricReader {
        private volatile CollectionRegistration registration = CollectionRegistration.noop();

        @Override
        public void register(CollectionRegistration reg) {
            this.registration = reg;
        }

        @Override
        public CompletableResultCode forceFlush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public io.opentelemetry.sdk.metrics.data.AggregationTemporality getAggregationTemporality(
            io.opentelemetry.sdk.metrics.InstrumentType instrumentType) {
            return io.opentelemetry.sdk.metrics.data.AggregationTemporality.CUMULATIVE;
        }

        private Collection<MetricData> collect() {
            return this.registration.collectAllMetrics();
        }
    }
}
