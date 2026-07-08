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

package org.apache.rocketmq.proxy.grpc.admin;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.lang.reflect.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ProxyAdminMetricsManager.
 * <p>
 * Covers OTel metrics initialization, idempotency, safe-no-op when uninitialized,
 * and correct recording of success/error metrics (RIP-2 §5.4.5 / §8.6).
 */
@RunWith(MockitoJUnitRunner.class)
public class ProxyAdminMetricsManagerTest {

    @Mock
    private Meter meter;

    @Mock
    private LongCounter longCounter;

    @Mock
    private DoubleHistogram doubleHistogram;

    @Before
    public void setUp() {
        // Set up mock Meter to return mock counter/histogram builders
        LongCounterBuilder counterBuilder = mock(LongCounterBuilder.class);
        DoubleHistogramBuilder histogramBuilder = mock(DoubleHistogramBuilder.class);

        when(meter.counterBuilder(ProxyAdminMetricsManager.COUNTER_ADMIN_RPC_TOTAL))
            .thenReturn(counterBuilder);
        when(counterBuilder.setDescription("Total number of proxy admin RPC invocations"))
            .thenReturn(counterBuilder);
        when(counterBuilder.build()).thenReturn(longCounter);

        when(meter.histogramBuilder(ProxyAdminMetricsManager.HISTOGRAM_ADMIN_RPC_LATENCY))
            .thenReturn(histogramBuilder);
        when(histogramBuilder.setDescription("Latency of proxy admin RPC invocations in milliseconds"))
            .thenReturn(histogramBuilder);
        when(histogramBuilder.setUnit("ms")).thenReturn(histogramBuilder);
        when(histogramBuilder.build()).thenReturn(doubleHistogram);
    }

    @After
    public void tearDown() throws Exception {
        // Reset static state between tests to ensure isolation
        resetStaticState();
    }

    /**
     * Reset ProxyAdminMetricsManager static fields using reflection.
     */
    private void resetStaticState() throws Exception {
        Field initializedField = ProxyAdminMetricsManager.class.getDeclaredField("initialized");
        initializedField.setAccessible(true);
        initializedField.set(null, false);

        Field counterField = ProxyAdminMetricsManager.class.getDeclaredField("adminRpcCounter");
        counterField.setAccessible(true);
        counterField.set(null, null);

        Field latencyField = ProxyAdminMetricsManager.class.getDeclaredField("adminRpcLatency");
        latencyField.setAccessible(true);
        latencyField.set(null, null);
    }

    // ==================== init() Tests ====================

    @Test
    public void testInit_WithValidMeter() {
        assertFalse(ProxyAdminMetricsManager.isInitialized());

        ProxyAdminMetricsManager.init(meter);

        assertTrue(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void testInit_WithNullMeter() {
        ProxyAdminMetricsManager.init(null);

        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void testInit_Idempotent_SecondCallIgnored() {
        ProxyAdminMetricsManager.init(meter);
        assertTrue(ProxyAdminMetricsManager.isInitialized());

        // Second init with a different meter should be ignored
        Meter anotherMeter = mock(Meter.class);
        ProxyAdminMetricsManager.init(anotherMeter);

        // Still initialized with the first meter
        assertTrue(ProxyAdminMetricsManager.isInitialized());
    }

    // ==================== isInitialized() Tests ====================

    @Test
    public void testIsInitialized_BeforeInit() {
        assertFalse(ProxyAdminMetricsManager.isInitialized());
    }

    @Test
    public void testIsInitialized_AfterInit() {
        ProxyAdminMetricsManager.init(meter);
        assertTrue(ProxyAdminMetricsManager.isInitialized());
    }

    // ==================== recordSuccess() Tests ====================

    @Test
    public void testRecordSuccess_WhenNotInitialized_NoException() {
        // Should be a no-op, not throw
        ProxyAdminMetricsManager.recordSuccess("ListClients", 100);
    }

    @Test
    public void testRecordSuccess_RecordsCounterAndLatency() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordSuccess("ListClients", 150);

        // Verify counter was incremented with correct labels
        ArgumentCaptor<Attributes> counterAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), counterAttrsCaptor.capture());
        Attributes counterAttrs = counterAttrsCaptor.getValue();
        assertEquals("ListClients", counterAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("success", counterAttrs.get(AttributeKey.stringKey("status")));

        // Verify latency was recorded with correct labels
        ArgumentCaptor<Attributes> latencyAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(doubleHistogram).record(eq(150.0), latencyAttrsCaptor.capture());
        Attributes latencyAttrs = latencyAttrsCaptor.getValue();
        assertEquals("ListClients", latencyAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("success", latencyAttrs.get(AttributeKey.stringKey("status")));
    }

    @Test
    public void testRecordSuccess_DescribeClient() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordSuccess("DescribeClient", 50);

        ArgumentCaptor<Attributes> attrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), attrsCaptor.capture());
        Attributes attrs = attrsCaptor.getValue();
        assertEquals("DescribeClient", attrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("success", attrs.get(AttributeKey.stringKey("status")));
    }

    // ==================== recordError() Tests ====================

    @Test
    public void testRecordError_WhenNotInitialized_NoException() {
        // Should be a no-op, not throw
        ProxyAdminMetricsManager.recordError("ListClients", 100);
    }

    @Test
    public void testRecordError_RecordsCounterAndLatency() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordError("ListClients", 200);

        // Verify counter was incremented with error status
        ArgumentCaptor<Attributes> counterAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), counterAttrsCaptor.capture());
        Attributes counterAttrs = counterAttrsCaptor.getValue();
        assertEquals("ListClients", counterAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("error", counterAttrs.get(AttributeKey.stringKey("status")));

        // Verify latency was recorded with error status
        ArgumentCaptor<Attributes> latencyAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(doubleHistogram).record(eq(200.0), latencyAttrsCaptor.capture());
        Attributes latencyAttrs = latencyAttrsCaptor.getValue();
        assertEquals("ListClients", latencyAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("error", latencyAttrs.get(AttributeKey.stringKey("status")));
    }

    // ==================== recordCall() Tests ====================

    @Test
    public void testRecordCall_Success() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordCall("ListClients", 100, true);

        // Should delegate to recordSuccess
        ArgumentCaptor<Attributes> counterAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), counterAttrsCaptor.capture());
        Attributes counterAttrs = counterAttrsCaptor.getValue();
        assertEquals("ListClients", counterAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("success", counterAttrs.get(AttributeKey.stringKey("status")));

        ArgumentCaptor<Attributes> latencyAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(doubleHistogram).record(eq(100.0), latencyAttrsCaptor.capture());
        Attributes latencyAttrs = latencyAttrsCaptor.getValue();
        assertEquals("ListClients", latencyAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("success", latencyAttrs.get(AttributeKey.stringKey("status")));
    }

    @Test
    public void testRecordCall_Failure() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordCall("DescribeClient", 200, false);

        // Should delegate to recordError
        ArgumentCaptor<Attributes> counterAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), counterAttrsCaptor.capture());
        Attributes counterAttrs = counterAttrsCaptor.getValue();
        assertEquals("DescribeClient", counterAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("error", counterAttrs.get(AttributeKey.stringKey("status")));

        ArgumentCaptor<Attributes> latencyAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(doubleHistogram).record(eq(200.0), latencyAttrsCaptor.capture());
        Attributes latencyAttrs = latencyAttrsCaptor.getValue();
        assertEquals("DescribeClient", latencyAttrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("error", latencyAttrs.get(AttributeKey.stringKey("status")));
    }

    @Test
    public void testRecordCall_WhenNotInitialized_NoException() {
        // Should be a no-op, not throw
        ProxyAdminMetricsManager.recordCall("ListClients", 100, true);
        ProxyAdminMetricsManager.recordCall("DescribeClient", 200, false);
    }

    // ==================== recordError(String, String) Tests ====================

    @Test
    public void testRecordErrorWithErrorType_WhenNotInitialized_NoException() {
        // Should be a no-op, not throw
        ProxyAdminMetricsManager.recordError("ListClients", "TIMEOUT");
    }

    @Test
    public void testRecordErrorWithErrorType_RecordsCounterWithLabels() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordError("DescribeClient", "TIMEOUT");

        ArgumentCaptor<Attributes> attrsCaptor = ArgumentCaptor.forClass(Attributes.class);
        verify(longCounter).add(eq(1L), attrsCaptor.capture());
        Attributes attrs = attrsCaptor.getValue();
        assertEquals("DescribeClient", attrs.get(AttributeKey.stringKey("rpc_method")));
        assertEquals("error", attrs.get(AttributeKey.stringKey("status")));
        assertEquals("TIMEOUT", attrs.get(AttributeKey.stringKey("error_type")));
    }

    @Test
    public void testRecordErrorWithErrorType_VariousErrorTypes() {
        ProxyAdminMetricsManager.init(meter);

        ProxyAdminMetricsManager.recordError("DisconnectClient", "PERMISSION_DENIED");
        ProxyAdminMetricsManager.recordError("GetConfig", "UNAVAILABLE");

        verify(longCounter, times(2)).add(eq(1L), any(Attributes.class));
    }

    // ==================== Constants Tests ====================

    @Test
    public void testMetricNameConstants() {
        assertEquals("rocketmq_proxy_admin_rpc_total", ProxyAdminMetricsManager.COUNTER_ADMIN_RPC_TOTAL);
        assertEquals("rocketmq_proxy_admin_rpc_latency", ProxyAdminMetricsManager.HISTOGRAM_ADMIN_RPC_LATENCY);
    }

    @Test
    public void testLabelKeyConstants() {
        assertEquals("rpc_method", ProxyAdminMetricsManager.LABEL_RPC_METHOD);
        assertEquals("status", ProxyAdminMetricsManager.LABEL_STATUS);
    }

    @Test
    public void testMethodNameConstants() {
        assertEquals("ListClients", ProxyAdminMetricsManager.METHOD_LIST_CLIENTS);
        assertEquals("DescribeClient", ProxyAdminMetricsManager.METHOD_DESCRIBE_CLIENT);
        assertEquals("ListClientsByGroup", ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_GROUP);
        assertEquals("ListClientsByTopic", ProxyAdminMetricsManager.METHOD_LIST_CLIENTS_BY_TOPIC);
    }

    @Test
    public void testStatusConstants() {
        assertEquals("success", ProxyAdminMetricsManager.STATUS_SUCCESS);
        assertEquals("error", ProxyAdminMetricsManager.STATUS_ERROR);
    }

    // Helper for eq() matching with long values
    private static long eq(long value) {
        return org.mockito.ArgumentMatchers.eq(value);
    }

    // Helper for eq() matching with double values
    private static double eq(double value) {
        return org.mockito.ArgumentMatchers.eq(value);
    }
}