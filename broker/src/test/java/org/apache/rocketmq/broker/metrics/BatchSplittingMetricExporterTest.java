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
package org.apache.rocketmq.broker.metrics;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.Data;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchSplittingMetricExporterTest {

    private MetricExporter delegate;
    private static final int MAX_DATA_POINTS = 10;

    @Before
    public void setUp() {
        delegate = mock(MetricExporter.class);
        when(delegate.export(anyCollection()))
            .thenReturn(CompletableResultCode.ofSuccess());
        when(delegate.flush())
            .thenReturn(CompletableResultCode.ofSuccess());
        when(delegate.shutdown())
            .thenReturn(CompletableResultCode.ofSuccess());
        when(delegate.getAggregationTemporality(
            any(InstrumentType.class)))
            .thenReturn(
                AggregationTemporality.CUMULATIVE);
    }

    @Test
    public void testConstructorRejectsNullDelegate() {
        assertThatThrownBy(() ->
            new BatchSplittingMetricExporter(
                null, () -> 100))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testConstructorRejectsNullSupplier() {
        assertThatThrownBy(() ->
            new BatchSplittingMetricExporter(
                delegate, null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testExportEmptyCollection() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);
        CompletableResultCode result =
            exporter.export(Collections.emptyList());

        assertThat(result.isSuccess()).isTrue();
        verify(delegate, times(1))
            .export(Collections.emptyList());
    }

    @Test
    public void testFastPathWhenBelowThreshold() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        List<MetricData> metrics = Arrays.asList(
            createMockMetricData("metric1", 3),
            createMockMetricData("metric2", 3),
            createMockMetricData("metric3", 3)
        );

        CompletableResultCode result =
            exporter.export(metrics);

        assertThat(result.isSuccess()).isTrue();
        verify(delegate, times(1)).export(metrics);
    }

    @Test
    public void testFastPathWhenExactlyAtThreshold() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        List<MetricData> metrics = Arrays.asList(
            createMockMetricData("metric1", 5),
            createMockMetricData("metric2", 5)
        );

        CompletableResultCode result =
            exporter.export(metrics);

        assertThat(result.isSuccess()).isTrue();
        verify(delegate, times(1)).export(metrics);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitWhenAboveThreshold() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        MetricData m1 = createMockMetricData("m1", 5);
        MetricData m2 = createMockMetricData("m2", 5);
        MetricData m3 = createMockMetricData("m3", 5);
        List<MetricData> metrics =
            Arrays.asList(m1, m2, m3);

        CompletableResultCode result =
            exporter.export(metrics);

        assertThat(result).isNotNull();

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(2))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(batches).hasSize(2);
        assertThat(batches.get(0))
            .containsExactly(m1, m2);
        assertThat(batches.get(1))
            .containsExactly(m3);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitSingleLargeMetricData() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        // A single MetricData with 25 points.
        // maxBatchSize=10, so it should be split
        // into 3 sub-MetricData: 10, 10, 5 points.
        // Each goes into its own batch.
        MetricData largeMetric =
            createRealLongGaugeMetricData(
                "large_metric", 25);
        List<MetricData> metrics =
            Collections.singletonList(largeMetric);

        CompletableResultCode result =
            exporter.export(metrics);

        assertThat(result).isNotNull();

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(3))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(batches).hasSize(3);

        // Verify each batch has correct point count
        assertThat(totalPoints(batches.get(0)))
            .isEqualTo(10);
        assertThat(totalPoints(batches.get(1)))
            .isEqualTo(10);
        assertThat(totalPoints(batches.get(2)))
            .isEqualTo(5);

        // Verify metadata preserved
        for (Collection<MetricData> batch : batches) {
            for (MetricData md : batch) {
                assertThat(md.getName())
                    .isEqualTo("large_metric");
                assertThat(md.getType())
                    .isEqualTo(
                        MetricDataType.LONG_GAUGE);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitSingleLargeMetricExactMultiple() {
        // 20 points / maxBatchSize 10 = exactly 2 batches
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        MetricData largeMetric =
            createRealLongGaugeMetricData(
                "exact_metric", 20);
        List<MetricData> metrics =
            Collections.singletonList(largeMetric);

        exporter.export(metrics);

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(2))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(totalPoints(batches.get(0)))
            .isEqualTo(10);
        assertThat(totalPoints(batches.get(1)))
            .isEqualTo(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitLargeMetricMixedWithSmall() {
        // maxBatchSize = 10
        // m1: 3 pts (small), large: 25 pts, m3: 4 pts
        // Expected:
        //   batch1: [m1] (3 pts) - flushed before large
        //   large split into: sub1(10), sub2(10), sub3(5)
        //   batch2: [sub1] (10 pts)
        //   batch3: [sub2] (10 pts)
        //   batch4: [sub3, m3] (5+4=9 pts)
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        MetricData m1 =
            createMockMetricData("m1", 3);
        MetricData large =
            createRealLongGaugeMetricData(
                "large", 25);
        MetricData m3 =
            createMockMetricData("m3", 4);

        exporter.export(Arrays.asList(m1, large, m3));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(4))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(batches).hasSize(4);

        // batch 1: m1 (3 pts)
        assertThat(totalPoints(batches.get(0)))
            .isEqualTo(3);
        // batch 2: sub1 of large (10 pts)
        assertThat(totalPoints(batches.get(1)))
            .isEqualTo(10);
        // batch 3: sub2 of large (10 pts)
        assertThat(totalPoints(batches.get(2)))
            .isEqualTo(10);
        // batch 4: sub3 of large (5) + m3 (4) = 9
        assertThat(totalPoints(batches.get(3)))
            .isEqualTo(9);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitMultipleBatches() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 5);

        MetricData m1 =
            createMockMetricData("m1", 3);
        MetricData m2 =
            createMockMetricData("m2", 3);
        MetricData m3 =
            createMockMetricData("m3", 3);
        MetricData m4 =
            createMockMetricData("m4", 3);
        MetricData m5 =
            createMockMetricData("m5", 3);
        List<MetricData> metrics =
            Arrays.asList(m1, m2, m3, m4, m5);

        exporter.export(metrics);

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(5))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(batches.get(0))
            .containsExactly(m1);
        assertThat(batches.get(1))
            .containsExactly(m2);
        assertThat(batches.get(2))
            .containsExactly(m3);
        assertThat(batches.get(3))
            .containsExactly(m4);
        assertThat(batches.get(4))
            .containsExactly(m5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitMixedSizeMetricData() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 10);

        MetricData m1 =
            createMockMetricData("m1", 2);
        MetricData m2 =
            createMockMetricData("m2", 7);
        MetricData m3 =
            createMockMetricData("m3", 3);
        MetricData m4 =
            createMockMetricData("m4", 8);
        MetricData m5 =
            createMockMetricData("m5", 1);
        List<MetricData> metrics =
            Arrays.asList(m1, m2, m3, m4, m5);

        exporter.export(metrics);

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(3))
            .export(captor.capture());

        List<Collection<MetricData>> batches =
            captor.getAllValues();
        assertThat(batches.get(0))
            .containsExactly(m1, m2);
        assertThat(batches.get(1))
            .containsExactly(m3);
        assertThat(batches.get(2))
            .containsExactly(m4, m5);
    }

    @Test
    public void testDelegateFailureIsPropagated() {
        when(delegate.export(anyCollection()))
            .thenReturn(
                CompletableResultCode.ofFailure());

        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);

        List<MetricData> metrics =
            Collections.singletonList(
                createMockMetricData("metric1", 5));
        CompletableResultCode result =
            exporter.export(metrics);

        assertThat(result.isSuccess()).isFalse();
    }

    @Test
    public void testFlushDelegatesToDelegate() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);
        CompletableResultCode result =
            exporter.flush();

        assertThat(result.isSuccess()).isTrue();
        verify(delegate, times(1)).flush();
    }

    @Test
    public void testShutdownDelegatesToDelegate() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);
        CompletableResultCode result =
            exporter.shutdown();

        assertThat(result.isSuccess()).isTrue();
        verify(delegate, times(1)).shutdown();
    }

    @Test
    public void testGetAggregationTemporality() {
        when(delegate.getAggregationTemporality(
            InstrumentType.COUNTER))
            .thenReturn(AggregationTemporality.DELTA);

        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);
        AggregationTemporality result =
            exporter.getAggregationTemporality(
                InstrumentType.COUNTER);

        assertThat(result)
            .isEqualTo(AggregationTemporality.DELTA);
        verify(delegate, times(1))
            .getAggregationTemporality(
                InstrumentType.COUNTER);
    }

    @Test
    public void testExportNullCollection() {
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> MAX_DATA_POINTS);
        exporter.export(null);
        verify(delegate, times(1)).export(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitPreservesAllPoints() {
        // Verify no points are lost during split.
        // 83 points with maxBatch=10 -> 9 batches
        // (8*10 + 1*3)
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 10);

        MetricData large =
            createRealLongGaugeMetricData(
                "big_metric", 83);

        exporter.export(
            Collections.singletonList(large));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        // 83 / 10 = 8 full + 1 partial = 9 batches
        verify(delegate, times(9))
            .export(captor.capture());

        int totalPts = 0;
        for (Collection<MetricData> batch
            : captor.getAllValues()) {
            for (MetricData md : batch) {
                totalPts +=
                    md.getData()
                        .getPoints().size();
            }
        }
        assertThat(totalPts).isEqualTo(83);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplitPointsContentPreserved() {
        // Verify actual point data values are preserved
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 3);

        MetricData metric =
            createRealLongGaugeMetricData(
                "val_metric", 5);

        exporter.export(
            Collections.singletonList(metric));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(2))
            .export(captor.capture());

        // Collect all point values from batches
        List<Long> allValues = new ArrayList<>();
        for (Collection<MetricData> batch
            : captor.getAllValues()) {
            for (MetricData md : batch) {
                for (PointData pt
                    : md.getData().getPoints()) {
                    LongPointData lp =
                        (LongPointData) pt;
                    allValues.add(lp.getValue());
                }
            }
        }

        // Values should be 0, 1, 2, 3, 4
        assertThat(allValues)
            .containsExactly(0L, 1L, 2L, 3L, 4L);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotCreatesNewMetricData() {
        // On fast path, delegate should receive
        // snapshotted MetricData, not the original.
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 100);

        MetricData original =
            createRealLongGaugeMetricData("test", 5);

        exporter.export(
            Collections.singletonList(original));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(1))
            .export(captor.capture());

        MetricData exported =
            captor.getValue().iterator().next();
        assertThat(exported).isNotSameAs(original);
        assertThat(exported.getName())
            .isEqualTo("test");
        assertThat(exported.getType())
            .isEqualTo(MetricDataType.LONG_GAUGE);
        assertThat(exported.getData().getPoints())
            .hasSize(5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotFallsBackToOriginal() {
        // Mock MetricData has no type set, so snapshot
        // will fail. Should fall back to original object.
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 100);

        MetricData mockMd =
            createMockMetricData("mock", 3);

        exporter.export(
            Collections.singletonList(mockMd));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(1))
            .export(captor.capture());

        MetricData exported =
            captor.getValue().iterator().next();
        assertThat(exported).isSameAs(mockMd);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotPointsAreIndependentCopy() {
        // Verify snapshot points collection is a separate
        // copy from the original, preventing concurrent
        // modification issues.
        BatchSplittingMetricExporter exporter =
            new BatchSplittingMetricExporter(
                delegate, () -> 100);

        MetricData original =
            createRealLongGaugeMetricData("test", 5);
        Collection<? extends PointData> originalPoints =
            original.getData().getPoints();

        exporter.export(
            Collections.singletonList(original));

        ArgumentCaptor<Collection<MetricData>> captor =
            ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(1))
            .export(captor.capture());

        MetricData exported =
            captor.getValue().iterator().next();
        Collection<? extends PointData> exportedPoints =
            exported.getData().getPoints();

        assertThat(exportedPoints)
            .isNotSameAs(originalPoints);
        assertThat(exportedPoints)
            .hasSize(originalPoints.size());
    }

    /**
     * Creates a mock MetricData with the specified
     * number of data points.
     */
    @SuppressWarnings("unchecked")
    private MetricData createMockMetricData(
        final String name,
        final int numPoints) {
        List<PointData> points = new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            points.add(mock(PointData.class));
        }

        Data<?> data = mock(Data.class);
        doReturn(points).when(data).getPoints();

        MetricData metricData = mock(MetricData.class);
        when(metricData.getName()).thenReturn(name);
        doReturn(data).when(metricData).getData();

        return metricData;
    }

    /**
     * Creates a real LONG_GAUGE MetricData using the
     * OTel SDK ImmutableMetricData, suitable for testing
     * the split logic which needs to reconstruct
     * MetricData objects.
     */
    private MetricData createRealLongGaugeMetricData(
        final String name,
        final int numPoints) {
        List<LongPointData> points =
            new ArrayList<>();
        for (int i = 0; i < numPoints; i++) {
            points.add(
                ImmutableLongPointData.create(
                    0L, 1L,
                    io.opentelemetry.api.common
                        .Attributes.empty(),
                    (long) i));
        }
        return ImmutableMetricData.createLongGauge(
            Resource.getDefault(),
            InstrumentationScopeInfo.empty(),
            name,
            "test description",
            "1",
            ImmutableGaugeData.create(points));
    }

    /**
     * Calculates total data points in a batch.
     */
    private int totalPoints(
        final Collection<MetricData> batch) {
        int total = 0;
        for (MetricData md : batch) {
            total +=
                md.getData().getPoints().size();
        }
        return total;
    }
}
