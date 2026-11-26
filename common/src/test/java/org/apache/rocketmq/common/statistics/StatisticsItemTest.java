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
package org.apache.rocketmq.common.statistics;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class StatisticsItemTest {

    private static final String[] ITEM_NAMES = {"count", "size", "rt"};

    @Test
    public void testConstructorRejectsNullOrEmptyItemNames() {
        assertThrows(InvalidParameterException.class, () -> new StatisticsItem("kind", "object", (String[]) null));
        assertThrows(InvalidParameterException.class, () -> new StatisticsItem("kind", "object"));
        assertThrows(InvalidParameterException.class, () -> new StatisticsItem("kind", "object", new String[0]));
    }

    @Test
    public void testConstructorInitializesState() {
        long before = System.currentTimeMillis();
        StatisticsItem item = newItem();
        long after = System.currentTimeMillis();

        assertEquals("kind", item.getStatKind());
        assertEquals("object", item.getStatObject());
        assertArrayEquals(ITEM_NAMES, item.getItemNames());
        assertArrayEquals(new long[] {0, 0, 0}, valuesOf(item));
        assertEquals(0, item.getInvokeTimes().get());
        assertNull(item.getInterceptor());
        assertThat(item.getLastTimeStamp().get()).isBetween(before, after);
    }

    @Test
    public void testIncItemsWithFewerValuesOnlyUpdatesLeadingItems() {
        StatisticsItem item = newItem();

        item.incItems(3, 4);
        item.incItems(1);

        assertArrayEquals(new long[] {4, 4, 0}, valuesOf(item));
        assertEquals(2, item.getInvokeTimes().get());
    }

    @Test
    public void testIncItemsIgnoresExtraValues() {
        StatisticsItem item = newItem();

        item.incItems(1, 2, 3, 4, 5);

        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(item));
        assertEquals(1, item.getInvokeTimes().get());
    }

    @Test
    public void testIncItemsUpdatesInvokeTimesAndLastTimeStamp() {
        StatisticsItem item = newItem();
        item.getLastTimeStamp().set(0L);

        long before = System.currentTimeMillis();
        item.incItems(1, 1, 1);
        // A call without values still counts as one invocation.
        item.incItems();
        long after = System.currentTimeMillis();

        assertEquals(2, item.getInvokeTimes().get());
        assertArrayEquals(new long[] {1, 1, 1}, valuesOf(item));
        assertThat(item.getLastTimeStamp().get()).isBetween(before, after);
    }

    @Test
    public void testIncItemsDelegatesOriginalValuesToInterceptor() {
        StatisticsItem item = newItem();
        RecordingInterceptor interceptor = new RecordingInterceptor();
        item.setInterceptor(interceptor);
        assertSame(interceptor, item.getInterceptor());

        long[] incs = {1, 2, 3, 4};
        item.incItems(incs);

        // The interceptor receives every value, without truncation to the item count.
        assertEquals(1, interceptor.received.size());
        assertArrayEquals(new long[] {1, 2, 3, 4}, interceptor.received.get(0));
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(item));
    }

    @Test
    public void testAllZerosReflectsAccumulatedValues() {
        assertTrue(newItem().allZeros());

        StatisticsItem zeroIncrements = newItem();
        zeroIncrements.incItems(0, 0);
        zeroIncrements.incItems();
        assertTrue(zeroIncrements.allZeros());

        StatisticsItem nonZero = newItem();
        nonZero.incItems(0, 0, 1);
        assertFalse(nonZero.allZeros());

        StatisticsItem netZero = newItem();
        netZero.incItems(5);
        netZero.incItems(-5);
        assertTrue(netZero.allZeros());
    }

    @Test
    public void testAllZerosReturnsTrueWhenNeverInvoked() {
        StatisticsItem item = newItem();
        // allZeros() short-circuits on invokeTimes == 0 without looking at the accumulated values.
        item.getItemAccumulates()[0].set(7);

        assertTrue(item.allZeros());
    }

    @Test
    public void testGetItemAccumulateReturnsLiveCounter() {
        StatisticsItem item = newItem();
        assertEquals(0, item.getItemAccumulate("size").get());

        item.incItems(0, 9);
        assertEquals(9, item.getItemAccumulate("size").get());
    }

    @Test
    public void testGetItemAccumulateForUnknownNameReturnsDetachedZero() {
        StatisticsItem item = newItem();

        AtomicLong unknown = item.getItemAccumulate("unknown");
        assertEquals(0, unknown.get());

        unknown.set(100);
        assertEquals(0, item.getItemAccumulate("unknown").get());
        assertArrayEquals(new long[] {0, 0, 0}, valuesOf(item));
    }

    @Test
    public void testSnapshotIsIndependentCopy() {
        StatisticsItem item = newItem();
        item.incItems(1, 2, 3);
        item.getLastTimeStamp().set(12345L);

        StatisticsItem snapshot = item.snapshot();

        assertNotSame(item, snapshot);
        assertEquals("kind", snapshot.getStatKind());
        assertEquals("object", snapshot.getStatObject());
        assertArrayEquals(ITEM_NAMES, snapshot.getItemNames());
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(snapshot));
        assertEquals(1, snapshot.getInvokeTimes().get());
        assertEquals(12345L, snapshot.getLastTimeStamp().get());

        item.incItems(10, 10, 10);
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(snapshot));
        assertEquals(1, snapshot.getInvokeTimes().get());

        snapshot.incItems(100);
        assertArrayEquals(new long[] {11, 12, 13}, valuesOf(item));
        assertEquals(2, item.getInvokeTimes().get());
    }

    @Test
    public void testSnapshotDoesNotCopyInterceptor() {
        StatisticsItem item = newItem();
        item.setInterceptor(new RecordingInterceptor());

        assertNull(item.snapshot().getInterceptor());
    }

    @Test
    public void testSubtractNullReturnsSnapshot() {
        StatisticsItem item = newItem();
        item.incItems(1, 2, 3);

        StatisticsItem result = item.subtract(null);

        assertNotSame(item, result);
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(result));
        assertEquals(1, result.getInvokeTimes().get());

        item.incItems(1, 1, 1);
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(result));
    }

    @Test
    public void testSubtractRejectsIncompatibleItem() {
        StatisticsItem item = newItem();

        assertThrows(IllegalArgumentException.class,
            () -> item.subtract(new StatisticsItem("otherKind", "object", ITEM_NAMES)));
        assertThrows(IllegalArgumentException.class,
            () -> item.subtract(new StatisticsItem("kind", "otherObject", ITEM_NAMES)));
        assertThrows(IllegalArgumentException.class,
            () -> item.subtract(new StatisticsItem("kind", "object", "count", "size", "other")));
        assertThrows(IllegalArgumentException.class,
            () -> item.subtract(new StatisticsItem("kind", "object", "rt", "size", "count")));
        assertThrows(IllegalArgumentException.class,
            () -> item.subtract(new StatisticsItem("kind", "object", "count", "size")));
    }

    @Test
    public void testSubtractComputesDelta() {
        StatisticsItem older = newItem();
        older.incItems(1, 2, 3);
        StatisticsItem newer = new StatisticsItem("kind", "object", new String[] {"count", "size", "rt"});
        newer.incItems(5, 10, 20);
        newer.incItems(5, 0, 1);

        StatisticsItem delta = newer.subtract(older);

        assertEquals("kind", delta.getStatKind());
        assertEquals("object", delta.getStatObject());
        assertArrayEquals(ITEM_NAMES, delta.getItemNames());
        assertArrayEquals(new long[] {9, 8, 18}, valuesOf(delta));
        assertEquals(1, delta.getInvokeTimes().get());

        assertArrayEquals(new long[] {10, 10, 21}, valuesOf(newer));
        assertEquals(2, newer.getInvokeTimes().get());
        assertArrayEquals(new long[] {1, 2, 3}, valuesOf(older));
        assertEquals(1, older.getInvokeTimes().get());

        delta.incItems(1, 1, 1);
        assertArrayEquals(new long[] {10, 10, 21}, valuesOf(newer));
    }

    @Test
    public void testSubtractCanProduceNegativeDelta() {
        StatisticsItem older = newItem();
        older.incItems(1, 2, 3);
        StatisticsItem newer = newItem();
        newer.incItems(5, 5, 5);
        newer.incItems(5, 5, 5);

        // subtract() does not clamp, so subtracting the newer item from the older one yields negative values.
        StatisticsItem delta = older.subtract(newer);

        assertArrayEquals(new long[] {-9, -8, -7}, valuesOf(delta));
        assertEquals(-1, delta.getInvokeTimes().get());
    }

    private static StatisticsItem newItem() {
        return new StatisticsItem("kind", "object", ITEM_NAMES);
    }

    private static long[] valuesOf(StatisticsItem item) {
        AtomicLong[] accumulates = item.getItemAccumulates();
        long[] values = new long[accumulates.length];
        for (int i = 0; i < accumulates.length; i++) {
            values[i] = accumulates[i].get();
        }
        return values;
    }

    private static final class RecordingInterceptor implements Interceptor {
        private final List<long[]> received = new ArrayList<>();

        @Override
        public void inc(long... deltas) {
            received.add(deltas);
        }

        @Override
        public void reset() {
        }
    }
}
