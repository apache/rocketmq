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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StatisticsBriefTest {

    @Test
    public void testConstructorRejectsEmptyOrMalformedShape() {
        // Existing outer-shape contract: null, empty, or non-pair rows must throw IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(null));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[0][]));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {null}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 1, 1}}));
    }

    @Test
    public void testConstructorRejectsNonPositiveSlotCount() {
        // Issue 10871: slotNum <= 0 causes division by zero in getSlotIndex or invalid array sizing.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 0}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, -1}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 2}, {20, 0}}));
    }

    @Test
    public void testConstructorRejectsNonIncreasingRanges() {
        // Issue 10871: bucket 0 implicitly starts at 0 and each subsequent rangeMax must strictly increase.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{0, 1}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{-5, 1}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 2}, {10, 2}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 2}, {5, 2}}));
        // Long.MIN_VALUE underflows rangeMax - previousRangeMax to a large positive width; the monotonic check must catch it first.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 2}, {Long.MIN_VALUE, 1}}));
    }

    @Test
    public void testConstructorRejectsSlotsExceedingRangeWidth() {
        // Issue 10871: slotNum > (rangeMax - previousRangeMax) truncates slot width to 0 and divides by zero.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 11}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 2}, {12, 3}}));
    }

    @Test
    public void testConstructorRejectsNonDivisibleRangeWidth() {
        // Issue 10871: a width that does not divide evenly truncates the slot step and can index past the current bucket.
        // {{10, 6}} then sample(9) throws ArrayIndexOutOfBoundsException; {{10, 3}, {20, 10}} then sample(9) writes into the next bucket.
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 6}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 3}, {20, 10}}));
    }

    @Test
    public void testConstructorRejectsSlotCountOverflow() {
        // Issue 10871: slotNum(meta) starts at 1 and casts total slots to int; exceeding Integer.MAX_VALUE overflows.
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{Integer.MAX_VALUE, Integer.MAX_VALUE}}));
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{Integer.MAX_VALUE, Integer.MAX_VALUE - 1}, {Integer.MAX_VALUE + 1L, 1}}));
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{Long.MAX_VALUE, Long.MAX_VALUE}}));
    }

    @Test
    public void testValidBoundaryAndSamplingBehavior() {
        // Aligns with BrokerStatsManager metadata where first bucket allows slotNum == rangeMax (width 1 per slot).
        StatisticsBrief brief = new StatisticsBrief(new long[][] {{10, 10}, {100, 10}});

        brief.sample(3);
        brief.sample(10);
        brief.sample(55);
        brief.sample(120);

        assertEquals(4, brief.getCnt());
        assertEquals(3, brief.getMin());
        assertEquals(120, brief.getMax());
        assertEquals(188, brief.getTotal());
        assertEquals(47.0, brief.getAvg(), 0.0);
        assertEquals(120, brief.tp999());
        // 4 samples, excludes = (long)(4 - 4 * 0.5f) = 2. Walking counts from the overflow slot (index 20)
        // downward first exceeds 2 at index 10, whose upper bound is 10 + (100 - 10) / 10 * 1 = 19.
        assertEquals(19, brief.getTPValue(0.5f));

        brief.reset();
        assertEquals(0, brief.getCnt());
        assertEquals(0, brief.getMin());
        assertEquals(0, brief.getMax());
        assertEquals(0, brief.getTotal());
        assertEquals(0.0, brief.getAvg(), 0.0);
    }
}
