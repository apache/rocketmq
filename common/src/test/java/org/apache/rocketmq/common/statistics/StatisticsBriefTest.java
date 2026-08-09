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
    public void constructor_WithNonPositiveSlotCount_ThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 0}}));
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, -1}}));
    }

    @Test
    public void constructor_WithNonIncreasingRanges_ThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{10, 2}, {10, 2}}));
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{10, 2}, {5, 2}}));
    }

    @Test
    public void constructor_WithMoreSlotsThanRangeWidth_ThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new StatisticsBrief(new long[][] {{10, 11}}));
        assertThrows(IllegalArgumentException.class,
            () -> new StatisticsBrief(new long[][] {{10, 2}, {12, 3}}));
    }

    @Test
    public void sample_WithValidMetadata_RecordsStatistics() {
        StatisticsBrief brief = new StatisticsBrief(new long[][] {{10, 2}, {20, 2}});

        brief.sample(5);
        brief.sample(15);

        assertEquals(2, brief.getCnt());
        assertEquals(5, brief.getMin());
        assertEquals(15, brief.getMax());
        assertEquals(20, brief.getTotal());
        assertEquals(10.0, brief.getAvg(), 0.0);
    }
}
