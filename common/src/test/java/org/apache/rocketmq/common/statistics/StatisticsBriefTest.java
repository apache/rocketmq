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

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticsBriefTest {

    // one range [0, 100) split into 10 slots of width 10
    private static final long[][] META = {{100, 10}};

    @Test
    public void testTpValue_allSamplesInFirstBucket() {
        StatisticsBrief brief = new StatisticsBrief(META);
        for (int i = 0; i < 100; i++) {
            brief.sample(5L);
        }
        // Regression: the reverse traversal used to stop at i == 1, so when every
        // sample falls into bucket 0 the percentile scan found nothing and returned 0.
        assertThat(brief.getTPValue(0.99f)).isEqualTo(5L);
        assertThat(brief.tp999()).isEqualTo(5L);
        assertThat(brief.getMax()).isEqualTo(5L);
        assertThat(brief.getCnt()).isEqualTo(100L);
    }

    @Test
    public void testTpValue_singleSampleInFirstBucket() {
        StatisticsBrief brief = new StatisticsBrief(META);
        brief.sample(3L);
        // count * ratio == 1 * 0.5 == 0.5 -> excludes == (long)(1 - 0.5) == 0 -> returns max
        assertThat(brief.getTPValue(0.5f)).isEqualTo(3L);
    }

    @Test
    public void testTpValue_lowAndHighSamples() {
        // two ranges: [0,100) with 10 slots, [100,1000) with 9 slots
        long[][] meta = {{100, 10}, {1000, 9}};
        StatisticsBrief brief = new StatisticsBrief(meta);
        for (int i = 0; i < 10; i++) {
            brief.sample(5L);
        }
        brief.sample(150L);

        // count = 11, ratio = 0.5 -> excludes = (long)(11 - 5.5) = 5.
        // Buckets above 0 hold a single sample (<= 5), so the percentile is served by
        // bucket 0: min(slot 0 upper bound 10, max 150) == 10. The old loop returned 0.
        assertThat(brief.getTPValue(0.5f)).isEqualTo(10L);
        assertThat(brief.getMax()).isEqualTo(150L);
    }

    @Test
    public void testTpValue_highSamplesStillUseHighBucket() {
        // two ranges: [0,100) with 10 slots, [100,1000) with 9 slots
        long[][] meta = {{100, 10}, {1000, 9}};
        StatisticsBrief brief = new StatisticsBrief(meta);
        brief.sample(95L);
        brief.sample(96L);
        brief.sample(150L);

        // count = 3, ratio = 0.5 -> excludes = 1.
        // Bucket 10 (value 150) contributes 1 (not > 1); bucket 9 (95, 96) brings the
        // running count to 3 > 1 -> slot 9 upper bound is 100, max is 150 -> 100.
        assertThat(brief.getTPValue(0.5f)).isEqualTo(100L);
    }

    @Test
    public void testResetClearsSamples() {
        StatisticsBrief brief = new StatisticsBrief(META);
        brief.sample(5L);
        brief.reset();
        assertThat(brief.getCnt()).isEqualTo(0L);
        assertThat(brief.getMax()).isEqualTo(0L);
        assertThat(brief.getMin()).isEqualTo(0L);
    }
}
