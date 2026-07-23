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

package org.apache.rocketmq.store.pop;

import java.util.TreeMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PopCheckPointTest {

    private static PopCheckPoint build(long startOffset) {
        PopCheckPoint ck = new PopCheckPoint();
        ck.setStartOffset(startOffset);
        return ck;
    }

    @Test
    public void testCompareToWithoutOverflow() {
        PopCheckPoint low = build(0);
        PopCheckPoint high = build((long) Integer.MAX_VALUE + 2L);

        // The legacy (int)(a - b) comparator overflowed and reported the high
        // offset as the smaller one. Long.compare must keep them ordered.
        assertThat(low.compareTo(high)).isNegative();
        assertThat(high.compareTo(low)).isPositive();
        assertThat(low.compareTo(build(0))).isZero();
    }

    @Test
    public void testTreeMapOrdersLargeStartOffsets() {
        // PopReviveService keeps checkpoints in a TreeMap<PopCheckPoint, ...>,
        // so a broken compareTo corrupts ordering of large offsets.
        TreeMap<PopCheckPoint, Boolean> map = new TreeMap<>();
        PopCheckPoint high = build((long) Integer.MAX_VALUE + 2L);
        PopCheckPoint low = build(0);
        map.put(high, true);
        map.put(low, true);

        assertThat(map.firstKey().getStartOffset()).isEqualTo(0L);
        assertThat(map.lastKey().getStartOffset()).isEqualTo((long) Integer.MAX_VALUE + 2L);
    }
}
