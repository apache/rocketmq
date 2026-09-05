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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the long-to-int comparator overflow fixed in
 * {@link PopCheckPoint#compareTo(PopCheckPoint)}.
 *
 * <p>When two {@code startOffset} values differ by more than
 * {@link Integer#MAX_VALUE}, the old cast-{@code (int)(a - b)} overflowed
 * and produced the wrong sign.
 */
public class PopCheckPointComparatorTest {

    private PopCheckPoint newCheckPoint(long startOffset) {
        PopCheckPoint cp = new PopCheckPoint();
        cp.setStartOffset(startOffset);
        return cp;
    }

    /**
     * The delta between the two startOffset values exceeds Integer.MAX_VALUE.
     * The old code: {@code (int)(a - b)} would overflow and return the wrong sign.
     * The fix: {@code Long.compare(a, b)} returns the correct ordering.
     */
    @Test
    public void compareToDoesNotOverflowOnLargeOffsetDelta() {
        long small = 0L;
        long large = (long) Integer.MAX_VALUE + 2L;

        PopCheckPoint cpSmall = newCheckPoint(small);
        PopCheckPoint cpLarge = newCheckPoint(large);

        // small offset should compare before large offset
        assertThat(cpSmall.compareTo(cpLarge)).isNegative();
        assertThat(cpLarge.compareTo(cpSmall)).isPositive();
    }

    /**
     * Offsets near Long.MIN_VALUE should also be ordered correctly.
     */
    @Test
    public void compareToOrdersNearMinValue() {
        PopCheckPoint a = newCheckPoint(Long.MIN_VALUE);
        PopCheckPoint b = newCheckPoint(Long.MIN_VALUE + 100);

        assertThat(a.compareTo(b)).isNegative();
        assertThat(b.compareTo(a)).isPositive();
    }

    /**
     * Equal offsets should compare as zero.
     */
    @Test
    public void compareToReturnsZeroForEqualOffsets() {
        PopCheckPoint a = newCheckPoint(12345L);
        PopCheckPoint b = newCheckPoint(12345L);

        assertThat(a.compareTo(b)).isZero();
    }
}
