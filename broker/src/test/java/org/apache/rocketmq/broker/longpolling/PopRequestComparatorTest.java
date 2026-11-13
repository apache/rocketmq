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
package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.MessageFilter;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Regression tests for the long-to-int comparator overflow fixed in PopRequest.COMPARATOR.
 *
 * <p>The {@code op} counter starts at {@link Long#MIN_VALUE}, so a delta between two
 * PopRequests can easily exceed {@link Integer#MAX_VALUE} and would overflow when
 * the old code cast a {@code long} subtraction to {@code int}. The {@code expired}
 * timestamp has the same hazard for requests far apart in time.
 */
public class PopRequestComparatorTest {

    private PopRequest newPopRequest(long expired) throws Exception {
        return new PopRequest(
            mock(RemotingCommand.class),
            mock(ChannelHandlerContext.class),
            expired,
            mock(SubscriptionData.class),
            mock(MessageFilter.class));
    }

    /**
     * Reflectively set the {@code op} field (assigned from a static AtomicLong
     * starting at {@link Long#MIN_VALUE}) so two PopRequests' op values differ by
     * more than {@link Integer#MAX_VALUE}.
     *
     * <p>The old {@code (int)(o1.op - o2.op)} cast truncated this delta to int and
     * flipped the sign, corrupting the {@code ConcurrentSkipListSet} ordering.
     * The fix uses {@link Long#compare} to preserve the correct sign.
     */
    @Test
    public void comparatorDoesNotOverflowOnOpDelta() throws Exception {
        resetOpCounterTo(Long.MIN_VALUE);
        PopRequest a = newPopRequest(1000L);
        PopRequest c = newPopRequest(1000L);

        // Force op delta beyond Integer.MAX_VALUE: a.op = MIN_VALUE,
        // c.op = MIN_VALUE + (MAX_VALUE + 2) -> delta = MAX_VALUE + 2 > int range.
        setOp(a, Long.MIN_VALUE);
        setOp(c, Long.MIN_VALUE + (long) Integer.MAX_VALUE + 2L);

        // a.op < c.op by more than 2^31. Old (int)(a.op - c.op) overflowed to a
        // positive value (wrongly a > c); Long.compare returns negative (a < c).
        assertThat(PopRequest.COMPARATOR.compare(a, c)).isNegative();
        assertThat(PopRequest.COMPARATOR.compare(c, a)).isPositive();
    }

    /**
     * Two requests whose {@code expired} timestamps differ by more than
     * {@link Integer#MAX_VALUE} must be ordered by the earlier expiry first.
     */
    @Test
    public void comparatorDoesNotOverflowOnExpiredDelta() throws Exception {
        resetOpCounterTo(Long.MIN_VALUE);
        long base = 0L;
        PopRequest earlier = newPopRequest(base);
        PopRequest later = newPopRequest(base + (long) Integer.MAX_VALUE + 2L);

        // earlier should compare before later
        assertThat(PopRequest.COMPARATOR.compare(earlier, later)).isNegative();
        assertThat(PopRequest.COMPARATOR.compare(later, earlier)).isPositive();
    }

    /**
     * The comparator must be consistent for equal {@code expired}: the one with
     * the smaller {@code op} comes first (FIFO by creation order).
     */
    @Test
    public void comparatorIsConsistentForEqualExpired() throws Exception {
        resetOpCounterTo(Long.MIN_VALUE);
        PopRequest first = newPopRequest(5000L);
        PopRequest second = newPopRequest(5000L);

        // equal expired -> tie-broken by op (first created is smaller) -> first < second
        assertThat(PopRequest.COMPARATOR.compare(first, second)).isNegative();
    }

    /**
     * Reflectively reset the static op counter so the overflow window is
     * deterministic and does not depend on how many PopRequests tests before
     * this one have created.
     */
    private static void resetOpCounterTo(long value) throws Exception {
        Field counterField = PopRequest.class.getDeclaredField("COUNTER");
        counterField.setAccessible(true);
        AtomicLong counter = (AtomicLong) counterField.get(null);
        counter.set(value);
    }

    /**
     * Reflectively set the {@code op} field on a PopRequest to force an op delta
     * that exceeds {@link Integer#MAX_VALUE} without having to create 2^31
     * real PopRequests.
     */
    private static void setOp(PopRequest pr, long value) throws Exception {
        Field opField = PopRequest.class.getDeclaredField("op");
        opField.setAccessible(true);
        opField.setLong(pr, value);
    }
}
