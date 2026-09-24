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
package org.apache.rocketmq.tools.command.message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class PrintMessageByQueueCommandTest {

    @Test
    public void testCompareByTagCountDescending() {
        PrintMessageByQueueCommand.TagCountBean many = newTagCountBean("many", 20L);
        PrintMessageByQueueCommand.TagCountBean few = newTagCountBean("few", 10L);

        // printMsgByQueue prints the tags with the highest count first.
        Assert.assertTrue(many.compareTo(few) < 0);
        Assert.assertTrue(few.compareTo(many) > 0);
    }

    @Test
    public void testCompareEqualTagCounts() {
        // A tag counted only once must still be ordered in front of a tag that was never counted.
        Assert.assertTrue(newTagCountBean("one", 1L).compareTo(newTagCountBean("zero", 0L)) < 0);
        Assert.assertEquals(0, newTagCountBean("a", 0L).compareTo(newTagCountBean("b", 0L)));
    }

    @Test
    public void testCompareTagCountBeyondIntRange() {
        // Tag counters are long values. Casting (o.count - count) to an int wrapped around once
        // the difference exceeded Integer.MAX_VALUE, which reversed the order of the tag summary.
        PrintMessageByQueueCommand.TagCountBean huge = newTagCountBean("huge", Long.MAX_VALUE);
        PrintMessageByQueueCommand.TagCountBean zero = newTagCountBean("zero", 0L);

        Assert.assertTrue(huge.compareTo(zero) < 0);
        Assert.assertTrue(zero.compareTo(huge) > 0);
    }

    @Test
    public void testCompareTagCountAntisymmetric() {
        long[] counts = new long[] {0L, 1L, Integer.MAX_VALUE + 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE};
        for (long left : counts) {
            for (long right : counts) {
                int forward = newTagCountBean("left", left).compareTo(newTagCountBean("right", right));
                int backward = newTagCountBean("right", right).compareTo(newTagCountBean("left", left));

                // An overflowing comparator can report "left < right" and "right < left" at the
                // same time, which breaks the contract TimSort relies on.
                Assert.assertEquals(Integer.signum(forward), -Integer.signum(backward));
            }
        }
    }

    @Test
    public void testSortTagCountBeansBeyondIntRange() {
        List<PrintMessageByQueueCommand.TagCountBean> beans = new ArrayList<>();
        beans.add(newTagCountBean("zero", 0L));
        beans.add(newTagCountBean("max", Long.MAX_VALUE));
        beans.add(newTagCountBean("intMax", Integer.MAX_VALUE + 3L));
        beans.add(newTagCountBean("small", 5L));

        Collections.sort(beans);

        Assert.assertEquals(Arrays.asList("max", "intMax", "small", "zero"), tags(beans));
    }

    @Test
    public void testSortTagCountBeansWithEqualCounts() {
        List<PrintMessageByQueueCommand.TagCountBean> beans = new ArrayList<>();
        beans.add(newTagCountBean("a", 7L));
        beans.add(newTagCountBean("b", 7L));
        beans.add(newTagCountBean("c", Long.MAX_VALUE));

        Collections.sort(beans);

        // Equal counters may be ordered either way, but every tag must survive the sort.
        Assert.assertEquals(3, beans.size());
        Assert.assertEquals("c", beans.get(0).getTag());
    }

    private static PrintMessageByQueueCommand.TagCountBean newTagCountBean(String tag, long count) {
        return new PrintMessageByQueueCommand.TagCountBean(tag, new AtomicLong(count));
    }

    private static List<String> tags(List<PrintMessageByQueueCommand.TagCountBean> beans) {
        List<String> tags = new ArrayList<>(beans.size());
        for (PrintMessageByQueueCommand.TagCountBean bean : beans) {
            tags.add(bean.getTag());
        }
        return tags;
    }
}
