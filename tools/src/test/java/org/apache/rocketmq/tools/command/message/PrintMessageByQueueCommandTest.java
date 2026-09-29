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
    public void testCompareToIsAntisymmetricBeyondIntegerMaxValue() {
        PrintMessageByQueueCommand.TagCountBean zero = newTagCountBean("tag-zero", 0L);
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);

        // (int) (o.count - this.count) wraps for this pair and returns a negative value in
        // both directions, so assert the sign of the comparison is properly inverted.
        Assert.assertTrue(zero.compareTo(overInt) > 0);
        Assert.assertTrue(overInt.compareTo(zero) < 0);
        Assert.assertEquals(Integer.signum(overInt.compareTo(zero)), -Integer.signum(zero.compareTo(overInt)));
    }

    @Test
    public void testCollectionsSortOrdersTagCountsDescending() {
        PrintMessageByQueueCommand.TagCountBean zero = newTagCountBean("tag-zero", 0L);
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean max = newTagCountBean("tag-max", Long.MAX_VALUE);

        List<PrintMessageByQueueCommand.TagCountBean> beans = new ArrayList<>();
        beans.add(zero);
        beans.add(max);
        beans.add(overInt);
        Collections.sort(beans);

        Assert.assertEquals(Arrays.asList("tag-max", "tag-over-int", "tag-zero"), tagsOf(beans));
    }

    @Test
    public void testEqualCountsCompareEqual() {
        PrintMessageByQueueCommand.TagCountBean first = newTagCountBean("tag-first", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean second = newTagCountBean("tag-second", (long) Integer.MAX_VALUE + 1L);

        // Equal counts must produce an equal comparison in both directions, otherwise
        // TimSort treats the two entries as an inconsistent pair.
        Assert.assertEquals(0, first.compareTo(second));
        Assert.assertEquals(0, second.compareTo(first));
    }

    @Test
    public void testCompareToHandlesLongMaxValueCounts() {
        PrintMessageByQueueCommand.TagCountBean overInt = newTagCountBean("tag-over-int", (long) Integer.MAX_VALUE + 1L);
        PrintMessageByQueueCommand.TagCountBean max = newTagCountBean("tag-max", Long.MAX_VALUE);

        // The largest possible gap between two long counters must still order correctly.
        Assert.assertTrue(overInt.compareTo(max) > 0);
        Assert.assertTrue(max.compareTo(overInt) < 0);
    }

    private static PrintMessageByQueueCommand.TagCountBean newTagCountBean(String tag, long count) {
        return new PrintMessageByQueueCommand.TagCountBean(tag, new AtomicLong(count));
    }

    private static List<String> tagsOf(List<PrintMessageByQueueCommand.TagCountBean> beans) {
        List<String> tags = new ArrayList<>(beans.size());
        for (PrintMessageByQueueCommand.TagCountBean bean : beans) {
            tags.add(bean.getTag());
        }
        return tags;
    }
}
