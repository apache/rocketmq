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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StatisticsBriefInterceptorTest {

    private final StatisticsItem item = new StatisticsItem("SEND", "topic", "count", "bytes", "latency");

    @Test
    public void testRejectsUnknownItemName() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> attachInterceptor(briefMeta("latency"), briefMeta("unknown")));

        assertEquals("illegal briefItemName: unknown", error.getMessage());
    }

    @Test
    public void testSamplesConfiguredItemsInMetadataOrder() {
        StatisticsBriefInterceptor interceptor = attachInterceptor(briefMeta("latency"), briefMeta("count"));

        item.incItems(2, 900, 13);
        item.incItems(4, 800, 21);

        StatisticsBrief[] briefs = interceptor.getStatisticsBriefs();
        assertEquals(2, briefs.length);
        assertEquals(2, briefs[0].getCnt());
        assertEquals(34, briefs[0].getTotal());
        assertEquals(2, briefs[1].getCnt());
        assertEquals(6, briefs[1].getTotal());
    }

    @Test
    public void testSkipsMissingValuesInPartialUpdate() {
        StatisticsBriefInterceptor interceptor = attachInterceptor(briefMeta("latency"), briefMeta("count"));

        item.incItems(3);
        item.incItems();

        StatisticsBrief[] briefs = interceptor.getStatisticsBriefs();
        assertEquals(0, briefs[0].getCnt());
        assertEquals(1, briefs[1].getCnt());
        assertEquals(3, briefs[1].getTotal());
    }

    @Test
    public void testResetStartsANewSamplingIntervalForEachItem() {
        StatisticsBriefInterceptor interceptor = attachInterceptor(briefMeta("latency"), briefMeta("count"));
        item.incItems(7, 900, 31);

        interceptor.reset();

        StatisticsBrief[] briefs = interceptor.getStatisticsBriefs();
        for (StatisticsBrief brief : briefs) {
            assertEquals(0, brief.getCnt());
            assertEquals(0, brief.getTotal());
        }

        item.incItems(2, 800, 5);

        assertEquals(1, briefs[0].getCnt());
        assertEquals(5, briefs[0].getMax());
        assertEquals(1, briefs[1].getCnt());
        assertEquals(2, briefs[1].getMax());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final StatisticsBriefInterceptor attachInterceptor(Pair<String, long[][]>... metadata) {
        StatisticsBriefInterceptor interceptor = new StatisticsBriefInterceptor(item, metadata);
        item.setInterceptor(interceptor);
        return interceptor;
    }

    private static Pair<String, long[][]> briefMeta(String itemName) {
        return Pair.of(itemName, new long[][] {{100, 10}});
    }
}
