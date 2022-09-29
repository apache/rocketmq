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
package org.apache.rocketmq.store.timer;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimerMetricsTest {


    @Test
    public void testTimingCount() {
        String baseDir = StoreTestUtils.createBaseDir();

        TimerMetrics first = new TimerMetrics(baseDir);
        Assert.assertTrue(first.load());
        first.addAndGet("AAA", 1000);
        first.addAndGet("BBB", 2000);
        Assert.assertEquals(1000, first.getTimingCount("AAA"));
        Assert.assertEquals(2000, first.getTimingCount("BBB"));
        long curr = System.currentTimeMillis();
        Assert.assertTrue(first.getTopicPair("AAA").getTimeStamp() > curr - 10);
        Assert.assertTrue(first.getTopicPair("AAA").getTimeStamp() <= curr);
        first.persist();

        TimerMetrics second = new TimerMetrics(baseDir);
        Assert.assertTrue(second.load());
        Assert.assertEquals(1000, second.getTimingCount("AAA"));
        Assert.assertEquals(2000, second.getTimingCount("BBB"));
        Assert.assertTrue(second.getTopicPair("BBB").getTimeStamp() > curr - 100);
        Assert.assertTrue(second.getTopicPair("BBB").getTimeStamp() <= curr);
        second.persist();
        StoreTestUtils.deleteFile(baseDir);
    }

    @Test
    public void testTimingDistribution() {
        String baseDir = StoreTestUtils.createBaseDir();
        TimerMetrics first = new TimerMetrics(baseDir);
        List<Integer> timerDist = new ArrayList<Integer>() {{
                add(5);
                add(60);
                add(300); // 5s, 1min, 5min
                add(900);
                add(3600);
                add(14400); // 15min, 1h, 4h
                add(28800);
                add(86400); // 8h, 24h
            }};
        for (int period : timerDist) {
            first.updateDistPair(period, period);
        }

        int temp = 0;

        for (int j = 0; j < 50; j++) {
            for (int period : timerDist) {
                Assert.assertEquals(first.getDistPair(period).getCount().get(),period + temp);
                first.updateDistPair(period, j);
            }
            temp += j;
        }

        StoreTestUtils.deleteFile(baseDir);
    }
}
