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

package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class MomentStatsItemTest {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);

    private MomentStatsItem item;

    @Before
    public void init() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        item = new MomentStatsItem("statName" , "statKey", scheduledExecutorService, log);
    }

    @Test
    public void testInit() {
        item.init();
    }

    @Test
    public void testPrintAtMinutes() {
        item.printAtMinutes();
    }

    @Test
    public void testGetValue() {
        AtomicLong value = item.getValue();
        Assert.assertEquals(0L, value.get());
    }

    @Test
    public void testGetStatsKey() {
        String statKey = item.getStatsKey();
        Assert.assertEquals("statKey", statKey);
    }

    @Test
    public void testGetStatsName() {
        String statName = item.getStatsName();
        Assert.assertEquals("statName", statName);
    }

}
