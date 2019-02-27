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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class MomentStatsItemSetTest {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);

    private MomentStatsItemSet set;

    @Before
    public void init() {
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        set = new MomentStatsItemSet("statName" , scheduledExecutorService, log);
    }

    @Test
    public void testInit() {
        set.init();
    }

    @Test
    public void testGetAndCreateStatsItem() {
        MomentStatsItem item  = set.getAndCreateStatsItem("statKey");
        Assert.assertNotNull(item);
    }

    @Test
    public void testGetStatsItemTable() {
        MomentStatsItem item  = set.getAndCreateStatsItem("statKey");
        ConcurrentMap<String, MomentStatsItem> map = set.getStatsItemTable();
        MomentStatsItem item2  = map.get("statKey");
        Assert.assertEquals(item, item2);
    }
}
