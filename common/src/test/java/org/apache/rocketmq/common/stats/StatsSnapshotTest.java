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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StatsSnapshotTest {

    private StatsSnapshot snapshot;

    @Before
    public void init() {
        snapshot = new StatsSnapshot();
    }

    @Test
    public void testSetSum() {
        snapshot.setSum(100L);
    }

    @Test
    public void testGetSum() {
        snapshot.setSum(100L);
        Assert.assertEquals(100L, snapshot.getSum());
    }

    @Test
    public void testSetTps() {
        snapshot.setTps(100.01);
    }

    @Test
    public void testGetTps() {
        snapshot.setTps(100.01);
        Assert.assertEquals(100.01, snapshot.getTps(), 0);
    }

    @Test
    public void testSetAvgpt() {
        snapshot.setAvgpt(99.99);
    }

    @Test
    public void testGetArgpt() {
        snapshot.setAvgpt(99.99);
        Assert.assertEquals(99.99, snapshot.getAvgpt(), 0);
    }

}
