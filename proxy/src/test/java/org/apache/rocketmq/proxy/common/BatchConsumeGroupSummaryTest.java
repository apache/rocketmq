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

package org.apache.rocketmq.proxy.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BatchConsumeGroupSummaryTest {

    @Test
    public void testConstructorAndGetters() {
        BatchConsumeGroupSummary summary = new BatchConsumeGroupSummary(
            "groupA", 3, 100, 50, 5, 200L, 10L);

        assertEquals("groupA", summary.getGroup());
        assertEquals(3, summary.getTotalClients());
        assertEquals(100, summary.getTotalUnackedMessages());
        assertEquals(50, summary.getTotalUnackedHandles());
        assertEquals(5, summary.getTotalExpiredHandles());
        assertEquals(200L, summary.getTotalRenewTimes());
        assertEquals(10L, summary.getTotalRenewRetryTimes());
    }
}
