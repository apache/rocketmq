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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BatchConsumeClientDiagnosticsTest {

    @Test
    public void testConstructorAndGetters() {
        Map<String, Integer> topicDistribution = new HashMap<>();
        topicDistribution.put("topicA", 3);
        topicDistribution.put("topicB", 7);

        BatchConsumeClientDiagnostics diagnostics = new BatchConsumeClientDiagnostics(
            "client-1", "channel-1",
            10, 5,
            20L, 2L, 1,
            topicDistribution,
            "POP", "CLUSTERING",
            32, 30000L,
            15L, 1000L);

        assertEquals("client-1", diagnostics.getClientId());
        assertEquals("channel-1", diagnostics.getChannelId());
        assertEquals(10, diagnostics.getUnackedMessageCount());
        assertEquals(5, diagnostics.getUnackedHandleCount());
        assertEquals(20L, diagnostics.getTotalRenewTimes());
        assertEquals(2L, diagnostics.getTotalRenewRetryTimes());
        assertEquals(1, diagnostics.getExpiredHandleCount());
        assertEquals(topicDistribution, diagnostics.getTopicDistribution());
        assertEquals("POP", diagnostics.getConsumeType());
        assertEquals("CLUSTERING", diagnostics.getMessageModel());
        assertEquals(32, diagnostics.getReceiveBatchSize());
        assertEquals(30000L, diagnostics.getLongPollingTimeoutMs());
        assertEquals(15L, diagnostics.getLastRttMs());
        assertEquals(1000L, diagnostics.getConnectTime());
    }
}
