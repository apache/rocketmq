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

package org.apache.rocketmq.broker.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.entity.TopicGroup;
import org.apache.rocketmq.common.lite.LiteLagInfo;
import org.apache.rocketmq.common.lite.LiteUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LiteConsumerLagCalculatorTest {

    private LiteConsumerLagCalculator liteConsumerLagCalculator;

    @Mock
    private BrokerController brokerController;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    private final BrokerConfig brokerConfig = new BrokerConfig();

    @Before
    public void setUp() {
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        when(brokerController.getConsumerOffsetManager()).thenReturn(consumerOffsetManager);

        liteConsumerLagCalculator = new LiteConsumerLagCalculator(brokerController);
    }

    @Test
    public void testUpdateLagInfo() {
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = LiteUtil.toLmqName(topic, "lmq1");
        long storeTimestamp = System.currentTimeMillis();

        liteConsumerLagCalculator.updateLagInfo(group, topic, lmqName, storeTimestamp);

        TopicGroup topicGroup = new TopicGroup(topic, group);
        PriorityBlockingQueue<LiteConsumerLagCalculator.LagTimeInfo> lagHeap =
            liteConsumerLagCalculator.topicGroupLagTimeMap.get(topicGroup);
        assertNotNull(lagHeap);
        assertEquals(1, lagHeap.size());
        LiteConsumerLagCalculator.LagTimeInfo lagInfo = lagHeap.peek();
        assertNotNull(lagInfo);
        assertEquals(lmqName, lagInfo.getLmqName());
        assertEquals(storeTimestamp, lagInfo.getLagTimestamp());
    }

    @Test
    public void testUpdateLagInfo_KeepSmallestWhenExceedsCapacity() {
        String group = "testGroup";
        String topic = "testTopic";

        // Set topK to 3, so the heap will retain at most 3 elements
        brokerConfig.setLiteLagLatencyTopK(3);

        // Add 5 elements with timestamps 1000, 2000, 3000, 4000, 5000
        // Expected result is to retain the smallest 3: 1000, 2000, 3000
        liteConsumerLagCalculator.updateLagInfo(group, topic,
            LiteUtil.toLmqName(topic, "lmq1"), 3000L);
        liteConsumerLagCalculator.updateLagInfo(group, topic,
            LiteUtil.toLmqName(topic, "lmq2"), 1000L);
        liteConsumerLagCalculator.updateLagInfo(group, topic,
            LiteUtil.toLmqName(topic, "lmq3"), 5000L);
        liteConsumerLagCalculator.updateLagInfo(group, topic,
            LiteUtil.toLmqName(topic, "lmq4"), 2000L);
        liteConsumerLagCalculator.updateLagInfo(group, topic,
            LiteUtil.toLmqName(topic, "lmq5"), 4000L);

        // Verify that the heap contains only 3 elements
        TopicGroup topicGroup = new TopicGroup(topic, group);
        PriorityBlockingQueue<LiteConsumerLagCalculator.LagTimeInfo> lagHeap =
            liteConsumerLagCalculator.topicGroupLagTimeMap.get(topicGroup);
        assertNotNull(lagHeap);
        assertEquals(3, lagHeap.size());

        // Verify that the retained elements have the smallest timestamps: 1000, 2000, 3000
        List<Long> timestamps = new ArrayList<>();
        for (LiteConsumerLagCalculator.LagTimeInfo info : lagHeap) {
            timestamps.add(info.getLagTimestamp());
        }
        Collections.sort(timestamps);
        assertEquals(3, timestamps.size());
        assertEquals(1000L, timestamps.get(0).longValue());
        assertEquals(2000L, timestamps.get(1).longValue());
        assertEquals(3000L, timestamps.get(2).longValue());
    }

    @Test
    public void testRemoveLagInfo() {
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName = LiteUtil.toLmqName(topic, "lmq1");
        long storeTimestamp = System.currentTimeMillis();

        liteConsumerLagCalculator.updateLagInfo(group, topic, lmqName, storeTimestamp);
        liteConsumerLagCalculator.removeLagInfo(group, topic, lmqName);

        TopicGroup topicGroup = new TopicGroup(topic, group);
        PriorityBlockingQueue<LiteConsumerLagCalculator.LagTimeInfo> lagHeap =
            liteConsumerLagCalculator.topicGroupLagTimeMap.get(topicGroup);
        assertTrue(lagHeap.isEmpty());
    }

    @Test
    public void testOffsetTableForEachByGroup() {
        String testTopic = "testTopic";
        String liteTopic = "lmq1";
        String testGroup = "testGroup";
        String otherGroup = "otherGroup";
        String lmqName = LiteUtil.toLmqName(testTopic, liteTopic);
        String key = lmqName + "@" + testGroup;

        // Prepare test data without thread-safe classes
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> queueOffsetMap = new ConcurrentHashMap<>();
        queueOffsetMap.put(0, 100L);
        offsetTable.put(key, queueOffsetMap);

        when(consumerOffsetManager.getOffsetTable()).thenReturn(offsetTable);

        // Test processing all groups
        final boolean[] processed = {false};
        liteConsumerLagCalculator.offsetTableForEachByGroup(null, (topicGroup, offset) -> {
            processed[0] = true;
            assertEquals(lmqName, topicGroup.topic);
            assertEquals(testGroup, topicGroup.group);
            assertEquals(Long.valueOf(100L), offset);
        });
        assertTrue(processed[0]);

        // Test processing specific group
        processed[0] = false;
        liteConsumerLagCalculator.offsetTableForEachByGroup(testGroup, (topicGroup, offset) -> {
            processed[0] = true;
            assertEquals(lmqName, topicGroup.topic);
            assertEquals(testGroup, topicGroup.group);
            assertEquals(Long.valueOf(100L), offset);
        });
        assertTrue(processed[0]);

        // Test processing non-matching group
        processed[0] = false;
        liteConsumerLagCalculator.offsetTableForEachByGroup(otherGroup,
            (topicGroup, offset) -> processed[0] = true);
        assertFalse(processed[0]);
    }

    @Test
    public void testGetLagTimestampTopK_NormalCase() {
        // Prepare test data
        String group = "testGroup";
        String parentTopic = "testParentTopic";
        String lmq1 = LiteUtil.toLmqName(parentTopic, "lmq1");
        String lmq2 = LiteUtil.toLmqName(parentTopic, "lmq2");
        String lmq3 = LiteUtil.toLmqName(parentTopic, "lmq3");

        long timestamp1 = 1000L;
        long timestamp2 = 2000L;
        long timestamp3 = 1500L;

        // Consumer offsets
        long consumerOffset1 = 50L;
//        long consumerOffset2 = 30L;
        long consumerOffset3 = 40L;

        // Max offsets
        long maxOffset1 = 100L;
//        long maxOffset2 = 80L;
        long maxOffset3 = 90L;

        // Create a spy of the calculator to allow partial mocking
        LiteConsumerLagCalculator spyCalculator = spy(liteConsumerLagCalculator);

        // Add lag info to the spy calculator
        spyCalculator.updateLagInfo(group, parentTopic, lmq1, timestamp1);
        spyCalculator.updateLagInfo(group, parentTopic, lmq2, timestamp2);
        spyCalculator.updateLagInfo(group, parentTopic, lmq3, timestamp3);

        // Mock getOffset and getMaxOffset methods on the spy
        doReturn(consumerOffset1).when(spyCalculator).getOffset(group, lmq1);
//        doReturn(consumerOffset2).when(spyCalculator).getOffset(group, lmq2);
        doReturn(consumerOffset3).when(spyCalculator).getOffset(group, lmq3);

        doReturn(maxOffset1).when(spyCalculator).getMaxOffset(lmq1);
//        doReturn(maxOffset2).when(spyCalculator).getMaxOffset(lmq2);
        doReturn(maxOffset3).when(spyCalculator).getMaxOffset(lmq3);

        // Test with topK = 2
        Pair<List<LiteLagInfo>, Long> result = spyCalculator.getLagTimestampTopK(group, parentTopic, 2);

        // Verify results
        assertNotNull(result);
        assertEquals(2, result.getObject1().size());

        // Should be sorted by timestamp in ascending order
        assertEquals(timestamp1, result.getObject1().get(0).getEarliestUnconsumedTimestamp());
        assertEquals(timestamp3, result.getObject1().get(1).getEarliestUnconsumedTimestamp());

        // Verify lag counts (maxOffset - consumerOffset)
        assertEquals(maxOffset1 - consumerOffset1, result.getObject1().get(0).getLagCount());
        assertEquals(maxOffset3 - consumerOffset3, result.getObject1().get(1).getLagCount());

        // Verify lite topics
        assertEquals("lmq1", result.getObject1().get(0).getLiteTopic());
        assertEquals("lmq3", result.getObject1().get(1).getLiteTopic());

        // Verify earliest timestamp
        assertEquals(timestamp1, result.getObject2().longValue());
    }

    @Test
    public void testGetLagCountTopK_NormalCase() {
        String group = "testGroup";
        String topic = "testTopic";
        String lmqName1 = LiteUtil.toLmqName(topic, "lmq1");
        String lmqName2 = LiteUtil.toLmqName(topic, "lmq2");
        String lmqName3 = LiteUtil.toLmqName(topic, "lmq3");

        // Prepare offset table data
        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> queueOffsetMap1 = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> queueOffsetMap2 = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> queueOffsetMap3 = new ConcurrentHashMap<>();

        long consumerOffset1 = 50L;
        long consumerOffset2 = 30L;
        long consumerOffset3 = 70L;

        queueOffsetMap1.put(0, consumerOffset1);
        queueOffsetMap2.put(0, consumerOffset2);
        queueOffsetMap3.put(0, consumerOffset3);

        offsetTable.put(lmqName1 + "@" + group, queueOffsetMap1);
        offsetTable.put(lmqName2 + "@" + group, queueOffsetMap2);
        offsetTable.put(lmqName3 + "@" + group, queueOffsetMap3);

        when(consumerOffsetManager.getOffsetTable()).thenReturn(offsetTable);

        // Mock store timestamps
        long timestamp1 = 1000L;
        long timestamp2 = 2000L;
        long timestamp3 = 1500L;

        // Create a spy of the calculator to allow partial mocking
        LiteConsumerLagCalculator spyCalculator = spy(liteConsumerLagCalculator);

        // Mock getStoreTimestamp method on the spy
        doReturn(timestamp1).when(spyCalculator).getStoreTimestamp(lmqName1, consumerOffset1);
        doReturn(timestamp2).when(spyCalculator).getStoreTimestamp(lmqName2, consumerOffset2);
        doReturn(timestamp3).when(spyCalculator).getStoreTimestamp(lmqName3, consumerOffset3);

        // Mock getMaxOffset method on the spy
        doReturn(100L).when(spyCalculator).getMaxOffset(lmqName1);
        doReturn(80L).when(spyCalculator).getMaxOffset(lmqName2);
        doReturn(90L).when(spyCalculator).getMaxOffset(lmqName3);

        // Test with topK = 2
        Pair<List<LiteLagInfo>, Long> result = spyCalculator.getLagCountTopK(group, 2);

        // Verify results
        assertNotNull(result);
        assertNotNull(result.getObject1());
        assertEquals(2, result.getObject1().size());

        // Should be sorted by lag count in descending order
        // lmq1: 100-50=50, lmq2: 80-30=50, lmq3: 90-70=20
        // So order should be lmq1(50), lmq2(50) or lmq2(50), lmq1(50) (both have same lag count)
        LiteLagInfo first = result.getObject1().get(0);
        LiteLagInfo second = result.getObject1().get(1);

        // Verify lag counts
        assertEquals(50L, first.getLagCount());
        assertEquals(50L, second.getLagCount());

        // Verify lite topics
        assertTrue(first.getLiteTopic().equals("lmq1") || first.getLiteTopic().equals("lmq2"));
        assertTrue(second.getLiteTopic().equals("lmq1") || second.getLiteTopic().equals("lmq2"));

        // Verify timestamps
        assertTrue(first.getEarliestUnconsumedTimestamp() == timestamp1 || first.getEarliestUnconsumedTimestamp() == timestamp2);
        assertTrue(second.getEarliestUnconsumedTimestamp() == timestamp1 || second.getEarliestUnconsumedTimestamp() == timestamp2);

        // Verify total lag count
        assertEquals(120L, result.getObject2().longValue()); // 50 + 50 + 20
    }

    @Test
    public void testCalculateLiteLagCount() {
        brokerConfig.setLiteLagCountMetricsEnable(true);

        String group = "testGroup";
        String parentTopic = "testParentTopic";
        String lmqName = LiteUtil.toLmqName(parentTopic, "lmq1");

        ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable = new ConcurrentHashMap<>();
        ConcurrentMap<Integer, Long> queueOffsetMap = new ConcurrentHashMap<>();
        queueOffsetMap.put(0, 50L);
        offsetTable.put(lmqName + "@" + group, queueOffsetMap);

        when(consumerOffsetManager.getOffsetTable()).thenReturn(offsetTable);

        LiteConsumerLagCalculator spyCalculator = spy(liteConsumerLagCalculator);
        doReturn(100L).when(spyCalculator).getMaxOffset(lmqName);

        final ConsumerLagCalculator.CalculateLagResult[] result = {null};
        spyCalculator.calculateLiteLagCount(lagResult -> result[0] = lagResult);

        assertNotNull(result[0]);
        assertEquals(group, result[0].group);
        // The metrics of liteTopic are aggregated under its parent topic
        assertEquals(parentTopic, result[0].topic);
        assertEquals(50L, result[0].lag);
    }

    @Test
    public void testCalculateLiteLagLatency() {
        brokerConfig.setLiteLagLatencyMetricsEnable(true);

        String group = "testGroup";
        String parentTopic = "testParentTopic";
        String lmqName = LiteUtil.toLmqName(parentTopic, "lmq1");
        long storeTimestamp = System.currentTimeMillis();

        liteConsumerLagCalculator.updateLagInfo(group, parentTopic, lmqName, storeTimestamp);

        final ConsumerLagCalculator.CalculateLagResult[] result = {null};
        liteConsumerLagCalculator.calculateLiteLagLatency(lagResult -> result[0] = lagResult);

        assertNotNull(result[0]);
        assertEquals(group, result[0].group);
        // The metrics of liteTopic are aggregated under its parent topic
        assertEquals(parentTopic, result[0].topic);
        assertEquals(storeTimestamp, result[0].earliestUnconsumedTimestamp);
    }

    @Test
    public void testUpdateLagInfoWithDuplicateElements() {
        String group = "testGroup";
        String parentTopic = "testParentTopic";
        String lmqName1 = "lmq1";
        String lmqName2 = "lmq2";
        String lmqName3 = "lmq3";
        long storeTimestamp1 = 1000L;
        long storeTimestamp2 = 2000L;
        long storeTimestamp3 = 3000L;

        // Add three LMQs with different timestamps, each added three times
        for (int i = 0; i < 3; i++) {
            liteConsumerLagCalculator.updateLagInfo(group, parentTopic, lmqName1, storeTimestamp1 + i * 100);
            liteConsumerLagCalculator.updateLagInfo(group, parentTopic, lmqName2, storeTimestamp2 + i * 100);
            liteConsumerLagCalculator.updateLagInfo(group, parentTopic, lmqName3, storeTimestamp3 + i * 100);
        }

        // Verify that the heap contains exactly 3 elements
        PriorityBlockingQueue<LiteConsumerLagCalculator.LagTimeInfo> lagHeap = liteConsumerLagCalculator.topicGroupLagTimeMap
                .get(new TopicGroup(parentTopic, group));
        assertNotNull(lagHeap);
        assertEquals(3, lagHeap.size());

        // Verify that each LMQ is present with its latest timestamp
        assertTrue(lagHeap.contains(new LiteConsumerLagCalculator.LagTimeInfo(lmqName1, storeTimestamp1 + 200)));
        assertTrue(lagHeap.contains(new LiteConsumerLagCalculator.LagTimeInfo(lmqName2, storeTimestamp2 + 200)));
        assertTrue(lagHeap.contains(new LiteConsumerLagCalculator.LagTimeInfo(lmqName3, storeTimestamp3 + 200)));
    }
}
