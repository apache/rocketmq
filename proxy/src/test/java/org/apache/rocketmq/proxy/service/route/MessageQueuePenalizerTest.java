/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.route;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageQueuePenalizerTest {

    /**
     * Test evaluatePenalty with null messageQueue should throw NullPointerException
     */
    @Test(expected = NullPointerException.class)
    public void testEvaluatePenalty_NullMessageQueue() {
        List<MessageQueuePenalizer<MessageQueue>> penalizers = new ArrayList<>();
        penalizers.add(mq -> 10);
        MessageQueuePenalizer.evaluatePenalty(null, penalizers);
    }

    /**
     * Test evaluatePenalty with null penalizers should return 0
     */
    @Test
    public void testEvaluatePenalty_NullPenalizers() {
        MessageQueue mq = new MessageQueue("topic", "broker", 0);
        int penalty = MessageQueuePenalizer.evaluatePenalty(mq, null);
        assertEquals(0, penalty);
    }

    /**
     * Test evaluatePenalty with empty penalizers should return 0
     */
    @Test
    public void testEvaluatePenalty_EmptyPenalizers() {
        MessageQueue mq = new MessageQueue("topic", "broker", 0);
        int penalty = MessageQueuePenalizer.evaluatePenalty(mq, Collections.emptyList());
        assertEquals(0, penalty);
    }

    /**
     * Test evaluatePenalty aggregates penalties from multiple penalizers by summing them up
     */
    @Test
    public void testEvaluatePenalty_MultiplePenalizers() {
        MessageQueue mq = new MessageQueue("topic", "broker", 0);
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Arrays.asList(
            q -> 10,
            q -> 20,
            q -> 5
        );
        int penalty = MessageQueuePenalizer.evaluatePenalty(mq, penalizers);
        assertEquals(35, penalty);
    }

    /**
     * Test evaluatePenalty with negative penalties (sum should still work)
     */
    @Test
    public void testEvaluatePenalty_NegativePenalties() {
        MessageQueue mq = new MessageQueue("topic", "broker", 0);
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Arrays.asList(
            q -> -5,
            q -> 10,
            q -> -3
        );
        int penalty = MessageQueuePenalizer.evaluatePenalty(mq, penalizers);
        assertEquals(2, penalty);
    }

    /**
     * Test selectLeastPenalty with null queues should return null
     */
    @Test
    public void testSelectLeastPenalty_NullQueues() {
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);
        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(null, penalizers, startIndex);
        assertNull(result);
    }

    /**
     * Test selectLeastPenalty with empty queues should return null
     */
    @Test
    public void testSelectLeastPenalty_EmptyQueues() {
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);
        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(
            Collections.emptyList(), penalizers, startIndex);
        assertNull(result);
    }

    /**
     * Test selectLeastPenalty selects the queue with the lowest penalty
     */
    @Test
    public void testSelectLeastPenalty_LowestPenalty() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        // Penalizer that assigns different penalties based on queue id
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getQueueId() == 0 ? 50 : (mq.getQueueId() == 1 ? 10 : 30)
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq1, result.getLeft());
        assertEquals(10, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenalty short-circuits when penalty <= 0
     */
    @Test
    public void testSelectLeastPenalty_ShortCircuitZeroPenalty() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        // mq1 has penalty 0, should short-circuit
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getQueueId() == 0 ? 50 : (mq.getQueueId() == 1 ? 0 : 30)
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq1, result.getLeft());
        assertEquals(0, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenalty short-circuits when penalty is negative
     */
    @Test
    public void testSelectLeastPenalty_ShortCircuitNegativePenalty() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        // mq1 has penalty -5, should short-circuit
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getQueueId() == 0 ? 50 : (mq.getQueueId() == 1 ? -5 : 30)
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq1, result.getLeft());
        assertEquals(-5, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenalty with round-robin behavior (rotating start index)
     * Verifies that startIndex affects the iteration order
     */
    @Test
    public void testSelectLeastPenalty_RoundRobinStartIndex() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        // All queues have penalty 0, so whichever is encountered first will be returned
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 0);

        // Starting from index 0
        AtomicInteger startIndex1 = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result1 = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex1);
        assertNotNull(result1);
        assertEquals(mq0, result1.getLeft());

        // Starting from index 1
        AtomicInteger startIndex2 = new AtomicInteger(1);
        Pair<MessageQueue, Integer> result2 = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex2);
        assertNotNull(result2);
        assertEquals(mq1, result2.getLeft());

        // Starting from index 2
        AtomicInteger startIndex3 = new AtomicInteger(2);
        Pair<MessageQueue, Integer> result3 = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex3);
        assertNotNull(result3);
        assertEquals(mq2, result3.getLeft());
    }

    /**
     * Test selectLeastPenalty increments startIndex for each iteration
     */
    @Test
    public void testSelectLeastPenalty_IncrementStartIndex() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);

        AtomicInteger startIndex = new AtomicInteger(0);
        MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex);

        // After iterating through 3 queues, startIndex should be incremented 3 times
        assertEquals(3, startIndex.get());
    }

    /**
     * Test selectLeastPenalty handles startIndex wrapping with Math.floorMod
     */
    @Test
    public void testSelectLeastPenalty_StartIndexWrapping() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        MessageQueue mq2 = new MessageQueue("topic", "broker", 2);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1, mq2);

        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 0);

        // Start with large index to test wrapping
        AtomicInteger startIndex = new AtomicInteger(100);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenalty(queues, penalizers, startIndex);

        assertNotNull(result);
        // 100 % 3 = 1, so should start from mq1
        assertEquals(mq1, result.getLeft());
    }

    /**
     * Test selectLeastPenaltyWithPriority with null queuesWithPriority should return null
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_NullQueues() {
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);
        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            null, penalizers, startIndex);
        assertNull(result);
    }

    /**
     * Test selectLeastPenaltyWithPriority with empty queuesWithPriority should return null
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_EmptyQueues() {
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);
        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            Collections.emptyList(), penalizers, startIndex);
        assertNull(result);
    }

    /**
     * Test selectLeastPenaltyWithPriority with single priority group delegates to selectLeastPenalty
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_SinglePriorityGroup() {
        MessageQueue mq0 = new MessageQueue("topic", "broker", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker", 1);
        List<MessageQueue> queues = Arrays.asList(mq0, mq1);

        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getQueueId() == 0 ? 20 : 10
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            Collections.singletonList(queues), penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq1, result.getLeft());
        assertEquals(10, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenaltyWithPriority selects queue with lowest penalty across multiple priority groups
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_MultiplePriorityGroups() {
        // Priority group 1 (higher priority)
        MessageQueue mq0 = new MessageQueue("topic", "broker-high", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker-high", 1);
        List<MessageQueue> highPriorityQueues = Arrays.asList(mq0, mq1);

        // Priority group 2 (lower priority)
        MessageQueue mq2 = new MessageQueue("topic", "broker-low", 0);
        MessageQueue mq3 = new MessageQueue("topic", "broker-low", 1);
        List<MessageQueue> lowPriorityQueues = Arrays.asList(mq2, mq3);

        List<List<MessageQueue>> queuesWithPriority = Arrays.asList(highPriorityQueues, lowPriorityQueues);

        // Assign penalties: high-priority queues have higher penalties, low-priority have lower
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getBrokerName().equals("broker-high") ? 50 : 10
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            queuesWithPriority, penalizers, startIndex);

        assertNotNull(result);
        // Should select from low-priority group because it has lower penalty
        assertTrue(result.getLeft().getBrokerName().equals("broker-low"));
        assertEquals(10, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenaltyWithPriority short-circuits when a priority group yields penalty <= 0
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_ShortCircuitZeroPenalty() {
        // Priority group 1
        MessageQueue mq0 = new MessageQueue("topic", "broker-high", 0);
        List<MessageQueue> highPriorityQueues = Collections.singletonList(mq0);

        // Priority group 2
        MessageQueue mq1 = new MessageQueue("topic", "broker-low", 0);
        List<MessageQueue> lowPriorityQueues = Collections.singletonList(mq1);

        List<List<MessageQueue>> queuesWithPriority = Arrays.asList(highPriorityQueues, lowPriorityQueues);

        // First group has penalty 0, should short-circuit
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getBrokerName().equals("broker-high") ? 0 : 100
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            queuesWithPriority, penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq0, result.getLeft());
        assertEquals(0, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenaltyWithPriority when first group encounters zero penalty during iteration
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_FirstGroupHasZeroPenalty() {
        // Priority group 1
        MessageQueue mq0 = new MessageQueue("topic", "broker1", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker1", 1);
        List<MessageQueue> group1 = Arrays.asList(mq0, mq1);

        // Priority group 2
        MessageQueue mq2 = new MessageQueue("topic", "broker2", 0);
        List<MessageQueue> group2 = Collections.singletonList(mq2);

        List<List<MessageQueue>> queuesWithPriority = Arrays.asList(group1, group2);

        // mq1 in first group has penalty 0
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(
            mq -> mq.getQueueId() == 1 && mq.getBrokerName().equals("broker1") ? 0 : 50
        );

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            queuesWithPriority, penalizers, startIndex);

        assertNotNull(result);
        assertEquals(mq1, result.getLeft());
        assertEquals(0, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenaltyWithPriority returns first encountered minimum when multiple groups have same minimum penalty
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_SameMinimumPenalty() {
        // Priority group 1
        MessageQueue mq0 = new MessageQueue("topic", "broker1", 0);
        List<MessageQueue> group1 = Collections.singletonList(mq0);

        // Priority group 2
        MessageQueue mq1 = new MessageQueue("topic", "broker2", 0);
        List<MessageQueue> group2 = Collections.singletonList(mq1);

        // Priority group 3
        MessageQueue mq2 = new MessageQueue("topic", "broker3", 0);
        List<MessageQueue> group3 = Collections.singletonList(mq2);

        List<List<MessageQueue>> queuesWithPriority = Arrays.asList(group1, group2, group3);

        // All have same penalty
        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> 10);

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            queuesWithPriority, penalizers, startIndex);

        assertNotNull(result);
        // Should return first encountered (from group1)
        assertEquals(mq0, result.getLeft());
        assertEquals(10, result.getRight().intValue());
    }

    /**
     * Test selectLeastPenaltyWithPriority with complex scenario:
     * Multiple priority groups with varying penalties
     */
    @Test
    public void testSelectLeastPenaltyWithPriority_ComplexScenario() {
        // Priority group 1: penalties 100, 90
        MessageQueue mq0 = new MessageQueue("topic", "broker1", 0);
        MessageQueue mq1 = new MessageQueue("topic", "broker1", 1);
        List<MessageQueue> group1 = Arrays.asList(mq0, mq1);

        // Priority group 2: penalties 50, 30
        MessageQueue mq2 = new MessageQueue("topic", "broker2", 0);
        MessageQueue mq3 = new MessageQueue("topic", "broker2", 1);
        List<MessageQueue> group2 = Arrays.asList(mq2, mq3);

        // Priority group 3: penalties 80, 20
        MessageQueue mq4 = new MessageQueue("topic", "broker3", 0);
        MessageQueue mq5 = new MessageQueue("topic", "broker3", 1);
        List<MessageQueue> group3 = Arrays.asList(mq4, mq5);

        List<List<MessageQueue>> queuesWithPriority = Arrays.asList(group1, group2, group3);

        List<MessageQueuePenalizer<MessageQueue>> penalizers = Collections.singletonList(mq -> {
            if (mq.getBrokerName().equals("broker1")) {
                return mq.getQueueId() == 0 ? 100 : 90;
            } else if (mq.getBrokerName().equals("broker2")) {
                return mq.getQueueId() == 0 ? 50 : 30;
            } else {
                return mq.getQueueId() == 0 ? 80 : 20;
            }
        });

        AtomicInteger startIndex = new AtomicInteger(0);
        Pair<MessageQueue, Integer> result = MessageQueuePenalizer.selectLeastPenaltyWithPriority(
            queuesWithPriority, penalizers, startIndex);

        assertNotNull(result);
        // Should select mq5 from group3 with penalty 20 (the global minimum)
        assertEquals(mq5, result.getLeft());
        assertEquals(20, result.getRight().intValue());
    }
}
