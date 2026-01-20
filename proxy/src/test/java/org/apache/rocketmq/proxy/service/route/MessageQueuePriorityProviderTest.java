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
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MessageQueuePriorityProviderTest {

    @Test
    public void testPriorityOfWithLambda() {
        // Test functional interface implementation using lambda
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> mq.getQueueId();
        
        MessageQueue queue1 = new MessageQueue("topic", "broker", 0);
        MessageQueue queue2 = new MessageQueue("topic", "broker", 5);
        MessageQueue queue3 = new MessageQueue("topic", "broker", 10);
        
        assertEquals(0, provider.priorityOf(queue1));
        assertEquals(5, provider.priorityOf(queue2));
        assertEquals(10, provider.priorityOf(queue3));
    }

    @Test
    public void testPriorityOfWithConstantValue() {
        // Test with constant priority
        MessageQueuePriorityProvider<MessageQueue> constantProvider = mq -> 1;
        
        MessageQueue queue1 = new MessageQueue("topic1", "broker1", 0);
        MessageQueue queue2 = new MessageQueue("topic2", "broker2", 5);
        
        assertEquals(1, constantProvider.priorityOf(queue1));
        assertEquals(1, constantProvider.priorityOf(queue2));
    }

    @Test
    public void testPriorityOfBasedOnBrokerName() {
        // Test priority based on broker name hash
        MessageQueuePriorityProvider<MessageQueue> brokerProvider = 
            mq -> mq.getBrokerName().hashCode() % 10;
        
        MessageQueue queue1 = new MessageQueue("topic", "broker-a", 0);
        MessageQueue queue2 = new MessageQueue("topic", "broker-b", 0);
        
        int priority1 = brokerProvider.priorityOf(queue1);
        int priority2 = brokerProvider.priorityOf(queue2);
        
        // Priorities should be deterministic for the same broker
        assertEquals(priority1, brokerProvider.priorityOf(queue1));
        assertEquals(priority2, brokerProvider.priorityOf(queue2));
    }

    @Test
    public void testBuildPriorityGroupsWithNullList() {
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> 0;
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(null, provider);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildPriorityGroupsWithEmptyList() {
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> 0;
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(
            Collections.emptyList(), provider);
        
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testBuildPriorityGroupsWithSinglePriority() {
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> 0;
        
        List<MessageQueue> queues = Arrays.asList(
            new MessageQueue("topic", "broker1", 0),
            new MessageQueue("topic", "broker1", 1),
            new MessageQueue("topic", "broker1", 2)
        );
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).size());
    }

    @Test
    public void testBuildPriorityGroupsWithMultiplePriorities() {
        // Priority based on queue ID: 0->high, 1->medium, 2->low
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> {
            if (mq.getQueueId() < 2) return 0; // High priority
            if (mq.getQueueId() < 4) return 1; // Medium priority
            return 2; // Low priority
        };
        
        List<MessageQueue> queues = Arrays.asList(
            new MessageQueue("topic", "broker", 0), // priority 0
            new MessageQueue("topic", "broker", 1), // priority 0
            new MessageQueue("topic", "broker", 2), // priority 1
            new MessageQueue("topic", "broker", 3), // priority 1
            new MessageQueue("topic", "broker", 4), // priority 2
            new MessageQueue("topic", "broker", 5)  // priority 2
        );
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(3, result.size());
        
        // First group (highest priority 0)
        assertEquals(2, result.get(0).size());
        assertEquals(0, result.get(0).get(0).getQueueId());
        assertEquals(1, result.get(0).get(1).getQueueId());
        
        // Second group (medium priority 1)
        assertEquals(2, result.get(1).size());
        assertEquals(2, result.get(1).get(0).getQueueId());
        assertEquals(3, result.get(1).get(1).getQueueId());
        
        // Third group (low priority 2)
        assertEquals(2, result.get(2).size());
        assertEquals(4, result.get(2).get(0).getQueueId());
        assertEquals(5, result.get(2).get(1).getQueueId());
    }

    @Test
    public void testBuildPriorityGroupsOrderedByPriority() {
        // Test that groups are ordered from high to low priority (ascending numeric value)
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> mq.getQueueId();
        
        List<MessageQueue> queues = Arrays.asList(
            new MessageQueue("topic", "broker", 5),
            new MessageQueue("topic", "broker", 0),
            new MessageQueue("topic", "broker", 3),
            new MessageQueue("topic", "broker", 1)
        );
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(4, result.size());
        
        // Verify order: 0, 1, 3, 5 (ascending)
        assertEquals(0, result.get(0).get(0).getQueueId());
        assertEquals(1, result.get(1).get(0).getQueueId());
        assertEquals(3, result.get(2).get(0).getQueueId());
        assertEquals(5, result.get(3).get(0).getQueueId());
    }

    @Test
    public void testBuildPriorityGroupsWithNegativePriorities() {
        // Test with negative priority values
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> mq.getQueueId() - 5;
        
        List<MessageQueue> queues = Arrays.asList(
            new MessageQueue("topic", "broker", 0), // priority -5
            new MessageQueue("topic", "broker", 5), // priority 0
            new MessageQueue("topic", "broker", 10) // priority 5
        );
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(3, result.size());
        
        // Verify order: -5, 0, 5 (ascending)
        assertEquals(0, result.get(0).get(0).getQueueId());
        assertEquals(5, result.get(1).get(0).getQueueId());
        assertEquals(10, result.get(2).get(0).getQueueId());
    }

    @Test
    public void testBuildPriorityGroupsWithMixedBrokers() {
        // Priority based on broker name
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> {
            if (mq.getBrokerName().equals("broker-high")) return 0;
            if (mq.getBrokerName().equals("broker-medium")) return 1;
            return 2;
        };
        
        List<MessageQueue> queues = Arrays.asList(
            new MessageQueue("topic", "broker-high", 0),
            new MessageQueue("topic", "broker-low", 0),
            new MessageQueue("topic", "broker-medium", 0),
            new MessageQueue("topic", "broker-high", 1),
            new MessageQueue("topic", "broker-medium", 1)
        );
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(3, result.size());
        
        // High priority group
        assertEquals(2, result.get(0).size());
        assertEquals("broker-high", result.get(0).get(0).getBrokerName());
        assertEquals("broker-high", result.get(0).get(1).getBrokerName());
        
        // Medium priority group
        assertEquals(2, result.get(1).size());
        assertEquals("broker-medium", result.get(1).get(0).getBrokerName());
        
        // Low priority group
        assertEquals(1, result.get(2).size());
        assertEquals("broker-low", result.get(2).get(0).getBrokerName());
    }

    @Test
    public void testBuildPriorityGroupsPreservesQueueOrder() {
        // Test that queues with same priority maintain their relative order
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> 0;
        
        List<MessageQueue> queues = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            queues.add(new MessageQueue("topic", "broker", i));
        }
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(10, result.get(0).size());
        
        // Verify order is maintained
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result.get(0).get(i).getQueueId());
        }
    }

    @Test
    public void testBuildPriorityGroupsWithCustomMessageQueue() {
        // Test with extended MessageQueue type
        class CustomMessageQueue extends MessageQueue {
            private int customPriority;
            
            public CustomMessageQueue(String topic, String brokerName, int queueId, int customPriority) {
                super(topic, brokerName, queueId);
                this.customPriority = customPriority;
            }
            
            public int getCustomPriority() {
                return customPriority;
            }
        }
        
        MessageQueuePriorityProvider<CustomMessageQueue> provider = 
            CustomMessageQueue::getCustomPriority;
        
        List<CustomMessageQueue> queues = Arrays.asList(
            new CustomMessageQueue("topic", "broker", 0, 2),
            new CustomMessageQueue("topic", "broker", 1, 0),
            new CustomMessageQueue("topic", "broker", 2, 1)
        );
        
        List<List<CustomMessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(3, result.size());
        
        // Verify order by custom priority: 0, 1, 2
        assertEquals(0, result.get(0).get(0).getCustomPriority());
        assertEquals(1, result.get(1).get(0).getCustomPriority());
        assertEquals(2, result.get(2).get(0).getCustomPriority());
    }

    @Test
    public void testBuildPriorityGroupsWithLargeNumberOfQueues() {
        // Test with large number of queues
        MessageQueuePriorityProvider<MessageQueue> provider = mq -> mq.getQueueId() % 5;
        
        List<MessageQueue> queues = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            queues.add(new MessageQueue("topic", "broker", i));
        }
        
        List<List<MessageQueue>> result = MessageQueuePriorityProvider.buildPriorityGroups(queues, provider);
        
        assertNotNull(result);
        assertEquals(5, result.size()); // 5 different priorities (0-4)
        
        // Each group should have 20 queues (100 / 5)
        for (List<MessageQueue> group : result) {
            assertEquals(20, group.size());
        }
    }
}
