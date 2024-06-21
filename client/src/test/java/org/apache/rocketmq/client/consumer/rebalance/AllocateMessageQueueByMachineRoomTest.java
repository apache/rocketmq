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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;

public class AllocateMessageQueueByMachineRoomTest extends TestCase {

    public void testAllocateMessageQueueByMachineRoom() {
        List<String> consumerIdList = createConsumerIdList(2);
        List<MessageQueue> messageQueueList = createMessageQueueList(10);
        Set<String> consumeridcs = new HashSet<>();
        consumeridcs.add("room1");
        AllocateMessageQueueByMachineRoom allocateStrategy = new AllocateMessageQueueByMachineRoom();
        allocateStrategy.setConsumeridcs(consumeridcs);

        // mqAll is null or mqAll empty
        try {
            allocateStrategy.allocate("", consumerIdList.get(0), new ArrayList<>(), consumerIdList);
        } catch (Exception e) {
            assert e instanceof IllegalArgumentException;
            Assert.assertEquals("mqAll is null or mqAll empty", e.getMessage());
        }

        Map<String, int[]> consumerAllocateQueue = new HashMap<>(consumerIdList.size());
        for (String consumerId : consumerIdList) {
            List<MessageQueue> queues = allocateStrategy.allocate("", consumerId, messageQueueList, consumerIdList);
            int[] queueIds = new int[queues.size()];
            for (int i = 0; i < queues.size(); i++) {
                queueIds[i] = queues.get(i).getQueueId();
            }
            consumerAllocateQueue.put(consumerId, queueIds);
        }
        Assert.assertArrayEquals(new int[] {0, 1, 4}, consumerAllocateQueue.get("CID_PREFIX0"));
        Assert.assertArrayEquals(new int[] {2, 3}, consumerAllocateQueue.get("CID_PREFIX1"));
    }

    private List<String> createConsumerIdList(int size) {
        List<String> consumerIdList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            consumerIdList.add("CID_PREFIX" + i);
        }
        return consumerIdList;
    }

    private List<MessageQueue> createMessageQueueList(int size) {
        List<MessageQueue> messageQueueList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            MessageQueue mq;
            if (i < size / 2) {
                mq = new MessageQueue("topic", "room1@broker-a", i);
            } else {
                mq = new MessageQueue("topic", "room2@broker-b", i);
            }
            messageQueueList.add(mq);
        }
        return messageQueueList;
    }
}

