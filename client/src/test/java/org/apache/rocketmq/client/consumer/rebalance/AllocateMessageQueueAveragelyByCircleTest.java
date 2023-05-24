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
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;

public class AllocateMessageQueueAveragelyByCircleTest extends TestCase {

    public void testAllocateMessageQueueAveragelyByCircle() {
        List<String> consumerIdList = createConsumerIdList(4);
        List<MessageQueue> messageQueueList = createMessageQueueList(10);
        // the consumerId not in cidAll
        List<MessageQueue> allocateQueues = new AllocateMessageQueueAveragelyByCircle().allocate("", "CID_PREFIX", messageQueueList, consumerIdList);
        Assert.assertEquals(0, allocateQueues.size());

        Map<String, int[]> consumerAllocateQueue = new HashMap<>(consumerIdList.size());
        for (String consumerId : consumerIdList) {
            List<MessageQueue> queues = new AllocateMessageQueueAveragelyByCircle().allocate("", consumerId, messageQueueList, consumerIdList);
            int[] queueIds = new int[queues.size()];
            for (int i = 0; i < queues.size(); i++) {
                queueIds[i] = queues.get(i).getQueueId();
            }
            consumerAllocateQueue.put(consumerId, queueIds);
        }
        Assert.assertArrayEquals(new int[] {0, 4, 8}, consumerAllocateQueue.get("CID_PREFIX0"));
        Assert.assertArrayEquals(new int[] {1, 5, 9}, consumerAllocateQueue.get("CID_PREFIX1"));
        Assert.assertArrayEquals(new int[] {2, 6}, consumerAllocateQueue.get("CID_PREFIX2"));
        Assert.assertArrayEquals(new int[] {3, 7}, consumerAllocateQueue.get("CID_PREFIX3"));
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
            MessageQueue mq = new MessageQueue("topic", "brokerName", i);
            messageQueueList.add(mq);
        }
        return messageQueueList;
    }
}

