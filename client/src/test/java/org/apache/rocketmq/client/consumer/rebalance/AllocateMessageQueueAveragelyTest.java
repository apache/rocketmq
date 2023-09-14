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

import junit.framework.TestCase;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class AllocateMessageQueueAveragelyTest extends TestCase {

    public void testAllocateMessageQueueAveragely() {
        List<String> consumerIdList = createConsumerIdList(4);
        List<MessageQueue> messageQueueList = createMessageQueueList(10);
        int[] results = new int[consumerIdList.size()];
        for (int i = 0; i < consumerIdList.size(); i++) {
            List<MessageQueue> result = new AllocateMessageQueueAveragely().allocate("", consumerIdList.get(i), messageQueueList, consumerIdList);
            results[i] = result.size();
        }
        Assert.assertArrayEquals(new int[]{3, 3, 2, 2}, results);
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
