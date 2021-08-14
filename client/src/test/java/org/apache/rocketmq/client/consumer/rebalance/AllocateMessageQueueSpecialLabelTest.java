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


import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class AllocateMessageQueueSpecialLabelTest {

    private String topic;
    private String consumerGroup;
    private static final String CID_PREFIX = "CID-";

    @Before
    public void init() {
        topic = "topic_test";
        consumerGroup = "consumerGroupId";
    }

    @Test
    public void testAllocateCase1() {
        int specialLabelSize = 2;
        int allConsumerSize = 6;
        int messageQueueSizePerBroker = 16;
        int brokerCount = 3;
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount);
    }

    @Test
    public void testAllocateCase2() {
        int specialLabelSize = 0;
        int allConsumerSize = 6;
        int messageQueueSizePerBroker = 16;
        int brokerCount = 3;
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount);
    }

    @Test
    public void testAllocateCase3() {
        int specialLabelSize = 2;
        int allConsumerSize = 6;
        int messageQueueSizePerBroker = 16;
        int brokerCount = 1;
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount);
    }

    @Test
    public void testAllocateCase4() {
        int specialLabelSize = 2;
        int allConsumerSize = 6;
        int messageQueueSizePerBroker = 16;
        int brokerCount = 3;
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount, 0);
    }

    @Test
    public void testAllocateCase5() {
        int specialLabelSize = 2;
        int allConsumerSize = 6;
        int messageQueueSizePerBroker = 16;
        int brokerCount = 3;
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCurrentCIDIllegalArgument() {
        List<String> consumerIdList = createConsumerIdList(2, 0);
        List<MessageQueue> messageQueueList = createMessageQueueList(6, 1);
        new AllocateMessageQueueSpecialLabel(2.0).allocate(consumerGroup, CID_PREFIX + "currentCID", messageQueueList, consumerIdList);
    }

    private void testAllocate(int specialLabelSize, int allConsumerSize, int messageQueueSizePerBroker, int brokerCount, double percentage) {
        AllocateMessageQueueSpecialLabel allocateMessageQueueSpecialLabel = new AllocateMessageQueueSpecialLabel(percentage);
        List<String> consumerIdList = createConsumerIdList(allConsumerSize - specialLabelSize, specialLabelSize);
        List<MessageQueue> messageQueueList = createMessageQueueList(messageQueueSizePerBroker, brokerCount);

        List<MessageQueue> messageQueueListAllocated = new ArrayList<>();
        consumerIdList.forEach(currentCID -> {
            List<MessageQueue> messageQueues = allocateMessageQueueSpecialLabel.allocate(consumerGroup, currentCID, messageQueueList, new ArrayList<>(consumerIdList));
            if (messageQueues != null && messageQueues.size() != 0) {
                messageQueues.forEach(messageQueue -> {
                    assert messageQueueListAllocated.contains(messageQueue);
                });
                messageQueueListAllocated.addAll(messageQueues);
            }
        });

        assert messageQueueList.size() == messageQueueListAllocated.size();


    }

    private void testAllocate(int specialLabelSize, int allConsumerSize, int messageQueueSizePerBroker, int brokerCount) {
        testAllocate(specialLabelSize, allConsumerSize, messageQueueSizePerBroker, brokerCount, 0.2);
    }

    public List<String> createConsumerIdList(int size, int specialLabel) {
        List<String> consumerIdList = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            consumerIdList.add(CID_PREFIX + String.valueOf(i));
        }
        for (int i = 0; i < specialLabel; i++) {
            consumerIdList.add(CID_PREFIX + String.valueOf(i) + System.getProperty("rocketmq.consumer.label", "%GRAY%"));
        }
        return consumerIdList;
    }


    public List<MessageQueue> createMessageQueueList(int size, int brokerCount) {
        if (brokerCount < 0 || brokerCount > 20) {
            brokerCount = 2;
        }
        List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>(size);
        for (int i = 0; i < brokerCount; i++) {
            for (int j = 0; j < size; j++) {
                MessageQueue mq = new MessageQueue(topic, "brokerName-" + i, j);
                messageQueueList.add(mq);
            }
        }
        return messageQueueList;
    }

    public static void main(String[] args) {
        System.out.println(new AllocateMessageQueueSpecialLabelTest().createConsumerIdList(16, 2));
    }

}
