/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * @author yubao.fyb@taoboa.com
 * @version $id$
 */
package com.alibaba.rocketmq.client.consumer.loadbalance;

import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragelyByCircle;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


/**
 * @author yubao.fyb@alibaba-inc.com created on 2013-07-03 16:24
 */
public class AllocateMessageQueueAveragelyTest {
    private AllocateMessageQueueStrategy allocateMessageQueueAveragely;
    private String currentCID;
    private String topic;
    private List<MessageQueue> messageQueueList;
    private List<String> consumerIdList;

    @Before
    public void init() {
        allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        topic = "topic_test";
    }

    @Test
    public void testConsumer1() {
        currentCID = "0";
        createConsumerIdList(1);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer1");
        Assert.assertEquals(result.size(), 5);
        Assert.assertEquals(result.containsAll(getMessageQueueList()), true);
    }

    public void createConsumerIdList(int size) {
        consumerIdList = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            consumerIdList.add(String.valueOf(i));
        }
    }

    public void createMessageQueueList(int size) {
        messageQueueList = new ArrayList<MessageQueue>(size);
        for (int i = 0; i < size; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            messageQueueList.add(mq);
        }
    }

    public void printMessageQueue(List<MessageQueue> messageQueueList, String name) {
        if (messageQueueList == null || messageQueueList.size() < 1)
            return;
        System.out.println(name + ".......................................start");
        for (MessageQueue messageQueue : messageQueueList) {
            System.out.println(messageQueue);
        }
        System.out.println(name + ".......................................end");
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    @Test
    public void testConsumer2() {
        currentCID = "1";
        createConsumerIdList(2);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer2");
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(2, 5)), true);

    }

    @Test
    public void testConsumer3CurrentCID0() {
        currentCID = "0";
        createConsumerIdList(3);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer3CurrentCID0");
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(0, 1)), true);
    }

    @Test
    public void testConsumer3CurrentCID1() {
        currentCID = "1";
        createConsumerIdList(3);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer3CurrentCID1");
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(1, 2)), true);
    }

    @Test
    public void testConsumer3CurrentCID2() {
        currentCID = "2";
        createConsumerIdList(3);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer3CurrentCID2");
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(2, 5)), true);
    }

    @Test
    public void testConsumer4() {
        currentCID = "1";
        createConsumerIdList(4);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer4");
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(1, 2)), true);
    }

    @Test
    public void testConsumer5() {
        currentCID = "1";
        createConsumerIdList(5);
        createMessageQueueList(5);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer5");
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(1, 2)), true);
    }

    @Test
    public void testConsumer6() {
        currentCID = "1";
        createConsumerIdList(2);
        createMessageQueueList(6);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testConsumer");
        Assert.assertEquals(result.size(), 3);
        Assert.assertEquals(result.containsAll(getMessageQueueList().subList(3, 6)), true);
    }

    @Test
    public void testCurrentCIDNotExists() {
        currentCID = String.valueOf(Integer.MAX_VALUE);
        createConsumerIdList(2);
        createMessageQueueList(6);
        List<MessageQueue> result =
                allocateMessageQueueAveragely.allocate("", currentCID, messageQueueList, consumerIdList);
        printMessageQueue(result, "testCurrentCIDNotExists");
        Assert.assertEquals(result.size(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCurrentCIDIllegalArgument() {
        createConsumerIdList(2);
        createMessageQueueList(6);
        allocateMessageQueueAveragely.allocate("", "", getMessageQueueList(), getConsumerIdList());
    }

    public List<String> getConsumerIdList() {
        return consumerIdList;
    }

    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMessageQueueIllegalArgument() {
        currentCID = "0";
        createConsumerIdList(2);
        allocateMessageQueueAveragely.allocate("", currentCID, null, getConsumerIdList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConsumerIdIllegalArgument() {
        currentCID = "0";
        createMessageQueueList(6);
        allocateMessageQueueAveragely.allocate("", currentCID, getMessageQueueList(), null);
    }

    @Test
    public void testAllocate() {
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        String topic = "topic_test";
        String currentCID = "CID";
        int queueSize = 19;
        int consumerSize = 10;
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        for (int i = 0; i < queueSize; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            mqAll.add(mq);
        }

        List<String> cidAll = new ArrayList<String>();
        for (int j = 0; j < consumerSize; j++) {
            cidAll.add("CID" + j);
        }
        System.out.println(mqAll.toString());
        System.out.println(cidAll.toString());
        for (int i = 0; i < consumerSize; i++) {
            List<MessageQueue> rs = allocateMessageQueueAveragely.allocate("", currentCID + i, mqAll, cidAll);
            System.out.println("rs[" + currentCID + i + "]:" + rs.toString());
        }
    }


    @Test
    public void testAllocateByCircle() {
        AllocateMessageQueueAveragelyByCircle circle = new AllocateMessageQueueAveragelyByCircle();
        String topic = "topic_test";
        String currentCID = "CID";
        int consumerSize = 3;
        int queueSize = 13;
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        for (int i = 0; i < queueSize; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            mqAll.add(mq);
        }

        List<String> cidAll = new ArrayList<String>();
        for (int j = 0; j < consumerSize; j++) {
            cidAll.add("CID" + j);
        }
        System.out.println(mqAll.toString());
        System.out.println(cidAll.toString());
        for (int i = 0; i < consumerSize; i++) {
            List<MessageQueue> rs = circle.allocate("", currentCID + i, mqAll, cidAll);
            System.out.println("rs[" + currentCID + i + "]:" + rs.toString());
        }
    }
}
