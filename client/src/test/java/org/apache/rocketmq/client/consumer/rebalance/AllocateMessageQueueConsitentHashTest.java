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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AllocateMessageQueueConsitentHashTest {

    private String topic;
    private static final String CID_PREFIX = "CID-";

    @Before
    public void init() {
        topic = "topic_test";
    }

    @Test
    public void testCurrentCIDNotExists() {
        String currentCID = String.valueOf(Integer.MAX_VALUE);
        List<String> consumerIdList = createConsumerIdList(2);
        List<MessageQueue> messageQueueList = createMessageQueueList(6);
        List<MessageQueue> result = new AllocateMessageQueueConsistentHash().allocate("", currentCID, messageQueueList, consumerIdList);
        Assert.assertEquals(result.size(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCurrentCIDIllegalArgument() {
        List<String> consumerIdList = createConsumerIdList(2);
        List<MessageQueue> messageQueueList = createMessageQueueList(6);
        new AllocateMessageQueueConsistentHash().allocate("", "", messageQueueList, consumerIdList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMessageQueueIllegalArgument() {
        String currentCID = "0";
        List<String> consumerIdList = createConsumerIdList(2);
        new AllocateMessageQueueConsistentHash().allocate("", currentCID, null, consumerIdList);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConsumerIdIllegalArgument() {
        String currentCID = "0";
        List<MessageQueue> messageQueueList = createMessageQueueList(6);
        new AllocateMessageQueueConsistentHash().allocate("", currentCID, messageQueueList, null);
    }

    @Test
    public void testAllocate1() {
        testAllocate(20, 10);
    }

    @Test
    public void testAllocate2() {
        testAllocate(10, 20);
    }

    @Test
    public void testRun100RandomCase() {
        for (int i = 0; i < 10; i++) {
            int consumerSize = new Random().nextInt(20) + 1;//1-20
            int queueSize = new Random().nextInt(20) + 1;//1-20
            testAllocate(queueSize, consumerSize);
            try {
                Awaitility.await().pollDelay(Duration.ofMillis(1)).until(()->true);
            } catch (Exception e) {
            }
        }
    }

    public void testAllocate(int queueSize, int consumerSize) {
        AllocateMessageQueueStrategy allocateMessageQueueConsistentHash = new AllocateMessageQueueConsistentHash(3);

        List<MessageQueue> mqAll = createMessageQueueList(queueSize);

        List<String> cidAll = createConsumerIdList(consumerSize);
        List<MessageQueue> allocatedResAll = new ArrayList<>();

        Map<MessageQueue, String> allocateToAllOrigin = new TreeMap<>();
        //test allocate all
        {

            List<String> cidBegin = new ArrayList<>(cidAll);

            for (String cid : cidBegin) {
                List<MessageQueue> rs = allocateMessageQueueConsistentHash.allocate("testConsumerGroup", cid, mqAll, cidBegin);
                for (MessageQueue mq : rs) {
                    allocateToAllOrigin.put(mq, cid);
                }
                allocatedResAll.addAll(rs);
            }

            Assert.assertTrue(
                verifyAllocateAll(cidBegin, mqAll, allocatedResAll));
        }

        Map<MessageQueue, String> allocateToAllAfterRemoveOne = new TreeMap<>();
        List<String> cidAfterRemoveOne = new ArrayList<>(cidAll);
        //test allocate remove one cid
        {
            String removeCID = cidAfterRemoveOne.remove(0);
            List<MessageQueue> mqShouldOnlyChanged = new ArrayList<>();
            Iterator<Map.Entry<MessageQueue, String>> it = allocateToAllOrigin.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, String> entry = it.next();
                if (entry.getValue().equals(removeCID)) {
                    mqShouldOnlyChanged.add(entry.getKey());
                }
            }

            List<MessageQueue> allocatedResAllAfterRemove = new ArrayList<>();
            for (String cid : cidAfterRemoveOne) {
                List<MessageQueue> rs = allocateMessageQueueConsistentHash.allocate("testConsumerGroup", cid, mqAll, cidAfterRemoveOne);
                allocatedResAllAfterRemove.addAll(rs);
                for (MessageQueue mq : rs) {
                    allocateToAllAfterRemoveOne.put(mq, cid);
                }
            }

            Assert.assertTrue("queueSize" + queueSize + "consumerSize:" + consumerSize + "\nmqAll:" + mqAll + "\nallocatedResAllAfterRemove" + allocatedResAllAfterRemove,
                verifyAllocateAll(cidAfterRemoveOne, mqAll, allocatedResAllAfterRemove));
            verifyAfterRemove(allocateToAllOrigin, allocateToAllAfterRemoveOne, removeCID);
        }

        List<String> cidAfterAdd = new ArrayList<>(cidAfterRemoveOne);
        //test allocate add one more cid
        {
            String newCid = CID_PREFIX + "NEW";
            cidAfterAdd.add(newCid);
            List<MessageQueue> mqShouldOnlyChanged = new ArrayList<>();
            List<MessageQueue> allocatedResAllAfterAdd = new ArrayList<>();
            Map<MessageQueue, String> allocateToAll3 = new TreeMap<>();
            for (String cid : cidAfterAdd) {
                List<MessageQueue> rs = allocateMessageQueueConsistentHash.allocate("testConsumerGroup", cid, mqAll, cidAfterAdd);
                allocatedResAllAfterAdd.addAll(rs);
                for (MessageQueue mq : rs) {
                    allocateToAll3.put(mq, cid);
                    if (cid.equals(newCid)) {
                        mqShouldOnlyChanged.add(mq);
                    }
                }
            }

            Assert.assertTrue(
                verifyAllocateAll(cidAfterAdd, mqAll, allocatedResAllAfterAdd));
            verifyAfterAdd(allocateToAllAfterRemoveOne, allocateToAll3, newCid);
        }
    }

    private boolean verifyAllocateAll(List<String> cidAll, List<MessageQueue> mqAll,
        List<MessageQueue> allocatedResAll) {
        if (cidAll.isEmpty()) {
            return allocatedResAll.isEmpty();
        }
        return mqAll.containsAll(allocatedResAll) && allocatedResAll.containsAll(mqAll);
    }

    private void verifyAfterRemove(Map<MessageQueue, String> allocateToBefore, Map<MessageQueue, String> allocateAfter,
        String removeCID) {
        for (MessageQueue mq : allocateToBefore.keySet()) {
            String allocateToOrigin = allocateToBefore.get(mq);
            if (allocateToOrigin.equals(removeCID)) {

            } else { //the rest queue should be the same
                Assert.assertTrue(allocateAfter.get(mq).equals(allocateToOrigin));//should be the same
            }
        }
    }

    private void verifyAfterAdd(Map<MessageQueue, String> allocateBefore, Map<MessageQueue, String> allocateAfter,
        String newCID) {
        for (MessageQueue mq : allocateAfter.keySet()) {
            String allocateToOrigin = allocateBefore.get(mq);
            String allocateToAfter = allocateAfter.get(mq);
            if (allocateToAfter.equals(newCID)) {

            } else { //the rest queue should be the same
                Assert.assertTrue("it was allocated to " + allocateToOrigin + ". Now, it is to " + allocateAfter.get(mq) + " mq:" + mq, allocateAfter.get(mq).equals(allocateToOrigin));//should be the same
            }
        }
    }

    private List<String> createConsumerIdList(int size) {
        List<String> consumerIdList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            consumerIdList.add(CID_PREFIX + String.valueOf(i));
        }
        return consumerIdList;
    }

    private List<MessageQueue> createMessageQueueList(int size) {
        List<MessageQueue> messageQueueList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            MessageQueue mq = new MessageQueue(topic, "brokerName", i);
            messageQueueList.add(mq);
        }
        return messageQueueList;
    }
}
