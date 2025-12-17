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

package org.apache.rocketmq.test.client.consumer.pop;

import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.common.SubscriptionGroupAttributes.PRIORITY_FACTOR_ATTRIBUTE;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class PopPriorityIT extends BasePopNormally {

    private final boolean popConsumerKVServiceEnable;
    private final boolean priorityOrderAsc;
    private int writeQueueNum = 8;

    public PopPriorityIT(boolean popConsumerKVServiceEnable, boolean priorityOrderAsc) {
        this.popConsumerKVServiceEnable = popConsumerKVServiceEnable;
        this.priorityOrderAsc = priorityOrderAsc;
    }

    @Parameterized.Parameters
    public static List<Object[]> params() {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[] {false, true});
        result.add(new Object[] {false, false});
        result.add(new Object[] {true, true});
        result.add(new Object[] {true, false});
        return result;
    }

    @Before
    public void setUp() {
        super.setUp();
        // reset default config if changed
        writeQueueNum = 8;
        brokerController1.getBrokerConfig().setPopFromRetryProbabilityForPriority(0);
        brokerController1.getBrokerConfig().setUseSeparateRetryQueue(false);
        brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(popConsumerKVServiceEnable);
        brokerController1.getBrokerConfig().setPriorityOrderAsc(priorityOrderAsc);
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, writeQueueNum, CQType.SimpleCQ, TopicMessageType.PRIORITY);
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void test_normal_send() {
        int priority = -1; // normal message
        Set<Integer> queueIdSet = new HashSet<>();
        for (int i = 0; i < 32; i++) {
            Message message = mockMessage(topic, priority, "");
            SendResult sendResult = producer.send(message, null).getSendResultObj();
            queueIdSet.add(sendResult.getMessageQueue().getQueueId());
        }
        assertTrue(queueIdSet.size() > 1);
    }

    @Test
    public void test_priority_send() {
        final int priority = 0; // priority message
        for (int i = 0; i < 32; i++) {
            Message message = mockMessage(topic, priority, "");
            SendResult sendResult = producer.send(message, null).getSendResultObj();
            assertEquals(priority, sendResult.getMessageQueue().getQueueId());
        }
    }

    @Test
    public void test_priority_consume_always_high_priority() throws Exception {
        int msgNumPerQueue = 20;
        final int maxPriority = priorityOrderAsc ? writeQueueNum - 1 : 0;
        for (int i = 0; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            for (int j = 0; j < msgNumPerQueue; j++) {
                producer.send(message);
            }
        }
        Assert.assertTrue(awaitDispatchMs(2000));
        for (int i = 0; i < msgNumPerQueue; i++) {
            PopResult popResult = popMessageAsync(Duration.ofSeconds(600).toMillis(), 1, 30000).get();
            TestUtil.waitForMonment(20); // wait lock release
            assertEquals(PopStatus.FOUND, popResult.getPopStatus());
            MessageExt message = popResult.getMsgFoundList().get(0);
            assertEquals(maxPriority, message.getPriority()); // not a coincidence
        }
    }

    @Test
    public void test_priority_consume_from_high_to_low() throws Exception {
        for (int i = 0; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            producer.send(message);
        }
        Assert.assertTrue(awaitDispatchMs(2000));
        for (int i = 0; i < writeQueueNum; i++) {
            PopResult popResult = popMessageAsync(Duration.ofSeconds(30).toMillis(), 1, 30000).get();
            assertEquals(PopStatus.FOUND, popResult.getPopStatus());
            MessageExt message = popResult.getMsgFoundList().get(0);
            int expectPriority = priorityOrderAsc ? writeQueueNum - 1 - i : i;
            assertEquals(0, message.getQueueOffset());
            assertEquals(expectPriority, message.getQueueId());
            assertEquals(expectPriority, message.getPriority());
        }
    }

    @Test
    public void test_priority_consume_disable() throws Exception {
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName(group);
        config.setAttributes(AttributeParser.parseToMap("+" + PRIORITY_FACTOR_ATTRIBUTE.getName() + "=0"));
        initConsumerGroup(config);

        int msgNumPerQueue = 200;
        for (int i = 0; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            for (int j = 0; j < msgNumPerQueue; j++) {
                producer.send(message);
            }
        }
        Assert.assertTrue(awaitDispatchMs(2000));
        int sampleCount = 800;
        int[] queueIdCount = new int[writeQueueNum];
        for (int i = 0; i < sampleCount; i++) {
            PopResult popResult = popMessageAsync(Duration.ofSeconds(600).toMillis(), 1, 30000).get();
            TestUtil.waitForMonment(10); // wait lock release
            assertEquals(PopStatus.FOUND, popResult.getPopStatus());
            MessageExt message = popResult.getMsgFoundList().get(0);
            queueIdCount[message.getQueueId()] = queueIdCount[message.getQueueId()] + 1;
        }

        double expectAverage = (double) sampleCount / writeQueueNum;
        for (int count : queueIdCount) {
            assertTrue(Math.abs(count - expectAverage) < expectAverage * 0.4);
        }
    }

    @Test
    public void test_priority_consume_retry_as_lowest() throws Exception {
        // retry as lowest by default
        int count = 100;
        for (int i = 0; i < count; i++) {
            Message message = mockMessage(topic, new Random().nextInt(writeQueueNum), String.valueOf(i));
            producer.send(message);
        }
        int invisibleTime = 3;
        PopResult popResult = popMessageAsync(Duration.ofSeconds(invisibleTime).toMillis(), 1, 30000).get();
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        String retryId = popResult.getMsgFoundList().get(0).getMsgId();
        TestUtil.waitForSeconds(invisibleTime + 3);
        Assert.assertTrue(awaitDispatchMs(2000));

        List<MessageExt> collect = new ArrayList<>();
        await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(35, TimeUnit.SECONDS)
            .until(() -> {
                PopResult result = popMessageAsync(Duration.ofSeconds(600).toMillis(), 32, 5000).get();
                if (PopStatus.FOUND.equals(result.getPopStatus())) {
                    collect.addAll(result.getMsgFoundList());
                    return false;
                }
                return true;
            });

        assertEquals(count, collect.size());
        assertEquals(1, collect.get(collect.size() - 1).getReconsumeTimes());
        assertEquals(retryId, collect.get(collect.size() - 1).getMsgId());
    }

    @Test
    public void test_priority_consume_retry_as_highest() throws Exception {
        brokerController1.getBrokerConfig().setPopFromRetryProbabilityForPriority(100);
        int count = 100;
        for (int i = 0; i < count; i++) {
            Message message = mockMessage(topic, new Random().nextInt(writeQueueNum), String.valueOf(i));
            producer.send(message);
        }
        int invisibleTime = 3;
        PopResult popResult = popMessageAsync(Duration.ofSeconds(invisibleTime).toMillis(), 1, 30000).get();
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        String retryId = popResult.getMsgFoundList().get(0).getMsgId();
        TestUtil.waitForSeconds(invisibleTime + 3);
        Assert.assertTrue(awaitDispatchMs(2000));

        List<MessageExt> collect = new ArrayList<>();
        await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(35, TimeUnit.SECONDS)
            .until(() -> {
                PopResult result = popMessageAsync(Duration.ofSeconds(600).toMillis(), 32, 5000).get();
                if (PopStatus.FOUND.equals(result.getPopStatus())) {
                    collect.addAll(result.getMsgFoundList());
                    return false;
                }
                return true;
            });

        assertEquals(count, collect.size());
        assertEquals(1, collect.get(0).getReconsumeTimes());
        assertEquals(retryId, collect.get(0).getMsgId());
    }

    @Test
    public void test_priority_consume_use_separate_retry_queue() throws Exception {
        brokerController1.getBrokerConfig().setUseSeparateRetryQueue(true);
        brokerController1.getBrokerConfig().setPopFromRetryProbabilityForPriority(100);
        for (int i = 0; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            producer.send(message);
        }
        Assert.assertTrue(awaitDispatchMs(2000));
        int invisibleTime = 3;
        PopResult popResult = popMessageAsync(Duration.ofSeconds(invisibleTime).toMillis(), writeQueueNum, 30000).get();
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(writeQueueNum, popResult.getMsgFoundList().size());
        TestUtil.waitForSeconds(invisibleTime + 3);

        popResult = popMessageAsync(Duration.ofSeconds(600).toMillis(), 32, 10000).get();
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(writeQueueNum, popResult.getMsgFoundList().size());
        for (int i = 0; i < writeQueueNum; i++) {
            MessageExt message = popResult.getMsgFoundList().get(i);
            assertEquals(0, message.getQueueOffset()); // means a separate retry queue
            assertEquals(1, message.getReconsumeTimes());
            int expectPriority = priorityOrderAsc ? writeQueueNum - 1 - i : i;
            assertEquals(expectPriority, message.getQueueId());
            assertEquals(expectPriority, message.getPriority());
        }
    }

    @Test
    public void test_priority_consume_use_separate_retry_queue_with_queue_expansion() throws Exception {
        // retry as lowest by default
        brokerController1.getBrokerConfig().setUseSeparateRetryQueue(true);
        for (int i = 0; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            producer.send(message);
        }
        Assert.assertTrue(awaitDispatchMs(2000));
        int invisibleTime = 3;
        PopResult popResult = popMessageAsync(Duration.ofSeconds(invisibleTime).toMillis(), writeQueueNum, 30000).get();
        assertEquals(PopStatus.FOUND, popResult.getPopStatus());
        assertEquals(writeQueueNum, popResult.getMsgFoundList().size());
        TestUtil.waitForSeconds(invisibleTime + 3); // wait retry created

        writeQueueNum = writeQueueNum * 2;
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, writeQueueNum, CQType.SimpleCQ, TopicMessageType.PRIORITY);
        for (int i = writeQueueNum / 2; i < writeQueueNum; i++) {
            Message message = mockMessage(topic, i, String.valueOf(i));
            producer.send(message);
        }
        Assert.assertTrue(awaitDispatchMs(2000));

        popResult = popMessageAsync(Duration.ofSeconds(invisibleTime).toMillis(), 32, 5000).get();
        List<MessageExt> msgList = popResult.getMsgFoundList();
        // asc == true, collect: [15 -> 8, 7 -> 0]
        // asc == false, collect: [8 -> 15, 0 -> 7]
        assertEquals(writeQueueNum, msgList.size());
        assertEquals(priorityOrderAsc ? writeQueueNum - 1 : writeQueueNum / 2, msgList.get(0).getQueueId());
        assertEquals(priorityOrderAsc ? writeQueueNum - 1 : writeQueueNum / 2, msgList.get(0).getPriority());
        assertEquals(priorityOrderAsc ? 0 : writeQueueNum / 2 - 1, msgList.get(msgList.size() - 1).getQueueId());
        assertEquals(priorityOrderAsc ? 0 : writeQueueNum / 2 - 1, msgList.get(msgList.size() - 1).getPriority());
        assertEquals(1, msgList.get(msgList.size() - 1).getReconsumeTimes());
        assertEquals(0, msgList.get(msgList.size() - 1).getQueueOffset()); // means a separate retry queue
    }

    private static Message mockMessage(String topic, int priority, String key) {
        Message msg = new Message(topic, "HW".getBytes());
        if (priority >= 0) {
            msg.setPriority(priority);
        }
        msg.setKeys(key);
        return msg;
    }
}
