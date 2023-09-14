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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

public class BatchAckIT extends BasePop {

    protected String topic;
    protected String group;
    protected RMQNormalProducer producer = null;
    protected RMQPopClient client = null;
    protected String brokerAddr;
    protected MessageQueue messageQueue;

    @Before
    public void setUp() {
        brokerAddr = brokerController1.getBrokerAddr();
        topic = MQRandomUtils.getRandomTopic();
        group = initConsumerGroup();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 8, CQType.SimpleCQ, TopicMessageType.NORMAL);
        producer = getProducer(NAMESRV_ADDR, topic);
        client = getRMQPopClient();
        messageQueue = new MessageQueue(topic, BROKER1_NAME, -1);
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testBatchAckNormallyWithPopBuffer() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(true);

        testBatchAck(() -> {
            try {
                return popMessageAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBatchAckNormallyWithOutPopBuffer() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(false);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(false);

        testBatchAck(() -> {
            try {
                return popMessageAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBatchAckOrderly() throws Throwable {
        testBatchAck(() -> {
            try {
                return popMessageOrderlyAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testBatchAck(Supplier<PopResult> popResultSupplier) throws Throwable {
        // Send 10 messages but do not ack, let them enter the retry topic
        producer.send(10);
        AtomicInteger firstMsgRcvNum = new AtomicInteger();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            PopResult popResult = popResultSupplier.get();
            if (popResult.getPopStatus().equals(PopStatus.FOUND)) {
                firstMsgRcvNum.addAndGet(popResult.getMsgFoundList().size());
            }
            assertEquals(10, firstMsgRcvNum.get());
        });
        // sleep 6s, expect messages to enter the retry topic
        TimeUnit.SECONDS.sleep(6);

        producer.send(20);
        List<String> extraInfoList = new ArrayList<>();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            PopResult popResult = popResultSupplier.get();
            if (popResult.getPopStatus().equals(PopStatus.FOUND)) {
                for (MessageExt messageExt : popResult.getMsgFoundList()) {
                    extraInfoList.add(messageExt.getProperty(MessageConst.PROPERTY_POP_CK));
                }
            }
            assertEquals(30, extraInfoList.size());
        });

        AckResult ackResult = client.batchAckMessageAsync(brokerAddr, topic, group, extraInfoList).get();
        assertEquals(AckStatus.OK, ackResult.getStatus());

        // sleep 6s, expected that messages that have been acked will not be re-consumed
        TimeUnit.SECONDS.sleep(6);
        PopResult popResult = popResultSupplier.get();
        assertEquals(PopStatus.POLLING_NOT_FOUND, popResult.getPopStatus());
    }

    private CompletableFuture<PopResult> popMessageAsync() {
        return client.popMessageAsync(
            brokerAddr, messageQueue, Duration.ofSeconds(3).toMillis(), 30, group, 3000, false,
            ConsumeInitMode.MIN, false, ExpressionType.TAG, "*");
    }

    private CompletableFuture<PopResult> popMessageOrderlyAsync() {
        return client.popMessageAsync(
            brokerAddr, messageQueue, Duration.ofSeconds(3).toMillis(), 30, group, 3000, false,
            ConsumeInitMode.MIN, true, ExpressionType.TAG, "*", null);
    }
}
