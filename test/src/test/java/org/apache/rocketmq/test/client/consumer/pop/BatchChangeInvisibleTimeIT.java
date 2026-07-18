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
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BatchChangeInvisibleTimeIT extends BasePop {

    protected static final long ORIGINAL_INVISIBLE_TIME = 3000L;
    protected static final long CHANGED_INVISIBLE_TIME = 10000L;

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
    public void testBatchChangeInvisibleTimeNormallyWithPopBuffer() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(true);
        brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(true);
        brokerController2.getBrokerConfig().setPopConsumerKVServiceEnable(true);

        testBatchChangeInvisibleTime(() -> {
            try {
                return popMessageAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBatchChangeInvisibleTimeNormallyWithoutPopBuffer() throws Throwable {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(false);
        brokerController2.getBrokerConfig().setEnablePopBufferMerge(false);
        brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(true);
        brokerController2.getBrokerConfig().setPopConsumerKVServiceEnable(true);

        testBatchChangeInvisibleTime(() -> {
            try {
                return popMessageAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testBatchChangeInvisibleTimeOrderly() throws Throwable {
        brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(true);
        brokerController2.getBrokerConfig().setPopConsumerKVServiceEnable(true);

        testBatchChangeInvisibleTime(() -> {
            try {
                return popMessageOrderlyAsync().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testBatchChangeInvisibleTime(Supplier<PopResult> popResultSupplier) throws Throwable {
        producer.send(10);
        List<String> extraInfoList = new ArrayList<>();
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            PopResult popResult = popResultSupplier.get();
            if (popResult.getPopStatus().equals(PopStatus.FOUND)) {
                for (MessageExt messageExt : popResult.getMsgFoundList()) {
                    extraInfoList.add(messageExt.getProperty(MessageConst.PROPERTY_POP_CK));
                }
            }
            assertEquals(10, extraInfoList.size());
        });

        List<AckResult> ackResultList = client.batchChangeInvisibleTimeAsync(
            brokerAddr, topic, group, extraInfoList, CHANGED_INVISIBLE_TIME).get();
        assertEquals(extraInfoList.size(), ackResultList.size());

        List<String> changedExtraInfoList = new ArrayList<>(ackResultList.size());
        for (AckResult ackResult : ackResultList) {
            assertEquals(AckStatus.OK, ackResult.getStatus());
            assertNotNull(ackResult.getExtraInfo());
            long invisibleTime = ExtraInfoUtil.getInvisibleTime(ExtraInfoUtil.split(ackResult.getExtraInfo()));
            assertTrue(invisibleTime >= CHANGED_INVISIBLE_TIME);
            assertTrue(invisibleTime <= CHANGED_INVISIBLE_TIME + 3000L);
            changedExtraInfoList.add(ackResult.getExtraInfo());
        }

        TimeUnit.MILLISECONDS.sleep(ORIGINAL_INVISIBLE_TIME + 3000L);
        PopResult popResult = popResultSupplier.get();
        assertEquals(PopStatus.POLLING_NOT_FOUND, popResult.getPopStatus());

        AckResult ackResult = client.batchAckMessageAsync(brokerAddr, topic, group, changedExtraInfoList).get();
        assertEquals(AckStatus.OK, ackResult.getStatus());
    }

    private CompletableFuture<PopResult> popMessageAsync() {
        return client.popMessageAsync(
            brokerAddr, messageQueue, ORIGINAL_INVISIBLE_TIME, 10, group, 3000, false,
            ConsumeInitMode.MIN, false, ExpressionType.TAG, "*");
    }

    private CompletableFuture<PopResult> popMessageOrderlyAsync() {
        return client.popMessageAsync(
            brokerAddr, messageQueue, ORIGINAL_INVISIBLE_TIME, 10, group, 3000, false,
            ConsumeInitMode.MIN, true, ExpressionType.TAG, "*", null);
    }
}
