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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.producer.order;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.balance.NormalMsgStaticBalanceIT;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.order.RMQOrderListener;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.test.util.MQWait;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class OrderMsgDynamicRebalanceIT extends BaseConf {
    private static Logger logger = Logger.getLogger(NormalMsgStaticBalanceIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s !", topic));
        producer = getProducer(nsAddr, topic);
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testTwoConsumerAndCrashOne() {
        int msgSize = 10;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*",
            new RMQOrderListener("1"));
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQOrderListener("2"));

        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        MQWait.waitConsumeAll(30 * 1000, producer.getAllMsgBody(), consumer1.getListener(),
            consumer2.getListener());
        consumer2.shutdown();

        mqMsgs = new MessageQueueMsg(mqs, msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        assertThat(recvAll).isEqualTo(true);

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer1.getListener()).getMsgs()))
            .isEqualTo(true);
        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer2.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testThreeConsumerAndCrashOne() {
        int msgSize = 10;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*",
            new RMQOrderListener("1"));
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQOrderListener("2"));
        RMQNormalConsumer consumer3 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQOrderListener("3"));

        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(), consumer1.getListener(),
            consumer2.getListener(), consumer3.getListener());
        consumer3.shutdown();

        mqMsgs = new MessageQueueMsg(mqs, msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        boolean recvAll = MQWait.waitConsumeAll(30 * 1000, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener(), consumer3.getListener());
        assertThat(recvAll).isEqualTo(true);

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer1.getListener()).getMsgs()))
            .isEqualTo(true);
        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer2.getListener()).getMsgs()))
            .isEqualTo(true);
        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer3.getListener()).getMsgs()))
            .isEqualTo(true);
    }
}
