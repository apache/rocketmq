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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
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

public class OrderMsgWithTagIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(OrderMsgIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(NAMESRV_ADDR, topic);
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testOrderMsgWithTagSubAll() {
        int msgSize = 10;
        String tag = "jueyin_tag";
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQOrderListener());

        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize, tag);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testOrderMsgWithTagSubTag() {
        int msgSize = 5;
        String tag = "jueyin_tag";
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, tag, new RMQOrderListener());

        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize, tag);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testOrderMsgWithTag1AndTag2SubTag1() {
        int msgSize = 5;
        String tag1 = "jueyin_tag_1";
        String tag2 = "jueyin_tag_2";
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, tag1, new RMQOrderListener());

        List<MessageQueue> mqs = producer.getMessageQueue();

        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize, tag2);
        producer.send(mqMsgs.getMsgsWithMQ());
        producer.clearMsg();

        mqMsgs = new MessageQueueMsg(mqs, msgSize, tag1);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testTwoConsumerSubTag() {
        int msgSize = 10;
        String tag1 = "jueyin_tag_1";
        String tag2 = "jueyin_tag_2";
        RMQNormalConsumer consumer1 = getConsumer(NAMESRV_ADDR, topic, tag1,
            new RMQOrderListener("consumer1"));
        RMQNormalConsumer consumer2 = getConsumer(NAMESRV_ADDR, topic, tag2,
            new RMQOrderListener("consumer2"));
        List<MessageQueue> mqs = producer.getMessageQueue();

        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize, tag1);
        producer.send(mqMsgs.getMsgsWithMQ());

        mqMsgs = new MessageQueueMsg(mqs, msgSize, tag2);
        producer.send(mqMsgs.getMsgsWithMQ());

        boolean recvAll = MQWait.waitConsumeAll(CONSUME_TIME, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        assertThat(recvAll).isEqualTo(true);

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer1.getListener()).getMsgs()))
            .isEqualTo(true);
        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer2.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testConsumeTwoTag() {
        int msgSize = 10;
        String tag1 = "jueyin_tag_1";
        String tag2 = "jueyin_tag_2";
        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic,
            String.format("%s||%s", tag1, tag2), new RMQOrderListener());

        List<MessageQueue> mqs = producer.getMessageQueue();

        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize, tag1);
        producer.send(mqMsgs.getMsgsWithMQ());

        mqMsgs = new MessageQueueMsg(mqs, msgSize, tag2);
        producer.send(mqMsgs.getMsgsWithMQ());

        boolean recvAll = MQWait.waitConsumeAll(CONSUME_TIME, producer.getAllMsgBody(),
            consumer.getListener());
        assertThat(recvAll).isEqualTo(true);

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }
}
