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
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.listener.rmq.order.RMQOrderListener;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class OrderMsgIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(OrderMsgIT.class);
    private RMQNormalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(NAMESRV_ADDR, topic);
        consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQOrderListener());
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testOrderMsg() {
        int msgSize = 10;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(mqs, msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testSendOneQueue() {
        int msgSize = 20;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(MQMessageFactory.getMessageQueues(mqs.get(0)),
            msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }

    @Test
    public void testSendRandomQueues() {
        int msgSize = 10;
        List<MessageQueue> mqs = producer.getMessageQueue();
        MessageQueueMsg mqMsgs = new MessageQueueMsg(
            MQMessageFactory.getMessageQueues(mqs.get(0), mqs.get(1), mqs.get(mqs.size() - 1)),
            msgSize);
        producer.send(mqMsgs.getMsgsWithMQ());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(mqMsgs.getMsgBodys());

        assertThat(VerifyUtils.verifyOrder(((RMQOrderListener) consumer.getListener()).getMsgs()))
            .isEqualTo(true);
    }
}
