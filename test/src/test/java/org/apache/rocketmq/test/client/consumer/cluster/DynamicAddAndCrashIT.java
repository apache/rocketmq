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

package org.apache.rocketmq.test.client.consumer.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.balance.NormalMsgStaticBalanceIT;
import org.apache.rocketmq.test.client.mq.MQAsyncProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.apache.rocketmq.test.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class DynamicAddAndCrashIT extends BaseConf {
    private static Logger logger = LogManager.getLogger(NormalMsgStaticBalanceIT.class);
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
    public void testAddOneConsumerAndCrashAfterWhile() {
        int msgSize = 150;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*", new RMQNormalListener());

        MQAsyncProducer asyncDefaultMQProducer = new MQAsyncProducer(producer, msgSize, 100);
        asyncDefaultMQProducer.start();
        TestUtils.waitForSeconds(waitTime);

        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        TestUtils.waitForSeconds(waitTime);
        consumer2.shutdown();

        asyncDefaultMQProducer.waitSendAll(waitTime * 6);

        MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(), consumer1.getListener(),
            consumer2.getListener());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        assertThat(recvAll).isEqualTo(true);
    }

    @Test
    public void testAddTwoConsumerAndCrashAfterWhile() {
        int msgSize = 150;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*", new RMQNormalListener());

        MQAsyncProducer asyncDefaultMQProducer = new MQAsyncProducer(producer, msgSize, 100);
        asyncDefaultMQProducer.start();
        TestUtils.waitForSeconds(waitTime);

        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        RMQNormalConsumer consumer3 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        TestUtils.waitForSeconds(waitTime);

        consumer2.shutdown();
        consumer3.shutdown();

        asyncDefaultMQProducer.waitSendAll(waitTime * 6);

        MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(), consumer1.getListener(),
            consumer2.getListener(), consumer3.getListener());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener(), consumer3.getListener());
        assertThat(recvAll).isEqualTo(true);
    }
}
