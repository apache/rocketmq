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

package org.apache.rocketmq.test.client.consumer.balance;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class NormalMsgStaticBalanceIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(NormalMsgStaticBalanceIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s !", topic));
        producer = getProducer(NAMESRV_ADDR, topic);
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testTwoConsumersBalance() {
        int msgSize = 400;
        RMQNormalConsumer consumer1 = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer2 = getConsumer(NAMESRV_ADDR, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        TestUtils.waitForSeconds(WAIT_TIME);

        producer.send(msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(CONSUME_TIME, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        assertThat(recvAll).isEqualTo(true);

        boolean balance = VerifyUtils.verifyBalance(msgSize,
            VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
                consumer1.getListener().getAllUndupMsgBody()).size(),
            VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
                consumer2.getListener().getAllUndupMsgBody()).size());
        assertThat(balance).isEqualTo(true);
    }

    @Test
    public void testFourConsumersBalance() {
        int msgSize = 600;
        String consumerGroup = initConsumerGroup();
        logger.info("use group: {}", consumerGroup);
        RMQNormalConsumer consumer1 = getConsumer(NAMESRV_ADDR, consumerGroup, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer2 = getConsumer(NAMESRV_ADDR, consumerGroup, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer3 = getConsumer(NAMESRV_ADDR, consumerGroup, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer4 = getConsumer(NAMESRV_ADDR, consumerGroup, topic, "*", new RMQNormalListener());
        TestUtils.waitForSeconds(WAIT_TIME);

        producer.send(msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(CONSUME_TIME, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener(), consumer3.getListener(),
            consumer4.getListener());
        assertThat(recvAll).isEqualTo(true);

        boolean balance = VerifyUtils
            .verifyBalance(msgSize,
                VerifyUtils
                    .getFilteredMessage(producer.getAllMsgBody(),
                        consumer1.getListener().getAllUndupMsgBody())
                    .size(),
                VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
                    consumer2.getListener().getAllUndupMsgBody()).size(),
                VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
                    consumer3.getListener().getAllUndupMsgBody()).size(),
                VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
                    consumer4.getListener().getAllUndupMsgBody()).size());
        assertThat(balance).isEqualTo(true);
    }
}
