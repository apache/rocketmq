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

package org.apache.rocketmq.test.client.consumer.tag;

import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.RandomUtils;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class TagMessageWithSameGroupConsumerIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;
    private String tag = "tag";

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
    public void testTwoConsumerWithSameGroup() {
        int msgSize = 20;
        String originMsgDCName = RandomUtils.getStringByUUID();
        String msgBodyDCName = RandomUtils.getStringByUUID();
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));
        getConsumer(nsAddr, consumer1.getConsumerGroup(), tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));
        producer.send(tag, msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer1.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer1.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testConsumerStartWithInterval() {
        int msgSize = 100;
        String originMsgDCName = RandomUtils.getStringByUUID();
        String msgBodyDCName = RandomUtils.getStringByUUID();

        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));
        producer.send(tag, msgSize, 100);
        TestUtils.waitForMoment(5);
        getConsumer(nsAddr, consumer1.getConsumerGroup(), tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));
        TestUtils.waitForMoment(5);

        consumer1.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer1.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testConsumerStartTwoAndCrashOneAfterWhile() {
        int msgSize = 100;
        String originMsgDCName = RandomUtils.getStringByUUID();
        String msgBodyDCName = RandomUtils.getStringByUUID();

        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), tag,
            new RMQNormalListener(originMsgDCName, msgBodyDCName));

        producer.send(tag, msgSize, 100);
        TestUtils.waitForMoment(5);
        consumer2.shutdown();
        mqClients.remove(1);
        TestUtils.waitForMoment(5);

        consumer1.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer1.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }
}
