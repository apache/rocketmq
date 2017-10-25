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

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class TagMessageWith1ConsumerIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        String consumerId = initConsumerGroup();
        logger.info(String.format("use topic: %s; consumerId: %s !", topic, consumerId));
        producer = getProducer(nsAddr, topic);
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testTagSmoke() {
        String tag = "jueyin";
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, tag, new RMQNormalListener());
        producer.send(tag, msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubAllMessageNoTag() {
        String subExprress = "*";
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExprress,
            new RMQNormalListener());
        producer.send(msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubAllMessageWithTag() {
        String tag = "jueyin";
        String subExpress = "*";
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());
        producer.send(tag, msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubAllMessageWithNullTag() {
        String tag = null;
        String subExpress = "*";
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());
        producer.send(tag, msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubNullWithTagNull() {
        String tag = null;
        String subExpress = null;
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());
        producer.send(tag, msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubAllWithKindsOfMessage() {
        String tag1 = null;
        String tag2 = "jueyin";
        String subExpress = "*";
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        List<Object> tag1Msgs = MQMessageFactory.getRMQMessage(tag1, topic, msgSize);
        List<Object> tag2Msgs = MQMessageFactory.getRMQMessage(tag2, topic, msgSize);

        producer.send(tag1Msgs);
        producer.send(tag2Msgs);
        producer.send(10);
        Assert.assertEquals("Not all are sent", msgSize * 3, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubNullWithKindsOfMessage() {
        String tag1 = null;
        String tag2 = "jueyin";
        String subExpress = null;
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        List<Object> tag1Msgs = MQMessageFactory.getRMQMessage(tag1, topic, msgSize);
        List<Object> tag2Msgs = MQMessageFactory.getRMQMessage(tag2, topic, msgSize);

        producer.send(tag1Msgs);
        producer.send(tag2Msgs);
        Assert.assertEquals("Not all are sent", msgSize * 2, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    public void testSubTagWithKindsOfMessage() {
        String tag1 = null;
        String tag2 = "jueyin";
        String subExpress = tag2;
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        List<Object> tag1Msgs = MQMessageFactory.getRMQMessage(tag1, topic, msgSize);
        List<Object> tag2Msgs = MQMessageFactory.getRMQMessage(tag2, topic, msgSize);

        producer.send(tag1Msgs);
        producer.send(tag2Msgs);
        producer.send(10);
        Assert.assertEquals("Not all are sent", msgSize * 3, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(MQMessageFactory.getMessageBody(tag2Msgs),
            consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(MQMessageFactory.getMessageBody(tag2Msgs));
    }
}
