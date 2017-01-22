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

package org.apache.rocketmq.test.client.consumer.broadcast.tag;

import org.apache.log4j.Logger;
import org.apache.rocketmq.test.client.consumer.broadcast.BaseBroadCastIT;
import org.apache.rocketmq.test.client.rmq.RMQBroadCastConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListner;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class BroadCastTwoConsumerSubDiffTagIT extends BaseBroadCastIT {
    private static Logger logger = Logger.getLogger(BroadCastTwoConsumerSubTagIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
    }

    @After
    public void tearDown() {
        super.shutDown();
    }

    @Test
    public void testTwoConsumerSubDiffTag() {
        int msgSize = 40;
        String tag = "jueyin_tag";

        RMQBroadCastConsumer consumer1 = getBroadCastConsumer(nsAddr, topic, "*",
            new RMQNormalListner());
        RMQBroadCastConsumer consumer2 = getBroadCastConsumer(nsAddr,
            consumer1.getConsumerGroup(), topic, tag, new RMQNormalListner());
        TestUtils.waitForSeconds(waitTime);

        producer.send(tag, msgSize);
        Assert.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());

        consumer1.getListner().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        consumer2.getListner().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer1.getListner().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer2.getListner().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }
}
