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

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.client.consumer.broadcast.BaseBroadcast;
import org.apache.rocketmq.test.client.rmq.RMQBroadCastConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class BroadcastTwoConsumerSubDiffTagIT extends BaseBroadcast {
    private static Logger logger = LoggerFactory.getLogger(BroadcastTwoConsumerSubTagIT.class);
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
        super.shutdown();
    }

    @Test
    public void testTwoConsumerSubDiffTag() {
        int msgSize = 40;
        String tag = "jueyin_tag";

        RMQBroadCastConsumer consumer1 = getBroadCastConsumer(NAMESRV_ADDR, topic, "*",
            new RMQNormalListener());
        RMQBroadCastConsumer consumer2 = getBroadCastConsumer(NAMESRV_ADDR,
            consumer1.getConsumerGroup(), topic, tag, new RMQNormalListener());
        TestUtils.waitForSeconds(WAIT_TIME);

        producer.send(tag, msgSize);
        Assert.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());

        consumer1.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);
        consumer2.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
            consumer1.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(),
            consumer2.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }
}
