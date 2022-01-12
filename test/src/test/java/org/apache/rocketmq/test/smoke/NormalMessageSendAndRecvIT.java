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

package org.apache.rocketmq.test.smoke;

import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class NormalMessageSendAndRecvIT extends BaseConf {
    private static Logger logger = Logger.getLogger(NormalMessageSendAndRecvIT.class);
    private RMQNormalConsumer consumer = null;
    private RMQNormalProducer producer = null;
    private String topic = null;
    private String group = null;

    @Before
    public void setUp() {
        topic = initTopic();
        group = initConsumerGroup();
        logger.info(String.format("Use topic %s group %s", topic, group));
        producer = getProducer(nsAddr, topic);
        consumer = getConsumer(nsAddr,  group, topic, "*", new RMQNormalListener());
    }

    @Test
    public void testHold() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        System.out.println("ns:" + nsAddr);
        System.out.println("topic:" + topic);
        System.out.println("group:" + group);
        latch.await();
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testSynSendMessage() {
        int msgSize = 10;
        producer.send(msgSize);
        Assert.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }
}
