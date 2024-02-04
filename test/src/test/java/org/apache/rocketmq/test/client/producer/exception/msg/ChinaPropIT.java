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

package org.apache.rocketmq.test.client.producer.exception.msg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.factory.MessageFactory;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class ChinaPropIT extends BaseConf {
    private static DefaultMQProducer producer = null;
    private static String topic = null;

    @Before
    public void setUp() {
        producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        topic = initTopic();
    }

    @After
    public void tearDown() {
        producer.shutdown();
    }

    /**
     * @since version3.4.6
     */
    @Test(expected = org.apache.rocketmq.client.exception.MQBrokerException.class)
    public void testSend20kChinaPropMsg() throws Exception {
        Message msg = MessageFactory.getRandomMessage(topic);
        msg.putUserProperty("key", RandomUtils.getCheseWord(32 * 1024 + 1));
        producer.send(msg);
    }

    /**
     * @since version3.4.6
     */
    @Test
    public void testSend10kChinaPropMsg() {

        Message msg = MessageFactory.getRandomMessage(topic);
        msg.putUserProperty("key", RandomUtils.getCheseWord(10 * 1024));
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
        }
        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
    }
}
