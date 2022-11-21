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

package org.apache.rocketmq.test.client.producer.oneway;

import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OneWaySendExceptionIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(TagMessageWith1ConsumerIT.class);
    private static boolean sendFail = false;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testSendMQNull() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        MessageQueue messageQueue = null;
        producer.sendOneway(msg, messageQueue);
    }

    @Test(expected = org.apache.rocketmq.client.exception.MQClientException.class)
    public void testSendSelectorNull() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        MessageQueueSelector selector = null;
        producer.sendOneway(msg, selector, 100);
    }

    @Test(expected = org.apache.rocketmq.client.exception.MQClientException.class)
    public void testSelectorThrowsException() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        producer.sendOneway(msg, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                String str = null;
                return list.get(str.length());
            }
        }, null);
    }
}
