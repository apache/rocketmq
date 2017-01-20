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

package org.apache.rocketmq.test.client.producer.async;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.factory.SendCallBackFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.apache.rocketmq.test.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class AsyncSendExceptionIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private static boolean sendFail = false;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
    }

    @After
    public void tearDown() {
        super.shutDown();
    }

    @Test
    public void testSendCallBackNull() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        SendCallback sendCallback = null;
        producer.send(msg, sendCallback);
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testSendMQNull() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        MessageQueue messageQueue = null;
        producer.send(msg, messageQueue, SendCallBackFactory.getSendCallBack());
    }

    @Test(expected = org.apache.rocketmq.client.exception.MQClientException.class)
    public void testSendSelectorNull() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        MessageQueueSelector selector = null;
        producer.send(msg, selector, 100, SendCallBackFactory.getSendCallBack());
    }

    @Test(expected = org.apache.rocketmq.client.exception.MQClientException.class)
    public void testSelectorThrowsException() throws Exception {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        producer.send(msg, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                String str = null;
                return list.get(str.length());
            }
        }, null, SendCallBackFactory.getSendCallBack());
    }

    @Test
    public void testQueueIdBigThanQueueNum() throws Exception {
        int queueId = 100;
        sendFail = false;
        MessageQueue mq = new MessageQueue(topic, broker1Name, queueId);
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);

        producer.send(msg, mq, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
            }

            @Override
            public void onException(Throwable throwable) {
                sendFail = true;
            }
        });

        int checkNum = 50;
        while (!sendFail && checkNum > 0) {
            checkNum--;
            TestUtils.waitForMonment(100);
        }
        producer.shutdown();
        assertThat(sendFail).isEqualTo(true);
    }

    @Test
    public void testQueueIdSmallZero() throws Exception {
        int queueId = -100;
        sendFail = true;
        MessageQueue mq = new MessageQueue(topic, broker1Name, queueId);
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);

        producer.send(msg, mq, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                sendFail = false;
            }

            @Override
            public void onException(Throwable throwable) {
                sendFail = true;
            }
        });

        int checkNum = 50;
        while (sendFail && checkNum > 0) {
            checkNum--;
            TestUtils.waitForMonment(100);
        }
        producer.shutdown();
        assertThat(sendFail).isEqualTo(false);
    }

}
