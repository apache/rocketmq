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

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.balance.NormalMsgStaticBalanceIT;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class MessageUserPropIT extends BaseConf {
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

    /**
     * @since version3.4.6
     */
    @Test
    public void testSendEnglishUserProp() {
        Message msg = MessageFactory.getRandomMessage(topic);
        String msgKey = "jueyinKey";
        String msgValue = "jueyinValue";
        msg.putUserProperty(msgKey, msgValue);

        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());

        producer.send(msg, null);
        assertThat(producer.getAllMsgBody().size()).isEqualTo(1);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        Message sendMsg = (Message) producer.getFirstMsg();
        Message recvMsg = (Message) consumer.getListener().getFirstMsg();
        assertThat(recvMsg.getUserProperty(msgKey)).isEqualTo(sendMsg.getUserProperty(msgKey));
    }

    /**
     * @since version3.4.6
     */
    @Test
    public void testSendChinaUserProp() {
        Message msg = MessageFactory.getRandomMessage(topic);
        String msgKey = "jueyinKey";
        String msgValue = "jueyinzhi";
        msg.putUserProperty(msgKey, msgValue);

        RMQNormalConsumer consumer = getConsumer(NAMESRV_ADDR, topic, "*", new RMQNormalListener());

        producer.send(msg, null);
        assertThat(producer.getAllMsgBody().size()).isEqualTo(1);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), CONSUME_TIME);

        Message sendMsg = (Message) producer.getFirstMsg();
        Message recvMsg = (Message) consumer.getListener().getFirstMsg();
        assertThat(recvMsg.getUserProperty(msgKey)).isEqualTo(sendMsg.getUserProperty(msgKey));
    }
}
