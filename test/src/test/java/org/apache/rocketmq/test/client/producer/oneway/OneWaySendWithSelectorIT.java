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
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.client.rmq.RMQAsyncSendProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListner;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class OneWaySendWithSelectorIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private static boolean sendFail = false;
    private RMQAsyncSendProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
        producer = getAsyncProducer(nsAddr, topic);
    }

    @After
    public void tearDown() {
        super.shutDown();
    }

    @Test
    public void testSendWithSelector() {
        int msgSize = 20;
        final int queueId = 0;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListner());

        producer.sendOneWay(msgSize, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                for (MessageQueue mq : list) {
                    if (mq.getQueueId() == queueId && mq.getBrokerName().equals(broker1Name)) {
                        return mq;
                    }
                }
                return list.get(0);
            }
        });
        assertThat(producer.getAllMsgBody().size()).isEqualTo(msgSize);

        consumer.getListner().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListner().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());

        VerifyUtils.verifyMessageQueueId(queueId, consumer.getListner().getAllOriginMsg());

        producer.clearMsg();
        consumer.clearMsg();

        producer.sendOneWay(msgSize, new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                for (MessageQueue mq : list) {
                    if (mq.getQueueId() == queueId && mq.getBrokerName().equals(broker2Name)) {
                        return mq;
                    }
                }
                return list.get(8);
            }
        });
        assertThat(producer.getAllMsgBody().size()).isEqualTo(msgSize);

        consumer.getListner().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListner().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());

        VerifyUtils.verifyMessageQueueId(queueId, consumer.getListner().getAllOriginMsg());
    }
}
