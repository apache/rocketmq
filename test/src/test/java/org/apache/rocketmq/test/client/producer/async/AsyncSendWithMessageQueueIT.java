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

import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.client.rmq.RMQAsyncSendProducer;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class AsyncSendWithMessageQueueIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
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
        super.shutdown();
    }

    @Test
    public void testAsyncSendWithMQ() {
        int msgSize = 20;
        int queueId = 0;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener());
        MessageQueue mq = new MessageQueue(topic, broker1Name, queueId);

        producer.asyncSend(msgSize, mq);
        producer.waitForResponse(10 * 1000);
        assertThat(producer.getSuccessMsgCount()).isEqualTo(msgSize);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());

        VerifyUtils.verifyMessageQueueId(queueId, consumer.getListener().getAllOriginMsg());

        producer.clearMsg();
        consumer.clearMsg();
        producer.getSuccessSendResult().clear();
        mq = new MessageQueue(topic, broker2Name, queueId);
        producer.asyncSend(msgSize, mq);
        producer.waitForResponse(10 * 1000);
        assertThat(producer.getSuccessMsgCount()).isEqualTo(msgSize);

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());

        VerifyUtils.verifyMessageQueueId(queueId, consumer.getListener().getAllOriginMsg());
    }
}
