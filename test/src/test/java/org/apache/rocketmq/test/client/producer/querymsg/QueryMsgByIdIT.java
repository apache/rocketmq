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

package org.apache.rocketmq.test.client.producer.querymsg;

import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class QueryMsgByIdIT extends BaseConf {
    private static Logger logger = Logger.getLogger(QueryMsgByIdIT.class);
    private RMQNormalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
        consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener());
    }

    @After
    public void tearDown() {
        shutdown();
    }

    @Test
    public void testQueryMsg() {
        int msgSize = 20;
        producer.send(msgSize);
        Assert.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        Assert.assertEquals("Not all are consumed", 0, VerifyUtils.verify(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()));

        MessageExt recvMsg = (MessageExt) consumer.getListener().getFirstMsg();
        MessageExt queryMsg = null;
        try {
            TestUtils.waitForMoment(3000);
            queryMsg = producer.getProducer().viewMessage(((MessageClientExt) recvMsg).getOffsetMsgId());
        } catch (Exception e) {
        }

        assertThat(queryMsg).isNotNull();
        assertThat(new String(queryMsg.getBody())).isEqualTo(new String(recvMsg.getBody()));
    }
}
