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

package org.apache.rocketmq.test.client.consumer.filter;

import org.apache.log4j.Logger;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.broadcast.BaseBroadCastIT;
import org.apache.rocketmq.test.client.consumer.broadcast.normal.NormalMsgTwoSameGroupConsumerIT;
import org.apache.rocketmq.test.client.rmq.RMQBroadCastConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQSqlConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class SqlFilterIT extends BaseConf {
    private static Logger logger = Logger.getLogger(SqlFilterIT.class);
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
        super.shutdown();
    }

    @Test
    public void testFilterConsumer() throws Exception {
        int msgSize = 16;

        String group = initConsumerGroup();
        MessageSelector selector = MessageSelector.bySql("(TAGS is not null and TAGS in ('TagA', 'TagB'))");
        RMQSqlConsumer consumer = ConsumerFactory.getRMQSqlConsumer(nsAddr, group, topic, selector, new RMQNormalListener(group + "_1"));
        Thread.sleep(3000);
        producer.send("TagA", msgSize);
        producer.send("TagB", msgSize);
        producer.send("TagC", msgSize);
        Assert.assertEquals("Not all sent succeeded", msgSize * 3, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(msgSize * 2, consumeTime);
        assertThat(producer.getAllMsgBody())
            .containsAllIn(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer.getListener().getAllMsgBody()));

        assertThat(consumer.getListener().getAllMsgBody().size()).isEqualTo(msgSize * 2);
    }
}
