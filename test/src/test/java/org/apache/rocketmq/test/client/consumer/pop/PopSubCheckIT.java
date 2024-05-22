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

package org.apache.rocketmq.test.client.consumer.pop;

import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.RandomUtil;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class PopSubCheckIT extends BaseConf {
    private static final Logger log = LoggerFactory.getLogger(PopSubCheckIT.class);
    private String group;

    private DefaultMQAdminExt defaultMQAdminExt;

    @Before
    public void setUp() throws Exception {
        group = initConsumerGroup();

        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(RandomUtil.getStringByUUID());
        defaultMQAdminExt.start();
    }

    @After
    public void tearDown() {
        defaultMQAdminExt.shutdown();
        super.shutdown();
    }

    @Ignore
    @Test
    public void testNormalPopAck() throws Exception {
        String topic = initTopic();
        log.info("use topic: {}; group: {} !", topic, group);

        RMQNormalProducer producer = getProducer(NAMESRV_ADDR, topic);
        producer.getProducer().setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);

        for (String brokerAddr : new String[]{brokerController1.getBrokerAddr(), brokerController2.getBrokerAddr()}) {
            defaultMQAdminExt.setMessageRequestMode(brokerAddr, topic, group, MessageRequestMode.POP, 8, 60_000);
        }

        RMQPopConsumer consumer = ConsumerFactory.getRMQPopConsumer(NAMESRV_ADDR, group,
            topic, "*", new RMQNormalListener());
        mqClients.add(consumer);

        int msgNum = 1;
        producer.send(msgNum);
        Assert.assertEquals("Not all sent succeeded", msgNum, producer.getAllUndupMsgBody().size());
        log.info(producer.getFirstMsg().toString());

        TestUtils.waitForSeconds(10);

        consumer.getListener().waitForMessageConsume(msgNum, 30_000);
        assertThat(VerifyUtils.getFilteredMessage(producer.getAllMsgBody(), consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
        for (Object o : consumer.getListener().getAllOriginMsg()) {
            MessageClientExt msg = (MessageClientExt) o;
            assertThat(msg.getProperty(MessageConst.PROPERTY_POP_CK)).named("check pop meta").isNotEmpty();
        }

        consumer.getListener().waitForMessageConsume(msgNum, 3_000 * 9);
        assertThat(consumer.getListener().getAllOriginMsg().size()).isEqualTo(msgNum);
    }
}
