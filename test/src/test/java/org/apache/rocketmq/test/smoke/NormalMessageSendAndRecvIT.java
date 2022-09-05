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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.message.MessageClientExt;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.awaitility.Awaitility;

import static com.google.common.truth.Truth.assertThat;

public class NormalMessageSendAndRecvIT extends BaseConf {
    private static final Logger logger = LoggerFactory.getLogger(NormalMessageSendAndRecvIT.class);
    private RMQNormalConsumer consumer = null;
    private RMQNormalProducer producer = null;
    private String topic = null;
    private String group = null;
    private DefaultMQAdminExt defaultMQAdminExt;

    @Before
    public void setUp() throws Exception {
        topic = initTopic();
        group = initConsumerGroup();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
        consumer = getConsumer(nsAddr, group, topic, "*", new RMQNormalListener());
        defaultMQAdminExt = getAdmin(nsAddr);
        defaultMQAdminExt.start();
    }

    @After
    public void tearDown() {
        BaseConf.shutdown();
    }

    @Test
    public void testSynSendMessage() throws Exception {
        AtomicReference<List<MessageQueue>> messageQueueList = new AtomicReference<>();
        AtomicReference<ConsumeStats> consumeStats = new AtomicReference<>();
        Awaitility.await().atMost(Duration.ofSeconds(120))
            .until(() -> {
                try {
                    consumeStats.set(defaultMQAdminExt.examineConsumeStats(group));
                    messageQueueList.set(producer.getProducer().fetchPublishMessageQueues(topic));
                    return !messageQueueList.get().isEmpty() && null != consumeStats.get()
                        && consumeStats.get().getOffsetTable().keySet().containsAll(messageQueueList.get());
                } catch (MQClientException e) {
                    logger.debug("Exception raised while checking producer and consumer are started", e);
                }
                return false;
            });

        int msgSize = 10;
        for (MessageQueue messageQueue : messageQueueList.get()) {
            producer.send(msgSize, messageQueue);
        }
        Assert.assertEquals("Not all sent succeeded", msgSize * messageQueueList.get().size(),
            producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        assertThat(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());

        for (Object o : consumer.getListener().getAllOriginMsg()) {
            MessageClientExt msg = (MessageClientExt) o;
            assertThat(msg.getProperty(MessageConst.PROPERTY_POP_CK)).isNull();
        }
        //shutdown to persist the offset
        consumer.getConsumer().shutdown();
        consumeStats.set(defaultMQAdminExt.examineConsumeStats(group));
        //+1 for the retry topic
        for (MessageQueue messageQueue : messageQueueList.get()) {
            Assert.assertTrue(consumeStats.get().getOffsetTable().containsKey(messageQueue));
            Assert.assertEquals(msgSize, consumeStats.get().getOffsetTable().get(messageQueue).getConsumerOffset());
            Assert.assertEquals(msgSize, consumeStats.get().getOffsetTable().get(messageQueue).getBrokerOffset());
        }

    }
}
