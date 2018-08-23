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

package org.apache.rocketmq.test.client.producer.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchSendIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private String topic = null;
    private Random random = new Random();

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("user topic[%s]!", topic));
    }

    @After
    public void tearDown() {
        super.shutdown();
    }

    @Test
    public void testBatchSend_ViewMessage() throws Exception {
        List<Message> messageList = new ArrayList<>();
        int batchNum = 100;
        for (int i = 0; i < batchNum; i++) {
            messageList.add(new Message(topic, RandomUtils.getStringByUUID().getBytes()));
        }

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        SendResult sendResult = producer.send(messageList);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());

        String[] offsetIds = sendResult.getOffsetMsgId().split(",");
        String[] msgIds = sendResult.getMsgId().split(",");
        Assert.assertEquals(messageList.size(), offsetIds.length);
        Assert.assertEquals(messageList.size(), msgIds.length);

        Thread.sleep(2000);

        for (int i = 0; i < 3; i++) {
            producer.viewMessage(offsetIds[random.nextInt(batchNum)]);
        }
        for (int i = 0; i < 3; i++) {
            producer.viewMessage(topic, msgIds[random.nextInt(batchNum)]);
        }
    }

    @Test
    public void testBatchSend_CheckProperties() throws Exception {
        List<Message> messageList = new ArrayList<>();
        Message message = new Message();
        message.setTopic(topic);
        message.setKeys("keys123");
        message.setTags("tags123");
        message.setWaitStoreMsgOK(false);
        message.setBuyerId("buyerid123");
        message.setFlag(123);
        message.setBody("body".getBytes());
        messageList.add(message);

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(nsAddr);
        SendResult sendResult = producer.send(messageList);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());

        String[] offsetIds = sendResult.getOffsetMsgId().split(",");
        String[] msgIds = sendResult.getMsgId().split(",");
        Assert.assertEquals(messageList.size(), offsetIds.length);
        Assert.assertEquals(messageList.size(), msgIds.length);

        Thread.sleep(2000);

        Message messageByOffset = producer.viewMessage(offsetIds[0]);
        Message messageByMsgId = producer.viewMessage(topic, msgIds[0]);

        System.out.println(messageByOffset);
        System.out.println(messageByMsgId);

        Assert.assertEquals(message.getTopic(), messageByMsgId.getTopic());
        Assert.assertEquals(message.getTopic(), messageByOffset.getTopic());

        Assert.assertEquals(message.getKeys(), messageByOffset.getKeys());
        Assert.assertEquals(message.getKeys(), messageByMsgId.getKeys());

        Assert.assertEquals(message.getTags(), messageByOffset.getTags());
        Assert.assertEquals(message.getTags(), messageByMsgId.getTags());

        Assert.assertEquals(message.isWaitStoreMsgOK(), messageByOffset.isWaitStoreMsgOK());
        Assert.assertEquals(message.isWaitStoreMsgOK(), messageByMsgId.isWaitStoreMsgOK());

        Assert.assertEquals(message.getBuyerId(), messageByOffset.getBuyerId());
        Assert.assertEquals(message.getBuyerId(), messageByMsgId.getBuyerId());

        Assert.assertEquals(message.getFlag(), messageByOffset.getFlag());
        Assert.assertEquals(message.getFlag(), messageByMsgId.getFlag());
    }

}
