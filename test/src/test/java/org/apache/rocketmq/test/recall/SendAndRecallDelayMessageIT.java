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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.test.recall;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class SendAndRecallDelayMessageIT extends BaseConf {

    private static String initTopic;
    private static String consumerGroup;
    private static RMQNormalProducer producer;
    private static RMQPopConsumer popConsumer;

    @Before
    public void init() {
        initTopic = initTopic();
        consumerGroup = initConsumerGroup();
        producer = getProducer(NAMESRV_ADDR, initTopic);
        popConsumer = ConsumerFactory.getRMQPopConsumer(NAMESRV_ADDR, consumerGroup, initTopic, "*", new RMQNormalListener());
        mqClients.add(popConsumer);
    }

    @AfterClass
    public static void tearDown() {
        shutdown();
    }

    @Test
    public void testSendAndRecv() throws Exception {
        int delaySecond = 1;
        String topic = MQRandomUtils.getRandomTopic();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.DELAY);
        MessageQueue messageQueue = new MessageQueue(topic, BROKER1_NAME, 0);
        String brokerAddress = brokerController1.getBrokerAddr();

        List<Message> sendList = buildSendMessageList(topic, delaySecond);
        List<Message> recvList = new ArrayList<>();

        for (Message message : sendList) {
            producer.getProducer().send(message);
        }

        await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(delaySecond + 15, TimeUnit.SECONDS)
            .until(() -> {
                PopResult popResult = popConsumer.pop(brokerAddress, messageQueue, 60 * 1000, -1);
                processPopResult(recvList, popResult);
                return recvList.size() == sendList.size();
            });
    }

    @Test
    public void testSendAndRecall() throws Exception {
        int delaySecond = 5;
        String topic = MQRandomUtils.getRandomTopic();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.DELAY);
        MessageQueue messageQueue = new MessageQueue(topic, BROKER1_NAME, 0);
        String brokerAddress = brokerController1.getBrokerAddr();

        List<Message> sendList = buildSendMessageList(topic, delaySecond);
        List<Message> recvList = new ArrayList<>();
        int recallCount = 0;

        for (Message message : sendList) {
            SendResult sendResult = producer.getProducer().send(message);
            if (sendResult.getRecallHandle() != null) {
                String messageId = producer.getProducer().recallMessage(topic, sendResult.getRecallHandle());
                assertEquals(sendResult.getMsgId(), messageId);
                recallCount += 1;
            }
        }
        assertEquals(sendList.size() - 2, recallCount); // one normal and one delay-level message
        try {
            await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(delaySecond + 15, TimeUnit.SECONDS)
                .until(() -> {
                    PopResult popResult = popConsumer.pop(brokerAddress, messageQueue, 60 * 1000, -1);
                    processPopResult(recvList, popResult);
                    return recvList.size() == sendList.size();
                });
        } catch (Exception e) {
        }
        assertEquals(sendList.size() - recallCount, recvList.size());
    }

    @Test
    public void testSendAndRecall_ukCollision() throws Exception {
        int delaySecond = 5;
        String topic = MQRandomUtils.getRandomTopic();
        String collisionTopic = MQRandomUtils.getRandomTopic();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.DELAY);
        IntegrationTestBase.initTopic(collisionTopic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.DELAY);
        MessageQueue messageQueue = new MessageQueue(topic, BROKER1_NAME, 0);
        String brokerAddress = brokerController1.getBrokerAddr();

        List<Message> sendList = buildSendMessageList(topic, delaySecond);
        List<Message> recvList = new ArrayList<>();
        int recallCount = 0;

        for (Message message : sendList) {
            SendResult sendResult = producer.getProducer().send(message);
            if (sendResult.getRecallHandle() != null) {
                RecallMessageHandle.HandleV1 handleEntity =
                    (RecallMessageHandle.HandleV1) RecallMessageHandle.decodeHandle(sendResult.getRecallHandle());
                String collisionHandle = RecallMessageHandle.HandleV1.buildHandle(collisionTopic,
                    handleEntity.getBrokerName(), handleEntity.getTimestampStr(), handleEntity.getMessageId());
                String messageId = producer.getProducer().recallMessage(collisionTopic, collisionHandle);
                assertEquals(sendResult.getMsgId(), messageId);
                recallCount += 1;
            }
        }
        assertEquals(sendList.size() - 2, recallCount); // one normal and one delay-level message

        try {
            await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(delaySecond + 15, TimeUnit.SECONDS)
                .until(() -> {
                    PopResult popResult = popConsumer.pop(brokerAddress, messageQueue, 60 * 1000, -1);
                    processPopResult(recvList, popResult);
                    return recvList.size() == sendList.size();
                });
        } catch (Exception e) {
        }
        assertEquals(sendList.size(), recvList.size());
    }

    private void processPopResult(List<Message> recvList, PopResult popResult) {
        if (popResult.getPopStatus() == PopStatus.FOUND && popResult.getMsgFoundList() != null) {
            recvList.addAll(popResult.getMsgFoundList());
        }
    }

    private List<Message> buildSendMessageList(String topic, int delaySecond) {
        Message msg0 = new Message(topic, "tag", "Hello RocketMQ".getBytes()); // not supported

        Message msg1 = new Message(topic, "tag", "Hello RocketMQ".getBytes()); // not supported
        msg1.setDelayTimeLevel(2);

        Message msg2 = new Message(topic, "tag", "Hello RocketMQ".getBytes());
        msg2.setDelayTimeMs(delaySecond * 1000L);

        Message msg3 = new Message(topic, "tag", "Hello RocketMQ".getBytes());
        msg3.setDelayTimeSec(delaySecond);

        Message msg4 = new Message(topic, "tag", "Hello RocketMQ".getBytes());
        msg4.setDeliverTimeMs(System.currentTimeMillis() + delaySecond * 1000L);

        return Arrays.asList(msg0, msg1, msg2, msg3, msg4);
    }
}
