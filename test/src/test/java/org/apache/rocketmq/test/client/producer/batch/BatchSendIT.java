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
import java.util.UUID;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.consumer.tag.TagMessageWith1ConsumerIT;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BatchSendIT extends BaseConf {
    private static Logger logger = LoggerFactory.getLogger(TagMessageWith1ConsumerIT.class);
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
        Assert.assertTrue(brokerController1.getMessageStore() instanceof DefaultMessageStore);
        Assert.assertTrue(brokerController2.getMessageStore() instanceof DefaultMessageStore);

        List<Message> messageList = new ArrayList<>();
        int batchNum = 100;
        for (int i = 0; i < batchNum; i++) {
            messageList.add(new Message(topic, RandomUtils.getStringByUUID().getBytes()));
        }

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        removeBatchUniqueId(producer);

        SendResult sendResult = producer.send(messageList);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());

        String[] offsetIds = sendResult.getOffsetMsgId().split(",");
        String[] msgIds = sendResult.getMsgId().split(",");
        Assert.assertEquals(messageList.size(), offsetIds.length);
        Assert.assertEquals(messageList.size(), msgIds.length);

        Thread.sleep(2000);

        for (int i = 0; i < 3; i++) {
            producer.viewMessage(topic, offsetIds[random.nextInt(batchNum)]);
        }
        for (int i = 0; i < 3; i++) {
            producer.viewMessage(topic, msgIds[random.nextInt(batchNum)]);
        }
    }

    @Test
    public void testBatchSend_SysInnerBatch() throws Exception {
        waitBrokerRegistered(NAMESRV_ADDR, CLUSTER_NAME, BROKER_NUM);

        String batchTopic = UUID.randomUUID().toString();
        IntegrationTestBase.initTopic(batchTopic, NAMESRV_ADDR, CLUSTER_NAME, CQType.BatchCQ);

        Assert.assertEquals(CQType.BatchCQ.toString(), brokerController1.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getAttributes().get(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName()));
        Assert.assertEquals(CQType.BatchCQ.toString(), brokerController2.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getAttributes().get(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName()));
        Assert.assertEquals(CQType.BatchCQ.toString(), brokerController3.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getAttributes().get(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName()));
        Assert.assertEquals(8, brokerController1.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController2.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController3.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(-1, brokerController1.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(-1, brokerController2.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(-1, brokerController3.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController1.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController2.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController3.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        MessageQueue messageQueue = producer.fetchPublishMessageQueues(batchTopic).iterator().next();

        int batchCount = 10;
        int batchNum = 10;
        for (int i = 0; i < batchCount; i++) {
            List<Message> messageList = new ArrayList<>();
            for (int j = 0; j < batchNum; j++) {
                messageList.add(new Message(batchTopic, RandomUtils.getStringByUUID().getBytes()));
            }
            SendResult sendResult = producer.send(messageList, messageQueue);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
            Assert.assertEquals(messageQueue.getQueueId(), sendResult.getMessageQueue().getQueueId());
            Assert.assertEquals(i * batchNum, sendResult.getQueueOffset());
            Assert.assertEquals(1, sendResult.getMsgId().split(",").length);
        }
        Thread.sleep(300);
        {
            DefaultMQPullConsumer defaultMQPullConsumer = ConsumerFactory.getRMQPullConsumer(NAMESRV_ADDR, "group");

            PullResult pullResult = defaultMQPullConsumer.pullBlockIfNotFound(messageQueue, "*", 5, batchCount * batchNum);
            Assert.assertEquals(PullStatus.FOUND, pullResult.getPullStatus());
            Assert.assertEquals(0, pullResult.getMinOffset());
            Assert.assertEquals(batchCount * batchNum, pullResult.getMaxOffset());
            Assert.assertEquals(batchCount * batchNum, pullResult.getMsgFoundList().size());
            MessageExt first = pullResult.getMsgFoundList().get(0);
            for (int i = 0; i < pullResult.getMsgFoundList().size(); i++) {
                MessageExt messageExt = pullResult.getMsgFoundList().get(i);
                if (i % batchNum == 0) {
                    first = messageExt;
                }
                Assert.assertEquals(i, messageExt.getQueueOffset());
                Assert.assertEquals(batchTopic, messageExt.getTopic());
                Assert.assertEquals(messageQueue.getQueueId(), messageExt.getQueueId());
                Assert.assertEquals(first.getBornHostString(), messageExt.getBornHostString());
                Assert.assertEquals(first.getBornHostNameString(), messageExt.getBornHostNameString());
                Assert.assertEquals(first.getBornTimestamp(), messageExt.getBornTimestamp());
                Assert.assertEquals(first.getStoreTimestamp(), messageExt.getStoreTimestamp());
            }
        }
    }

    @Test
    public void testBatchSend_SysOuterBatch() throws Exception {
        Assert.assertTrue(brokerController1.getMessageStore() instanceof DefaultMessageStore);
        Assert.assertTrue(brokerController2.getMessageStore() instanceof DefaultMessageStore);
        Assert.assertTrue(brokerController3.getMessageStore() instanceof DefaultMessageStore);

        String batchTopic = UUID.randomUUID().toString();
        IntegrationTestBase.initTopic(batchTopic, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        Assert.assertEquals(8, brokerController1.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController2.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController3.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(0, brokerController1.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController2.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController3.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController1.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController2.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController3.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        MessageQueue messageQueue = producer.fetchPublishMessageQueues(batchTopic).iterator().next();

        int batchCount = 10;
        int batchNum = 10;
        for (int i = 0; i < batchCount; i++) {
            List<Message> messageList = new ArrayList<>();
            for (int j = 0; j < batchNum; j++) {
                messageList.add(new Message(batchTopic, RandomUtils.getStringByUUID().getBytes()));
            }
            SendResult sendResult = producer.send(messageList, messageQueue);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
            Assert.assertEquals(messageQueue.getQueueId(), sendResult.getMessageQueue().getQueueId());
            Assert.assertEquals(i * batchNum, sendResult.getQueueOffset());
            Assert.assertEquals(10, sendResult.getMsgId().split(",").length);
        }
        Thread.sleep(300);
        {
            DefaultMQPullConsumer defaultMQPullConsumer = ConsumerFactory.getRMQPullConsumer(NAMESRV_ADDR, "group");

            long startOffset = 5;
            PullResult pullResult = defaultMQPullConsumer.pullBlockIfNotFound(messageQueue, "*", startOffset, batchCount * batchNum);
            Assert.assertEquals(PullStatus.FOUND, pullResult.getPullStatus());
            Assert.assertEquals(0, pullResult.getMinOffset());
            Assert.assertEquals(batchCount * batchNum, pullResult.getMaxOffset());
            Assert.assertEquals(batchCount * batchNum - startOffset, pullResult.getMsgFoundList().size());
            for (int i = 0; i < pullResult.getMsgFoundList().size(); i++) {
                MessageExt messageExt = pullResult.getMsgFoundList().get(i);
                Assert.assertEquals(i + startOffset, messageExt.getQueueOffset());
                Assert.assertEquals(batchTopic, messageExt.getTopic());
                Assert.assertEquals(messageQueue.getQueueId(), messageExt.getQueueId());
            }
        }
    }

    @Test
    public void testBatchSend_CompressionBody() throws Exception {
        Assert.assertTrue(brokerController1.getMessageStore() instanceof DefaultMessageStore);
        Assert.assertTrue(brokerController2.getMessageStore() instanceof DefaultMessageStore);
        Assert.assertTrue(brokerController3.getMessageStore() instanceof DefaultMessageStore);

        String batchTopic = UUID.randomUUID().toString();
        IntegrationTestBase.initTopic(batchTopic, NAMESRV_ADDR, CLUSTER_NAME, CQType.SimpleCQ);
        Assert.assertEquals(8, brokerController1.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController2.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(8, brokerController3.getTopicConfigManager().getTopicConfigTable().get(batchTopic).getReadQueueNums());
        Assert.assertEquals(0, brokerController1.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController2.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController3.getMessageStore().getMinOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController1.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController2.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));
        Assert.assertEquals(0, brokerController3.getMessageStore().getMaxOffsetInQueue(batchTopic, 0));

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        MessageQueue messageQueue = producer.fetchPublishMessageQueues(batchTopic).iterator().next();
        int bodyCompressionThreshold = producer.getCompressMsgBodyOverHowmuch();

        int bodyLen = bodyCompressionThreshold + 1;
        int batchCount = 10;
        int batchNum = 10;
        for (int i = 0; i < batchCount; i++) {
            List<Message> messageList = new ArrayList<>();
            for (int j = 0; j < batchNum; j++) {
                messageList.add(new Message(batchTopic, RandomUtils.getStringWithNumber(bodyLen).getBytes()));
            }
            SendResult sendResult = producer.send(messageList, messageQueue);
            Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
            Assert.assertEquals(messageQueue.getQueueId(), sendResult.getMessageQueue().getQueueId());
            Assert.assertEquals(i * batchNum, sendResult.getQueueOffset());
            Assert.assertEquals(10, sendResult.getMsgId().split(",").length);
        }
        Thread.sleep(300);
        {
            DefaultMQPullConsumer defaultMQPullConsumer = ConsumerFactory.getRMQPullConsumer(NAMESRV_ADDR, "group");
            defaultMQPullConsumer.setDecodeDecompressBody(true);
            long startOffset = 5;
            PullResult pullResult = defaultMQPullConsumer.pullBlockIfNotFound(messageQueue, "*", startOffset, batchCount * batchNum);
            Assert.assertEquals(PullStatus.FOUND, pullResult.getPullStatus());
            Assert.assertEquals(0, pullResult.getMinOffset());
            Assert.assertEquals(batchCount * batchNum, pullResult.getMaxOffset());
            Assert.assertEquals(batchCount * batchNum - startOffset, pullResult.getMsgFoundList().size());
            for (int i = 0; i < pullResult.getMsgFoundList().size(); i++) {
                MessageExt messageExt = pullResult.getMsgFoundList().get(i);
                Assert.assertEquals(i + startOffset, messageExt.getQueueOffset());
                Assert.assertEquals(batchTopic, messageExt.getTopic());
                Assert.assertEquals(messageQueue.getQueueId(), messageExt.getQueueId());
                if (bodyLen != messageExt.getBody().length) {
                    logger.error("decompress fail? {} {}", i, messageExt);
                }
                Assert.assertEquals(bodyLen, messageExt.getBody().length);
                Assert.assertTrue((messageExt.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) != 0);
                Assert.assertTrue(messageExt.getStoreSize() < bodyLen);
            }
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

        DefaultMQProducer producer = ProducerFactory.getRMQProducer(NAMESRV_ADDR);
        removeBatchUniqueId(producer);

        SendResult sendResult = producer.send(messageList);
        Assert.assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());

        String[] offsetIds = sendResult.getOffsetMsgId().split(",");
        String[] msgIds = sendResult.getMsgId().split(",");
        Assert.assertEquals(messageList.size(), offsetIds.length);
        Assert.assertEquals(messageList.size(), msgIds.length);

        Thread.sleep(2000);

        Message messageByOffset = producer.viewMessage(topic, offsetIds[0]);
        Message messageByMsgId = producer.viewMessage(topic, msgIds[0]);

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

    // simulate legacy batch message send.
    private void removeBatchUniqueId(DefaultMQProducer producer) {
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return null;
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                MessageBatch messageBatch = (MessageBatch) context.getMessage();
                if (messageBatch.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) != null) {
                    messageBatch.getProperties().remove(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                }
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {
            }
        });
    }

}
