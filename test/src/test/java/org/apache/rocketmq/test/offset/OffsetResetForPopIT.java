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

package org.apache.rocketmq.test.offset;

import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopConsumer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQAdminTestUtils;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class OffsetResetForPopIT extends BaseConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetResetForPopIT.class);

    private String topic;
    private String group;
    private RMQNormalProducer producer = null;
    private RMQPopConsumer consumer = null;
    private DefaultMQAdminExt adminExt;
    private boolean consumerStarted;
    private boolean useServerSideResetOffset;
    private boolean popConsumerKVServiceEnable;
    private boolean enablePopBufferMerge;

    @Before
    public void setUp() throws Exception {
        useServerSideResetOffset = brokerController1.getBrokerConfig().isUseServerSideResetOffset();
        popConsumerKVServiceEnable = brokerController1.getBrokerConfig().isPopConsumerKVServiceEnable();
        enablePopBufferMerge = brokerController1.getBrokerConfig().isEnablePopBufferMerge();
        // reset pop offset rely on server side offset
        brokerController1.getBrokerConfig().setUseServerSideResetOffset(true);
        brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(false); // force disable before fifo resetOffset issue fixed

        adminExt = BaseConf.getAdmin(NAMESRV_ADDR);
        adminExt.start();

        topic = MQRandomUtils.getRandomTopic();
        this.createAndWaitTopicRegister(BROKER1_NAME, topic);
        group = initConsumerGroup();
        LOGGER.info(String.format("use topic: %s, group: %s", topic, group));
        producer = getProducer(NAMESRV_ADDR, topic);
    }

    @After
    public void tearDown() {
        try {
            if (consumerStarted) {
                consumer.shutdown();
            }
        } finally {
            shutdown();
            brokerController1.getBrokerConfig().setUseServerSideResetOffset(useServerSideResetOffset);
            brokerController1.getBrokerConfig().setPopConsumerKVServiceEnable(popConsumerKVServiceEnable);
            brokerController1.getBrokerConfig().setEnablePopBufferMerge(enablePopBufferMerge);
        }
    }

    private void createAndWaitTopicRegister(String brokerName, String topic) throws Exception {
        String brokerAddress = CommandUtil.fetchMasterAddrByBrokerName(adminExt, brokerName);
        TopicConfig topicConfig = new TopicConfig(topic);
        topicConfig.setReadQueueNums(1);
        topicConfig.setWriteQueueNums(1);
        adminExt.createAndUpdateTopicConfig(brokerAddress, topicConfig);

        await().atMost(30, TimeUnit.SECONDS).until(
            () -> MQAdminTestUtils.checkTopicExist(adminExt, topic));
    }

    private void startConsumer() {
        consumer.start();
        consumerStarted = true;
    }

    private void resetOffsetInner(long resetOffset) throws Exception {
        adminExt.resetOffsetByQueueId(brokerController1.getBrokerAddr(),
            consumer.getConsumerGroup(), consumer.getTopic(), 0, resetOffset);
    }

    private void ackMessageSync(MessageExt messageExt) throws Exception {
        consumer.ackAsync(brokerController1.getBrokerAddr(),
            messageExt.getProperty(MessageConst.PROPERTY_POP_CK)).get(10, TimeUnit.SECONDS);
    }

    private void ackMessageSync(List<MessageExt> messageExtList) throws Exception {
        if (messageExtList != null) {
            for (MessageExt messageExt : messageExtList) {
                ackMessageSync(messageExt);
            }
        }
    }

    @Test
    public void testResetOffsetAfterPop() throws Exception {
        int messageCount = 10;
        int resetOffset = 4;
        producer.send(messageCount);
        consumer = new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener());
        startConsumer();

        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        PopResult popResult = consumer.pop(brokerController1.getBrokerAddr(), mq);
        Assert.assertEquals(10, popResult.getMsgFoundList().size());

        resetOffsetInner(resetOffset);
        popResult = consumer.pop(brokerController1.getBrokerAddr(), mq);
        Assert.assertTrue(popResult != null && popResult.getMsgFoundList() != null);
        Assert.assertEquals(messageCount - resetOffset, popResult.getMsgFoundList().size());
    }

    @Test
    public void testResetOffsetThenAckOldForPopOrderly() throws Exception {
        int messageCount = 10;
        int resetOffset = 2;
        producer.send(messageCount);
        consumer = new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener());
        startConsumer();

        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        PopResult popResult1 = consumer.popOrderly(brokerController1.getBrokerAddr(), mq);
        Assert.assertEquals(10, popResult1.getMsgFoundList().size());

        resetOffsetInner(resetOffset);
        ConsumeStats consumeStats = adminExt.examineConsumeStats(group, topic);
        Assert.assertEquals(resetOffset, consumeStats.getOffsetTable().get(mq).getConsumerOffset());

        PopResult popResult2 = consumer.popOrderly(brokerController1.getBrokerAddr(), mq);
        Assert.assertTrue(popResult2 != null && popResult2.getMsgFoundList() != null);
        Assert.assertEquals(messageCount - resetOffset, popResult2.getMsgFoundList().size());

        // ack old msg, expect has no effect
        ackMessageSync(popResult1.getMsgFoundList());
        Assert.assertTrue(brokerController1.getConsumerOrderInfoManager()
            .checkBlock(null, topic, group, 0, RMQPopConsumer.DEFAULT_INVISIBLE_TIME));

        // ack new msg
        ackMessageSync(popResult2.getMsgFoundList());
        Assert.assertFalse(brokerController1.getConsumerOrderInfoManager()
            .checkBlock(null, topic, group, 0, RMQPopConsumer.DEFAULT_INVISIBLE_TIME));
    }

    @Test
    public void testRestOffsetToSkipMsgForPopOrderly() throws Exception {
        int messageCount = 10;
        int resetOffset = 4;
        producer.send(messageCount);
        consumer = new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener());
        resetOffsetInner(resetOffset);
        startConsumer();

        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        PopResult popResult = consumer.popOrderly(brokerController1.getBrokerAddr(), mq);
        Assert.assertEquals(messageCount - resetOffset, popResult.getMsgFoundList().size());
        Assert.assertTrue(brokerController1.getConsumerOrderInfoManager()
            .checkBlock(null, topic, group, 0, RMQPopConsumer.DEFAULT_INVISIBLE_TIME));

        ackMessageSync(popResult.getMsgFoundList());
        TimeUnit.SECONDS.sleep(1);
        Assert.assertFalse(brokerController1.getConsumerOrderInfoManager()
            .checkBlock(null, topic, group, 0, RMQPopConsumer.DEFAULT_INVISIBLE_TIME));
    }

    @Test
    public void testResetOffsetAfterPopWhenOpenBufferAndWait() throws Exception {
        int messageCount = 10;
        int resetOffset = 4;
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        producer.send(messageCount);
        consumer = new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener());
        startConsumer();

        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        PopResult popResult = consumer.pop(brokerController1.getBrokerAddr(), mq);
        Assert.assertEquals(10, popResult.getMsgFoundList().size());

        resetOffsetInner(resetOffset);
        TimeUnit.MILLISECONDS.sleep(brokerController1.getBrokerConfig().getPopCkStayBufferTimeOut());

        popResult = consumer.pop(brokerController1.getBrokerAddr(), mq);
        Assert.assertTrue(popResult != null && popResult.getMsgFoundList() != null);
        Assert.assertEquals(messageCount - resetOffset, popResult.getMsgFoundList().size());
    }

    @Test
    public void testResetOffsetWhilePopWhenOpenBuffer() {
        testResetOffsetWhilePop(8, false, false, 5);
    }

    @Test
    public void testResetOffsetWhilePopWhenOpenBufferAndAck() {
        testResetOffsetWhilePop(8, false, true, 5);
    }

    @Test
    public void testMultipleResetOffsetWhilePopWhenOpenBufferAndAck() {
        testResetOffsetWhilePop(8, false, true, 3, 5);
    }

    @Test
    public void testResetFutureOffsetWhilePopWhenOpenBufferAndAck() {
        testResetOffsetWhilePop(2, true, true, 8);
    }

    @Test
    public void testMultipleResetFutureOffsetWhilePopWhenOpenBufferAndAck() {
        testResetOffsetWhilePop(2, true, true, 5, 8);
    }

    private void testResetOffsetWhilePop(int targetCount, boolean resetFuture, boolean needAck,
        int... resetOffset) {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        producer.send(10);

        // max pop one message per request
        consumer =
            new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener(), 1);

        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        AtomicInteger counter = new AtomicInteger(0);
        startConsumer();
        // POP, ACK and reset are sequential; keep them within the test's lifetime.
        await().pollInSameThread().pollInterval(10, TimeUnit.MILLISECONDS).atMost(10, TimeUnit.SECONDS).until(() -> {
            PopResult popResult = consumer.pop(brokerController1.getBrokerAddr(), mq);
            if (popResult == null || popResult.getMsgFoundList() == null) {
                return false;
            }
            if (needAck) {
                ackMessageSync(popResult.getMsgFoundList());
            }
            int count = counter.addAndGet(popResult.getMsgFoundList().size());
            if (count == targetCount) {
                for (int offset : resetOffset) {
                    resetOffsetInner(offset);
                }
            }
            if (resetFuture) {
                Assert.assertTrue("Reset should skip messages", count < 10);
            }
            return count >= targetCount + 10 - resetOffset[resetOffset.length - 1];
        });
    }

    @Test
    public void testResetFutureOffsetWhilePopOrderlyAndAck() {
        testResetOffsetWhilePopOrderly(1,
            Lists.newArrayList(0, 5, 6, 7, 8, 9), Lists.newArrayList(5), 6);
    }

    @Test
    public void testMultipleResetFutureOffsetWhilePopOrderlyAndAck() {
        testResetOffsetWhilePopOrderly(1,
            Lists.newArrayList(0, 5, 6, 7, 8, 9), Lists.newArrayList(3, 5), 6);
    }

    @Test
    public void testResetOffsetWhilePopOrderlyAndAck() {
        testResetOffsetWhilePopOrderly(5,
            Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            Lists.newArrayList(3), 12);
    }

    @Test
    public void testMultipleResetOffsetWhilePopOrderlyAndAck() {
        testResetOffsetWhilePopOrderly(5,
            Lists.newArrayList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            Lists.newArrayList(3, 1), 14);
    }

    private void testResetOffsetWhilePopOrderly(int targetCount, List<Integer> expectMsgReceive,
        List<Integer> resetOffset, int expectCount) {
        brokerController1.getBrokerConfig().setEnablePopBufferMerge(true);
        for (int i = 0; i < 10; i++) {
            Message msg = new Message(topic, (String.valueOf(i)).getBytes());
            producer.send(msg);
        }
        consumer = new RMQPopConsumer(NAMESRV_ADDR, topic, "*", group, new RMQNormalListener(), 1);
        MessageQueue mq = new MessageQueue(topic, BROKER1_NAME, 0);
        Set<Integer> msgReceive = new HashSet<>();
        AtomicInteger counter = new AtomicInteger(0);
        startConsumer();

        await().pollInSameThread().pollInterval(10, TimeUnit.MILLISECONDS).atMost(10, TimeUnit.SECONDS).until(() -> {
            PopResult popResult = consumer.popOrderly(brokerController1.getBrokerAddr(), mq);
            if (popResult == null || popResult.getMsgFoundList() == null) {
                return false;
            }
            for (MessageExt messageExt : popResult.getMsgFoundList()) {
                msgReceive.add(Integer.valueOf(new String(messageExt.getBody())));
                ackMessageSync(messageExt);
            }
            int count = counter.addAndGet(popResult.getMsgFoundList().size());
            if (count == targetCount) {
                for (int offset : resetOffset) {
                    resetOffsetInner(offset);
                }
            }
            return count >= expectCount;
        });
        Assert.assertEquals(expectCount, counter.get());
        Assert.assertEquals(new HashSet<>(expectMsgReceive), msgReceive);
    }
}
