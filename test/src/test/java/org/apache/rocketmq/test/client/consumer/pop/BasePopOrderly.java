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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import static org.junit.Assert.assertEquals;

@Ignore
public class BasePopOrderly extends BasePop {
    protected String topic;
    protected String group;
    protected RMQNormalProducer producer = null;
    protected RMQPopClient client = null;
    protected String brokerAddr;
    protected MessageQueue messageQueue;
    protected final Map<String, List<MsgRcv>> msgRecv = new ConcurrentHashMap<>();
    protected final List<String> msgRecvSequence = new CopyOnWriteArrayList<>();
    protected final List<Object> msgDataRecv = new CopyOnWriteArrayList<>();

    @Before
    public void setUp() {
        brokerController1.getBrokerConfig().setEnableNotifyAfterPopOrderLockRelease(true);
        brokerAddr = brokerController1.getBrokerAddr();
        topic = MQRandomUtils.getRandomTopic();
        group = initConsumerGroup();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.FIFO);
        producer = getProducer(NAMESRV_ADDR, topic);
        client = getRMQPopClient();
        messageQueue = new MessageQueue(topic, BROKER1_NAME, -1);
    }

    @After
    public void tearDown() {
        shutdown();
    }

    protected void sendMessage(int num) {
        MessageQueueMsg mqMsgs = new MessageQueueMsg(Lists.newArrayList(messageQueue), num);
        producer.send(mqMsgs.getMsgsWithMQ());
    }

    protected void assertMessageRecvOrder() {
        VerifyUtils.verifyOrderMsg(msgDataRecv);
    }

    protected void assertMsgRecv(int seqId, int expectNum) {
        String msgId = msgRecvSequence.get(seqId);
        List<MsgRcv> msgRcvList = msgRecv.get(msgId);
        assertEquals(expectNum, msgRcvList.size());
        assertConsumeTimes(msgRcvList);
    }

    protected void assertConsumeTimes(List<MsgRcv> msgRcvList) {
        for (int i = 0; i < msgRcvList.size(); i++) {
            assertEquals(i, msgRcvList.get(i).messageExt.getReconsumeTimes());
        }
    }

    protected void assertMsgRecv(int seqId, int expectNum, List<Integer> expectReconsumeTimes) {
        String msgId = msgRecvSequence.get(seqId);
        List<MsgRcv> msgRcvList = msgRecv.get(msgId);
        assertEquals(expectNum, msgRcvList.size());
        assertConsumeTimes(msgRcvList, expectReconsumeTimes);
    }

    protected void assertConsumeTimes(List<MsgRcv> msgRcvList, List<Integer> expectReconsumeTimes) {
        for (int i = 0; i < msgRcvList.size(); i++) {
            assertEquals(expectReconsumeTimes.get(i).intValue(), msgRcvList.get(i).messageExt.getReconsumeTimes());
        }
    }

    protected void onRecvNewMessage(MessageExt messageExt) {
        msgDataRecv.add(new String(messageExt.getBody()));
        msgRecvSequence.add(messageExt.getMsgId());
        msgRecv.compute(messageExt.getMsgId(), (k, msgRcvList) -> {
            if (msgRcvList == null) {
                msgRcvList = new CopyOnWriteArrayList<>();
            }
            msgRcvList.add(new MsgRcv(System.currentTimeMillis(), messageExt));
            return msgRcvList;
        });
    }

    protected CompletableFuture<PopResult> popMessageOrderlyAsync(long invisibleTime, int maxNums, long timeout) {
        return popMessageOrderlyAsync(invisibleTime, maxNums, timeout, null);
    }

    protected CompletableFuture<PopResult> popMessageOrderlyAsync(long invisibleTime, int maxNums, long timeout, String attemptId) {
        return client.popMessageAsync(
            brokerAddr, messageQueue, invisibleTime, maxNums, group, timeout, true,
            ConsumeInitMode.MIN, true, ExpressionType.TAG, "*", attemptId);
    }

    protected CompletableFuture<AckResult> ackMessageAsync(MessageExt messageExt) {
        return client.ackMessageAsync(brokerAddr, topic, group, messageExt.getProperty(MessageConst.PROPERTY_POP_CK));
    }

    protected CompletableFuture<AckResult> changeInvisibleTimeAsync(MessageExt messageExt, long invisibleTime) {
        return client.changeInvisibleTimeAsync(
            brokerAddr, BROKER1_NAME, topic, group,
            messageExt.getProperty(MessageConst.PROPERTY_POP_CK), invisibleTime);
    }
}
