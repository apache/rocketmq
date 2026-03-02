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

package org.apache.rocketmq.test.client.consumer.pop;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.client.rmq.RMQPopClient;
import org.apache.rocketmq.test.message.MessageQueueMsg;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class NotificationIT extends BasePop {
    protected String topic;
    protected String group;
    protected RMQNormalProducer producer = null;
    protected RMQPopClient client = null;
    protected String brokerAddr;
    protected MessageQueue messageQueue;

    @Before
    public void setUp() {
        brokerAddr = brokerController1.getBrokerAddr();
        topic = MQRandomUtils.getRandomTopic();
        group = initConsumerGroup();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 8, CQType.SimpleCQ, TopicMessageType.NORMAL);
        producer = getProducer(NAMESRV_ADDR, topic);
        client = getRMQPopClient();
        messageQueue = new MessageQueue(topic, BROKER1_NAME, -1);
    }

    @Test
    @Ignore
    public void testNotification() throws Exception {
        long pollTime = 500;
        CompletableFuture<Boolean> future1 = client.notification(brokerAddr, topic, group, messageQueue.getQueueId(), pollTime, System.currentTimeMillis(), 5000);
        CompletableFuture<Boolean> future2 = client.notification(brokerAddr, topic, group, messageQueue.getQueueId(), pollTime, System.currentTimeMillis(), 5000);
        sendMessage(1);
        Boolean result2 = future2.get();
        assertThat(result2).isTrue();
        client.popMessageAsync(brokerAddr, messageQueue, 10000, 1, group, 1000, false,
            ConsumeInitMode.MIN, false, null, null).get();
        Boolean result1 = future1.get();
        assertThat(result1).isFalse();
    }

    @Test
    public void testNotificationOrderly() throws Exception {
        long pollTime = 500;
        String attemptId = "attemptId";
        CompletableFuture<Boolean> future1 = client.notification(brokerAddr, topic, group, messageQueue.getQueueId(), true, attemptId, pollTime, System.currentTimeMillis(), 5000);
        CompletableFuture<Boolean> future2 = client.notification(brokerAddr, topic, group, messageQueue.getQueueId(), true, attemptId, pollTime, System.currentTimeMillis(), 5000);
        sendMessage(1);
        Boolean result1 = future1.get();
        assertThat(result1).isTrue();
        client.popMessageAsync(brokerAddr, messageQueue, 10000, 1, group, 1000, false,
            ConsumeInitMode.MIN, true, null, null, attemptId).get();
        Boolean result2 = future2.get();
        assertThat(result2).isTrue();

        String attemptId2 = "attemptId2";
        CompletableFuture<Boolean> future3 = client.notification(brokerAddr, topic, group, messageQueue.getQueueId(), true, attemptId2, pollTime, System.currentTimeMillis(), 5000);
        assertThat(future3.get()).isFalse();
    }

    protected void sendMessage(int num) {
        MessageQueueMsg mqMsgs = new MessageQueueMsg(Lists.newArrayList(messageQueue), num);
        producer.send(mqMsgs.getMsgsWithMQ());
    }

}
