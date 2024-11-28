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

import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDataEncoder;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.test.client.rmq.RMQPopConsumer;
import org.apache.rocketmq.test.factory.ConsumerFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQRandomUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class RecallWithTraceIT extends BaseConf {
    private static String topic;
    private static String group;
    private static DefaultMQProducer producer;
    private static RMQPopConsumer popConsumer;

    @BeforeClass
    public static void init() throws MQClientException {
        System.setProperty("com.rocketmq.recall.default.trace.enable", Boolean.TRUE.toString());
        topic = MQRandomUtils.getRandomTopic();
        IntegrationTestBase.initTopic(topic, NAMESRV_ADDR, BROKER1_NAME, 1, CQType.SimpleCQ, TopicMessageType.NORMAL);
        group = initConsumerGroup();
        producer = new DefaultMQProducer(group, true, topic);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.start();
        popConsumer = ConsumerFactory.getRMQPopConsumer(NAMESRV_ADDR, group, topic, "*", new RMQNormalListener());
        mqClients.add(popConsumer);
        mqClients.add(producer);
    }

    @AfterClass
    public static void tearDown() {
        shutdown();
    }

    @Test
    public void testRecallTrace() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        String msgId = MessageClientIDSetter.createUniqID();
        String recallHandle = RecallMessageHandle.HandleV1.buildHandle(topic, BROKER1_NAME,
            String.valueOf(System.currentTimeMillis() + 30000), msgId);
        producer.recallMessage(topic, recallHandle);

        MessageQueue messageQueue = new MessageQueue(topic, BROKER1_NAME, 0);
        String brokerAddress = brokerController1.getBrokerAddr();
        AtomicReference<MessageExt> traceMessage = new AtomicReference();
        await()
            .pollInterval(1, TimeUnit.SECONDS)
            .atMost(15, TimeUnit.SECONDS)
            .until(() -> {
                PopResult popResult = popConsumer.pop(brokerAddress, messageQueue, 60 * 1000, -1);
                boolean found = popResult.getPopStatus().equals(PopStatus.FOUND);
                traceMessage.set(found ? popResult.getMsgFoundList().get(0) : null);
                return found;
            });

        Assert.assertNotNull(traceMessage.get());
        TraceContext context =
            TraceDataEncoder.decoderFromTraceDataString(new String(traceMessage.get().getBody())).get(0);
        Assert.assertEquals(TraceType.Recall, context.getTraceType());
        Assert.assertEquals(group, context.getGroupName());
        Assert.assertTrue(context.isSuccess());
        Assert.assertEquals(msgId, context.getTraceBeans().get(0).getMsgId());
    }
}
