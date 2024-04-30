/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.route;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class MessageQueueSelectorTest extends BaseServiceTest {

    private static final String BROKER_NAME_0 = "broker-0";
    private static final String BROKER_NAME_1 = "broker-1";
    private static final String BROKER_ADDR_0 = "127.0.0.1:10911";
    private static final String BROKER_ADDR_1 = "127.0.0.1:10912";

    private MQFaultStrategy mqFaultStrategy;

    @Override
    public void before() throws Throwable {
        super.before();
        mqFaultStrategy = mock(MQFaultStrategy.class);
    }

    @Test
    public void testReadMessageQueue() {
        queueData.setPerm(PermName.PERM_READ);
        queueData.setReadQueueNums(0);
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), null, true);
        assertTrue(messageQueueSelector.getQueues().isEmpty());

        queueData.setPerm(PermName.PERM_READ);
        queueData.setReadQueueNums(3);
        messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), null, true);
        assertEquals(3, messageQueueSelector.getQueues().size());
        assertEquals(1, messageQueueSelector.getBrokerActingQueues().size());
        for (int i = 0; i < messageQueueSelector.getQueues().size(); i++) {
            AddressableMessageQueue messageQueue = messageQueueSelector.getQueues().get(i);
            assertEquals(i, messageQueue.getQueueId());
        }

        AddressableMessageQueue brokerQueue = messageQueueSelector.getQueueByBrokerName(BROKER_NAME);
        assertEquals(brokerQueue, messageQueueSelector.getBrokerActingQueues().get(0));
        assertEquals(brokerQueue, messageQueueSelector.selectOne(true));
        assertEquals(brokerQueue, messageQueueSelector.selectOneByIndex(3, true));

        AddressableMessageQueue queue = messageQueueSelector.selectOne(false);
        messageQueueSelector.selectOne(false);
        messageQueueSelector.selectOne(false);
        assertEquals(queue, messageQueueSelector.selectOne(false));
    }

    @Test
    public void testWriteMessageQueue() {
        queueData.setPerm(PermName.PERM_WRITE);
        queueData.setReadQueueNums(0);
        MessageQueueWriteSelector messageQueueSelector = new MessageQueueWriteSelector(new TopicRouteWrapper(topicRouteData, TOPIC), null);
        assertTrue(messageQueueSelector.getQueues().isEmpty());

        queueData.setPerm(PermName.PERM_WRITE);
        queueData.setWriteQueueNums(3);
        messageQueueSelector = new MessageQueueWriteSelector(new TopicRouteWrapper(topicRouteData, TOPIC), null);
        assertEquals(3, messageQueueSelector.getQueues().size());
        assertEquals(1, messageQueueSelector.getBrokerActingQueues().size());
        for (int i = 0; i < messageQueueSelector.getQueues().size(); i++) {
            AddressableMessageQueue messageQueue = messageQueueSelector.getQueues().get(i);
            assertEquals(i, messageQueue.getQueueId());
        }

        AddressableMessageQueue brokerQueue = messageQueueSelector.getQueueByBrokerName(BROKER_NAME);
        assertEquals(brokerQueue, messageQueueSelector.getBrokerActingQueues().get(0));
        assertEquals(brokerQueue, messageQueueSelector.selectOne(true));
        assertEquals(brokerQueue, messageQueueSelector.selectOneByIndex(3, true));

        AddressableMessageQueue queue = messageQueueSelector.selectOne(false);
        messageQueueSelector.selectOne(false);
        messageQueueSelector.selectOne(false);
        assertEquals(queue, messageQueueSelector.selectOne(false));
    }

    @Test
    public void testWriteSelectOneByPipeline() throws Exception {
        HashMap<Long, String> brokerAddrs0 = new HashMap<>();
        brokerAddrs0.put(MixAll.MASTER_ID, BROKER_ADDR_0);
        BrokerData brokerData0 = new BrokerData(CLUSTER_NAME, BROKER_NAME_0, brokerAddrs0);
        HashMap<Long, String> brokerAddrs1 = new HashMap<>();
        brokerAddrs1.put(MixAll.MASTER_ID, BROKER_ADDR_1);
        BrokerData brokerData1 = new BrokerData(CLUSTER_NAME, BROKER_NAME_1, brokerAddrs1);
        topicRouteData.setBrokerDatas(Arrays.asList(brokerData0, brokerData1));

        QueueData queueData0 = new QueueData();
        queueData0.setBrokerName(BROKER_NAME_0);
        queueData0.setPerm(PermName.PERM_WRITE);
        queueData0.setWriteQueueNums(3);
        QueueData queueData1 = new QueueData();
        queueData1.setBrokerName(BROKER_NAME_1);
        queueData1.setPerm(PermName.PERM_WRITE);
        queueData1.setWriteQueueNums(3);
        topicRouteData.setQueueDatas(Arrays.asList(queueData0, queueData1));

        MessageQueueWriteSelector messageQueueWriteSelector = new MessageQueueWriteSelector(new TopicRouteWrapper(topicRouteData, TOPIC), mqFaultStrategy);

        // broker-0 is not available, expected to select broker-1
        TopicPublishInfo.QueueFilter availableFilter = mq -> !BROKER_NAME_0.equals(mq.getBrokerName());
        TopicPublishInfo.QueueFilter reachableFilter = mq -> true;

        when(mqFaultStrategy.isSendLatencyFaultEnable()).thenReturn(true);
        when(mqFaultStrategy.getAvailableFilter()).thenReturn(availableFilter);
        when(mqFaultStrategy.getReachableFilter()).thenReturn(reachableFilter);

        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(false);
            assertEquals(BROKER_NAME_1, messageQueue.getBrokerName());
        }

        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(true);
            assertEquals(BROKER_NAME_1, messageQueue.getBrokerName());
        }

        // both are not available, broker-1 is not reachable, expected to select broker-0
        availableFilter = mq -> false;
        reachableFilter = mq -> !BROKER_NAME_1.equals(mq.getBrokerName());

        when(mqFaultStrategy.getAvailableFilter()).thenReturn(availableFilter);
        when(mqFaultStrategy.getReachableFilter()).thenReturn(reachableFilter);

        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(false);
            assertEquals(BROKER_NAME_0, messageQueue.getBrokerName());
        }

        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(true);
            assertEquals(BROKER_NAME_0, messageQueue.getBrokerName());
        }

        // both are not available and reachable, expected to select by RR
        availableFilter = mq -> false;
        reachableFilter = mq -> false;

        when(mqFaultStrategy.getAvailableFilter()).thenReturn(availableFilter);
        when(mqFaultStrategy.getReachableFilter()).thenReturn(reachableFilter);

        HashMap<MessageQueue, AtomicInteger> resultMap = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(false);
            resultMap.computeIfAbsent(messageQueue.getMessageQueue(), k -> new AtomicInteger()).incrementAndGet();
        }
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_0, 0)).get());
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_0, 1)).get());
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_0, 2)).get());
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_1, 0)).get());
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_1, 1)).get());
        assertEquals(1, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_1, 2)).get());

        resultMap.clear();
        for (int i = 0; i < 6; i++) {
            AddressableMessageQueue messageQueue = messageQueueWriteSelector.selectOneByPipeline(true);
            resultMap.computeIfAbsent(messageQueue.getMessageQueue(), k -> new AtomicInteger()).incrementAndGet();
        }
        assertEquals(3, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_0, -1)).get());
        assertEquals(3, resultMap.get(new MessageQueue(TOPIC, BROKER_NAME_1, -1)).get());
    }
}