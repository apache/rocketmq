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

import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MessageQueueSelectorTest extends BaseServiceTest {

    @Test
    public void testReadMessageQueue() {
        queueData.setPerm(PermName.PERM_READ);
        queueData.setReadQueueNums(0);
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), true);
        assertTrue(messageQueueSelector.getQueues().isEmpty());

        queueData.setPerm(PermName.PERM_READ);
        queueData.setReadQueueNums(3);
        messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), true);
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
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), false);
        assertTrue(messageQueueSelector.getQueues().isEmpty());

        queueData.setPerm(PermName.PERM_WRITE);
        queueData.setWriteQueueNums(3);
        messageQueueSelector = new MessageQueueSelector(new TopicRouteWrapper(topicRouteData, TOPIC), false);
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
}