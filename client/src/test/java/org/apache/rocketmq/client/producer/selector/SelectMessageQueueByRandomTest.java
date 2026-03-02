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
package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class SelectMessageQueueByRandomTest {

    private final SelectMessageQueueByRandom selector = new SelectMessageQueueByRandom();

    private final String defaultBroker = "defaultBroker";

    private final String defaultTopic = "defaultTopic";

    @Test
    public void testSelectRandomMessageQueue() {
        List<MessageQueue> messageQueues = createMessageQueues(10);
        Message message = new Message(defaultTopic, "tag", "key", "body".getBytes());
        MessageQueue selectedQueue = selector.select(messageQueues, message, null);
        assertNotNull(selectedQueue);
        assertEquals(messageQueues.size(), 10);
        assertEquals(defaultTopic, selectedQueue.getTopic());
        assertEquals(defaultBroker, selectedQueue.getBrokerName());
    }

    @Test
    public void testSelectEmptyMessageQueue() {
        List<MessageQueue> emptyQueues = new ArrayList<>();
        Message message = new Message(defaultTopic, "tag", "key", "body".getBytes());
        assertThrows(IllegalArgumentException.class, () -> selector.select(emptyQueues, message, null));
    }

    @Test
    public void testSelectSingleMessageQueue() {
        List<MessageQueue> singleQueueList = createMessageQueues(1);
        Message message = new Message(defaultTopic, "tag", "key", "body".getBytes());
        MessageQueue selectedQueue = selector.select(singleQueueList, message, null);
        assertNotNull(selectedQueue);
        assertEquals(defaultTopic, selectedQueue.getTopic());
        assertEquals(defaultBroker, selectedQueue.getBrokerName());
        assertEquals(singleQueueList.get(0).getQueueId(), selectedQueue.getQueueId());
    }

    private List<MessageQueue> createMessageQueues(final int count) {
        List<MessageQueue> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(new MessageQueue(defaultTopic, defaultBroker, i));
        }
        return result;
    }
}
