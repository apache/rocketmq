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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectMessageQueueByMachineRoomTest {

    private String topic = "TopicTest";

    @Test
    public void testSelect() throws Exception {
        SelectMessageQueueByMachineRoom selector = new SelectMessageQueueByMachineRoom();
        Message message = new Message(topic, new byte[] {});
        List<MessageQueue> messageQueues = new ArrayList<MessageQueue>();
        for (int i = 0; i < 10; i++) {
            MessageQueue messageQueue = new MessageQueue(topic, String.format("MR%d@DefaultBroker", i % 2), i);
            messageQueues.add(messageQueue);
        }

        String orderId = "123";
        String anotherOrderId = "234";
        selector.setConsumeridcs(new HashSet<>());
        assertThat(selector.select(messageQueues, message, null)).isNotNull();
        MessageQueue selected = selector.select(messageQueues, message, orderId);
        assertThat(selected).isNotNull();
        assertThat(selector.select(messageQueues, message, anotherOrderId)).isNotEqualTo(selected);

        selector.getConsumeridcs().add("MR0");
        selected = selector.select(messageQueues, message, orderId);
        assertThat(selected).isNotNull();
        assertThat(selector.select(messageQueues, message, anotherOrderId)).isNotEqualTo(selected);
        assertThat(selected.getBrokerName().split("@")[0]).isEqualTo("MR0");
    }

}