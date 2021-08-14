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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectMessageQueueSpecialLabelTest {

    private String topic;

    @Before
    public void init() {
        topic = "topic_test";
    }

    @Test
    public void testSelect() {

        int messageQueueCount = 16;
        int brokerCount = 2;
        double percentage = 0.2;
        SelectMessageQueueSpecialLabel selectMessageQueueSpecialLabel = new SelectMessageQueueSpecialLabel(percentage);

        List<MessageQueue> messageQueueList = createMessageQueueList(messageQueueCount, brokerCount);
        Message message = new Message(topic, new byte[] {});

        MessageQueue messageQueueGray = selectMessageQueueSpecialLabel.select(messageQueueList, message, MixAll.GRAY_DEPLOYMENT);
        MessageQueue messageQueueNormal = selectMessageQueueSpecialLabel.select(messageQueueList, message, null);

        int queueSizePerBroker = (int) Math.ceil(messageQueueCount * percentage);

        assertThat(messageQueueGray.getQueueId() >=0 && messageQueueGray.getQueueId() < queueSizePerBroker);
        assertThat(messageQueueNormal.getQueueId() >=queueSizePerBroker && messageQueueNormal.getQueueId() < messageQueueCount);
    }


    public List<MessageQueue> createMessageQueueList(int size, int brokerCount) {
        if (brokerCount < 0 || brokerCount > 20) {
            brokerCount = 2;
        }
        List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>(size);
        for (int i = 0; i < brokerCount; i++) {
            for (int j = 0; j < size; j++) {
                MessageQueue mq = new MessageQueue(topic, "brokerName-" + i, j);
                messageQueueList.add(mq);
            }
        }
        return messageQueueList;
    }

}
