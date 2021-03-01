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

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class SelectMessageQueueRetryTest {

    private String topic = "TEST";

    @Test
    public void testSelect() throws Exception {

        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        List<MessageQueue> messageQueueList = new ArrayList();
        for (int i = 0; i < 3; i++) {
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName("broker-" + i);
            mq.setQueueId(0);
            mq.setTopic(topic);
            messageQueueList.add(mq);
        }

        topicPublishInfo.setMessageQueueList(messageQueueList);

        Set<String> retryBrokerNameSet = retryBroker(topicPublishInfo);
        //always in Set （broker-0，broker-1，broker-2）
        assertThat(retryBroker(topicPublishInfo)).isEqualTo(retryBrokerNameSet);
    }

    private Set<String> retryBroker(TopicPublishInfo topicPublishInfo) {
        MessageQueue mqTmp = null;
        Set<String> retryBrokerNameSet = new HashSet();
        for (int times = 0; times < 3; times++) {
            String lastBrokerName = null == mqTmp ? null : mqTmp.getBrokerName();
            mqTmp = topicPublishInfo.selectOneMessageQueue(lastBrokerName);
            retryBrokerNameSet.add(mqTmp.getBrokerName());
        }
        return retryBrokerNameSet;
    }

}