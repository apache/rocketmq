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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicPublishInfoTest {

    private static final String TOPIC = "TopicPublishInfoTest";

    private TopicPublishInfo emptyRouteInfo() {
        // A topic whose route exists but has no writable queue (e.g. read-only
        // brokers) yields an empty queue list, so ok() is false.
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        topicPublishInfo.setMessageQueueList(new ArrayList<>());
        return topicPublishInfo;
    }

    @Test
    public void testSelectOneMessageQueueWithEmptyQueueList() {
        TopicPublishInfo topicPublishInfo = emptyRouteInfo();

        assertThat(topicPublishInfo.selectOneMessageQueue()).isNull();
    }

    @Test
    public void testSelectOneMessageQueueWithNullQueueList() {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        topicPublishInfo.setMessageQueueList(null);

        assertThat(topicPublishInfo.selectOneMessageQueue()).isNull();
    }

    @Test
    public void testSelectOneMessageQueueLastBrokerNameWithEmptyQueueList() {
        TopicPublishInfo topicPublishInfo = emptyRouteInfo();

        assertThat(topicPublishInfo.selectOneMessageQueue("broker-a")).isNull();
    }

    @Test
    public void testSelectOneMessageQueueLastBrokerNameWithNullQueueList() {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        topicPublishInfo.setMessageQueueList(null);

        assertThat(topicPublishInfo.selectOneMessageQueue("broker-a")).isNull();
    }

    @Test
    public void testSelectOneMessageQueueAvoidsLastBrokerWhenPossible() {
        TopicPublishInfo topicPublishInfo = new TopicPublishInfo();
        List<MessageQueue> messageQueueList = new ArrayList<>();
        messageQueueList.add(new MessageQueue(TOPIC, "broker-a", 0));
        messageQueueList.add(new MessageQueue(TOPIC, "broker-b", 0));
        topicPublishInfo.setMessageQueueList(messageQueueList);

        MessageQueue selected = topicPublishInfo.selectOneMessageQueue("broker-a");
        assertThat(selected).isNotNull();
        assertThat(selected.getBrokerName()).isEqualTo("broker-b");

        assertThat(topicPublishInfo.selectOneMessageQueue((String) null)).isNotNull();
        assertThat(topicPublishInfo.selectOneMessageQueue()).isNotNull();
    }
}
