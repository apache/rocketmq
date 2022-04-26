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

package org.apache.rocketmq.proxy.grpc.v2.service.local;

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SystemProperties;
import io.grpc.Context;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalWriteQueueSelectorTest {
    private LocalWriteQueueSelector localWriteQueueSelector;
    private String topic = "test-topic";
    private String brokerName = "broker";
    private int writeQueueId = 8;
    private String messageGroup = "message-group";

    @Before
    public void setup() {
        TopicConfigManager topicConfigManager = Mockito.mock(TopicConfigManager.class);
        TopicConfig topicConfig = new TopicConfig(topic, writeQueueId, writeQueueId);
        Mockito.when(topicConfigManager.selectTopicConfig(topic)).thenReturn(topicConfig);
        localWriteQueueSelector = new LocalWriteQueueSelector(brokerName, topicConfigManager, null);
    }

    @Test
    public void testSelectQueueWithNormalMessage() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder().setTopic(Resource.newBuilder().setName(topic).build()))
            .build();
        SelectableMessageQueue selectableMessageQueue = localWriteQueueSelector.selectQueue(Context.current(), sendMessageRequest);
        assertThat(selectableMessageQueue.getBrokerName()).isEqualTo(brokerName);
        assertThat(selectableMessageQueue.getTopic()).isEqualTo(topic);
        int selectQueueId = selectableMessageQueue.getQueueId();
        selectableMessageQueue = localWriteQueueSelector.selectQueue(Context.current(), sendMessageRequest);
        assertThat(selectableMessageQueue.getQueueId()).isEqualTo((selectQueueId + 1) % writeQueueId);
    }

    @Test
    public void testSelectQueueWithFifoMessage() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.newBuilder()
            .addMessages(Message.newBuilder()
                .setSystemProperties(SystemProperties.newBuilder()
                    .setMessageGroup(messageGroup)
                    .build())
                .setTopic(Resource.newBuilder().setName(topic).build()))
            .build();
        SelectableMessageQueue selectableMessageQueue = localWriteQueueSelector.selectQueue(Context.current(), sendMessageRequest);
        assertThat(selectableMessageQueue.getBrokerName()).isEqualTo(brokerName);
        assertThat(selectableMessageQueue.getTopic()).isEqualTo(topic);
        int selectQueueId = selectableMessageQueue.getQueueId();
        selectableMessageQueue = localWriteQueueSelector.selectQueue(Context.current(), sendMessageRequest);
        assertThat(selectableMessageQueue.getQueueId()).isEqualTo(selectQueueId);
    }
}