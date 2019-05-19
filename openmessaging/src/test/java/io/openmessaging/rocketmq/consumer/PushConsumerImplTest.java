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
package io.openmessaging.rocketmq.consumer;

import io.openmessaging.*;
import io.openmessaging.consumer.Consumer;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.manager.ResourceManager;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class PushConsumerImplTest {
    private Consumer consumer;

    @Mock
    private DefaultMQPushConsumer rocketmqPushConsumer;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
                .getMessagingAccessPoint("oms:rocketmq://IP1:9876,IP2:9876/namespace");
        final ResourceManager resourceManager = messagingAccessPoint.resourceManager();
        resourceManager.createNamespace(NonStandardKeys.PUSH_CONSUMER + "_TestGroup");
        consumer = messagingAccessPoint.createConsumer();

        Field field = PushConsumerImpl.class.getDeclaredField("rocketmqPushConsumer");
        field.setAccessible(true);
        DefaultMQPushConsumer innerConsumer = (DefaultMQPushConsumer) field.get(consumer);
        field.set(consumer, rocketmqPushConsumer); //Replace

        when(rocketmqPushConsumer.getMessageListener()).thenReturn(innerConsumer.getMessageListener());
        consumer.start();
    }

    @Test
    public void testConsumeMessage() {
        final byte[] testBody = new byte[]{'a', 'b'};

        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic("HELLO_QUEUE");
        consumer.bindQueue("HELLO_QUEUE", new MessageListener() {
            @Override
            public void onReceived(Message message, Context context) {
                assertThat(message.header().getMessageId()).isEqualTo("NewMsgId");
                assertThat(message.getData()).isEqualTo(testBody);
                context.ack();
            }
        });
        ((MessageListenerConcurrently) rocketmqPushConsumer
                .getMessageListener()).consumeMessage(Collections.singletonList(consumedMsg), null);
    }
}