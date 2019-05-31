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
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.manager.ResourceManager;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.util.HashSet;
import java.util.Set;
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
    private PushConsumer pushConsumer;

    @Mock
    private DefaultMQPushConsumer rocketmqPushConsumer;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        final MessagingAccessPoint messagingAccessPoint = OMS
                .getMessagingAccessPoint("oms:rocketmq://IP1:9876,IP2:9876/namespace");
        final KeyValue attributes = messagingAccessPoint.attributes();
        attributes.put(NonStandardKeys.CONSUMER_ID, "TestGroup");
        pushConsumer = messagingAccessPoint.createPushConsumer();

        Field field = PushConsumerImpl.class.getDeclaredField("rocketmqPushConsumer");
        field.setAccessible(true);
        DefaultMQPushConsumer innerConsumer = (DefaultMQPushConsumer) field.get(pushConsumer);
        field.set(pushConsumer, rocketmqPushConsumer); //Replace

        when(rocketmqPushConsumer.getMessageListener()).thenReturn(innerConsumer.getMessageListener());
        pushConsumer.start();
    }

    @Test
    public void testConsumeMessage() {
        final byte[] testBody = new byte[]{'a', 'b'};

        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic("HELLO_QUEUE");
        Set<String> queueNames = new HashSet<>(8);
        queueNames.add("HELLO_QUEUE");
        pushConsumer.bindQueue(queueNames, new MessageListener() {
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