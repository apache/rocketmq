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

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.consumer.BatchMessageListener;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;
import io.openmessaging.message.Message;
import io.openmessaging.rocketmq.domain.NonStandardKeys;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

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
        consumedMsg.setQueueId(0);
        consumedMsg.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));
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

    @Test
    public void testBatchConsumeMessage() {
        final byte[] testBody = new byte[]{'a', 'b'};

        MessageExt consumedMsg = new MessageExt();
        consumedMsg.setMsgId("NewMsgId");
        consumedMsg.setBody(testBody);
        consumedMsg.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg.setTopic("HELLO_QUEUE");
        consumedMsg.setQueueId(0);
        consumedMsg.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));

        final byte[] testBody1 = new byte[]{'c', 'd'};
        MessageExt consumedMsg1 = new MessageExt();
        consumedMsg1.setMsgId("NewMsgId1");
        consumedMsg1.setBody(testBody1);
        consumedMsg1.putUserProperty(NonStandardKeys.MESSAGE_DESTINATION, "TOPIC");
        consumedMsg1.setTopic("HELLO_QUEUE");
        consumedMsg1.setQueueId(0);
        consumedMsg1.setStoreHost(new InetSocketAddress("127.0.0.1", 9876));
        List<MessageExt> messageExts = new ArrayList<MessageExt>() {
            {
                add(consumedMsg);
                add(consumedMsg1);
            }
        };

        Set<String> queueNames = new HashSet<>(8);
        queueNames.add("HELLO_QUEUE");
        pushConsumer.bindQueue(queueNames, new BatchMessageListener() {
            @Override public void onReceived(List<Message> batchMessage, Context context) {
                assertThat(batchMessage.size()).isEqualTo(2);
                Message message1 = null;
                Message message2 = null;
                for (Message message : batchMessage) {
                    if (message.header().getMessageId().equals("NewMsgId")) {
                        message1 = message;
                    }
                    if (message.header().getMessageId().equals("NewMsgId1")) {
                        message2 = message;
                    }
                }
                assertThat(message1).isNotNull();
                assertThat(message1.getData()).isEqualTo(testBody);
                assertThat(message1.header().getDestination()).isEqualTo("HELLO_QUEUE");
                assertThat(message1.extensionHeader().getPartiton()).isEqualTo(0);
                assertThat(message2).isNotNull();
                assertThat(message2.getData()).isEqualTo(testBody1);
                assertThat(message2.header().getDestination()).isEqualTo("HELLO_QUEUE");
                assertThat(message2.extensionHeader().getPartiton()).isEqualTo(0);

                context.ack();
            }
        });

        ((MessageListenerConcurrently) rocketmqPushConsumer
            .getMessageListener()).consumeMessage(messageExts, null);

    }

}