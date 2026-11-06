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
package org.apache.rocketmq.client.impl.consumer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OrderlyRetryMessageSenderTest {
    private static final String ORIGINAL_TOPIC = "topic";
    private static final String RETRY_TOPIC = "%RETRY%group";

    @Mock
    private DefaultMQPushConsumerImpl consumer;
    @Mock
    private MQClientInstance clientInstance;
    @Mock
    private DefaultMQProducer producer;

    private MessageExt originalMessage;
    private Message retryMessage;

    @Before
    public void setUp() {
        when(consumer.getmQClientFactory()).thenReturn(clientInstance);
        when(clientInstance.getDefaultMQProducer()).thenReturn(producer);

        originalMessage = new MessageExt();
        originalMessage.setTopic(ORIGINAL_TOPIC);
        originalMessage.setBrokerName("broker-b");

        retryMessage = new Message(RETRY_TOPIC, "body".getBytes(StandardCharsets.UTF_8));
        retryMessage.putUserProperty("key", "value");
        retryMessage.setTransactionId("transaction-id");
    }

    @Test
    public void prefersOriginalBrokerAndCopiesMessage() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-a", "broker-b"));

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(messageCaptor.capture(), eq(retryQueue("broker-b")));
        verify(producer, never()).send(any(Message.class));
        Message sentMessage = messageCaptor.getValue();
        assertNotSame(retryMessage, sentMessage);
        assertNotSame(retryMessage.getProperties(), sentMessage.getProperties());
        assertEquals(retryMessage.getProperties(), sentMessage.getProperties());
        assertEquals("transaction-id", sentMessage.getTransactionId());
    }

    @Test
    public void ignoresOriginalBrokerWhenItIsNotInCurrentTopicRoute() throws Exception {
        originalMessage.setBrokerName("broker-c");
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-b", "broker-a"));

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        verify(producer).send(any(Message.class), eq(retryQueue("broker-a")));
        verify(producer, never()).send(any(Message.class), eq(retryQueue("broker-c")));
    }

    @Test
    public void usesBusinessTopicRouteWhenRetryMessageIsConsumedAgain() throws Exception {
        originalMessage.setTopic(RETRY_TOPIC);
        MessageAccessor.putProperty(originalMessage, MessageConst.PROPERTY_RETRY_TOPIC, ORIGINAL_TOPIC);
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-b"));

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        verify(consumer).fetchSubscribeMessageQueues(ORIGINAL_TOPIC);
        verify(producer).send(any(Message.class), eq(retryQueue("broker-b")));
    }

    @Test
    public void retriesWithFreshMessageOnAnotherTopicBroker() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-a", "broker-b"));
        doAnswer(invocation -> {
            Message failedMessage = invocation.getArgument(0);
            failedMessage.setTopic("mutated-by-producer");
            throw new MQClientException("first broker failed", null);
        }).when(producer).send(any(Message.class), eq(retryQueue("broker-b")));

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(messageCaptor.capture(), eq(retryQueue("broker-a")));
        assertEquals(RETRY_TOPIC, messageCaptor.getValue().getTopic());
        assertEquals(RETRY_TOPIC, retryMessage.getTopic());
    }

    @Test
    public void fallsBackToDefaultRoutingWhenOriginalTopicRouteIsUnavailable() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC))
            .thenThrow(new MQClientException("route unavailable", null));

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(producer).send(messageCaptor.capture());
        verify(producer, never()).send(any(Message.class), any(MessageQueue.class));
        assertNotSame(retryMessage, messageCaptor.getValue());
    }

    @Test
    public void fallsBackToDefaultRoutingWhenOriginalTopicRouteIsEmpty() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(Collections.emptySet());

        OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage);

        verify(producer).send(any(Message.class));
        verify(producer, never()).send(any(Message.class), any(MessageQueue.class));
    }

    @Test
    public void doesNotFallBackOutsideTopicRouteWhenTopicBrokersFail() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-a", "broker-b"));
        doThrow(new MQClientException("broker-b failed", null))
            .when(producer).send(any(Message.class), eq(retryQueue("broker-b")));
        doThrow(new MQClientException("broker-a failed", null))
            .when(producer).send(any(Message.class), eq(retryQueue("broker-a")));

        MQClientException exception = assertThrows(MQClientException.class,
            () -> OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage));

        assertEquals("broker-a failed", exception.getErrorMessage());
        verify(producer, never()).send(any(Message.class));
    }

    @Test
    public void preservesInterruptionWithoutTryingAnotherBroker() throws Exception {
        when(consumer.fetchSubscribeMessageQueues(ORIGINAL_TOPIC)).thenReturn(routes("broker-a", "broker-b"));
        doThrow(new InterruptedException("interrupted"))
            .when(producer).send(any(Message.class), eq(retryQueue("broker-b")));

        try {
            assertThrows(InterruptedException.class,
                () -> OrderlyRetryMessageSender.send(consumer, originalMessage, retryMessage));

            assertTrue(Thread.currentThread().isInterrupted());
            verify(producer, never()).send(any(Message.class), eq(retryQueue("broker-a")));
        } finally {
            Thread.interrupted();
        }
    }

    private Set<MessageQueue> routes(String... brokerNames) {
        Set<MessageQueue> result = new HashSet<>();
        Arrays.stream(brokerNames).forEach(brokerName ->
            result.add(new MessageQueue(ORIGINAL_TOPIC, brokerName, 0)));
        return result;
    }

    private MessageQueue retryQueue(String brokerName) {
        return new MessageQueue(RETRY_TOPIC, brokerName, 0);
    }
}
