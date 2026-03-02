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

package org.apache.rocketmq.client.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProduceAccumulatorTest {
    private boolean compareMessageBatch(MessageBatch a, MessageBatch b) {
        if (!a.getTopic().equals(b.getTopic())) {
            return false;
        }
        if (!Arrays.equals(a.getBody(), b.getBody())) {
            return false;
        }
        return true;
    }

    private class MockMQProducer extends DefaultMQProducer {
        private Message beSendMessage = null;
        private MessageQueue beSendMessageQueue = null;

        @Override
        public SendResult sendDirect(Message msg, MessageQueue mq,
            SendCallback sendCallback) {
            this.beSendMessage = msg;
            this.beSendMessageQueue = mq;

            SendResult sendResult = new SendResult();
            sendResult.setMsgId("123");
            if (sendCallback != null) {
                sendCallback.onSuccess(sendResult);
            }
            return sendResult;
        }
    }

    @Test
    public void testProduceAccumulator_async() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        MockMQProducer mockMQProducer = new MockMQProducer();

        ProduceAccumulator produceAccumulator = new ProduceAccumulator("test");
        produceAccumulator.start();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        List<Message> messages = new ArrayList<Message>();
        messages.add(new Message("testTopic", "1".getBytes()));
        messages.add(new Message("testTopic", "22".getBytes()));
        messages.add(new Message("testTopic", "333".getBytes()));
        messages.add(new Message("testTopic", "4444".getBytes()));
        messages.add(new Message("testTopic", "55555".getBytes()));
        for (Message message : messages) {
            produceAccumulator.send(message, new SendCallback() {
                final CountDownLatch finalCountDownLatch = countDownLatch;

                @Override
                public void onSuccess(SendResult sendResult) {
                    finalCountDownLatch.countDown();
                }

                @Override
                public void onException(Throwable e) {
                    finalCountDownLatch.countDown();
                }
            }, mockMQProducer);
        }
        assertThat(countDownLatch.await(3000L, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(mockMQProducer.beSendMessage instanceof MessageBatch).isTrue();

        MessageBatch messageBatch1 = (MessageBatch) mockMQProducer.beSendMessage;
        MessageBatch messageBatch2 = MessageBatch.generateFromList(messages);
        messageBatch2.setBody(messageBatch2.encode());

        assertThat(compareMessageBatch(messageBatch1, messageBatch2)).isTrue();
    }

    @Test
    public void testProduceAccumulator_sync() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        final MockMQProducer mockMQProducer = new MockMQProducer();

        final ProduceAccumulator produceAccumulator = new ProduceAccumulator("test");
        produceAccumulator.batchMaxDelayMs(3000);
        produceAccumulator.start();

        List<Message> messages = new ArrayList<Message>();
        messages.add(new Message("testTopic", "1".getBytes()));
        messages.add(new Message("testTopic", "22".getBytes()));
        messages.add(new Message("testTopic", "333".getBytes()));
        messages.add(new Message("testTopic", "4444".getBytes()));
        messages.add(new Message("testTopic", "55555".getBytes()));
        final CountDownLatch countDownLatch = new CountDownLatch(messages.size());

        for (final Message message : messages) {
            new Thread(new Runnable() {
                final ProduceAccumulator finalProduceAccumulator = produceAccumulator;
                final CountDownLatch finalCountDownLatch = countDownLatch;
                final MockMQProducer finalMockMQProducer = mockMQProducer;
                final Message finalMessage = message;

                @Override
                public void run() {
                    try {
                        finalProduceAccumulator.send(finalMessage, finalMockMQProducer);
                        finalCountDownLatch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        assertThat(countDownLatch.await(5000L, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(mockMQProducer.beSendMessage instanceof MessageBatch).isTrue();

        MessageBatch messageBatch1 = (MessageBatch) mockMQProducer.beSendMessage;
        MessageBatch messageBatch2 = MessageBatch.generateFromList(messages);
        messageBatch2.setBody(messageBatch2.encode());

        assertThat(messageBatch1.getTopic()).isEqualTo(messageBatch2.getTopic());
        // The execution order is uncertain, just compare the length
        assertThat(messageBatch1.getBody().length).isEqualTo(messageBatch2.getBody().length);
    }

    @Test
    public void testProduceAccumulator_sendWithMessageQueue() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        MockMQProducer mockMQProducer = new MockMQProducer();

        MessageQueue messageQueue = new MessageQueue("topicTest", "brokerTest", 0);
        final ProduceAccumulator produceAccumulator = new ProduceAccumulator("test");
        produceAccumulator.start();

        Message message = new Message("testTopic", "1".getBytes());
        produceAccumulator.send(message, messageQueue, mockMQProducer);
        assertThat(mockMQProducer.beSendMessageQueue).isEqualTo(messageQueue);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        produceAccumulator.send(message, messageQueue, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                countDownLatch.countDown();
            }
        }, mockMQProducer);
        assertThat(countDownLatch.await(3000L, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(mockMQProducer.beSendMessageQueue).isEqualTo(messageQueue);
    }
}
