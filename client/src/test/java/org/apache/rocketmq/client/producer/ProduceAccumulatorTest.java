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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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

    @Test
    public void testSyncSend_slowBatchDoesNotBlockOtherBatches() throws Exception {
        final MessageQueue slowQueue = new MessageQueue("testTopic", "broker", 0);
        final MessageQueue fastQueue = new MessageQueue("testTopic", "broker", 1);
        final CountDownLatch slowSendStarted = new CountDownLatch(1);
        final CountDownLatch releaseSlowSend = new CountDownLatch(1);
        DefaultMQProducer producer = new DefaultMQProducer() {
            @Override
            public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) throws InterruptedException {
                if (slowQueue.equals(mq)) {
                    slowSendStarted.countDown();
                    releaseSlowSend.await(10, TimeUnit.SECONDS);
                }
                SendResult sendResult = new SendResult();
                sendResult.setMsgId("123");
                return sendResult;
            }
        };
        final ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.start();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<SendResult> slow = executor.submit(() -> accumulator.send(new Message("testTopic", "slow".getBytes()), slowQueue, producer));
            assertThat(slowSendStarted.await(3, TimeUnit.SECONDS)).isTrue();

            Future<SendResult> fast = executor.submit(() -> accumulator.send(new Message("testTopic", "fast".getBytes()), fastQueue, producer));
            assertThat(fast.get(2, TimeUnit.SECONDS)).isNotNull();

            releaseSlowSend.countDown();
            assertThat(slow.get(3, TimeUnit.SECONDS)).isNotNull();
        } finally {
            releaseSlowSend.countDown();
            executor.shutdownNow();
            accumulator.shutdown();
        }
    }

    @Test
    public void testSyncSend_failurePropagatesToEveryMessageInBatch() throws Exception {
        final int threads = 5;
        final CountDownLatch allAdded = new CountDownLatch(threads);
        final AtomicInteger sendTimes = new AtomicInteger();
        DefaultMQProducer producer = new DefaultMQProducer() {
            @Override
            public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQBrokerException {
                sendTimes.incrementAndGet();
                throw new MQBrokerException(1, "mock broker failure");
            }
        };
        final ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.batchMaxDelayMs(1000);
        accumulator.start();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Throwable>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final Message message = new Message("testTopic", ("body" + i).getBytes());
                futures.add(executor.submit(() -> {
                    allAdded.countDown();
                    try {
                        accumulator.send(message, producer);
                        return null;
                    } catch (Throwable t) {
                        return t;
                    }
                }));
            }
            for (Future<Throwable> future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS)).isInstanceOf(MQBrokerException.class);
            }
            assertThat(sendTimes.get()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            accumulator.shutdown();
        }
    }

    @Test
    public void testSyncSend_interruptWhileWaitingDoesNotStrandMessage() throws Exception {
        final CountDownLatch sent = new CountDownLatch(1);
        DefaultMQProducer producer = new DefaultMQProducer() {
            @Override
            public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) {
                sent.countDown();
                SendResult sendResult = new SendResult();
                sendResult.setMsgId("123");
                return sendResult;
            }
        };
        final ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.batchMaxDelayMs(500);
        accumulator.start();
        final AtomicReference<Object> outcome = new AtomicReference<>();
        final AtomicReference<Boolean> interruptedAfterReturn = new AtomicReference<>();
        Thread sender = new Thread(() -> {
            try {
                outcome.set(accumulator.send(new Message("testTopic", "1".getBytes()), producer));
            } catch (Throwable t) {
                outcome.set(t);
            }
            interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
        });
        try {
            sender.start();
            Thread.sleep(100);
            sender.interrupt();
            sender.join(3000);
            assertThat(sender.isAlive()).isFalse();
            assertThat(sent.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(outcome.get()).isInstanceOf(SendResult.class);
            assertThat(interruptedAfterReturn.get()).isTrue();
        } finally {
            sender.interrupt();
            accumulator.shutdown();
        }
    }

    @Test
    public void testSyncSend_concurrentNoLossNoDuplicate() throws Exception {
        final int threads = 32;
        final int messagesPerThread = 300;
        final MessageQueue[] queues = new MessageQueue[] {
            new MessageQueue("testTopic", "broker", 0),
            new MessageQueue("testTopic", "broker", 1),
            new MessageQueue("testTopic", "broker", 2)
        };
        final Set<String> sentBodies = ConcurrentHashMap.newKeySet();
        final AtomicInteger sentCount = new AtomicInteger();
        DefaultMQProducer producer = new DefaultMQProducer() {
            @Override
            public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) {
                for (Message m : (MessageBatch) msg) {
                    sentBodies.add(new String(m.getBody()));
                    sentCount.incrementAndGet();
                }
                SendResult sendResult = new SendResult();
                sendResult.setMsgId("123");
                return sendResult;
            }
        };
        final ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.batchMaxDelayMs(1);
        accumulator.batchMaxBytes(64);
        accumulator.start();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    int succeeded = 0;
                    for (int i = 0; i < messagesPerThread; i++) {
                        Message message = new Message("testTopic", (threadId + "-" + i).getBytes());
                        if (accumulator.send(message, queues[i % queues.length], producer) != null) {
                            succeeded++;
                        }
                    }
                    return succeeded;
                }));
            }
            int succeeded = 0;
            for (Future<Integer> future : futures) {
                succeeded += future.get(60, TimeUnit.SECONDS);
            }
            assertThat(succeeded).isEqualTo(threads * messagesPerThread);
            assertThat(sentCount.get()).isEqualTo(threads * messagesPerThread);
            assertThat(sentBodies).hasSize(threads * messagesPerThread);
        } catch (Exception e) {
            fail("concurrent sync send failed", e);
        } finally {
            executor.shutdownNow();
            accumulator.shutdown();
        }
    }

    @Test
    public void testTryAddMessage_concurrentAccountingIsExact() throws Exception {
        final int totalHoldSize = 1000;
        final int threads = 16;
        final int attemptsPerThread = 500;
        final ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.totalBatchMaxBytes(totalHoldSize);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger accepted = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    for (int i = 0; i < attemptsPerThread; i++) {
                        if (accumulator.tryAddMessage(new Message("testTopic", new byte[] {1}))) {
                            accepted.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
            assertThat(accepted.get()).isEqualTo(totalHoldSize);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testAsyncSend_synchronousSendFailureReleasesHoldSize() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer() {
            @Override
            public SendResult sendDirect(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException {
                throw new MQClientException("executor rejected", null);
            }
        };
        ProduceAccumulator accumulator = new ProduceAccumulator("test");
        accumulator.batchMaxBytes(1);
        Message message = new Message("testTopic", "22".getBytes());
        final CountDownLatch failed = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        assertThat(accumulator.tryAddMessage(message)).isTrue();
        accumulator.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
            }

            @Override
            public void onException(Throwable e) {
                error.set(e);
                failed.countDown();
            }
        }, producer);

        assertThat(failed.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isInstanceOf(MQClientException.class);
        Field field = ProduceAccumulator.class.getDeclaredField("currentlyHoldSize");
        field.setAccessible(true);
        assertThat(((AtomicLong) field.get(accumulator)).get()).isEqualTo(0L);
    }
}
