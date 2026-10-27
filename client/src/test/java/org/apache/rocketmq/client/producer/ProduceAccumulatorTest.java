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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProduceAccumulatorTest {
    private static final String TOPIC = "testTopic";

    private final ProduceAccumulator accumulator = new ProduceAccumulator("test");
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @After
    public void tearDown() {
        executor.shutdownNow();
        accumulator.shutdown();
    }

    /**
     * Records every batch it receives and answers with {@link #reply}, synchronously or through the callback.
     */
    private static class FakeProducer extends DefaultMQProducer {
        interface Reply {
            SendResult apply(Message msg, MessageQueue mq) throws Exception;
        }

        private final Reply reply;
        private final AtomicInteger sendTimes = new AtomicInteger();
        private volatile Message lastBatch;
        private volatile MessageQueue lastQueue;

        FakeProducer() {
            this((msg, mq) -> ok());
        }

        FakeProducer(Reply reply) {
            this.reply = reply;
        }

        @Override
        public SendResult sendDirect(Message msg, MessageQueue mq,
            SendCallback sendCallback) throws MQClientException, MQBrokerException, InterruptedException {
            sendTimes.incrementAndGet();
            lastBatch = msg;
            lastQueue = mq;
            SendResult result;
            try {
                result = reply.apply(msg, mq);
            } catch (MQClientException | MQBrokerException | InterruptedException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            if (sendCallback != null) {
                sendCallback.onSuccess(result);
            }
            return result;
        }

        static SendResult ok() {
            SendResult sendResult = new SendResult();
            sendResult.setMsgId("123");
            return sendResult;
        }
    }

    private static class RecordingCallback implements SendCallback {
        private final CountDownLatch done = new CountDownLatch(1);
        private final AtomicInteger successes = new AtomicInteger();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void onSuccess(SendResult sendResult) {
            successes.incrementAndGet();
            done.countDown();
        }

        @Override
        public void onException(Throwable e) {
            error.set(e);
            done.countDown();
        }

        boolean await() throws InterruptedException {
            return done.await(3, TimeUnit.SECONDS);
        }
    }

    private static List<Message> messages(String... bodies) {
        List<Message> messages = new ArrayList<>();
        for (String body : bodies) {
            messages.add(new Message(TOPIC, body.getBytes()));
        }
        return messages;
    }

    /**
     * Reserves hold size first, like {@link DefaultMQProducer#sendByAccumulator} does.
     */
    private Message reserved(String body) {
        Message message = new Message(TOPIC, body.getBytes());
        assertThat(accumulator.tryAddMessage(message)).isTrue();
        return message;
    }

    private static MessageQueue queue(int id) {
        return new MessageQueue(TOPIC, "broker", id);
    }

    @Test
    public void testAsyncSend_batchesMessages() throws Exception {
        FakeProducer producer = new FakeProducer();
        accumulator.start();
        List<Message> messages = messages("1", "22", "333", "4444", "55555");
        List<RecordingCallback> callbacks = new ArrayList<>();
        for (Message message : messages) {
            RecordingCallback callback = new RecordingCallback();
            callbacks.add(callback);
            accumulator.send(message, null, callback, producer);
        }
        for (RecordingCallback callback : callbacks) {
            assertThat(callback.await()).isTrue();
        }

        MessageBatch expected = MessageBatch.generateFromList(messages);
        expected.setBody(expected.encode());
        assertThat(producer.sendTimes.get()).isEqualTo(1);
        assertThat(producer.lastBatch.getTopic()).isEqualTo(expected.getTopic());
        assertThat(Arrays.equals(producer.lastBatch.getBody(), expected.getBody())).isTrue();
    }

    @Test
    public void testSyncSend_batchesConcurrentMessages() throws Exception {
        FakeProducer producer = new FakeProducer();
        accumulator.batchMaxDelayMs(3000);
        List<Message> messages = messages("1", "22", "333", "4444", "55555");
        List<Future<SendResult>> results = new ArrayList<>();
        for (Message message : messages) {
            results.add(executor.submit(() -> accumulator.send(message, null, producer)));
        }
        for (Future<SendResult> result : results) {
            assertThat(result.get(5, TimeUnit.SECONDS)).isNotNull();
        }

        MessageBatch expected = MessageBatch.generateFromList(messages);
        expected.setBody(expected.encode());
        assertThat(producer.sendTimes.get()).isEqualTo(1);
        assertThat(producer.lastBatch.getTopic()).isEqualTo(expected.getTopic());
        // The execution order is uncertain, just compare the length
        assertThat(producer.lastBatch.getBody().length).isEqualTo(expected.getBody().length);
    }

    @Test
    public void testSendWithMessageQueue() throws Exception {
        FakeProducer producer = new FakeProducer();
        accumulator.start();
        MessageQueue messageQueue = new MessageQueue("topicTest", "brokerTest", 0);

        accumulator.send(new Message(TOPIC, "1".getBytes()), messageQueue, producer);
        assertThat(producer.lastQueue).isEqualTo(messageQueue);

        producer.lastQueue = null;
        RecordingCallback callback = new RecordingCallback();
        accumulator.send(new Message(TOPIC, "1".getBytes()), messageQueue, callback, producer);
        assertThat(callback.await()).isTrue();
        assertThat(producer.lastQueue).isEqualTo(messageQueue);
    }

    @Test
    public void testSyncSend_slowBatchDoesNotBlockOtherBatches() throws Exception {
        CountDownLatch slowSendStarted = new CountDownLatch(1);
        CountDownLatch releaseSlowSend = new CountDownLatch(1);
        FakeProducer producer = new FakeProducer((msg, mq) -> {
            if (queue(0).equals(mq)) {
                slowSendStarted.countDown();
                releaseSlowSend.await(10, TimeUnit.SECONDS);
            }
            return FakeProducer.ok();
        });
        try {
            Future<SendResult> slow = executor.submit(() -> accumulator.send(new Message(TOPIC, "slow".getBytes()), queue(0), producer));
            assertThat(slowSendStarted.await(3, TimeUnit.SECONDS)).isTrue();

            Future<SendResult> fast = executor.submit(() -> accumulator.send(new Message(TOPIC, "fast".getBytes()), queue(1), producer));
            assertThat(fast.get(2, TimeUnit.SECONDS)).isNotNull();

            releaseSlowSend.countDown();
            assertThat(slow.get(3, TimeUnit.SECONDS)).isNotNull();
        } finally {
            releaseSlowSend.countDown();
        }
    }

    @Test
    public void testSyncSend_failurePropagatesToEveryMessageInBatch() throws Exception {
        FakeProducer producer = new FakeProducer((msg, mq) -> {
            throw new MQBrokerException(1, "mock broker failure");
        });
        accumulator.batchMaxDelayMs(1000);
        List<Future<Throwable>> outcomes = new ArrayList<>();
        for (String body : new String[] {"a", "b", "c", "d", "e"}) {
            Message message = reserved(body);
            outcomes.add(executor.submit(() -> {
                try {
                    accumulator.send(message, null, producer);
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            }));
        }
        for (Future<Throwable> outcome : outcomes) {
            assertThat(outcome.get(5, TimeUnit.SECONDS)).isInstanceOf(MQBrokerException.class);
        }
        assertThat(producer.sendTimes.get()).isEqualTo(1);
        assertThat(accumulator.getCurrentlyHoldSize()).isZero();
    }

    @Test
    public void testSyncSend_interruptWhileWaitingDoesNotStrandMessage() throws Exception {
        FakeProducer producer = new FakeProducer();
        accumulator.batchMaxDelayMs(500);
        AtomicReference<Object> outcome = new AtomicReference<>();
        AtomicReference<Boolean> interruptedAfterReturn = new AtomicReference<>();
        Thread sender = new Thread(() -> {
            try {
                outcome.set(accumulator.send(new Message(TOPIC, "1".getBytes()), null, producer));
            } catch (Throwable t) {
                outcome.set(t);
            }
            interruptedAfterReturn.set(Thread.currentThread().isInterrupted());
        });
        sender.start();
        Thread.sleep(100);
        sender.interrupt();
        sender.join(3000);

        assertThat(sender.isAlive()).isFalse();
        assertThat(producer.sendTimes.get()).isEqualTo(1);
        assertThat(outcome.get()).isInstanceOf(SendResult.class);
        assertThat(interruptedAfterReturn.get()).isTrue();
    }

    @Test
    public void testSyncSend_concurrentNoLossNoDuplicate() throws Exception {
        int threads = 32;
        int messagesPerThread = 300;
        Set<String> sentBodies = ConcurrentHashMap.newKeySet();
        AtomicInteger sentCount = new AtomicInteger();
        FakeProducer producer = new FakeProducer((msg, mq) -> {
            for (Message m : (MessageBatch) msg) {
                sentBodies.add(new String(m.getBody()));
                sentCount.incrementAndGet();
            }
            return FakeProducer.ok();
        });
        accumulator.batchMaxDelayMs(1);
        accumulator.batchMaxBytes(64);
        List<Future<Integer>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            futures.add(executor.submit(() -> {
                int succeeded = 0;
                for (int i = 0; i < messagesPerThread; i++) {
                    Message message = new Message(TOPIC, (threadId + "-" + i).getBytes());
                    if (accumulator.send(message, queue(i % 3), producer) != null) {
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
    }

    @Test
    public void testTryAddMessage_concurrentAccountingIsExact() throws Exception {
        int totalHoldSize = 1000;
        accumulator.totalBatchMaxBytes(totalHoldSize);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 16; t++) {
            futures.add(executor.submit(() -> {
                start.await();
                for (int i = 0; i < 500; i++) {
                    if (accumulator.tryAddMessage(new Message(TOPIC, new byte[] {1}))) {
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
    }

    @Test
    public void testAsyncSend_synchronousSendFailureReleasesHoldSize() throws Exception {
        FakeProducer producer = new FakeProducer((msg, mq) -> {
            throw new MQClientException("executor rejected", null);
        });
        accumulator.batchMaxBytes(1);
        RecordingCallback callback = new RecordingCallback();
        accumulator.send(reserved("22"), null, callback, producer);

        assertThat(callback.await()).isTrue();
        assertThat(callback.error.get()).isInstanceOf(MQClientException.class);
        assertThat(accumulator.getCurrentlyHoldSize()).isZero();
    }

    @Test
    public void testSentBatchesAreRemoved() throws Exception {
        FakeProducer producer = new FakeProducer();
        accumulator.start();
        List<RecordingCallback> callbacks = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            accumulator.send(new Message(TOPIC, "s".getBytes()), queue(i), producer);
            RecordingCallback callback = new RecordingCallback();
            callbacks.add(callback);
            accumulator.send(new Message(TOPIC, "a".getBytes()), queue(i), callback, producer);
        }
        for (RecordingCallback callback : callbacks) {
            assertThat(callback.await()).isTrue();
        }
        assertThat(accumulator.pendingBatchCount()).isZero();
    }

    @Test
    public void testAsyncSend_throwingCallbackDoesNotAffectOthers() {
        FakeProducer producer = new FakeProducer();
        accumulator.batchMaxDelayMs(30 * 1000);
        accumulator.batchMaxBytes(1);
        RecordingCallback throwing = new RecordingCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                super.onSuccess(sendResult);
                throw new IllegalStateException("user callback bug");
            }
        };
        RecordingCallback other = new RecordingCallback();

        accumulator.send(reserved("1"), null, throwing, producer);
        accumulator.send(reserved("2"), null, other, producer);

        assertThat(throwing.successes.get() + other.successes.get()).isEqualTo(2);
        assertThat(throwing.error.get()).isNull();
        assertThat(other.error.get()).isNull();
        assertThat(accumulator.getCurrentlyHoldSize()).isZero();
    }
}
