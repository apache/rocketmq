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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ProduceAccumulatorLifecycleTest {
    @Test
    public void testRunningProducerDoesNotJoinStoppedProducerBatch() throws Exception {
        assertProducerIsolation(null, false);
    }

    @Test
    public void testExplicitQueueBatchIsIsolatedByProducer() throws Exception {
        assertProducerIsolation(new MessageQueue("testTopic", "broker", 0), false);
    }

    @Test
    public void testReplacementProducerWithSameGroupUsesItsOwnBatch() throws Exception {
        assertProducerIsolation(null, true);
    }

    private void assertProducerIsolation(MessageQueue queue, boolean replaceSameGroup) throws Exception {
        ProduceAccumulator accumulator = newAccumulator();
        RecordingProducer producerA = new RecordingProducer("producer-a");
        RecordingProducer producerB = new RecordingProducer(replaceSameGroup ? "producer-a" : "producer-b");
        prepareProducer(producerA, accumulator);
        prepareProducer(producerB, accumulator);
        SendCallback callbackA = mock(SendCallback.class);
        SendCallback callbackB = mock(SendCallback.class);
        producerA.start();
        if (!replaceSameGroup) {
            producerB.start();
        }
        try {
            producerA.send(new Message("testTopic", new byte[] {1}), queue, callbackA);
            producerA.shutdown();
            if (replaceSameGroup) {
                producerB.start();
            }
            producerB.send(new Message("testTopic", new byte[] {2}), queue, callbackB);
            flushAsyncBatches(accumulator);

            verify(callbackA, times(1)).onException(any(MQClientException.class));
            verify(callbackB, times(1)).onSuccess(any(SendResult.class));
            assertThat(producerA.directSends.get()).isEqualTo(1);
            assertThat(producerB.directSends.get()).isEqualTo(1);
            assertThat(heldBytes(accumulator)).isZero();
            assertThat(asyncBatches(accumulator)).isEmpty();
        } finally {
            producerA.shutdown();
            producerB.shutdown();
        }
    }

    @Test
    public void testCompletedBatchDoesNotRetainItsProducer() throws Exception {
        ProduceAccumulator accumulator = newAccumulator();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        doAnswer(invocation -> {
            SendCallback callback = invocation.getArgument(2);
            SendResult result = new SendResult();
            result.setMsgId("123");
            callback.onSuccess(result);
            return result;
        }).when(producer).sendDirect(any(Message.class), isNull(), any(SendCallback.class));
        enqueue(accumulator, producer, mock(SendCallback.class));
        flushAsyncBatches(accumulator);
        assertThat(heldBytes(accumulator)).isZero();
        assertThat(asyncBatches(accumulator)).isEmpty();
    }

    @Test
    public void testSynchronousSendInitiationFailureReleasesHeldBytes() throws Exception {
        ProduceAccumulator accumulator = newAccumulator();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        SendCallback callback = mock(SendCallback.class);
        doAnswer(invocation -> {
            throw new MQClientException("send initiation failed", null);
        }).when(producer).sendDirect(any(Message.class), isNull(), any(SendCallback.class));
        enqueue(accumulator, producer, callback);
        flushAsyncBatches(accumulator);
        verify(callback).onException(any(MQClientException.class));
        assertThat(heldBytes(accumulator)).isZero();
    }

    @Test
    public void testInlineCallbackThenThrowDoesNotReleaseQuotaTwice() throws Exception {
        ProduceAccumulator accumulator = newAccumulator();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        doAnswer(invocation -> {
            SendCallback callback = invocation.getArgument(2);
            callback.onException(new MQClientException("callback failure", null));
            throw new MQClientException("send invocation also failed", null);
        }).when(producer).sendDirect(any(Message.class), isNull(), any(SendCallback.class));
        enqueue(accumulator, producer, mock(SendCallback.class));
        flushAsyncBatches(accumulator);
        assertThat(heldBytes(accumulator)).isZero();
    }

    @Test
    public void testThrowingFailureCallbackStillReleasesHeldBytes() throws Exception {
        ProduceAccumulator accumulator = newAccumulator();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        SendCallback callback = mock(SendCallback.class);
        doAnswer(invocation -> {
            throw new MQClientException("send initiation failed", null);
        }).when(producer).sendDirect(any(Message.class), isNull(), any(SendCallback.class));
        doAnswer(invocation -> {
            throw new IllegalStateException("user callback failed");
        }).when(callback).onException(any(Throwable.class));
        enqueue(accumulator, producer, callback);
        try {
            flushAsyncBatches(accumulator);
        } catch (InvocationTargetException expected) {
            assertThat(expected.getCause()).isInstanceOf(IllegalStateException.class);
        }
        assertThat(heldBytes(accumulator)).isZero();
    }

    private ProduceAccumulator newAccumulator() {
        ProduceAccumulator accumulator = spy(new ProduceAccumulator("lifecycle-test"));
        // Exercise real batch ownership without background flushing. Guard lifetime is tested separately.
        doNothing().when(accumulator).start();
        doNothing().when(accumulator).shutdown();
        accumulator.batchMaxDelayMs(30000);
        return accumulator;
    }

    private void enqueue(ProduceAccumulator accumulator, DefaultMQProducer producer, SendCallback callback)
        throws Exception {
        Message message = new Message("testTopic", new byte[] {1});
        assertThat(accumulator.tryAddMessage(message)).isTrue();
        accumulator.send(message, callback, producer);
        assertThat(heldBytes(accumulator)).isEqualTo(1);
    }

    private void prepareProducer(DefaultMQProducer producer, ProduceAccumulator accumulator) throws Exception {
        producer.setAutoBatch(true);
        Field implementation = DefaultMQProducer.class.getDeclaredField("defaultMQProducerImpl");
        implementation.setAccessible(true);
        implementation.set(producer, mock(DefaultMQProducerImpl.class));
        Field sharedAccumulator = DefaultMQProducer.class.getDeclaredField("produceAccumulator");
        sharedAccumulator.setAccessible(true);
        sharedAccumulator.set(producer, accumulator);
    }

    private Map<?, ?> asyncBatches(ProduceAccumulator accumulator) throws Exception {
        Field batchesField = ProduceAccumulator.class.getDeclaredField("asyncSendBatchs");
        batchesField.setAccessible(true);
        return (Map<?, ?>) batchesField.get(accumulator);
    }

    private void flushAsyncBatches(ProduceAccumulator accumulator) throws Exception {
        for (Object batch : new ArrayList<>(asyncBatches(accumulator).values())) {
            Method send = batch.getClass().getDeclaredMethod("send", SendCallback.class);
            send.setAccessible(true);
            send.invoke(batch, new Object[] {null});
        }
    }

    private long heldBytes(ProduceAccumulator accumulator) throws Exception {
        Field field = ProduceAccumulator.class.getDeclaredField("currentlyHoldSize");
        field.setAccessible(true);
        return ((AtomicLong) field.get(accumulator)).get();
    }

    private static class RecordingProducer extends DefaultMQProducer {
        private boolean stopped;
        private final AtomicInteger directSends = new AtomicInteger();

        RecordingProducer(String group) {
            super(group);
        }

        @Override
        public void shutdown() {
            super.shutdown();
            stopped = true;
        }

        @Override
        public SendResult sendDirect(Message message, MessageQueue queue, SendCallback callback)
            throws MQClientException {
            directSends.incrementAndGet();
            if (stopped) {
                throw new MQClientException("producer already shut down", null);
            }
            SendResult result = new SendResult();
            result.setMsgId("123");
            if (callback != null) {
                callback.onSuccess(result);
            }
            return result;
        }
    }
}
