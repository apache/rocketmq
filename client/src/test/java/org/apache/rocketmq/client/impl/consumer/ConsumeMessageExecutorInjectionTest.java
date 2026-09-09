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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Assume;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ConsumeMessageExecutorInjectionTest {
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> services() {
        return Arrays.asList(new Object[][] {
            {ConsumeMessageConcurrentlyService.class, MessageListenerConcurrently.class},
            {ConsumeMessageOrderlyService.class, MessageListenerOrderly.class},
            {ConsumeMessagePopConcurrentlyService.class, MessageListenerConcurrently.class},
            {ConsumeMessagePopOrderlyService.class, MessageListenerOrderly.class}
        });
    }

    private final Class<? extends ConsumeMessageService> serviceClass;
    private final Class<?> listenerClass;

    public ConsumeMessageExecutorInjectionTest(Class<? extends ConsumeMessageService> serviceClass, Class<?> listenerClass) {
        this.serviceClass = serviceClass;
        this.listenerClass = listenerClass;
    }

    @Test
    public void testSharingAndOwnership() throws Exception {
        ThreadPoolExecutor shared = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ConsumeMessageService first = createService(shared);
        ConsumeMessageService second = createService(shared);
        try {
            ExecutorService firstExecutor = (ExecutorService) FieldUtils.readDeclaredField(first, "consumeExecutor", true);
            ExecutorService secondExecutor = (ExecutorService) FieldUtils.readDeclaredField(second, "consumeExecutor", true);
            assertSame(shared, firstExecutor);
            assertSame(shared, secondExecutor);
            Thread worker = firstExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            assertSame(worker, secondExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS));
            first.updateCorePoolSize(10);
            assertEquals(1, shared.getCorePoolSize());
            assertEquals(-1, first.getCorePoolSize());
            first.shutdown(5000);
            assertFalse(shared.isShutdown());
            assertSame(worker, secondExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS));
        } finally {
            first.shutdown(5000);
            second.shutdown(5000);
            shared.shutdownNow();
        }
    }

    @Test
    public void testCancellationDoesNotChangeOrdinaryConsumerOffsets() throws Exception {
        Assume.assumeTrue(serviceClass == ConsumeMessageConcurrentlyService.class);
        ThreadPoolExecutor shared = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("broadcast-discard-test");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeExecutor(shared);
        DefaultMQPushConsumerImpl impl = mock(DefaultMQPushConsumerImpl.class);
        when(impl.getDefaultMQPushConsumer()).thenReturn(consumer);
        OffsetStore offsetStore = mock(OffsetStore.class);
        when(impl.getOffsetStore()).thenReturn(offsetStore);
        ConsumeMessageConcurrentlyService service = new ConsumeMessageConcurrentlyService(impl,
            mock(MessageListenerConcurrently.class));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            shared.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            MessageQueue queue = new MessageQueue("system-topic", "broker", 0);
            MessageExt message = new MessageExt();
            message.setTopic(queue.getTopic());
            message.setQueueOffset(0);
            message.setBody(new byte[] {1});
            ProcessQueue processQueue = new ProcessQueue();
            processQueue.putMessage(Collections.singletonList(message));
            service.submitConsumeRequest(Collections.singletonList(message), processQueue, queue, true);
            assertTrue(((Future<?>) shared.getQueue().poll()).cancel(false));
            assertEquals(1, processQueue.getMsgCount().get());
            verifyNoInteractions(offsetStore);
            service.shutdown(5000);
            assertFalse(shared.isShutdown());
        } finally {
            release.countDown();
            service.shutdown(5000);
            shared.shutdownNow();
        }
    }

    @Test
    public void testVirtualThreadExecutorIsUsedDirectly() throws Exception {
        Method factory;
        try {
            factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        } catch (NoSuchMethodException e) {
            Assume.assumeNoException("Requires JDK 21 or later", e);
            return;
        }
        ExecutorService shared = (ExecutorService) factory.invoke(null);
        ConsumeMessageService service = createService(shared);
        try {
            ExecutorService actual = (ExecutorService) FieldUtils.readDeclaredField(service, "consumeExecutor", true);
            assertSame(shared, actual);
            Method isVirtual = Thread.class.getMethod("isVirtual");
            assertTrue(actual.submit(() -> (Boolean) isVirtual.invoke(Thread.currentThread())).get(5, TimeUnit.SECONDS));
            service.shutdown(5000);
            assertFalse(shared.isShutdown());
            assertTrue(shared.submit(() -> (Boolean) isVirtual.invoke(Thread.currentThread())).get(5, TimeUnit.SECONDS));
        } finally {
            service.shutdown(5000);
            shared.shutdownNow();
        }
    }

    @Test
    public void testExternalTasksAreLeftToTheOwnerOnShutdown() throws Exception {
        ExecutorService shared = Executors.newSingleThreadExecutor();
        ConsumeMessageService service = createService(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> running = shared.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            service.shutdown(0);
            assertFalse(shared.isShutdown());
            assertFalse(running.isDone());
            release.countDown();
            running.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            service.shutdown(0);
            shared.shutdownNow();
        }
    }

    private ConsumeMessageService createService(ExecutorService executor) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("shared-executor-test");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeExecutor(executor);
        DefaultMQPushConsumerImpl impl = mock(DefaultMQPushConsumerImpl.class);
        when(impl.getDefaultMQPushConsumer()).thenReturn(consumer);
        when(impl.messageModel()).thenReturn(MessageModel.BROADCASTING);
        return serviceClass.getConstructor(DefaultMQPushConsumerImpl.class, listenerClass)
            .newInstance(impl, mock(listenerClass));
    }
}
