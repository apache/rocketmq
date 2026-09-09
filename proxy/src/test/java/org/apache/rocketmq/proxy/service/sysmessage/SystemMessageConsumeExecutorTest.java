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
package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SystemMessageConsumeExecutorTest {
    @Test
    public void testDefaults() {
        ProxyConfig config = new ProxyConfig();
        int processors = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(config);
        try {
            assertEquals(processors, executor.getCorePoolSize());
            assertEquals(processors * 2, executor.getMaximumPoolSize());
            assertEquals(10000, executor.getQueue().remainingCapacity());
            assertTrue(executor.allowsCoreThreadTimeOut());
            assertTrue(executor.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.DiscardOldestPolicy);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConfiguredPoolDiscardsAndCancelsOldest() throws Exception {
        ProxyConfig config = new ProxyConfig();
        config.setSystemMessageConsumerThreadPoolCoreSize(1);
        config.setSystemMessageConsumerThreadPoolMaxSize(1);
        config.setSystemMessageConsumerThreadPoolQueueCapacity(1);
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(config);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> running = executor.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> oldest = executor.submit(() -> fail("Oldest task should have been discarded"));
            Future<Integer> newest = executor.submit(() -> 42);
            assertTrue(oldest.isCancelled());
            assertFalse(running.isCancelled());
            release.countDown();
            assertEquals(42, newest.get(5, TimeUnit.SECONDS).intValue());
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            try {
                executor.submit(() -> { });
                fail("Stopped executor must reject submission");
            } catch (RejectedExecutionException expected) {
                assertTrue(executor.isTerminated());
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
    @Test
    public void testProxyDiscardCleansBroadcastProcessQueue() throws Exception {
        ProxyConfig config = new ProxyConfig();
        config.setSystemMessageConsumerThreadPoolCoreSize(1);
        config.setSystemMessageConsumerThreadPoolMaxSize(1);
        config.setSystemMessageConsumerThreadPoolQueueCapacity(1);
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(config);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("proxy-discard-test");
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeExecutor(executor);
        DefaultMQPushConsumerImpl impl = mock(DefaultMQPushConsumerImpl.class);
        when(impl.getDefaultMQPushConsumer()).thenReturn(consumer);
        when(impl.getConsumerStatsManager()).thenReturn(mock(ConsumerStatsManager.class));
        OffsetStore offsetStore = mock(OffsetStore.class);
        when(impl.getOffsetStore()).thenReturn(offsetStore);
        MessageListenerConcurrently listener = mock(MessageListenerConcurrently.class);
        ConsumeMessageConcurrentlyService service = new ConsumeMessageConcurrentlyService(impl, listener);
        ScheduledExecutorService retryScheduler = mock(ScheduledExecutorService.class);
        FieldUtils.writeDeclaredField(service, "scheduledExecutorService", retryScheduler, true);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            executor.submit(() -> {
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
            Future<Integer> newest = executor.submit(() -> 42);
            assertEquals(0, processQueue.getMsgCount().get());
            verify(offsetStore).updateOffset(queue, 1L, true);
            verifyNoInteractions(listener, retryScheduler);
            verify(impl, never()).sendMessageBack(any(MessageExt.class), anyInt(), any(MessageQueue.class));
            service.shutdown(5000);
            assertFalse(executor.isShutdown());
            release.countDown();
            assertEquals(42, newest.get(5, TimeUnit.SECONDS).intValue());
        } finally {
            release.countDown();
            service.shutdown(5000);
            executor.shutdownNow();
        }
    }

}
