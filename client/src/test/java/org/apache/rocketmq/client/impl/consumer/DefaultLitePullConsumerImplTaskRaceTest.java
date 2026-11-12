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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultLitePullConsumerImplTaskRaceTest {

    @Test
    public void testConcurrentAssignmentUpdatesKeepTasksConsistent() throws Exception {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("taskRaceGroup");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        DefaultLitePullConsumerImpl consumerImpl = new DefaultLitePullConsumerImpl(consumer, null);
        ScheduledThreadPoolExecutor pullExecutor = new ScheduledThreadPoolExecutor(1);
        Field executorField = DefaultLitePullConsumerImpl.class.getDeclaredField("scheduledThreadPoolExecutor");
        executorField.setAccessible(true);
        executorField.set(consumerImpl, pullExecutor);

        MessageQueue originalQueue = new MessageQueue("topic", "broker", 0);
        MessageQueue firstQueue = new MessageQueue("topic", "broker", 1);
        MessageQueue secondQueue = new MessageQueue("topic", "broker", 2);
        consumerImpl.updateAssignQueueAndStartPullTask("topic", Collections.singleton(originalQueue),
            Collections.singleton(originalQueue));

        CountDownLatch firstReconciliationStarted = new CountDownLatch(1);
        CountDownLatch continueFirstReconciliation = new CountDownLatch(1);
        CountDownLatch secondUpdateFinished = new CountDownLatch(1);
        AtomicInteger containsCalls = new AtomicInteger();
        Set<MessageQueue> firstAssignment = new HashSet<MessageQueue>() {
            @Override
            public boolean contains(Object value) {
                if (containsCalls.incrementAndGet() == 2) {
                    firstReconciliationStarted.countDown();
                    try {
                        if (!continueFirstReconciliation.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting to continue task reconciliation");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while waiting to continue task reconciliation", e);
                    }
                }
                return super.contains(value);
            }
        };
        firstAssignment.add(firstQueue);
        Set<MessageQueue> secondAssignment = Collections.singleton(secondQueue);

        ExecutorService updateExecutor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstUpdate = updateExecutor.submit(() -> consumerImpl.updateAssignQueueAndStartPullTask(
                "topic", firstAssignment, firstAssignment));
            assertTrue(firstReconciliationStarted.await(5, TimeUnit.SECONDS));

            Future<?> secondUpdate = updateExecutor.submit(() -> {
                consumerImpl.updateAssignQueueAndStartPullTask("topic", secondAssignment, secondAssignment);
                secondUpdateFinished.countDown();
            });
            assertFalse("Second assignment interleaved with task reconciliation",
                secondUpdateFinished.await(100, TimeUnit.MILLISECONDS));
            assertEquals(Collections.singleton(firstQueue), consumerImpl.assignment());

            continueFirstReconciliation.countDown();
            firstUpdate.get(5, TimeUnit.SECONDS);
            secondUpdate.get(5, TimeUnit.SECONDS);

            assertEquals(secondAssignment, consumerImpl.assignment());
            Field taskTableField = DefaultLitePullConsumerImpl.class.getDeclaredField("taskTable");
            taskTableField.setAccessible(true);
            Map<?, ?> taskTable = (Map<?, ?>) taskTableField.get(consumerImpl);
            assertEquals(secondAssignment, taskTable.keySet());
        } finally {
            continueFirstReconciliation.countDown();
            updateExecutor.shutdownNow();
            pullExecutor.shutdownNow();
        }
    }
}
