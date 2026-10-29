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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Reproduces the taskTable race in DefaultLitePullConsumerImpl class.
 *
 * updateAssignPullTask, updatePullTask and startPullTask perform a non-atomic
 * "containsKey then put" on taskTable. This unit test file is intended to
 * surface a data race
 */
public class DefaultLitePullConsumerImplTaskTableRaceTest {

    private static final int REPETITIONS = 500;

    private DefaultLitePullConsumerImpl consumer;
    private ConcurrentMap<MessageQueue, Object> taskTable;
    private ExecutorService racers;
    private AtomicInteger scheduleCallsThisRound;

    private Method updateAssignPullTaskMethod;
    private Method updatePullTaskMethod;
    private Method removePullTaskMethod;

    @Before
    public void setUp() throws Exception {
        consumer = new DefaultLitePullConsumerImpl(new DefaultLitePullConsumer(), null);
        scheduleCallsThisRound = new AtomicInteger(0);
        racers = Executors.newFixedThreadPool(2);

        ScheduledThreadPoolExecutor mockExecutor = mock(ScheduledThreadPoolExecutor.class);
        doAnswer(invocation -> {
            scheduleCallsThisRound.incrementAndGet();
            return null;
        }).when(mockExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        setPrivateField("scheduledThreadPoolExecutor", mockExecutor);

        // noinspection unchecked
        taskTable = (ConcurrentMap<MessageQueue, Object>) getPrivateField("taskTable");

        updateAssignPullTaskMethod = resolveMethod("updateAssignPullTask", Collection.class);
        updatePullTaskMethod = resolveMethod("updatePullTask", String.class, Set.class);
        removePullTaskMethod = resolveMethod("removePullTask", String.class);
    }

    @After
    public void tearDown() {
        racers.shutdownNow();
    }

    /**
     * Checks to see if concurrent
     * org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl.updatePullTask
     * method
     * does not double schedule on the same queue.
     * 
     * @throws Exception
     */
    @Test
    public void testUpdatePullTaskDoesNotDoubleScheduleSameQueue() throws Exception {
        String topic = "raceTopic";
        MessageQueue mq = new MessageQueue(topic, "brokerA", 0);
        Set<MessageQueue> mqDivided = Collections.singleton(mq);

        Runnable action = () -> consumer.updateAssignQueueAndStartPullTask(topic, Collections.emptySet(), mqDivided);

        assertNoRaceCondition(
                null, action, action,
                () -> scheduleCallsThisRound.get() > 1,
                "Unsynchronized containsKey-then-put race in startPullTask() (via updatePullTask) "
                        + "produced duplicate schedules.");
    }

    /**
     * Checks to see if concurrent
     * org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl.updateAssignPullTask
     * does not double schedule for the same queue.
     * 
     * @throws Exception
     */
    @Test
    public void testUpdateAssignPullTaskDoesNotDoubleScheduleSameQueue() throws Exception {
        String topic = "raceTopicAssign";
        MessageQueue mq = new MessageQueue(topic, "brokerA", 0);
        Collection<MessageQueue> mqNewSet = Collections.singleton(mq);

        Runnable action = () -> invoke(updateAssignPullTaskMethod, mqNewSet);

        assertNoRaceCondition(
                null, action, action,
                () -> scheduleCallsThisRound.get() > 1,
                "Unsynchronized containsKey-then-put race in startPullTask() (via updateAssignPullTask, "
                        + "the assign()/start() path) produced duplicate schedules.");
    }

    /**
     * Tests
     * org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl.removePullTask
     * is threadsafe.
     */
    @Test
    public void testRemovePullTaskDoesNotCorruptUnrelatedTopicAddition() throws Exception {
        String removedTopic = "raceTopicRemove";
        String keptTopic = "raceTopicKeep";
        MessageQueue mqToRemove = new MessageQueue(removedTopic, "brokerA", 0);
        MessageQueue mqToKeep = new MessageQueue(keptTopic, "brokerA", 0);

        Runnable setup = () -> invoke(updateAssignPullTaskMethod, Collections.singleton(mqToRemove));
        Runnable removeAction = () -> invoke(removePullTaskMethod, removedTopic);
        Runnable addAction = () -> invoke(updatePullTaskMethod, keptTopic, Collections.singleton(mqToKeep));

        assertNoRaceCondition(
                setup, removeAction, addAction,
                () -> {
                    boolean removedTopicStillPresent = taskTable.keySet().stream()
                            .anyMatch(mq -> mq.getTopic().equals(removedTopic));
                    return removedTopicStillPresent || !taskTable.containsKey(mqToKeep)
                            || scheduleCallsThisRound.get() != 1;
                },
                "Shared taskTable lock corruption detected during concurrent add/remove operations.");
    }

    private void assertNoRaceCondition(Runnable roundSetup, Runnable action1, Runnable action2,
            Supplier<Boolean> failureCondition, String errorMessage) throws Exception {
        int failedRounds = 0;
        // in order to reproduce potential data race, run it REPETITIONS number of
        // times.
        for (int round = 0; round < REPETITIONS; round++) {
            taskTable.clear();
            scheduleCallsThisRound.set(0);

            if (roundSetup != null) {
                roundSetup.run();
                scheduleCallsThisRound.set(0);
            }

            raceTwo(action1, action2);

            if (failureCondition.get()) {
                failedRounds++;
            }
        }
        assertEquals(errorMessage + " Saw " + failedRounds + "/" + REPETITIONS + " corrupted rounds.",
                0, failedRounds);
    }

    /**
     * Using a cyclicbarrier to gate tasks start at same time.
     * 
     * @param first
     * @param second
     * @throws Exception
     */
    private void raceTwo(Runnable first, Runnable second) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> runAtBarrier(first, barrier), racers);
        CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> runAtBarrier(second, barrier), racers);
        CompletableFuture.allOf(f1, f2).get(5, TimeUnit.SECONDS);
    }

    private void runAtBarrier(Runnable action, CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        action.run();
    }

    private void setPrivateField(String fieldName, Object value) throws Exception {
        Field field = DefaultLitePullConsumerImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(consumer, value);
    }

    private Object getPrivateField(String fieldName) throws Exception {
        Field field = DefaultLitePullConsumerImpl.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(consumer);
    }

    private Method resolveMethod(String name, Class<?>... parameterTypes) throws NoSuchMethodException {
        Method method = DefaultLitePullConsumerImpl.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private void invoke(Method method, Object... args) {
        try {
            method.invoke(consumer, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
