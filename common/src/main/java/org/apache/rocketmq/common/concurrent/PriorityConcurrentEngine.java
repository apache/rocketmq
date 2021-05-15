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
package org.apache.rocketmq.common.concurrent;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.UtilAll;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PriorityConcurrentEngine extends ConcurrentEngine {

    /**
     * highest priority
     */
    public static final Integer MAX_PRIORITY = Integer.MIN_VALUE;

    /**
     * lowest priority
     */
    public static final Integer MIN_PRIORITY = Integer.MAX_VALUE;

    private static PeriodicConcurrentConsumeService CONSUME_SERVICE;

    private static final ConcurrentNavigableMap<Integer, Queue<Object>> PRIORITY_MAP = new ConcurrentSkipListMap<>();

    public static void runPriorityAsync(Runnable... tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public static void runPriorityAsync(Queue<Runnable> tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public static void runPriorityAsync(Collection<Runnable> tasks) {
        runPriorityAsync(MIN_PRIORITY, tasks);
    }

    public static void runPriorityAsync(Integer priority, Runnable... tasks) {
        runPriorityAsync(priority, UtilAll.newArrayList(tasks));
    }

    public static void runPriorityAsync(Integer priority, Queue<Runnable> tasks) {
        runPriorityAsync(priority, pollAllTask(tasks));
    }

    public static void runPriorityAsync(Integer priority, Collection<Runnable> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        Queue<Object> queue = PRIORITY_MAP.putIfAbsent(priority, new ConcurrentLinkedQueue<>());
        if (null == queue) {
            queue = PRIORITY_MAP.get(priority);
        }
        for (Runnable runnable : tasks) {
            queue.offer(runnable);
        }
    }

    @SafeVarargs
    public static <T> void supplyPriorityAsync(CallableSupplier<T>... tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    public static <T> void supplyPriorityAsync(Queue<CallableSupplier<T>> tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    public static <T> void supplyPriorityAsync(Collection<CallableSupplier<T>> tasks) {
        supplyPriorityAsync(MIN_PRIORITY, tasks);
    }

    @SafeVarargs
    public static <T> void supplyPriorityAsync(Integer priority, CallableSupplier<T>... tasks) {
        supplyPriorityAsync(priority, UtilAll.newArrayList(tasks));
    }

    public static <T> void supplyPriorityAsync(Integer priority, Queue<CallableSupplier<T>> tasks) {
        supplyPriorityAsync(priority, pollAllTask(tasks));
    }

    public static <T> void supplyPriorityAsync(Integer priority, Collection<CallableSupplier<T>> tasks) {
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        Queue<Object> queue = PRIORITY_MAP.putIfAbsent(priority, new ConcurrentLinkedQueue<>());
        if (null == queue) {
            queue = PRIORITY_MAP.get(priority);
        }
        for (CallableSupplier<T> supplier : tasks) {
            queue.offer(supplier);
        }
    }

    public static synchronized void invokeAllNow() {
        for (Queue<Object> queue : PRIORITY_MAP.values()) {
            Queue<Runnable> runnables = new ConcurrentLinkedQueue<>();
            Queue<CallableSupplier<Object>> callableSuppliers = new ConcurrentLinkedQueue<>();
            while (!queue.isEmpty()) {
                Object element = queue.poll();
                if (element instanceof Runnable) {
                    runnables.offer((Runnable) element);
                } else if (element instanceof CallableSupplier) {
                    callableSuppliers.offer((CallableSupplier<Object>) element);
                }
            }
            PriorityConcurrentEngine.runAsync(runnables);
            PriorityConcurrentEngine.supplyCallableAsync(callableSuppliers);
        }
    }

    public static synchronized void startAutoConsumer() {
        if (null == CONSUME_SERVICE) {
            CONSUME_SERVICE = new PeriodicConcurrentConsumeService();
        }
        if (!CONSUME_SERVICE.isStopped()) {
            CONSUME_SERVICE.start();
        }
    }

    public static void shutdown() {
        if (CONSUME_SERVICE != null && !CONSUME_SERVICE.isStopped()) {
            CONSUME_SERVICE.shutdown();
        }
        ConcurrentEngine.shutdown();
    }
}
