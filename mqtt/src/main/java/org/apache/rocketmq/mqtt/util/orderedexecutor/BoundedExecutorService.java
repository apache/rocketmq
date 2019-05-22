/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.mqtt.util.orderedexecutor;

import com.google.common.util.concurrent.ForwardingExecutorService;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements {@link ExecutorService} and allows limiting the number of tasks to
 * be scheduled in the thread's queue.
 */
public class BoundedExecutorService extends ForwardingExecutorService {
    private final BlockingQueue<Runnable> queue;
    private final ThreadPoolExecutor thread;
    private final int maxTasksInQueue;

    public BoundedExecutorService(ThreadPoolExecutor thread, int maxTasksInQueue) {
        this.queue = thread.getQueue();
        this.thread = thread;
        this.maxTasksInQueue = maxTasksInQueue;
    }

    @Override
    protected ExecutorService delegate() {
        return this.thread;
    }

    private void checkQueue(int numberOfTasks) {
        if (maxTasksInQueue > 0 && (queue.size() + numberOfTasks) > maxTasksInQueue) {
            throw new RejectedExecutionException("Queue at limit of " + maxTasksInQueue + " items");
        }
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        checkQueue(tasks.size());
        return super.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        checkQueue(tasks.size());
        return super.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        checkQueue(tasks.size());
        return super.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        checkQueue(tasks.size());
        return super.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        checkQueue(1);
        super.execute(command);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        checkQueue(1);
        return super.submit(task);
    }

    @Override
    public Future<?> submit(Runnable task) {
        checkQueue(1);
        return super.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        checkQueue(1);
        return super.submit(task, result);
    }
}
