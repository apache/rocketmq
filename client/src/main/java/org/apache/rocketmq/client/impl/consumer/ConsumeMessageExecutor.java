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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.future.FutureTaskExt;

/**
 * A consumption service's task scope within an externally owned executor. Closing this scope
 * never shuts down the executor or waits for another consumer's tasks.
 */
class ConsumeMessageExecutor extends AbstractExecutorService {
    private final ExecutorService executor;
    private final Set<Task<?>> tasks = new HashSet<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition termination = lock.newCondition();
    private volatile boolean shutdown;
    private int runningTasks;

    ConsumeMessageExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new Task<>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new Task<>(callable);
    }

    @Override
    public void execute(Runnable command) {
        Task<?> task = command instanceof ConsumeMessageExecutor.Task
            ? (Task<?>) command : new Task<>(command, null);
        lock.lock();
        try {
            if (shutdown) {
                throw new RejectedExecutionException("Consumption service has stopped");
            }
            tasks.add(task);
        } finally {
            lock.unlock();
        }
        try {
            executor.execute(task);
        } catch (RuntimeException | Error e) {
            task.cancel(false);
            throw e;
        }
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            termination.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Task<?>> snapshot;
        List<Runnable> pending = new ArrayList<>();
        lock.lock();
        try {
            shutdown = true;
            snapshot = new ArrayList<>(tasks);
            for (Task<?> task : snapshot) {
                if (!task.started) {
                    pending.add(task);
                }
            }
            termination.signalAll();
        } finally {
            lock.unlock();
        }
        for (Task<?> task : snapshot) {
            task.cancel(true);
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).remove(task);
            }
        }
        return pending;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        lock.lock();
        try {
            return isTerminatedLocked();
        } finally {
            lock.unlock();
        }
    }

    private boolean isTerminatedLocked() {
        return shutdown && tasks.isEmpty() && runningTasks == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long remaining = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (!isTerminatedLocked()) {
                if (remaining <= 0) {
                    return false;
                }
                remaining = termination.awaitNanos(remaining);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private class Task<T> extends FutureTaskExt<T> {
        private boolean started;

        Task(Runnable runnable, T value) {
            super(runnable, value);
        }

        Task(Callable<T> callable) {
            super(callable);
        }

        @Override
        public void run() {
            lock.lock();
            try {
                if (isCancelled()) {
                    return;
                }
                started = true;
                runningTasks++;
            } finally {
                lock.unlock();
            }
            try {
                super.run();
            } finally {
                lock.lock();
                try {
                    runningTasks--;
                    termination.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        @Override
        protected void done() {
            lock.lock();
            try {
                tasks.remove(this);
                termination.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
