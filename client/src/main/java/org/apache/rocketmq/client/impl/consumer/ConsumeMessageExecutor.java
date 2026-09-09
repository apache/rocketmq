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
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A consumption service's task scope within an externally owned executor. Closing this scope
 * never shuts down the executor or waits for another consumer's tasks.
 */
class ConsumeMessageExecutor extends AbstractExecutorService {
    private final ExecutorService executor;
    private final Consumer<Runnable> discardedTaskHandler;
    private final Set<Task<?>> tasks = new HashSet<>();
    private boolean shutdown;
    private int runningTasks;

    ConsumeMessageExecutor(ExecutorService executor) {
        this(executor, task -> { });
    }

    ConsumeMessageExecutor(ExecutorService executor, Consumer<Runnable> discardedTaskHandler) {
        this.executor = executor;
        this.discardedTaskHandler = discardedTaskHandler;
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
        synchronized (this) {
            if (shutdown) {
                throw new RejectedExecutionException("Consumption service has stopped");
            }
            tasks.add(task);
        }
        try {
            executor.execute(task);
        } catch (RuntimeException | Error e) {
            task.rejected = true;
            task.cancel(false);
            throw e;
        }
    }

    @Override
    public synchronized void shutdown() {
        shutdown = true;
        notifyAll();
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Task<?>> snapshot;
        synchronized (this) {
            shutdown = true;
            snapshot = new ArrayList<>(tasks);
            notifyAll();
        }
        List<Runnable> pending = new ArrayList<>();
        for (Task<?> task : snapshot) {
            synchronized (this) {
                if (!task.started) {
                    pending.add(task);
                }
            }
            task.cancel(true);
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).remove(task);
            }
        }
        return pending;
    }

    @Override
    public synchronized boolean isShutdown() {
        return shutdown;
    }

    @Override
    public synchronized boolean isTerminated() {
        return shutdown && tasks.isEmpty() && runningTasks == 0;
    }

    @Override
    public synchronized boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long remaining = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remaining;
        while (!isTerminated()) {
            if (remaining <= 0) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = deadline - System.nanoTime();
        }
        return true;
    }

    private class Task<T> extends FutureTask<T> {
        private boolean started;
        private volatile boolean rejected;
        private final Runnable command;

        Task(Runnable runnable, T value) {
            super(runnable, value);
            this.command = runnable;
        }

        Task(Callable<T> callable) {
            super(callable);
            this.command = null;
        }

        @Override
        public void run() {
            synchronized (ConsumeMessageExecutor.this) {
                if (isCancelled()) {
                    return;
                }
                started = true;
                runningTasks++;
            }
            try {
                super.run();
            } finally {
                synchronized (ConsumeMessageExecutor.this) {
                    runningTasks--;
                    ConsumeMessageExecutor.this.notifyAll();
                }
            }
        }

        @Override
        protected void done() {
            boolean discarded;
            synchronized (ConsumeMessageExecutor.this) {
                discarded = isCancelled() && !started && !shutdown && !rejected && command != null;
            }
            try {
                if (discarded) {
                    discardedTaskHandler.accept(command);
                }
            } finally {
                synchronized (ConsumeMessageExecutor.this) {
                    tasks.remove(this);
                    ConsumeMessageExecutor.this.notifyAll();
                }
            }
        }
    }
}
