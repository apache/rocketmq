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

package org.apache.rocketmq.broker.latency;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerFixedThreadPoolExecutor extends ThreadPoolExecutor {
    public BrokerFixedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
        final TimeUnit unit,
        final BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public BrokerFixedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
        final TimeUnit unit,
        final BlockingQueue<Runnable> workQueue, final ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public BrokerFixedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
        final TimeUnit unit,
        final BlockingQueue<Runnable> workQueue, final RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public BrokerFixedThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
        final TimeUnit unit,
        final BlockingQueue<Runnable> workQueue, final ThreadFactory threadFactory,
        final RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(final Runnable runnable, final T value) {
        return new FutureTaskExt<>(runnable, value);
    }
}
