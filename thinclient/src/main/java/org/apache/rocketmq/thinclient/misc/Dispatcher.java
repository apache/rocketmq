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

package org.apache.rocketmq.thinclient.misc;

import com.google.common.util.concurrent.AbstractIdleService;
import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A driver to notify downstream to dispatch task.
 *
 * <p>Data flows from the upstream worker to the downstream processor, but there are many worker threads from
 * upstream which produce data concurrently, and the downstream processor could process the data quickly, so it is
 * not necessary for all workers to trigger the downstream task. To address this issue, there is a task queue whose
 * capacity is 1.
 *
 * <p>The invocation of {@link #notify()} from upstream is collected, once there is no task existed in task queue,
 * new task is created and delivered into the task queue, or the existed task in the queue could ensure the data workers
 * represented is processed by {@link #dispatch()} in time.
 *
 * <pre>
 *                ┌───────────┐
 *                │ processor │
 *                └──────▲────┘
 *                       │ dispatch
 *                       │
 *                   ┌───┴──┐
 *                   │      │  task queue
 *                   │      │
 *                   │size=1│
 *                   │      │
 *        ┌─────────►│      ◄──────────┐
 * notify │          └───▲──┘          │
 *        │       notify │      notify │
 *    ┌───┴────┐    ┌────┴───┐    ┌────┴───┐
 *    │ worker │    │ worker │    │ worker │
 *    └────────┘    └────────┘    └────────┘
 * </pre>
 */
public abstract class Dispatcher extends AbstractIdleService {
    private static final Logger LOGGER = LoggerFactory.getLogger(Dispatcher.class);
    private final String clientId;
    /**
     * Flag indicates that whether task is queued or not.
     */
    private final AtomicBoolean dispatched;
    private final ThreadPoolExecutor dispatcherExecutor;

    public Dispatcher(String clientId) {
        this.clientId = clientId;
        this.dispatched = new AtomicBoolean(false);
        this.dispatcherExecutor = new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("Dispatcher"));
    }

    @Override
    public void startUp() {
    }

    @Override
    public void shutDown() throws InterruptedException {
        dispatcherExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(dispatcherExecutor)) {
            LOGGER.error("[Bug] Failed to shutdown the batch dispatcher, clientId={}", clientId);
        }
    }

    /**
     * The implementation should never ever throw any exception.
     */
    public abstract void dispatch();

    /**
     * Signal downstream to execute {@link #dispatch()} in time.
     */
    public void signal() {
        if (dispatched.compareAndSet(false, true)) {
            try {
                dispatcherExecutor.submit(() -> {
                    dispatched.compareAndSet(true, false);
                    try {
                        dispatch();
                    } catch (Throwable t) {
                        LOGGER.error("Exception raised while dispatching task, clientId={}", clientId, t);
                    }
                });
            } catch (Throwable t) {
                if (!dispatcherExecutor.isShutdown()) {
                    LOGGER.error("[Bug] Failed to submit dispatch task, clientId={}", clientId, t);
                }
            }
        }
    }
}
