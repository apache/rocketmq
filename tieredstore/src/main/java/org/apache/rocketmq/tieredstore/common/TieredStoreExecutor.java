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
package org.apache.rocketmq.tieredstore.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.utils.ThreadUtils;

public class TieredStoreExecutor {

    public static final int QUEUE_CAPACITY = 10000;

    // Visible for monitor
    public static BlockingQueue<Runnable> dispatchThreadPoolQueue;
    public static BlockingQueue<Runnable> fetchDataThreadPoolQueue;
    public static BlockingQueue<Runnable> compactIndexFileThreadPoolQueue;

    public static ScheduledExecutorService commonScheduledExecutor;
    public static ScheduledExecutorService commitExecutor;
    public static ScheduledExecutorService cleanExpiredFileExecutor;

    public static ExecutorService dispatchExecutor;
    public static ExecutorService fetchDataExecutor;
    public static ExecutorService compactIndexFileExecutor;

    public static void init() {
        commonScheduledExecutor = ThreadUtils.newScheduledThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            new ThreadFactoryImpl("TieredCommonExecutor_"));

        commitExecutor = ThreadUtils.newScheduledThreadPool(
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            new ThreadFactoryImpl("TieredCommitExecutor_"));

        cleanExpiredFileExecutor = ThreadUtils.newScheduledThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            new ThreadFactoryImpl("TieredCleanFileExecutor_"));

        dispatchThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        dispatchExecutor = ThreadUtils.newThreadPoolExecutor(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            dispatchThreadPoolQueue,
            new ThreadFactoryImpl("TieredDispatchExecutor_"),
            new ThreadPoolExecutor.DiscardOldestPolicy());

        fetchDataThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        fetchDataExecutor = ThreadUtils.newThreadPoolExecutor(
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            Math.max(64, Runtime.getRuntime().availableProcessors() * 8),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            fetchDataThreadPoolQueue,
            new ThreadFactoryImpl("TieredFetchExecutor_"));

        compactIndexFileThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        compactIndexFileExecutor = ThreadUtils.newThreadPoolExecutor(
            1,
            1,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            compactIndexFileThreadPoolQueue,
            new ThreadFactoryImpl("TieredCompactIndexFileExecutor_"));
    }

    public static void shutdown() {
        shutdownExecutor(dispatchExecutor);
        shutdownExecutor(commonScheduledExecutor);
        shutdownExecutor(commitExecutor);
        shutdownExecutor(cleanExpiredFileExecutor);
        shutdownExecutor(fetchDataExecutor);
        shutdownExecutor(compactIndexFileExecutor);
    }

    private static void shutdownExecutor(ExecutorService executor) {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }
}
