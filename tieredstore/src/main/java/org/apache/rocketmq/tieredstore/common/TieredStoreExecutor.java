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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;

public class TieredStoreExecutor {
    private static final int QUEUE_CAPACITY = 10000;
    public static ExecutorService DISPATCH_EXECUTOR;
    public static ScheduledExecutorService COMMON_SCHEDULED_EXECUTOR;
    public static ScheduledExecutorService COMMIT_EXECUTOR;
    public static ScheduledExecutorService CLEAN_EXPIRED_FILE_EXECUTOR;
    public static ExecutorService FETCH_DATA_EXECUTOR;
    public static ExecutorService COMPACT_INDEX_FILE_EXECUTOR;

    public static void init() {
        BlockingQueue<Runnable> dispatchThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        DISPATCH_EXECUTOR = new ThreadPoolExecutor(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            dispatchThreadPoolQueue,
            new ThreadFactoryImpl("TieredCommonExecutor_"));

        COMMON_SCHEDULED_EXECUTOR = new ScheduledThreadPoolExecutor(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            new ThreadFactoryImpl("TieredCommonScheduledExecutor_"));

        COMMIT_EXECUTOR = new ScheduledThreadPoolExecutor(
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            new ThreadFactoryImpl("TieredCommitExecutor_"));

        CLEAN_EXPIRED_FILE_EXECUTOR = new ScheduledThreadPoolExecutor(
            Math.max(4, Runtime.getRuntime().availableProcessors()),
            new ThreadFactoryImpl("TieredCleanExpiredFileExecutor_"));

        BlockingQueue<Runnable> fetchDataThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        FETCH_DATA_EXECUTOR = new ThreadPoolExecutor(
            Math.max(16, Runtime.getRuntime().availableProcessors() * 4),
            Math.max(64, Runtime.getRuntime().availableProcessors() * 8),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            fetchDataThreadPoolQueue,
            new ThreadFactoryImpl("TieredFetchDataExecutor_"));

        BlockingQueue<Runnable> compactIndexFileThreadPoolQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        COMPACT_INDEX_FILE_EXECUTOR = new ThreadPoolExecutor(
            1,
            1,
            1000 * 60,
            TimeUnit.MILLISECONDS,
            compactIndexFileThreadPoolQueue,
            new ThreadFactoryImpl("TieredCompactIndexFileExecutor_"));
    }

    public static void shutdown() {
        shutdownExecutor(DISPATCH_EXECUTOR);
        shutdownExecutor(COMMON_SCHEDULED_EXECUTOR);
        shutdownExecutor(COMMIT_EXECUTOR);
        shutdownExecutor(CLEAN_EXPIRED_FILE_EXECUTOR);
        shutdownExecutor(FETCH_DATA_EXECUTOR);
        shutdownExecutor(COMPACT_INDEX_FILE_EXECUTOR);
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
