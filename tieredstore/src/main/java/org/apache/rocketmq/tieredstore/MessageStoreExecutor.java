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
package org.apache.rocketmq.tieredstore;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStoreExecutor {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final BlockingQueue<Runnable> bufferCommitThreadPoolQueue;
    private final BlockingQueue<Runnable> bufferFetchThreadPoolQueue;

    private final ScheduledExecutorService commonExecutor;
    private final ExecutorService bufferCommitExecutor;
    private final ExecutorService bufferFetchExecutor;

    public MessageStoreExecutor() {
        this(10000);
    }

    public MessageStoreExecutor(int maxQueueCapacity) {
        int processors = Runtime.getRuntime().availableProcessors();

        this.commonExecutor = ThreadUtils.newScheduledThreadPool(
            Math.max(4, processors),
            new ThreadFactoryImpl("TieredCommonExecutor_"));

        this.bufferCommitThreadPoolQueue = new LinkedBlockingQueue<>(maxQueueCapacity);
        this.bufferCommitExecutor = ThreadUtils.newThreadPoolExecutor(
            Math.max(4, processors),
            Math.max(16, processors * 2),
            TimeUnit.MINUTES.toMillis(1), TimeUnit.MILLISECONDS,
            this.bufferCommitThreadPoolQueue,
            new ThreadFactoryImpl("BufferCommitExecutor_"));

        this.bufferFetchThreadPoolQueue = new LinkedBlockingQueue<>(maxQueueCapacity);
        this.bufferFetchExecutor = ThreadUtils.newThreadPoolExecutor(
            Math.max(4, processors),
            Math.max(16, processors * 2),
            TimeUnit.MINUTES.toMillis(1), TimeUnit.MILLISECONDS,
            this.bufferFetchThreadPoolQueue,
            new ThreadFactoryImpl("BufferFetchExecutor_"));
    }

    public ScheduledExecutorService getCommonExecutor() {
        return commonExecutor;
    }

    public ExecutorService getBufferCommitExecutor() {
        return bufferCommitExecutor;
    }

    public ExecutorService getBufferFetchExecutor() {
        return bufferFetchExecutor;
    }

    private void shutdownExecutor(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        this.shutdownExecutor(this.commonExecutor);
        this.shutdownExecutor(this.bufferCommitExecutor);
        this.shutdownExecutor(this.bufferFetchExecutor);
        log.info("MessageStoreExecutor shutdown complete");
    }
}
