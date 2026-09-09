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
package org.apache.rocketmq.proxy.service.sysmessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SystemMessageConsumeExecutorTest {
    @Test
    public void testDefaults() {
        ProxyConfig config = new ProxyConfig();
        int processors = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(config);
        try {
            assertEquals(processors * 2, executor.getCorePoolSize());
            assertEquals(processors * 2, executor.getMaximumPoolSize());
            assertEquals(Integer.MAX_VALUE, executor.getQueue().remainingCapacity());
            assertTrue(executor.allowsCoreThreadTimeOut());
            assertTrue(executor.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testConfiguredPoolPreservesQueuedTasks() throws Exception {
        ProxyConfig config = new ProxyConfig();
        config.setSystemMessageConsumerThreadPoolCoreSize(1);
        ThreadPoolExecutor executor = SystemMessageConsumeExecutor.create(config);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> running = executor.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            List<Future<Integer>> queued = new ArrayList<>();
            for (int i = 0; i < 10001; i++) {
                final int result = i;
                queued.add(executor.submit(() -> result));
            }
            assertEquals(10001, executor.getQueue().size());
            assertEquals(1, executor.getPoolSize());
            assertFalse(running.isCancelled());
            assertFalse(queued.get(0).isDone());
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            running.get(5, TimeUnit.SECONDS);
            for (int i = 0; i < queued.size(); i++) {
                assertEquals(i, queued.get(i).get(5, TimeUnit.SECONDS).intValue());
            }
            try {
                executor.submit(() -> { });
                fail("Stopped executor must reject submission");
            } catch (RejectedExecutionException expected) {
                assertTrue(executor.isTerminated());
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
