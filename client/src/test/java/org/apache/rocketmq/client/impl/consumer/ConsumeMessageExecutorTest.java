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

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsumeMessageExecutorTest {
    @Test
    public void testShutdownDoesNotWaitForOtherConsumerOrCloseExecutor() throws Exception {
        ExecutorService shared = Executors.newFixedThreadPool(2);
        ConsumeMessageExecutor first = new ConsumeMessageExecutor(shared);
        ConsumeMessageExecutor second = new ConsumeMessageExecutor(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            second.submit(() -> {
                entered.countDown();
                await(release);
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertEquals(1, first.submit(() -> 1).get(5, TimeUnit.SECONDS).intValue());
            first.shutdown();
            assertTrue(first.awaitTermination(5, TimeUnit.SECONDS));
            assertFalse(shared.isShutdown());
            assertFalse(second.isShutdown());
            assertEquals(2, second.submit(() -> 2).get(5, TimeUnit.SECONDS).intValue());
            try {
                first.submit(() -> 3);
                fail("Stopped consumer accepted a task");
            } catch (RejectedExecutionException expected) {
                assertTrue(first.isTerminated());
            }
        } finally {
            release.countDown();
            first.shutdownNow();
            second.shutdownNow();
            shared.shutdownNow();
        }
    }

    @Test
    public void testShutdownNowOnlyCancelsOwnTasks() throws Exception {
        ThreadPoolExecutor shared = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ConsumeMessageExecutor first = new ConsumeMessageExecutor(shared);
        ConsumeMessageExecutor second = new ConsumeMessageExecutor(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> other = second.submit(() -> {
                entered.countDown();
                await(release);
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> pending = first.submit(() -> fail("Cancelled task executed"));
            assertEquals(1, first.shutdownNow().size());
            assertTrue(pending.isCancelled());
            assertTrue(first.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(shared.getQueue().isEmpty());
            assertFalse(other.isCancelled());
            release.countDown();
            other.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            first.shutdownNow();
            second.shutdownNow();
            shared.shutdownNow();
        }
    }

    @Test
    public void testCancellationDoesNotReportRunningTaskAsTerminated() throws Exception {
        ExecutorService shared = Executors.newSingleThreadExecutor();
        ConsumeMessageExecutor scope = new ConsumeMessageExecutor(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            scope.submit(() -> {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException expected) {
                    interrupted.countDown();
                    await(release);
                }
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            scope.shutdownNow();
            assertTrue(interrupted.await(5, TimeUnit.SECONDS));
            assertFalse(scope.awaitTermination(0, TimeUnit.SECONDS));
            release.countDown();
            assertTrue(scope.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            scope.shutdownNow();
            shared.shutdownNow();
        }
    }

    @Test
    public void testRejectedAndDiscardedTasksReleaseTracking() throws Exception {
        ThreadPoolExecutor shared = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(1));
        ConsumeMessageExecutor scope = new ConsumeMessageExecutor(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            scope.submit(() -> {
                entered.countDown();
                await(release);
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            Future<?> queued = scope.submit(() -> fail("Discarded task executed"));
            try {
                scope.submit(() -> { });
                fail("Expected rejection");
            } catch (RejectedExecutionException expected) {
                assertFalse(scope.isShutdown());
            }
            Runnable oldest = shared.getQueue().poll();
            assertTrue(((Future<?>) oldest).cancel(false));
            assertTrue(queued.isCancelled());
            scope.shutdown();
            release.countDown();
            assertTrue(scope.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            scope.shutdownNow();
            shared.shutdownNow();
        }
    }

    @Test
    public void testVirtualThreadExecutor() throws Exception {
        Method factory;
        try {
            factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        } catch (NoSuchMethodException e) {
            Assume.assumeNoException("Requires JDK 21 or later", e);
            return;
        }
        ExecutorService shared = (ExecutorService) factory.invoke(null);
        ConsumeMessageExecutor first = new ConsumeMessageExecutor(shared);
        ConsumeMessageExecutor second = new ConsumeMessageExecutor(shared);
        try {
            Method isVirtual = Thread.class.getMethod("isVirtual");
            assertTrue(first.submit(() -> (Boolean) isVirtual.invoke(Thread.currentThread())).get(5, TimeUnit.SECONDS));
            first.shutdown();
            assertTrue(first.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(second.submit(() -> (Boolean) isVirtual.invoke(Thread.currentThread())).get(5, TimeUnit.SECONDS));
            assertFalse(shared.isShutdown());
        } finally {
            first.shutdownNow();
            second.shutdownNow();
            shared.shutdownNow();
        }
    }

    @Test
    public void testVirtualThreadCanAwaitConsumerTermination() throws Exception {
        Method factory;
        try {
            factory = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
        } catch (NoSuchMethodException e) {
            Assume.assumeNoException("Requires JDK 21 or later", e);
            return;
        }
        ExecutorService shared = (ExecutorService) factory.invoke(null);
        ConsumeMessageExecutor scope = new ConsumeMessageExecutor(shared);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch waiting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            scope.submit(() -> {
                entered.countDown();
                await(release);
            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            scope.shutdown();
            Future<Boolean> terminated = shared.submit(() -> {
                waiting.countDown();
                return scope.awaitTermination(5, TimeUnit.SECONDS);
            });
            assertTrue(waiting.await(5, TimeUnit.SECONDS));
            // Must also progress when the JVM is configured with a single virtual-thread carrier.
            shared.submit(release::countDown).get(5, TimeUnit.SECONDS);
            assertTrue(terminated.get(5, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            scope.shutdownNow();
            shared.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
