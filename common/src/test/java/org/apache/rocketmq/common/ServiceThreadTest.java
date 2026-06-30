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

package org.apache.rocketmq.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ServiceThread}.
 *
 * <p>{@link ServiceThread} used to coordinate {@link ServiceThread#wakeup()} and
 * {@link ServiceThread#waitForRunning(long)} through a {@code CountDownLatch2} that was
 * {@code reset()} on every wait. A {@code wakeup()} landing between the fast-path check and
 * {@code reset()} performed a {@code countDown()} that {@code reset()} immediately discarded, so the
 * loop blocked for the whole interval while {@code hasNotified} stayed {@code true} (turning every
 * later {@code wakeup()} into a no-op). See apache/rocketmq#10543.
 *
 * <p>The implementation now uses {@link java.util.concurrent.locks.LockSupport} park/unpark, whose
 * permit semantics cannot drop a signal. The regression tests below assert the wakeup is always
 * delivered promptly, i.e. never stalls for the full interval.
 */
public class ServiceThreadTest {

    @Test
    public void testShutdown() {
        shutdown(false, false);
        shutdown(false, true);
        shutdown(true, false);
        shutdown(true, true);
    }

    @Test
    public void testMakeStop() {
        ServiceThread testServiceThread = startTestServiceThread();
        testServiceThread.makeStop();
        assertEquals(true, testServiceThread.isStopped());
    }

    @Test
    public void testWakeup() {
        ServiceThread testServiceThread = startTestServiceThread();
        testServiceThread.wakeup();
        assertEquals(true, testServiceThread.hasNotified.get());
    }

    @Test(timeout = 5000)
    public void testWaitForRunning() {
        ServiceThread testServiceThread = startTestServiceThread();
        // Not notified: returns after the (short) interval with the flag cleared.
        testServiceThread.waitForRunning(50);
        assertEquals(false, testServiceThread.hasNotified.get());
        // wakeup() arms the notification.
        testServiceThread.wakeup();
        assertEquals(true, testServiceThread.hasNotified.get());
        // The next waitForRunning() must consume the notification immediately, never blocking for
        // the (huge) interval -- this is exactly what the lost-wakeup race used to break.
        long begin = System.currentTimeMillis();
        testServiceThread.waitForRunning(TimeUnit.MINUTES.toMillis(1));
        long elapsed = System.currentTimeMillis() - begin;
        assertEquals(false, testServiceThread.hasNotified.get());
        assertTrue("waitForRunning() must fast-path on a pending notification, elapsed=" + elapsed + "ms",
            elapsed < 1000);
    }

    /**
     * A single {@code wakeup()} must wake a long-interval wait almost immediately, instead of
     * letting it block for the full interval (which is what the lost-wakeup race used to cause).
     */
    @Test(timeout = 5000)
    public void testWakeupDeliveredPromptly() throws Exception {
        TestServiceThread service = new TestServiceThread();
        AtomicBoolean returned = new AtomicBoolean(false);
        long longInterval = TimeUnit.SECONDS.toMillis(10);

        Thread waiter = new Thread(() -> {
            service.doWait(longInterval);
            returned.set(true);
        }, "waiter");
        waiter.start();

        // Let the waiter enter the park loop.
        Thread.sleep(200);

        long startNanos = System.nanoTime();
        service.wakeup();
        waiter.join(2000);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertTrue("waitForRunning() did not return after wakeup() within 2s, elapsed=" + elapsedMs + "ms",
            returned.get());
        assertTrue("wake latency should be far below the interval, elapsed=" + elapsedMs + "ms",
            elapsedMs < 2000);
    }

    /**
     * Hammer the exact pattern that triggered the lost-wakeup race: a {@code wakeup()} fired right
     * as the waiter is entering the wait. With the LockSupport-based implementation no signal may be
     * lost, so every iteration must return well within its (long) interval.
     */
    @Test(timeout = 60000)
    public void testNoWakeupLostUnderStress() throws Exception {
        int iterations = 1000;
        long longInterval = TimeUnit.SECONDS.toMillis(5);
        int lost = 0;

        for (int i = 0; i < iterations; i++) {
            TestServiceThread service = new TestServiceThread();
            AtomicBoolean returned = new AtomicBoolean(false);

            Thread waiter = new Thread(() -> {
                service.doWait(longInterval);
                returned.set(true);
            }, "waiter-" + i);
            waiter.start();

            // Increase the chance the wakeup lands in the CAS-to-park window.
            Thread.yield();
            service.wakeup();

            // With the fix the waiter returns in microseconds; a lost signal would block for the
            // full 5s interval, so a 2s join is more than enough to distinguish the two.
            waiter.join(2000);
            if (!returned.get()) {
                lost++;
                waiter.interrupt();
                waiter.join(1000);
            }
        }

        assertEquals("ServiceThread must not lose any wakeup signal", 0, lost);
    }

    /**
     * Single consumer draining {@code waitForRunning} in a tight loop while several threads race to
     * {@code wakeup()} it. A lost wakeup shows up as a wait that blocks for the full interval.
     */
    @Test(timeout = 30000)
    public void serviceThreadShouldNotLoseWakeupUnderStress() throws Exception {
        final int stressIterations = 10000;
        final int wakerThreads = 4;
        final long waitTimeoutMs = 20;
        final long lostWakeupThresholdMs = 18;

        StressServiceThread service = new StressServiceThread();
        AtomicInteger activeIteration = new AtomicInteger(-1);
        AtomicInteger completedIteration = new AtomicInteger(-1);
        AtomicInteger lostWakeups = new AtomicInteger(0);
        AtomicInteger maxElapsedMs = new AtomicInteger(0);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(wakerThreads + 1);

        try {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < stressIterations; i++) {
                        activeIteration.set(i);
                        long elapsed = service.awaitOnce(waitTimeoutMs);
                        maxElapsedMs.accumulateAndGet((int) elapsed, Math::max);
                        if (elapsed >= lostWakeupThresholdMs) {
                            lostWakeups.incrementAndGet();
                            running.set(false);
                            break;
                        }
                        completedIteration.set(i);
                        Thread.yield();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    running.set(false);
                }
            });

            for (int w = 0; w < wakerThreads; w++) {
                executor.submit(() -> {
                    while (running.get()) {
                        int iteration = activeIteration.get();
                        if (iteration >= 0 && completedIteration.get() < iteration) {
                            service.wakeup();
                        }
                        Thread.yield();
                    }
                });
            }

            executor.shutdown();
            assertTrue("stress test did not finish", executor.awaitTermination(25, TimeUnit.SECONDS));

            Throwable error = failure.get();
            if (error != null) {
                throw new AssertionError("stress test failed", error);
            }
            assertEquals("ServiceThread lost wakeups under stress (maxElapsedMs=" + maxElapsedMs.get() + ")",
                0, lostWakeups.get());
        } finally {
            running.set(false);
            executor.shutdownNow();
        }
    }

    private ServiceThread startTestServiceThread() {
        return startTestServiceThread(false);
    }

    private ServiceThread startTestServiceThread(boolean daemon) {
        ServiceThread testServiceThread = new TestServiceThread();
        testServiceThread.setDaemon(daemon);
        // test start
        testServiceThread.start();
        assertEquals(false, testServiceThread.isStopped());
        return testServiceThread;
    }

    public void shutdown(boolean daemon, boolean interrupt) {
        ServiceThread testServiceThread = startTestServiceThread(daemon);
        shutdown0(interrupt, testServiceThread);
        // repeat
        shutdown0(interrupt, testServiceThread);
    }

    private void shutdown0(boolean interrupt, ServiceThread testServiceThread) {
        if (interrupt) {
            testServiceThread.shutdown(true);
        } else {
            testServiceThread.shutdown();
        }
        assertEquals(true, testServiceThread.isStopped());
        assertEquals(true, testServiceThread.hasNotified.get());
    }

    private static class TestServiceThread extends ServiceThread {

        @Override
        public void run() {
            doNothing();
        }

        private void doNothing() {
        }

        @Override
        public String getServiceName() {
            return "TestServiceThread";
        }

        void doWait(long intervalMillis) {
            waitForRunning(intervalMillis);
        }
    }

    private static final class StressServiceThread extends ServiceThread {

        @Override
        public String getServiceName() {
            return "StressServiceThread";
        }

        @Override
        public void run() {
        }

        long awaitOnce(long intervalMillis) {
            long begin = System.nanoTime();
            waitForRunning(intervalMillis);
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        }
    }
}
