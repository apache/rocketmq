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

package org.apache.rocketmq.broker.config.v2;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for PR #11606: the RocksDB handle must not be released before the threads
 * stopped by {@code shutdown()} have exited, including when the caller is interrupted.
 */
public class ConfigStorageShutdownInterruptTest {

    private static final long FLUSH_WORKER_EXIT_DELAY_MILLIS = 500L;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    /** Holds the compaction executor busy; a monitor wait is not interruptible. */
    private final Object compactionGate = new Object();

    private ObservingConfigStorage storage;

    @Before
    public void setUp() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(tf.newFolder("store").getAbsolutePath());
        storage = new ObservingConfigStorage(config);
        Assert.assertTrue("config storage failed to start", storage.start());
        storage.startFlushWorker();
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.shutdown();
        }
    }

    /** The pool is idle, so awaitTermination() returns without checking the flag. */
    @Test
    public void testInterruptBeforeShutdownStillWaitsForFlushWorker() throws Exception {
        AtomicBoolean shutdownResult = new AtomicBoolean(false);
        AtomicBoolean interruptStatusAtExit = new AtomicBoolean(false);

        Thread caller = shutdownCaller(shutdownResult, interruptStatusAtExit, true);
        caller.start();

        caller.join(TimeUnit.SECONDS.toMillis(60));
        assertShutdownWaitedForFlushWorker(caller, shutdownResult, interruptStatusAtExit);
    }

    /** The caller is interrupted while blocked in awaitTermination(), as in the review. */
    @Test
    public void testInterruptDuringCompactionWaitStillWaitsForFlushWorker() throws Exception {
        ThreadPoolExecutor compactionPool = compactionPoolOf(storage);
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicBoolean shutdownResult = new AtomicBoolean(false);
        AtomicBoolean interruptStatusAtExit = new AtomicBoolean(false);

        Thread caller = shutdownCaller(shutdownResult, interruptStatusAtExit, false);

        synchronized (compactionGate) {
            // Occupy the compaction executor so that awaitTermination really blocks.
            compactionPool.submit(new Runnable() {
                @Override
                public void run() {
                    taskStarted.countDown();
                    synchronized (compactionGate) {
                        // Released when the test thread leaves the synchronized block.
                    }
                }
            });
            Assert.assertTrue("compaction task did not start", taskStarted.await(5, TimeUnit.SECONDS));

            caller.start();
            waitUntilBlocked(caller);

            caller.interrupt();
            // Let the caller leave awaitTermination and reach preShutdown().
            Thread.sleep(200);
        }

        caller.join(TimeUnit.SECONDS.toMillis(60));
        assertShutdownWaitedForFlushWorker(caller, shutdownResult, interruptStatusAtExit);
    }

    private Thread shutdownCaller(AtomicBoolean shutdownResult, AtomicBoolean interruptStatusAtExit,
        boolean interruptBeforeShutdown) {
        final Thread caller = new Thread(new Runnable() {
            @Override
            public void run() {
                if (interruptBeforeShutdown) {
                    Thread.currentThread().interrupt();
                }
                try {
                    shutdownResult.set(storage.shutdown());
                } finally {
                    interruptStatusAtExit.set(Thread.currentThread().isInterrupted());
                }
            }
        }, "config-storage-shutdown-caller");
        caller.setDaemon(true);
        return caller;
    }

    private void assertShutdownWaitedForFlushWorker(Thread caller, AtomicBoolean shutdownResult,
        AtomicBoolean interruptStatusAtExit) {
        Assert.assertFalse("shutdown caller did not finish", caller.isAlive());
        Assert.assertTrue("shutdown should still complete successfully", shutdownResult.get());
        Assert.assertFalse("flush worker must have exited before the DB handle is released",
            storage.flushWorkerAliveAfterJoin);
        Assert.assertFalse("interrupt flag must not be set while cleanup is still running",
            storage.interruptedSeenInPreShutdown);
        Assert.assertTrue("interrupt status must be restored when shutdown returns",
            interruptStatusAtExit.get());
    }

    private static void waitUntilBlocked(Thread thread) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.TIMED_WAITING || state == Thread.State.WAITING) {
                return;
            }
            Thread.sleep(5);
        }
        Assert.fail("shutdown caller never blocked in awaitTermination");
    }

    private static ThreadPoolExecutor compactionPoolOf(AbstractRocksDBStorage target) throws Exception {
        Field field = AbstractRocksDBStorage.class.getDeclaredField("manualCompactionThread");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(target);
    }

    /** A real ServiceThread that stays alive long enough for a premature join() to observe it. */
    static class BlockingFlushWorker extends ServiceThread {

        private final CountDownLatch entered = new CountDownLatch(1);
        private volatile long exitAtNanos;

        @Override
        public String getServiceName() {
            return "test-flush-worker";
        }

        @Override
        public void run() {
            exitAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(FLUSH_WORKER_EXIT_DELAY_MILLIS);
            entered.countDown();
            while (System.nanoTime() < exitAtNanos) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException ignored) {
                }
            }
        }

        boolean awaitEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return entered.await(timeout, unit);
        }

        boolean workerAlive() {
            Thread workerThread = this.thread;
            return workerThread != null && workerThread.isAlive();
        }
    }

    /** Records what the shutdown thread sees around the flush worker join. */
    static class ObservingConfigStorage extends ConfigStorage {

        private final BlockingFlushWorker flushWorker = new BlockingFlushWorker();

        volatile boolean interruptedSeenInPreShutdown;
        volatile boolean flushWorkerAliveAfterJoin;

        ObservingConfigStorage(MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        void startFlushWorker() throws InterruptedException {
            flushWorker.start();
            Assert.assertTrue("flush worker did not start", flushWorker.awaitEntered(5, TimeUnit.SECONDS));
        }

        @Override
        protected void preShutdown() {
            interruptedSeenInPreShutdown = Thread.currentThread().isInterrupted();
            flushWorker.shutdown();
            flushWorkerAliveAfterJoin = flushWorker.workerAlive();
            super.preShutdown();
        }
    }
}
