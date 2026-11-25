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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.config.AbstractRocksDBStorage;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for the shutdown ordering issue reported on PR #11606.
 *
 * <p>When the shutdown caller is interrupted while waiting for the manual compaction executor, the
 * interrupt flag must not be restored until every remaining cleanup step has run. Restoring it too
 * early makes the downstream {@code Thread.join()} in {@code ServiceThread#shutdown()} throw
 * immediately, so the flush thread is never waited for while the RocksDB handle is still released.
 */
public class ConfigStorageShutdownInterruptTest {

    private static final String FLUSH_THREAD_NAME = "FlushSyncService";

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    /**
     * Held by the test thread to keep the compaction executor busy. A monitor wait cannot be
     * cancelled by {@code Thread.interrupt()}, which is what makes {@code awaitTermination} block.
     */
    private final Object compactionGate = new Object();

    private ObservingConfigStorage storage;

    @Before
    public void setUp() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(tf.newFolder("store").getAbsolutePath());
        storage = new ObservingConfigStorage(config);
        Assert.assertTrue("config storage failed to start", storage.start());
    }

    @After
    public void tearDown() {
        if (storage != null) {
            storage.shutdown();
        }
    }

    @Test
    public void testInterruptedShutdownWaitsForFlushThread() throws Exception {
        ThreadPoolExecutor compactionPool = compactionPoolOf(storage);
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicBoolean shutdownResult = new AtomicBoolean(false);
        AtomicBoolean interruptStatusAtExit = new AtomicBoolean(false);

        final Thread caller = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    shutdownResult.set(storage.shutdown());
                } finally {
                    interruptStatusAtExit.set(Thread.currentThread().isInterrupted());
                }
            }
        }, "config-storage-shutdown-caller");
        caller.setDaemon(true);

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
        Assert.assertFalse("shutdown caller did not finish", caller.isAlive());
        Assert.assertTrue("shutdown should still complete successfully", shutdownResult.get());
        Assert.assertFalse("interrupt flag must not be set while cleanup is still running",
            storage.interruptedSeenInPreShutdown);
        Assert.assertFalse("flush thread must have exited before the DB handle is released",
            storage.flushThreadAliveAfterJoin);
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

    private static boolean isFlushThreadAlive() {
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            Thread thread = entry.getKey();
            if (FLUSH_THREAD_NAME.equals(thread.getName()) && thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records what the shutdown thread observes right before and right after the flush service join.
     */
    static class ObservingConfigStorage extends ConfigStorage {

        volatile boolean interruptedSeenInPreShutdown;
        volatile boolean flushThreadAliveAfterJoin;

        ObservingConfigStorage(MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        protected void preShutdown() {
            interruptedSeenInPreShutdown = Thread.currentThread().isInterrupted();
            super.preShutdown();
            flushThreadAliveAfterJoin = isFlushThreadAlive();
        }
    }
}
