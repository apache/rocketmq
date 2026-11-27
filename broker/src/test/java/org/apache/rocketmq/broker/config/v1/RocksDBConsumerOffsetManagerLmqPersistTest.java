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
package org.apache.rocketmq.broker.config.v1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Level;
import org.apache.rocketmq.logging.ch.qos.logback.classic.Logger;
import org.apache.rocketmq.logging.ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.rocketmq.logging.ch.qos.logback.core.Appender;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class RocksDBConsumerOffsetManagerLmqPersistTest {
    private static final String GROUP = "lmq-group";
    private static final String TOPIC = MixAll.LMQ_PREFIX + "offset-test";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final List<CountDownLatch> releases = new ArrayList<>();
    private RocksDBConsumerOffsetManager manager;
    private BrokerController controller;
    private BrokerConfig brokerConfig;
    private RocksDBConfigManager storage;

    @Before
    public void setUp() throws Exception {
        controller = Mockito.mock(BrokerController.class);
        brokerConfig = new BrokerConfig();
        brokerConfig.setPersistConsumerOffsetIncrementally(true);
        brokerConfig.setConsumerOffsetUpdateVersionStep(Long.MAX_VALUE);
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(temporaryFolder.newFolder("store").getAbsolutePath());
        Mockito.when(controller.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(controller.getMessageStoreConfig()).thenReturn(storeConfig);
        loadNewManager();
    }

    @After
    public void tearDown() throws Exception {
        for (CountDownLatch release : releases) {
            release.countDown();
        }
        if (manager != null) {
            ScheduledExecutorService executor = workerExecutor();
            if (executor != null) {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
            Mockito.doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            Mockito.doCallRealMethod().when(storage).flushWALChecked();
            Mockito.doCallRealMethod().when(storage).updateKvDataVersion();
            assertTrue(manager.stop());
        }
    }

    @Test(timeout = 15000)
    public void defaultAsyncPersistIntervalUsesFixedDelay() throws Exception {
        assertEquals(100, brokerConfig.getConsumerOffsetBatchIntervalMs());
        assertWorkerDelay(100);
    }

    @Test(timeout = 15000)
    public void asyncPersistIntervalIsReadWhenWorkerStarts() throws Exception {
        assertTrue(manager.stop());
        brokerConfig.setConsumerOffsetBatchIntervalMs(321);
        assertTrue(manager.load());
        assertWorkerDelay(321);
        ScheduledExecutorService executor = workerExecutor();
        brokerConfig.setConsumerOffsetBatchIntervalMs(17);
        assertTrue(manager.load());
        assertSame(executor, workerExecutor());
        assertWorkerDelay(321);
        assertTrue(manager.stop());
        assertTrue(manager.load());
        assertWorkerDelay(17);
    }

    @Test(timeout = 15000)
    public void asyncPersistIntervalRejectsNonPositiveValues() {
        for (long interval : new long[] {0, -1, Long.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                () -> brokerConfig.setConsumerOffsetBatchIntervalMs(interval));
        }
        assertEquals(100, brokerConfig.getConsumerOffsetBatchIntervalMs());
        brokerConfig.setConsumerOffsetBatchIntervalMs(1);
        assertEquals(1, brokerConfig.getConsumerOffsetBatchIntervalMs());
    }

    @Test(timeout = 15000)
    public void repeatedLoadKeepsSinglePeriodicTask() throws Exception {
        ScheduledExecutorService executor = workerExecutor();
        try (WorkerBlock ignored = new WorkerBlock(executor, newRelease())) {
            assertTrue(manager.load());
            assertSame(executor, workerExecutor());
            assertEquals(1, workerPool().getQueue().size());
        }
    }

    @Test(timeout = 15000)
    public void concurrentCommitsKeepSinglePeriodicTask() throws Exception {
        ExecutorService writers = Executors.newFixedThreadPool(8);
        try (WorkerBlock ignored = new WorkerBlock(workerExecutor(), newRelease())) {
            List<Future<?>> committed = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                String topic = TOPIC + i;
                committed.add(writers.submit(() -> manager.commitOffset("client", GROUP, topic, 0, 1)));
            }
            for (Future<?> future : committed) {
                future.get(5, TimeUnit.SECONDS);
            }
            assertEquals(1, workerPool().getQueue().size());
            assertEquals(8, pendingCount());
        } finally {
            writers.shutdownNow();
        }
        manager.persist();
        for (int i = 0; i < 8; i++) {
            assertEquals(Long.valueOf(1), readStoredOffset(TOPIC + i));
        }
    }

    @Test(timeout = 15000)
    public void stoppedManagerKeepsCommitsInMemoryAndCanReload() throws Exception {
        for (boolean incremental : new boolean[] {false, true}) {
            brokerConfig.setPersistConsumerOffsetIncrementally(incremental);
            manager.commitOffset("client", GROUP, TOPIC, 0, 1);
            ScheduledExecutorService executor = workerExecutor();
            assertTrue(manager.stop());
            assertTrue(executor.isTerminated());
            assertNull(workerExecutor());
            assertCommitOnlyKeptInMemory(manager);
            assertTrue(manager.stop());
            assertTrue(manager.load());
            assertNotNull(workerExecutor());
        }
    }

    @Test(timeout = 15000)
    public void unloadedManagerKeepsCommitsInMemoryAndClosesWithoutExecutor() throws Exception {
        String path = temporaryFolder.newFolder("unloaded").getAbsolutePath();
        RocksDBConsumerOffsetManager unloaded = new RocksDBConsumerOffsetManager(controller, false, path);
        try {
            for (boolean incremental : new boolean[] {false, true}) {
                brokerConfig.setPersistConsumerOffsetIncrementally(incremental);
                assertCommitOnlyKeptInMemory(unloaded);
            }
            assertNull(persisterField(unloaded, "executor"));
            assertTrue(unloaded.stop());
        } finally {
            assertTrue(unloaded.stop());
        }
    }

    @Test(timeout = 15000)
    public void failedMetadataLoadDoesNotStartExecutor() throws Exception {
        assertTrue(manager.stop());
        manager = new RocksDBConsumerOffsetManager(controller);
        storage = Mockito.spy(manager.rocksDBConfigManager);
        manager.rocksDBConfigManager = storage;
        Mockito.doReturn(false).when(storage).loadDataVersion();
        assertFalse(manager.load());
        assertTrue(storage.isLoaded());
        assertCommitOnlyKeptInMemory(manager);
        assertNull(workerExecutor());
        assertTrue(manager.stop());
        assertFalse(storage.isLoaded());
        assertFalse(manager.load());
    }

    @Test(timeout = 15000)
    public void coalescesUpdatesWithoutWritingOnCommitThread() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            for (int i = 0; i < 4096; i++) {
                manager.commitOffset("client", GROUP, TOPIC, 0, i);
            }
            assertEquals(4095, manager.queryOffset(GROUP, TOPIC, 0));
            assertEquals(1, pendingCount());
            assertNull(readStoredOffset(TOPIC));
            Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
        }
        manager.persist();
        assertEquals(Long.valueOf(4095), readStoredOffset(TOPIC));
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void unchangedOffsetsKeepPendingWriteButDoNotWriteAcrossBatches() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            for (String topic : new String[] {TOPIC, "ordinary-unchanged"}) {
                Mockito.clearInvocations(storage);
                manager.commitOffset("client", GROUP, topic, 0, 1000);
                manager.commitOffset("client", GROUP, topic, 0, 1000);
                assertEquals(1, pendingCount());
                assertNull(readStoredOffset(topic));
                manager.persist();
                assertEquals(Long.valueOf(1000), readStoredOffset(topic));
                Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));

                Mockito.clearInvocations(storage);
                for (int i = 0; i < 10; i++) {
                    manager.commitOffset("client", GROUP, topic, 0, 1000);
                    assertEquals(0, pendingCount());
                    flushOneBatch();
                    manager.persist();
                }
                Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));

                manager.commitOffset("client", GROUP, topic, 0, 1001);
                assertEquals(1, pendingCount());
                manager.persist();
                assertEquals(Long.valueOf(1001), readStoredOffset(topic));
                manager.commitOffset("client", GROUP, topic, 0, 1000);
                assertEquals(1, pendingCount());
                manager.persist();
                assertEquals(Long.valueOf(1000), readStoredOffset(topic));
                Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
            }
        }
    }

    @Test(timeout = 15000)
    public void newQueueWithSameValueIsStillPersisted() throws Exception {
        String topic = "ordinary-new-queue";
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, topic, 0, 0);
            manager.persist();
            assertEquals(Long.valueOf(0), readStoredOffset(topic));
            manager.commitOffset("client", GROUP, topic, 1, 0);
            assertEquals(1, pendingCount());
            manager.persist();
            Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
        }
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(0, manager.queryOffset(GROUP, topic, 0));
        assertEquals(0, manager.queryOffset(GROUP, topic, 1));
    }

    @Test(timeout = 15000)
    public void concurrentFirstCommitsKeepAllQueueOffsets() throws Exception {
        ExecutorService writers = Executors.newFixedThreadPool(8);
        try (WorkerBlock ignored = blockWorker()) {
            for (String topic : new String[] {TOPIC, "ordinary-first-commits"}) {
                String key = topic + "@" + GROUP;
                ConcurrentMap<String, ConcurrentMap<Integer, Long>> table =
                    Mockito.spy(new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>());
                manager.setOffsetTable(table);
                CountDownLatch firstReads = new CountDownLatch(8);
                Mockito.doAnswer(invocation -> {
                    Object current = invocation.callRealMethod();
                    firstReads.countDown();
                    await(firstReads);
                    return current;
                }).when(table).get(key);
                List<Future<?>> committed = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    final int queueId = i;
                    committed.add(writers.submit(() ->
                        manager.commitOffset("client", GROUP, topic, queueId, queueId + 1000)));
                }
                for (Future<?> future : committed) {
                    future.get(5, TimeUnit.SECONDS);
                }
                assertEquals(8, table.get(key).size());
                assertEquals(1, pendingCount());
                manager.persist();
                byte[] value = storage.configRocksDBStorage.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY,
                    StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8));
                RocksDBOffsetSerializeWrapper persisted = JSON.parseObject(value, RocksDBOffsetSerializeWrapper.class);
                assertEquals(8, persisted.getOffsetTable().size());
                for (int queueId = 0; queueId < 8; queueId++) {
                    assertEquals(Long.valueOf(queueId + 1000), persisted.getOffsetTable().get(queueId));
                }
            }
        } finally {
            writers.shutdownNow();
        }
    }

    @Test(timeout = 15000)
    public void unchangedCommitDuringWriteDoesNotRequeue() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        Mockito.doAnswer(invocation -> {
            writing.countDown();
            await(release);
            return invocation.callRealMethod();
        }).when(storage).batchPutWithWal(any(WriteBatch.class));
        manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
        await(writing);
        manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
        assertEquals(0, pendingCount());
        release.countDown();
        manager.persist();
        assertEquals(Long.valueOf(1000), readStoredOffset(TOPIC));
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void unchangedCommitDoesNotCancelFailedBatchRetry() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            Mockito.doThrow(new RocksDBException("injected unchanged offset write failure"))
                .doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            manager.persist();
            assertEquals(1, pendingCount());
            assertNull(readStoredOffset(TOPIC));
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            assertEquals(1, pendingCount());
            manager.persist();
            assertEquals(0, pendingCount());
            assertEquals(Long.valueOf(1000), readStoredOffset(TOPIC));
            Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
        }
    }

    @Test(timeout = 15000)
    public void unchangedCommitDoesNotTakePendingLockAndKeepsVersionSemantics() throws Exception {
        ExecutorService committing = Executors.newSingleThreadExecutor();
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            manager.persist();
            Mockito.clearInvocations(storage);
            brokerConfig.setConsumerOffsetUpdateVersionStep(1);
            long before = manager.getDataVersion().getCounter().get();
            synchronized (persisterField("pendingLock")) {
                committing.submit(() -> manager.commitOffset("client", GROUP, TOPIC, 0, 1000))
                    .get(5, TimeUnit.SECONDS);
            }
            assertEquals(before + 1, manager.getDataVersion().getCounter().get());
            assertEquals(0, pendingCount());
            brokerConfig.setPersistConsumerOffsetIncrementally(false);
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            assertEquals(before + 2, manager.getDataVersion().getCounter().get());
            Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
            Mockito.verify(storage, Mockito.never()).persistKvDataVersion();
            Mockito.verify(storage, Mockito.never()).updateKvDataVersion();
        } finally {
            committing.shutdownNow();
        }
    }

    @Test(timeout = 15000)
    public void cleanedTopicCanBeRecreatedWithSameOffset() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            manager.persist();
            manager.cleanOffsetByTopic(TOPIC);
            assertEquals(-1, manager.queryOffset(GROUP, TOPIC, 0));
            assertNull(readStoredOffset(TOPIC));
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            assertEquals(1, pendingCount());
            manager.persist();
            assertEquals(Long.valueOf(1000), readStoredOffset(TOPIC));
        }
    }

    @Test(timeout = 15000)
    public void resetOffsetIsPersistedEvenIfNextCommitIsUnchanged() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
            manager.persist();
            manager.assignResetOffset(TOPIC, GROUP, 0, 10);
            assertEquals(1, pendingCount());
            manager.commitOffset("client", GROUP, TOPIC, 0, 10);
            manager.persist();
            assertEquals(Long.valueOf(10), readStoredOffset(TOPIC));
        }
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(10, manager.queryOffset(GROUP, TOPIC, 0));
    }

    @Test(timeout = 15000)
    public void resetOffsetsArePersistedWithoutLaterCommits() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            for (String topic : new String[] {TOPIC, "ordinary-reset"}) {
                manager.commitOffset("client", GROUP, topic, 0, 100);
                manager.persist();
                for (long offset : new long[] {10, 200, 0}) {
                    Mockito.clearInvocations(storage);
                    manager.assignResetOffset(topic, GROUP, 0, offset);
                    assertTrue(manager.hasOffsetReset(topic, GROUP, 0));
                    assertEquals(offset, manager.queryOffset(GROUP, topic, 0));
                    assertEquals(1, pendingCount());
                    Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
                    flushOneBatch();
                    assertEquals(Long.valueOf(offset), readStoredOffset(topic));
                    assertEquals(0, pendingCount());
                }
            }
        }
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(0, manager.queryOffset(GROUP, TOPIC, 0));
        assertEquals(0, manager.queryOffset(GROUP, "ordinary-reset", 0));
    }

    @Test(timeout = 15000)
    public void invalidResetAndMissingCloneSourceDoNotRegisterPendingWrites() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.assignResetOffset(null, GROUP, 0, 10);
            manager.assignResetOffset("", GROUP, 0, 10);
            manager.assignResetOffset(TOPIC, null, 0, 10);
            manager.assignResetOffset(TOPIC, "", 0, 10);
            manager.assignResetOffset(TOPIC, GROUP, -1, 10);
            manager.assignResetOffset(TOPIC, GROUP, 0, -1);
            manager.cloneOffset("missing-group", GROUP, TOPIC);
            assertEquals(0, pendingCount());
            assertEquals(-1, manager.queryOffset(GROUP, TOPIC, 0));
        }
    }

    @Test(timeout = 15000)
    public void clonedOffsetsArePersisted() throws Exception {
        String topic = "ordinary-clone";
        String destGroup = GROUP + "-clone";
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, topic, 0, 100);
            manager.commitOffset("client", GROUP, topic, 1, 200);
            manager.persist();
            manager.cloneOffset(GROUP, destGroup, topic);
            assertEquals(1, pendingCount());
            manager.persist();
        }
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(100, manager.queryOffset(destGroup, topic, 0));
        assertEquals(200, manager.queryOffset(destGroup, topic, 1));
    }

    @Test(timeout = 15000)
    public void nonIncrementalResetAndCloneDoNotRegisterPendingWrites() throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
        manager.assignResetOffset(TOPIC, GROUP, 0, 10);
        manager.cloneOffset(GROUP, GROUP + "-clone", TOPIC);
        assertEquals(0, pendingCount());
        manager.persist();
        assertEquals(Long.valueOf(10), readStoredOffset(TOPIC));
    }

    @Test(timeout = 15000)
    public void ordinaryIncrementalOffsetsAreCoalescedAsynchronously() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, "ordinary-topic", 0, 17);
            manager.commitOffset("client", GROUP, "ordinary-topic", 0, 18);
            assertEquals(18, manager.queryOffset(GROUP, "ordinary-topic", 0));
            assertEquals(1, pendingCount());
            assertNull(readStoredOffset("ordinary-topic"));
            Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
        }
        manager.persist();
        assertEquals(Long.valueOf(18), readStoredOffset("ordinary-topic"));
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void ordinaryTopicBatchKeepsAllQueueOffsets() throws Exception {
        String topic = "ordinary-multi-queue";
        try (WorkerBlock ignored = blockWorker()) {
            for (int queueId = 0; queueId < 16; queueId++) {
                manager.commitOffset("client", GROUP, topic, queueId, queueId + 100);
            }
            manager.commitOffset("client", GROUP, topic, 7, 5);
            assertEquals(1, pendingCount());
            assertNull(readStoredOffset(topic));
        }
        manager.persist();
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
        assertTrue(manager.stop());
        loadNewManager();
        for (int queueId = 0; queueId < 16; queueId++) {
            assertEquals(queueId == 7 ? 5 : queueId + 100, manager.queryOffset(GROUP, topic, queueId));
        }
    }

    @Test(timeout = 15000)
    public void batchKeepsOffsetsAcrossQueueCountsAndBackwardUpdates() throws Exception {
        int[] queueCounts = {1, 2, 3, 16, 64};
        try (WorkerBlock ignored = blockWorker()) {
            for (int queueCount : queueCounts) {
                String topic = "queue-count-" + queueCount;
                for (int queueId = 0; queueId < queueCount; queueId++) {
                    manager.commitOffset("client", GROUP, topic, queueId, queueId + 100);
                }
                manager.commitOffset("client", GROUP, topic, queueCount - 1, 5);
            }
            assertEquals(queueCounts.length, pendingCount());
        }
        manager.persist();
        for (int queueCount : queueCounts) {
            String topic = "queue-count-" + queueCount;
            byte[] key = (topic + "@" + GROUP).getBytes(StandardCharsets.UTF_8);
            byte[] value = storage.configRocksDBStorage.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY,
                StandardCharsets.UTF_8), key);
            RocksDBOffsetSerializeWrapper persisted = JSON.parseObject(value, RocksDBOffsetSerializeWrapper.class);
            assertEquals(queueCount, persisted.getOffsetTable().size());
            for (int queueId = 0; queueId < queueCount; queueId++) {
                assertEquals(Long.valueOf(queueId == queueCount - 1 ? 5 : queueId + 100),
                    persisted.getOffsetTable().get(queueId));
            }
        }
    }

    @Test(timeout = 15000)
    public void asynchronousBatchWrapperKeepsIndependentBrowserCompatibleValues() throws Exception {
        verifyBatchWrapperSerialization(true);
    }

    @Test(timeout = 15000)
    public void fullPersistWrapperKeepsIndependentBrowserCompatibleValues() throws Exception {
        verifyBatchWrapperSerialization(false);
    }

    @Test(timeout = 15000)
    public void missingOffsetsAreSkippedWithoutBlockingOtherPendingKeys() throws Exception {
        String missingTopic = TOPIC + "-missing";
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, missingTopic, 0, 10);
            manager.getOffsetTable().remove(missingTopic + "@" + GROUP);
            manager.commitOffset("client", GROUP, TOPIC, 0, 20);
            assertEquals(2, pendingCount());
        }
        manager.persist();
        assertNull(readStoredOffset(missingTopic));
        assertEquals(Long.valueOf(20), readStoredOffset(TOPIC));
        assertEquals(0, pendingCount());
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void ordinaryTopicDeletionCancelsPendingBatch() throws Exception {
        String topic = "ordinary-deleted";
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, topic, 0, 100);
            manager.cleanOffsetByTopic(topic);
            assertEquals(-1, manager.queryOffset(GROUP, topic, 0));
            assertEquals(0, pendingCount());
        }
        manager.persist();
        assertNull(readStoredOffset(topic));
        Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void nonIncrementalLmqOffsetsKeepPeriodicPersistence() throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        manager.commitOffset("client", GROUP, TOPIC, 0, 21);
        assertEquals(21, manager.queryOffset(GROUP, TOPIC, 0));
        assertNull(readStoredOffset(TOPIC));
        assertEquals(0, pendingCount());
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(-1, manager.queryOffset(GROUP, TOPIC, 0));
        manager.commitOffset("client", GROUP, TOPIC, 0, 22);
        manager.persist();
        assertEquals(Long.valueOf(22), readStoredOffset(TOPIC));
        assertEquals(0, pendingCount());
    }

    @Test(timeout = 15000)
    public void splitsLargePendingSetIntoBatches() throws Exception {
        List<Integer> batchSizes = new ArrayList<>();
        try (WorkerBlock ignored = blockWorker()) {
            Mockito.doAnswer(invocation -> {
                WriteBatch batch = invocation.getArgument(0);
                batchSizes.add(batch.count());
                return invocation.callRealMethod();
            }).when(storage).batchPutWithWal(any(WriteBatch.class));
            for (int i = 0; i < 5100; i++) {
                manager.commitOffset("client", GROUP, TOPIC + i, 0, i);
            }
            assertEquals(5100, pendingCount());
            Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
            manager.persist();
            assertEquals(0, pendingCount());
            assertEquals(3, batchSizes.size());
            assertEquals(Integer.valueOf(2500), batchSizes.get(0));
            assertEquals(Integer.valueOf(2500), batchSizes.get(1));
            assertEquals(Integer.valueOf(100), batchSizes.get(2));
        }
        for (int i = 0; i < 5100; i++) {
            assertEquals(Long.valueOf(i), readStoredOffset(TOPIC + i));
        }
        Mockito.verify(storage, Mockito.times(3)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void persistDrainsMultipleBatchesWithoutConsumingInterrupt() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            for (int i = 0; i < 5100; i++) {
                manager.commitOffset("client", GROUP, TOPIC + i, 0, i);
            }
            Thread.currentThread().interrupt();
            try {
                manager.persist();
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            assertEquals(0, pendingCount());
            Mockito.verify(storage, Mockito.times(3)).batchPutWithWal(any(WriteBatch.class));
            for (int i = 0; i < 5100; i++) {
                assertEquals(Long.valueOf(i), readStoredOffset(TOPIC + i));
            }
        }
    }

    @Test(timeout = 15000)
    public void concurrentCommitsKeepAllDirtyKeys() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            ExecutorService writers = Executors.newFixedThreadPool(8);
            try {
                List<Future<?>> committed = new ArrayList<>();
                for (int i = 0; i < 8; i++) {
                    String topic = TOPIC + i;
                    committed.add(writers.submit(() -> {
                        for (int offset = 0; offset < 1000; offset++) {
                            manager.commitOffset("client", GROUP, topic, 0, offset);
                        }
                    }));
                }
                for (Future<?> future : committed) {
                    future.get(5, TimeUnit.SECONDS);
                }
                assertEquals(8, pendingCount());
            } finally {
                writers.shutdownNow();
            }
        }
        manager.persist();
        for (int i = 0; i < 8; i++) {
            assertEquals(Long.valueOf(999), readStoredOffset(TOPIC + i));
        }
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void splitsBatchByEncodedBytes() throws Exception {
        String largeName = new String(new char[65536]).replace('\0', 'x');
        List<Integer> batchSizes = new ArrayList<>();
        try (WorkerBlock ignored = blockWorker()) {
            Mockito.doAnswer(invocation -> {
                WriteBatch batch = invocation.getArgument(0);
                batchSizes.add(batch.count());
                return invocation.callRealMethod();
            }).when(storage).batchPutWithWal(any(WriteBatch.class));
            for (int i = 0; i < 12; i++) {
                manager.commitOffset("client", GROUP, TOPIC + largeName + i, 0, i);
            }
            manager.persist();
            assertEquals(2, batchSizes.size());
            assertEquals(Integer.valueOf(8), batchSizes.get(0));
            assertEquals(Integer.valueOf(4), batchSizes.get(1));
        }
        for (int i = 0; i < 12; i++) {
            assertEquals(Long.valueOf(i), readStoredOffset(TOPIC + largeName + i));
        }
        Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void keyLimitLogIsRateLimitedAndUsesMilliseconds() throws Exception {
        try (WorkerBlock ignored = blockWorker(); BoundaryLogCapture capture = new BoundaryLogCapture()) {
            commitDistinctOffsets(5100);
            flushOneBatch();
            assertEquals(1, capture.events.size());
            ILoggingEvent event = capture.events.get(0);
            assertEquals(Level.INFO, event.getLevel());
            Object[] arguments = event.getArgumentArray();
            assertEquals(100L, arguments[0]);
            assertEquals(2500, arguments[1]);
            assertTrue(((Number) arguments[2]).longValue() > 0);
            assertTrue(((Number) arguments[3]).longValue() >= 0);
            assertEquals(2500, arguments[4]);
            assertEquals(512 * 1024, arguments[5]);
            assertEquals(6, arguments.length);
            assertFalse(event.getFormattedMessage().contains("pendingKeys="));
            flushOneBatch();
            assertEquals(1, capture.events.size());
            commitDistinctOffsets(2501);
            makeBoundaryLogDue();
            flushOneBatch();
            assertEquals(2, capture.events.size());
        }
    }

    @Test(timeout = 15000)
    public void byteLimitLogCapturesSizeBeforeWriteBatchIsCleared() throws Exception {
        try (WorkerBlock ignored = blockWorker(); BoundaryLogCapture capture = new BoundaryLogCapture()) {
            String largeName = new String(new char[65536]).replace('\0', 'x');
            for (int i = 0; i < 12; i++) {
                manager.commitOffset("client", GROUP, TOPIC + largeName + i, 0, i);
            }
            flushOneBatch();
            assertEquals(1, capture.events.size());
            Object[] arguments = capture.events.get(0).getArgumentArray();
            assertEquals(8, arguments[1]);
            assertTrue(((Number) arguments[2]).longValue() >= 512 * 1024);
            assertEquals(4, pendingCount());
        }
    }

    @Test(timeout = 15000)
    public void onlyFullBatchesLogLimitsWithoutCheckingRemainingWork() throws Exception {
        try (WorkerBlock ignored = blockWorker(); BoundaryLogCapture capture = new BoundaryLogCapture()) {
            commitDistinctOffsets(2500);
            flushOneBatch();
            assertEquals(0, pendingCount());
            assertEquals(1, capture.events.size());
            String largeName = new String(new char[65536]).replace('\0', 'x');
            for (int i = 0; i < 8; i++) {
                manager.commitOffset("client", GROUP, TOPIC + largeName + i, 0, i);
            }
            makeBoundaryLogDue();
            flushOneBatch();
            assertEquals(0, pendingCount());
            assertTrue((Long) persisterField("lastBatchBytes") >= 512 * 1024);
            assertEquals(2, capture.events.size());
            commitDistinctOffsets(1);
            makeBoundaryLogDue();
            flushOneBatch();
            assertEquals(2, capture.events.size());
        }
    }

    @Test(timeout = 15000)
    public void failedBatchDoesNotLogLimitsBeforeSuccessfulRetry() throws Exception {
        try (WorkerBlock ignored = blockWorker(); BoundaryLogCapture capture = new BoundaryLogCapture()) {
            commitDistinctOffsets(2501);
            Mockito.doThrow(new RocksDBException("injected boundary write failure"))
                .when(storage).batchPutWithWal(any(WriteBatch.class));
            InvocationTargetException failure = assertThrows(InvocationTargetException.class, this::flushOneBatch);
            assertTrue(failure.getCause() instanceof RocksDBException);
            assertEquals(2501, pendingCount());
            assertTrue(capture.events.isEmpty());
            Mockito.doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            flushOneBatch();
            assertEquals(1, capture.events.size());
        }
    }

    @Test(timeout = 15000)
    public void updateDuringWriteIsRequeuedIncludingBackwardOffset() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        AtomicInteger writes = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            if (writes.incrementAndGet() == 1) {
                writing.countDown();
                await(release);
            }
            return invocation.callRealMethod();
        }).when(storage).batchPutWithWal(any(WriteBatch.class));
        manager.commitOffset("client", GROUP, TOPIC, 0, 1000);
        await(writing);
        manager.commitOffset("client", GROUP, TOPIC, 0, 50);
        assertEquals(50, manager.queryOffset(GROUP, TOPIC, 0));
        release.countDown();
        manager.persist();
        assertEquals(Long.valueOf(50), readStoredOffset(TOPIC));
        assertEquals(2, writes.get());
    }

    @Test(timeout = 15000)
    public void batchSelectionBufferIsReusedAndClearedAfterSuccess() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            List<?> selected = (List<?>) persisterField("selectedKeys");
            assertTrue(selected.isEmpty());
            Mockito.doAnswer(invocation -> {
                assertSame(selected, persisterField("selectedKeys"));
                assertEquals(1, selected.size());
                assertEquals(TOPIC + "@" + GROUP, selected.get(0));
                return invocation.callRealMethod();
            }).when(storage).batchPutWithWal(any(WriteBatch.class));
            for (long offset : new long[] {7, 9}) {
                manager.commitOffset("client", GROUP, TOPIC, 0, offset);
                flushOneBatch();
                assertTrue(selected.isEmpty());
                assertEquals(0, pendingCount());
                assertEquals(Long.valueOf(offset), readStoredOffset(TOPIC));
            }
            Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
        }
    }

    @Test(timeout = 15000)
    public void failedBatchClearsSelectionBufferAfterRequeuingKeys() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            List<?> selected = (List<?>) persisterField("selectedKeys");
            manager.commitOffset("client", GROUP, TOPIC, 0, 7);
            Mockito.doThrow(new RocksDBException("injected buffer reuse write failure"))
                .doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            InvocationTargetException failure = assertThrows(InvocationTargetException.class, this::flushOneBatch);
            assertTrue(failure.getCause() instanceof RocksDBException);
            assertTrue(selected.isEmpty());
            assertEquals(1, pendingCount());
            assertNull(readStoredOffset(TOPIC));
            manager.commitOffset("client", GROUP, TOPIC, 0, 9);
            flushOneBatch();
            assertSame(selected, persisterField("selectedKeys"));
            assertTrue(selected.isEmpty());
            assertEquals(0, pendingCount());
            assertEquals(Long.valueOf(9), readStoredOffset(TOPIC));
            Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
        }
    }

    @Test(timeout = 15000)
    public void failedBatchRetriesLatestState() throws Exception {
        CountDownLatch retrying = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        AtomicInteger writes = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            int attempt = writes.incrementAndGet();
            if (attempt == 1) {
                throw new RocksDBException("injected write failure");
            }
            if (attempt == 2) {
                retrying.countDown();
                await(release);
            }
            return invocation.callRealMethod();
        }).when(storage).batchPutWithWal(any(WriteBatch.class));
        manager.commitOffset("client", GROUP, TOPIC, 0, 7);
        await(retrying);
        assertNull(readStoredOffset(TOPIC));
        manager.commitOffset("client", GROUP, TOPIC, 0, 9);
        release.countDown();
        manager.persist();
        assertEquals(Long.valueOf(9), readStoredOffset(TOPIC));
        assertTrue(writes.get() >= 3);
    }

    @Test(timeout = 15000)
    public void deleteWaitsForInFlightPutAndCancelsPendingUpdates() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        Mockito.doAnswer(invocation -> {
            writing.countDown();
            await(release);
            return invocation.callRealMethod();
        }).when(storage).batchPutWithWal(any(WriteBatch.class));
        manager.commitOffset("client", GROUP, TOPIC, 0, 100);
        await(writing);
        ExecutorService deleting = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch started = new CountDownLatch(1);
            Future<?> deleted = deleting.submit(() -> {
                started.countDown();
                manager.removeConsumerOffset(TOPIC + "@" + GROUP);
            });
            await(started);
            assertFalse(deleted.isDone());
            release.countDown();
            deleted.get(5, TimeUnit.SECONDS);
            manager.persist();
            assertNull(readStoredOffset(TOPIC));
            Mockito.doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            manager.commitOffset("client", GROUP, TOPIC, 0, 101);
            manager.persist();
            assertEquals(Long.valueOf(101), readStoredOffset(TOPIC));
        } finally {
            release.countDown();
            deleting.shutdownNow();
        }
    }

    @Test(timeout = 15000)
    public void cleanTopicCancelsQueuedPutBeforeMemoryRemoval() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 100);
            manager.cleanOffsetByTopic(TOPIC);
            assertEquals(-1, manager.queryOffset(GROUP, TOPIC, 0));
            assertEquals(0, pendingCount());
        }
        manager.persist();
        assertNull(readStoredOffset(TOPIC));
        Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void queuedDeleteAndRecreateWritesLatestOffset() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 100);
            manager.removeConsumerOffset(TOPIC + "@" + GROUP);
            manager.commitOffset("client", GROUP, TOPIC, 0, 101);
            assertEquals(1, pendingCount());
        }
        manager.persist();
        assertEquals(Long.valueOf(101), readStoredOffset(TOPIC));
        Mockito.verify(storage, Mockito.times(1)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void persistDoesNotWaitForUpdatesAddedAfterItsSnapshot() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        ExecutorService persisting = Executors.newSingleThreadExecutor();
        try (WorkerBlock ignored = blockWorker()) {
            Mockito.doAnswer(invocation -> {
                writing.countDown();
                await(release);
                return invocation.callRealMethod();
            }).when(storage).batchPutWithWal(any(WriteBatch.class));
            manager.commitOffset("client", GROUP, TOPIC, 0, 1);
            Future<?> persisted = persisting.submit(() -> {
                manager.persist();
            });
            await(writing);
            manager.commitOffset("client", GROUP, TOPIC + "-later", 0, 2);
            release.countDown();
            persisted.get(5, TimeUnit.SECONDS);
            assertEquals(Long.valueOf(1), readStoredOffset(TOPIC));
            assertNull(readStoredOffset(TOPIC + "-later"));
        } finally {
            release.countDown();
            persisting.shutdownNow();
        }
        manager.persist();
        assertEquals(Long.valueOf(2), readStoredOffset(TOPIC + "-later"));
    }

    @Test(timeout = 15000)
    public void stopDrainsPendingOffsetsAndNewManagerLoadsThem() throws Exception {
        WorkerBlock blocked = blockWorker();
        ExecutorService stopping = Executors.newSingleThreadExecutor();
        try {
            manager.commitOffset("client", GROUP, TOPIC, 0, 33);
            Future<Boolean> stopped = stopping.submit(manager::stop);
            assertFalse(stopped.isDone());
            blocked.close();
            assertTrue(stopped.get(5, TimeUnit.SECONDS));
            loadNewManager();
            assertEquals(33, manager.queryOffset(GROUP, TOPIC, 0));
            manager.commitOffset("client", GROUP, TOPIC, 0, 34);
            manager.persist();
            assertEquals(Long.valueOf(34), readStoredOffset(TOPIC));
        } finally {
            blocked.close();
            stopping.shutdownNow();
        }
    }

    @Test(timeout = 15000)
    public void commitAdvancesVersionWithoutWritingOnCommitThread() throws Exception {
        brokerConfig.setConsumerOffsetUpdateVersionStep(1);
        for (boolean incremental : new boolean[] {false, true}) {
            brokerConfig.setPersistConsumerOffsetIncrementally(incremental);
            long before = manager.getDataVersion().getCounter().get();
            manager.commitOffset("client", GROUP, TOPIC, 0, 44);
            manager.commitOffset("client", GROUP, TOPIC, 0, 45);
            assertEquals(before + 2, manager.getDataVersion().getCounter().get());
        }
        Mockito.verify(storage, Mockito.never()).updateKvDataVersion();
        Mockito.verify(storage, Mockito.never()).persistKvDataVersion();
    }

    @Test(timeout = 15000)
    public void commitDoesNotWaitForVersionPersistence() throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(2);
        CountDownLatch versionWriting = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        try (WorkerBlock ignored = blockWorker()) {
            brokerConfig.setConsumerOffsetUpdateVersionStep(1);
            Mockito.doAnswer(invocation -> {
                assertFalse(Thread.holdsLock(persisterField("pendingLock")));
                versionWriting.countDown();
                await(release);
                return invocation.callRealMethod();
            }).when(storage).persistKvDataVersion();
            Future<?> persisted = callers.submit(() -> {
                manager.persist();
            });
            await(versionWriting);
            long before = manager.getDataVersion().getCounter().get();
            callers.submit(() -> manager.commitOffset("client", GROUP, TOPIC, 0, 46))
                .get(1, TimeUnit.SECONDS);
            assertEquals(before + 1, manager.getDataVersion().getCounter().get());
            assertEquals(46, manager.queryOffset(GROUP, TOPIC, 0));
            release.countDown();
            persisted.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
        manager.persist();
        assertEquals(Long.valueOf(46), readStoredOffset(TOPIC));
    }

    @Test(timeout = 15000)
    public void persistAndStopWriteVersionAfterOffsets() throws Exception {
        brokerConfig.setConsumerOffsetUpdateVersionStep(1);
        for (boolean incremental : new boolean[] {false, true}) {
            brokerConfig.setPersistConsumerOffsetIncrementally(incremental);
            manager.commitOffset("client", GROUP, TOPIC, 0, 60);
            manager.persist();
            long persisted = manager.getDataVersion().getCounter().get();
            assertTrue(manager.stop());
            loadNewManager();
            assertEquals(persisted, manager.getDataVersion().getCounter().get());
            assertEquals(60, manager.queryOffset(GROUP, TOPIC, 0));
        }
        manager.commitOffset("client", GROUP, TOPIC, 0, 61);
        long committed = manager.getDataVersion().getCounter().get();
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(committed, manager.getDataVersion().getCounter().get());
        assertEquals(61, manager.queryOffset(GROUP, TOPIC, 0));
    }

    @Test(timeout = 15000)
    public void closeKeepsBatchSyncAndStorageCloseUnderWriteLock() throws Exception {
        try (WorkerBlock blocked = blockWorker()) {
            Object writeLock = persisterField("writeLock");
            List<String> events = new ArrayList<>();
            Mockito.doAnswer(invocation -> {
                assertTrue(Thread.holdsLock(writeLock));
                events.add("batch");
                return invocation.callRealMethod();
            }).when(storage).batchPutWithWal(any(WriteBatch.class));
            Mockito.doAnswer(invocation -> {
                assertTrue(Thread.holdsLock(writeLock));
                events.add("version");
                return invocation.callRealMethod();
            }).when(storage).persistKvDataVersion();
            Mockito.doAnswer(invocation -> {
                assertTrue(Thread.holdsLock(writeLock));
                events.add("sync");
                return invocation.callRealMethod();
            }).when(storage).flushWALChecked();
            Mockito.doAnswer(invocation -> {
                assertTrue(Thread.holdsLock(writeLock));
                events.add("close");
                return invocation.callRealMethod();
            }).when(storage).stop();
            manager.commitOffset("client", GROUP, TOPIC, 0, 56);
            blocked.close();
            assertTrue(manager.stop());
            assertTrue(events.contains("batch"));
            assertEquals("version", events.get(events.size() - 3));
            assertEquals("sync", events.get(events.size() - 2));
            assertEquals("close", events.get(events.size() - 1));
            Mockito.doCallRealMethod().when(storage).stop();
        }
    }

    @Test(timeout = 15000)
    public void failedCloseDrainKeepsDatabaseOpenForRetry() throws Exception {
        try (WorkerBlock blocked = blockWorker()) {
            manager.commitOffset("client", GROUP, TOPIC, 0, 57);
            Mockito.doThrow(new RocksDBException("injected close drain failure"))
                .when(storage).batchPutWithWal(any(WriteBatch.class));
            blocked.close();
            assertFalse(manager.stop());
            assertTrue(storage.isLoaded());
            Mockito.verify(storage, Mockito.never()).stop();
            assertEquals(1, pendingCount());
            Mockito.doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            assertTrue(manager.stop());
            loadNewManager();
            assertEquals(57, manager.queryOffset(GROUP, TOPIC, 0));
        }
    }

    @Test(timeout = 15000)
    public void syncFailureDoesNotCloseDatabase() throws Exception {
        manager.commitOffset("client", GROUP, TOPIC, 0, 55);
        Mockito.doThrow(new RocksDBException("injected sync failure")).when(storage).flushWALChecked();
        assertFalse(manager.stop());
        assertTrue(storage.isLoaded());
        Mockito.doCallRealMethod().when(storage).flushWALChecked();
        assertTrue(manager.stop());
        loadNewManager();
        assertEquals(55, manager.queryOffset(GROUP, TOPIC, 0));
    }

    @Test(timeout = 15000)
    public void persistFailureIsLoggedWithoutThrowing() throws Exception {
        manager.commitOffset("client", GROUP, TOPIC, 0, 58);
        Mockito.doThrow(new RocksDBException("injected persist failure")).when(storage).flushWALChecked();
        manager.persist();
        Mockito.doCallRealMethod().when(storage).flushWALChecked();
        manager.persist();
        assertEquals(Long.valueOf(58), readStoredOffset(TOPIC));
    }

    @Test(timeout = 15000)
    public void nonIncrementalCommitDoesNotTakePendingLock() throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(false);
        ExecutorService committing = Executors.newSingleThreadExecutor();
        try {
            synchronized (persisterField("pendingLock")) {
                committing.submit(() -> manager.commitOffset("client", GROUP, TOPIC, 0, 3))
                    .get(5, TimeUnit.SECONDS);
            }
        } finally {
            committing.shutdownNow();
        }
        assertEquals(3, manager.queryOffset(GROUP, TOPIC, 0));
    }

    @Test(timeout = 15000)
    public void incrementalCommitUpdatesMemoryBeforePendingLock() throws Exception {
        ExecutorService committing = Executors.newSingleThreadExecutor();
        try (WorkerBlock ignored = blockWorker()) {
            Future<?> committed;
            synchronized (persisterField("pendingLock")) {
                committed = committing.submit(() -> manager.commitOffset("client", GROUP, TOPIC, 0, 4));
                while (manager.queryOffset(GROUP, TOPIC, 0) != 4) {
                    Thread.sleep(1);
                }
                assertFalse(committed.isDone());
            }
            committed.get(5, TimeUnit.SECONDS);
            assertEquals(1, pendingCount());
        } finally {
            committing.shutdownNow();
        }
        manager.persist();
        assertEquals(Long.valueOf(4), readStoredOffset(TOPIC));
    }

    private void verifyBatchWrapperSerialization(boolean incremental) throws Exception {
        brokerConfig.setPersistConsumerOffsetIncrementally(incremental);
        String[] topics = {TOPIC + "-large", TOPIC + "-max", "ordinary-wrapper"};
        long[] offsets = {9007199254740993L, Long.MAX_VALUE, 71};
        try (WorkerBlock ignored = incremental ? blockWorker() : null) {
            for (int i = 0; i < topics.length; i++) {
                manager.commitOffset("client", GROUP, topics[i], 0, offsets[i]);
            }
            manager.commitOffset("client", GROUP, topics[2], 1, Long.MIN_VALUE);
            if (incremental) {
                assertEquals(topics.length, pendingCount());
            } else {
                assertEquals(0, pendingCount());
            }
        }
        manager.persist();
        String columnFamily = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);
        for (String topic : topics) {
            String key = topic + "@" + GROUP;
            byte[] bytes = storage.configRocksDBStorage.get(columnFamily, key.getBytes(StandardCharsets.UTF_8));
            RocksDBOffsetSerializeWrapper expected = new RocksDBOffsetSerializeWrapper();
            expected.setOffsetTable(manager.getOffsetTable().get(key));
            assertArrayEquals(JSON.toJSONBytes(expected, JSONWriter.Feature.BrowserCompatible), bytes);
        }
        assertEquals(Long.valueOf(offsets[0]), readStoredOffset(topics[0]));
        assertEquals(Long.valueOf(offsets[1]), readStoredOffset(topics[1]));
        assertTrue(new String(storage.configRocksDBStorage.get(columnFamily,
            (topics[0] + "@" + GROUP).getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8)
            .contains("\"9007199254740993\""));
        manager.commitOffset("client", GROUP, topics[0], 0, 17);
        manager.persist();
        assertEquals(Long.valueOf(17), readStoredOffset(topics[0]));
        assertEquals(Long.valueOf(Long.MAX_VALUE), readStoredOffset(topics[1]));
    }

    private void loadNewManager() {
        manager = new RocksDBConsumerOffsetManager(controller);
        assertTrue(manager.load());
        storage = Mockito.spy(manager.rocksDBConfigManager);
        manager.rocksDBConfigManager = storage;
    }

    private void assertWorkerDelay(long intervalMillis) throws Exception {
        try (WorkerBlock ignored = new WorkerBlock(workerExecutor(), newRelease())) {
            Object task = workerPool().getQueue().peek();
            assertNotNull(task);
            Field period = task.getClass().getDeclaredField("period");
            period.setAccessible(true);
            assertEquals(-TimeUnit.MILLISECONDS.toNanos(intervalMillis), period.getLong(task));
        }
    }

    private ScheduledThreadPoolExecutor workerPool() throws Exception {
        ScheduledExecutorService executor = workerExecutor();
        Field delegate = executor.getClass().getDeclaredField("e");
        delegate.setAccessible(true);
        return (ScheduledThreadPoolExecutor) delegate.get(executor);
    }

    private void assertCommitOnlyKeptInMemory(RocksDBConsumerOffsetManager target) throws Exception {
        target.commitOffset("client", GROUP, TOPIC, 0, 2);
        assertEquals(2, target.queryOffset(GROUP, TOPIC, 0));
        assertEquals(0, ((Set<?>) persisterField(target, "pendingKeys")).size());
    }

    private WorkerBlock blockWorker() throws Exception {
        manager.commitOffset("client", GROUP, "ordinary-seed", 0, 1);
        manager.persist();
        WorkerBlock blocked = new WorkerBlock(workerExecutor(), newRelease());
        Mockito.clearInvocations(storage);
        return blocked;
    }

    private ScheduledExecutorService workerExecutor() throws Exception {
        return (ScheduledExecutorService) persisterField("executor");
    }

    private int pendingCount() throws Exception {
        return ((Set<?>) persisterField("pendingKeys")).size();
    }

    private Object persisterField(String name) throws Exception {
        return persisterField(manager, name);
    }

    private Object persisterField(RocksDBConsumerOffsetManager target, String name) throws Exception {
        Object persister = persister(target);
        Field field = persister.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(persister);
    }

    private Object persister(RocksDBConsumerOffsetManager target) throws Exception {
        Field component = RocksDBConsumerOffsetManager.class.getDeclaredField("asyncPersister");
        component.setAccessible(true);
        return component.get(target);
    }

    private void commitDistinctOffsets(int count) {
        for (int i = 0; i < count; i++) {
            String topic = TOPIC + i;
            long offset = manager.queryOffset(GROUP, topic, 0) + 1;
            manager.commitOffset("client", GROUP, topic, 0, offset);
        }
    }

    private void flushOneBatch() throws Exception {
        Object component = persister(manager);
        Method method = component.getClass().getDeclaredMethod("flushBatch", long.class);
        method.setAccessible(true);
        method.invoke(component, 100L);
    }

    private void makeBoundaryLogDue() throws Exception {
        Object component = persister(manager);
        Field field = component.getClass().getDeclaredField("lastBoundaryLogTimeMs");
        field.setAccessible(true);
        synchronized (persisterField("writeLock")) {
            field.setLong(component, System.currentTimeMillis() - 60_001);
        }
    }

    private Long readStoredOffset(String topic) throws Exception {
        byte[] key = (topic + "@" + GROUP).getBytes(StandardCharsets.UTF_8);
        byte[] value = storage.configRocksDBStorage.get(new String(RocksDB.DEFAULT_COLUMN_FAMILY,
            StandardCharsets.UTF_8), key);
        return value == null ? null : JSON.parseObject(value, RocksDBOffsetSerializeWrapper.class).getOffsetTable().get(0);
    }

    private CountDownLatch newRelease() {
        CountDownLatch release = new CountDownLatch(1);
        releases.add(release);
        return release;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue("Timed out waiting for the test sync point", latch.await(5, TimeUnit.SECONDS));
    }

    private final class BoundaryLogCapture implements AutoCloseable {
        private final List<ILoggingEvent> events = new ArrayList<>();
        private final Logger logger = (Logger) RocksDBConsumerOffsetManager.log;
        private final Level previousLevel = logger.getLevel();
        private final Appender<ILoggingEvent> appender;

        @SuppressWarnings("unchecked")
        private BoundaryLogCapture() {
            appender = Mockito.mock(Appender.class);
            Mockito.doAnswer(invocation -> {
                ILoggingEvent event = invocation.getArgument(0);
                if (event.getMessage().startsWith("Async offset batch limits:")) {
                    assertFalse(Thread.holdsLock(persisterField("pendingLock")));
                    assertFalse(Thread.holdsLock(persisterField("writeLock")));
                    events.add(event);
                }
                return null;
            }).when(appender).doAppend(any(ILoggingEvent.class));
            logger.setLevel(Level.INFO);
            logger.addAppender(appender);
        }

        @Override
        public void close() {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }
    }

    private static final class WorkerBlock implements AutoCloseable {
        private final CountDownLatch release;
        private final Future<?> task;

        private WorkerBlock(ScheduledExecutorService executor, CountDownLatch release) throws Exception {
            this.release = release;
            CountDownLatch entered = new CountDownLatch(1);
            task = executor.submit(() -> {
                entered.countDown();
                await(release);
                return null;
            });
            await(entered);
        }

        @Override
        public void close() throws Exception {
            release.countDown();
            task.get(5, TimeUnit.SECONDS);
        }
    }

}
