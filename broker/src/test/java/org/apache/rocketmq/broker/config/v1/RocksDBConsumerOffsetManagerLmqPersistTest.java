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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
            Mockito.doCallRealMethod().when(storage).batchPutWithWal(any(WriteBatch.class));
            Mockito.doCallRealMethod().when(storage).flushWALChecked();
            Mockito.doCallRealMethod().when(storage).updateKvDataVersion();
            assertTrue(manager.stop());
        }
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
        try (WorkerBlock ignored = blockWorker()) {
            for (int i = 0; i < 1100; i++) {
                manager.commitOffset("client", GROUP, TOPIC + i, 0, i);
            }
            assertEquals(1100, pendingCount());
            Mockito.verify(storage, Mockito.never()).batchPutWithWal(any(WriteBatch.class));
        }
        manager.persist();
        for (int i = 0; i < 1100; i++) {
            assertEquals(Long.valueOf(i), readStoredOffset(TOPIC + i));
        }
        Mockito.verify(storage, Mockito.times(3)).batchPutWithWal(any(WriteBatch.class));
    }

    @Test(timeout = 15000)
    public void persistDrainsMultipleBatchesWithoutConsumingInterrupt() throws Exception {
        try (WorkerBlock ignored = blockWorker()) {
            for (int i = 0; i < 1100; i++) {
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
            for (int i = 0; i < 1100; i++) {
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
        try (WorkerBlock ignored = blockWorker()) {
            for (int i = 0; i < 6; i++) {
                manager.commitOffset("client", GROUP, TOPIC + largeName + i, 0, i);
            }
        }
        manager.persist();
        for (int i = 0; i < 6; i++) {
            assertEquals(Long.valueOf(i), readStoredOffset(TOPIC + largeName + i));
        }
        Mockito.verify(storage, Mockito.times(2)).batchPutWithWal(any(WriteBatch.class));
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
    public void stopWaitsForInFlightVersionWrite() throws Exception {
        brokerConfig.setConsumerOffsetUpdateVersionStep(1);
        CountDownLatch versionWriting = new CountDownLatch(1);
        CountDownLatch release = newRelease();
        Mockito.doAnswer(invocation -> {
            versionWriting.countDown();
            await(release);
            return invocation.callRealMethod();
        }).when(storage).updateKvDataVersion();
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<?> committed = callers.submit(() -> manager.commitOffset("client", GROUP, TOPIC, 0, 44));
            await(versionWriting);
            Future<Boolean> stopped = callers.submit(manager::stop);
            assertFalse(stopped.isDone());
            release.countDown();
            committed.get(5, TimeUnit.SECONDS);
            assertTrue(stopped.get(5, TimeUnit.SECONDS));
            loadNewManager();
            assertEquals(44, manager.queryOffset(GROUP, TOPIC, 0));
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
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
        Field component = RocksDBConsumerOffsetManager.class.getDeclaredField("asyncPersister");
        component.setAccessible(true);
        Object persister = component.get(target);
        Field field = persister.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(persister);
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
