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
package org.apache.rocketmq.broker.pop;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDBException;

public class PopConsumerCacheStateTest {
    private final String path = PopConsumerRocksdbStoreTest.getRandomStorePath();
    private final AtomicInteger writes = new AtomicInteger();
    private volatile CountDownLatch entered;
    private volatile CountDownLatch release;
    private volatile boolean fail;
    private volatile boolean failAfterWrite;
    private PopConsumerRocksdbStore store;
    private PopConsumerCache cache;

    @Before
    public void setup() {
        BrokerController broker = Mockito.mock(BrokerController.class);
        BrokerConfig config = new BrokerConfig();
        config.setPopCkStayBufferTime(0);
        Mockito.when(broker.getBrokerConfig()).thenReturn(config);
        store = new PopConsumerRocksdbStore(path, 8 * 1024 * 1024, 4 * 1024 * 1024) {
            @Override
            protected void writeBatch(List<PopConsumerWriteBatchService.Mutation> mutations) throws RocksDBException {
                Assert.assertEquals("PopKVGroupCommit", Thread.currentThread().getName());
                writes.incrementAndGet();
                if (entered != null) {
                    entered.countDown();
                    try {
                        Assert.assertTrue(release.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }
                if (fail) {
                    throw new RocksDBException("injected write failure");
                }
                super.writeBatch(mutations);
                if (failAfterWrite) {
                    throw new RocksDBException("injected uncertain write outcome");
                }
            }
        };
        cache = new PopConsumerCache(broker, store, Mockito.mock(PopConsumerLockService.class), record -> { });
        Assert.assertTrue(store.start());
    }

    @After
    public void shutdown() {
        if (release != null) {
            release.countDown();
        }
        store.shutdown();
        PopConsumerRocksdbStoreTest.deleteStoreDirectory(path);
    }

    private PopConsumerRecord record(long invisibleTime) {
        return new PopConsumerRecord(System.currentTimeMillis() - 1000, "g", "t", 0, 0, invisibleTime, 1, null);
    }

    private void await(CompletableFuture<?> future) throws Exception {
        future.get(10, TimeUnit.SECONDS);
    }

    private void assertFailed(CompletableFuture<?> future) throws Exception {
        try {
            await(future);
            Assert.fail("Expected persistence failure");
        } catch (ExecutionException expected) {
            Assert.assertTrue(expected.getCause() instanceof RocksDBException);
        }
    }

    private CompletableFuture<Integer> transfer() {
        return cache.cleanupRecordsAsync(record -> Assert.fail("Checkpoint must not be expired"));
    }

    private CompletableFuture<Void> change(PopConsumerRecord old, PopConsumerRecord next) {
        return cache.changeAsync(old, Collections.singletonList(next), Collections.singletonList(old));
    }

    private void blockWriter() {
        release = new CountDownLatch(1);
        entered = new CountDownLatch(1);
    }

    private void assertOnly(PopConsumerRecord expected) {
        List<PopConsumerRecord> records = store.scanExpiredRecords(0, Long.MAX_VALUE, 10);
        Assert.assertEquals(1, records.size());
        Assert.assertArrayEquals(expected.getKeyBytes(), records.get(0).getKeyBytes());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void transferThenChangeUsesPersistedCheckpoint() throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord next = record(120000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        blockWriter();
        CompletableFuture<Integer> transfer = transfer();
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> change = change(old, next);
        Assert.assertFalse(change.isDone());
        Assert.assertEquals(1, cache.getCacheSize());
        release.countDown();
        await(CompletableFuture.allOf(transfer, change));
        await(transfer());
        assertOnly(next);
    }

    @Test(timeout = 15000)
    public void changeThenTransferCannotRestoreOldCheckpoint() throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord next = record(120000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        blockWriter();
        CompletableFuture<Void> change = change(old, next);
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Integer> transfer = transfer();
        Assert.assertFalse(transfer.isDone());
        release.countDown();
        await(CompletableFuture.allOf(change, transfer));
        assertOnly(next);
    }

    @Test(timeout = 15000)
    public void transferThenAckRemovesDiskAndCache() throws Exception {
        PopConsumerRecord old = record(60000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        blockWriter();
        CompletableFuture<Integer> transfer = transfer();
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> ack = cache.ackAsync(old);
        release.countDown();
        await(CompletableFuture.allOf(transfer, ack));
        await(transfer());
        Assert.assertTrue(store.scanExpiredRecords(0, Long.MAX_VALUE, 10).isEmpty());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void cachedAckDoesNotWriteRocksdb() throws Exception {
        PopConsumerRecord old = record(60000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        await(cache.ackAsync(old));
        await(transfer());
        Assert.assertEquals(0, writes.get());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void failedTransferThenChangeCannotRestoreOldCheckpoint() throws Exception {
        checkFailedTransferThenChange(false);
    }

    @Test(timeout = 15000)
    public void uncertainTransferThenChangeCannotRestoreOldCheckpoint() throws Exception {
        checkFailedTransferThenChange(true);
    }

    private void checkFailedTransferThenChange(boolean uncertain) throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord next = record(120000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        fail = !uncertain;
        failAfterWrite = uncertain;
        assertFailed(transfer());
        Assert.assertEquals(1, cache.getCacheSize());
        fail = false;
        failAfterWrite = false;
        await(change(old, next));
        await(transfer());
        assertOnly(next);
    }

    @Test(timeout = 15000)
    public void uncertainTransferThenAckStillDeletesDisk() throws Exception {
        PopConsumerRecord old = record(60000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        failAfterWrite = true;
        assertFailed(transfer());
        Assert.assertEquals(1, store.scanExpiredRecords(0, Long.MAX_VALUE, 10).size());
        failAfterWrite = false;
        await(cache.ackAsync(old));
        await(transfer());
        Assert.assertTrue(store.scanExpiredRecords(0, Long.MAX_VALUE, 10).isEmpty());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void failedChangeRetainsOldCachedCheckpoint() throws Exception {
        PopConsumerRecord old = record(60000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        fail = true;
        assertFailed(change(old, record(120000)));
        Assert.assertEquals(1, cache.getCacheSize());
        fail = false;
        await(transfer());
        assertOnly(old);
    }

    @Test(timeout = 15000)
    public void staleAckCannotRemoveNewCachedCheckpoint() throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord next = record(120000);
        await(cache.writeRecordsAsync(Collections.singletonList(next)));
        await(cache.ackAsync(old));
        Assert.assertEquals(1, cache.getCacheSize());
        await(transfer());
        assertOnly(next);
    }

    @Test(timeout = 15000)
    public void consecutiveChangesShareOneWriteBatch() throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord middle = record(120000);
        PopConsumerRecord next = record(180000);
        await(cache.writeRecordsAsync(Collections.singletonList(old)));
        blockWriter();
        CompletableFuture<Integer> transfer = transfer();
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> first = change(old, middle);
        CompletableFuture<Void> second = change(middle, next);
        release.countDown();
        await(CompletableFuture.allOf(transfer, first, second));
        Assert.assertEquals(2, writes.get());
        assertOnly(next);
    }

    @Test(timeout = 15000)
    public void consecutiveDiskAcksShareOneWriteBatch() throws Exception {
        PopConsumerRecord first = record(60000);
        PopConsumerRecord second = record(60000);
        second.setOffset(2);
        await(cache.writeRecordsAsync(Arrays.asList(first, second)));
        blockWriter();
        CompletableFuture<Integer> transfer = transfer();
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> ackFirst = cache.ackAsync(first);
        CompletableFuture<Void> ackSecond = cache.ackAsync(second);
        release.countDown();
        await(CompletableFuture.allOf(transfer, ackFirst, ackSecond));
        Assert.assertEquals(2, writes.get());
        Assert.assertTrue(store.scanExpiredRecords(0, Long.MAX_VALUE, 10).isEmpty());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void queuedPopChangeAndAckObservePreviousCacheOperations() throws Exception {
        PopConsumerRecord old = record(60000);
        PopConsumerRecord next = record(120000);
        blockWriter();
        CompletableFuture<Void> initial = store.deleteRecordsAsync(Collections.singletonList(old));
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<Void> pop = cache.writeRecordsAsync(Collections.singletonList(old));
        CompletableFuture<Void> change = change(old, next);
        CompletableFuture<Void> ack = cache.ackAsync(next);
        CompletableFuture<Integer> transfer = transfer();
        release.countDown();
        await(CompletableFuture.allOf(initial, pop, change, ack, transfer));
        Assert.assertEquals(3, writes.get());
        Assert.assertTrue(store.scanExpiredRecords(0, Long.MAX_VALUE, 10).isEmpty());
        Assert.assertEquals(0, cache.getCacheSize());
    }

    @Test(timeout = 15000)
    public void callbackPoolCanSynchronouslyClearCache() throws Exception {
        CountDownLatch callbacksEntered = new CountDownLatch(4);
        blockWriter();
        CompletableFuture<Void> initial = store.writeRecordsAsync(Collections.singletonList(record(60000)));
        Assert.assertTrue(entered.await(10, TimeUnit.SECONDS));
        CompletableFuture<?>[] callbacks = new CompletableFuture<?>[4];
        try {
            for (int i = 0; i < callbacks.length; i++) {
                PopConsumerRecord record = record(60000);
                record.setOffset(i + 10);
                callbacks[i] = cache.writeRecordsAsync(Collections.singletonList(record)).thenRun(() -> {
                    callbacksEntered.countDown();
                    try {
                        Assert.assertTrue(callbacksEntered.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    cache.clearRecords("g", "t", 0);
                });
            }
            release.countDown();
            await(initial);
            await(CompletableFuture.allOf(callbacks));
            Assert.assertEquals(0, cache.getCacheSize());
        } finally {
            while (callbacksEntered.getCount() > 0) {
                callbacksEntered.countDown();
            }
        }
    }

}
