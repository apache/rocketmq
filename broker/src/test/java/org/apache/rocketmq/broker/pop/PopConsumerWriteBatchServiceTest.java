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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class PopConsumerWriteBatchServiceTest {
    private List<PopConsumerWriteBatchService.Mutation> mutation(int key, boolean delete) {
        return Collections.singletonList(new PopConsumerWriteBatchService.Mutation(
            new byte[] {(byte) key}, delete ? null : new byte[] {1}));
    }

    private void await(CountDownLatch latch) throws InterruptedException {
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private void assertFailed(CompletableFuture<?> future) throws Exception {
        try {
            future.get(10, TimeUnit.SECONDS);
            Assert.fail("Expected persistence failure");
        } catch (ExecutionException expected) {
            Assert.assertNotNull(expected.getCause());
        }
    }

    @Test(timeout = 15000)
    public void batchesInOrderAndCompletesOnlyAfterWrite() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<List<PopConsumerWriteBatchService.Mutation>> batches = new ArrayList<>();
        AtomicReference<String> writerThread = new AtomicReference<>();
        AtomicReference<String> completionThread = new AtomicReference<>();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            writerThread.set(Thread.currentThread().getName());
            batches.add(new ArrayList<>(mutations));
            if (batches.size() == 1) {
                entered.countDown();
                await(release);
            }
        });
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            CompletableFuture<Void> callback = first.thenRun(() -> completionThread.set(Thread.currentThread().getName()));
            await(entered);
            CompletableFuture<Void> put = service.submit(mutation(2, false));
            CompletableFuture<Void> delete = service.submit(mutation(2, true));
            Assert.assertFalse(first.isDone());
            Assert.assertFalse(put.isDone());
            Assert.assertFalse(delete.isDone());
            release.countDown();
            CompletableFuture.allOf(callback, put, delete).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(2, batches.size());
            Assert.assertEquals(2, batches.get(1).size());
            Assert.assertNotNull(batches.get(1).get(0).value);
            Assert.assertNull(batches.get(1).get(1).value);
            Assert.assertNotEquals(writerThread.get(), completionThread.get());
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void failedBatchFailsEveryRequestAndWriterContinues() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                entered.countDown();
                await(release);
            } else if (call == 2) {
                throw new IllegalStateException("injected write failure");
            }
        });
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            await(entered);
            CompletableFuture<Void> second = service.submit(mutation(2, false));
            CompletableFuture<Void> third = service.submit(mutation(3, true));
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            assertFailed(second);
            assertFailed(third);
            service.submit(mutation(4, false)).get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void fullQueueRejectsWithoutBlockingAndShutdownDrains() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            entered.countDown();
            await(release);
        });
        service.start();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            futures.add(service.submit(mutation(0, false)));
            await(entered);
            for (int i = 1; i < 4096; i++) {
                futures.add(service.submit(mutation(i, false)));
            }
            assertFailed(service.submit(mutation(1, true)));
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(service::shutdown);
            Assert.assertFalse(shutdown.isDone());
            release.countDown();
            shutdown.get(10, TimeUnit.SECONDS);
            for (CompletableFuture<Void> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
            assertFailed(service.submit(mutation(1, false)));
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void boundsBytesAndDoesNotSplitAtomicRequests() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> batchSizes = new ArrayList<>();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            batchSizes.add(mutations.size());
            if (batchSizes.size() == 1) {
                entered.countDown();
                await(release);
            }
        });
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(0, false));
            await(entered);
            PopConsumerWriteBatchService.Mutation large = new PopConsumerWriteBatchService.Mutation(
                new byte[] {1}, new byte[1024 * 1024]);
            CompletableFuture<Void> atomic = service.submit(Arrays.asList(large, large));
            CompletableFuture<Void> next = service.submit(mutation(2, true));
            // 64 references to a 1 MiB mutation exceed the pending-byte budget without a huge allocation.
            assertFailed(service.submit(Collections.nCopies(64, large)));
            release.countDown();
            CompletableFuture.allOf(first, atomic, next).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(Arrays.asList(1, 2, 1), batchSizes);
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void completionCanUseBlockingStorageApi() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            if (calls.incrementAndGet() == 1) {
                entered.countDown();
                await(release);
            }
        });
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            await(entered);
            CompletableFuture<Void> callback = first.thenRun(() -> service.submit(mutation(2, false), false).join());
            release.countDown();
            callback.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(2, calls.get());
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void blockedCallbackDoesNotBlockWriter() throws Exception {
        CountDownLatch firstWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch secondWrite = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            if (writes.incrementAndGet() == 1) {
                firstWrite.countDown();
                await(releaseWrite);
            } else {
                secondWrite.countDown();
            }
        });
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            await(firstWrite);
            first.thenRun(() -> {
                callbackEntered.countDown();
                try {
                    await(releaseCallback);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            releaseWrite.countDown();
            await(callbackEntered);
            CompletableFuture<Void> next = service.submit(mutation(2, false));
            await(secondWrite);
            Assert.assertFalse(next.isDone());
            releaseCallback.countDown();
            next.get(10, TimeUnit.SECONDS);
        } finally {
            releaseWrite.countDown();
            releaseCallback.countDown();
            service.shutdown();
        }
    }
}
