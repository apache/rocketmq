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

import java.nio.ByteBuffer;
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
    public void unboundedQueueAcceptsBeyondFormerLimitAndShutdownDrains() throws Exception {
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
            for (int i = 1; i < 5000; i++) {
                futures.add(service.submit(mutation(i, false)));
            }
            futures.add(service.submit(mutation(1, true)));
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
    public void limitsBatchBytesWithoutLimitingPendingBytesOrSplittingAtomicRequests() throws Exception {
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
            // Exceed the former 64 MiB pending-byte limit without a huge allocation.
            CompletableFuture<Void> oversized = service.submit(Collections.nCopies(64, large));
            release.countDown();
            CompletableFuture.allOf(first, atomic, next, oversized).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(Arrays.asList(1, 2, 1, 64), batchSizes);
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
    public void blockedCallbackDoesNotBlockWriterOrOtherCompletions() throws Exception {
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
            next.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(1, releaseCallback.getCount());
            releaseCallback.countDown();
        } finally {
            releaseWrite.countDown();
            releaseCallback.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void callbacksInSameBatchRunConcurrently() throws Exception {
        CountDownLatch firstWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        CountDownLatch callbacksEntered = new CountDownLatch(2);
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger secondBatchSize = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            if (writes.incrementAndGet() == 1) {
                firstWrite.countDown();
                await(releaseWrite);
            } else if (writes.get() == 2) {
                secondBatchSize.set(mutations.size());
            }
        }, 2);
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(0, false));
            await(firstWrite);
            List<CompletableFuture<Void>> callbacks = new ArrayList<>();
            for (int i = 1; i <= 2; i++) {
                callbacks.add(service.submit(mutation(i, false)).thenRun(() -> {
                    callbacksEntered.countDown();
                    try {
                        await(callbacksEntered);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    // Both completion workers can wait for private synchronous writes.
                    service.submit(mutation(3, true), false).join();
                }));
            }
            releaseWrite.countDown();
            first.get(10, TimeUnit.SECONDS);
            CompletableFuture.allOf(callbacks.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
            // Two requests queued while the first write was blocked must share a batch.
            Assert.assertEquals(2, secondBatchSize.get());
            Assert.assertTrue(writes.get() >= 3);
        } finally {
            releaseWrite.countDown();
            while (callbacksEntered.getCount() > 0) {
                callbacksEntered.countDown();
            }
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void callbackCanChainAnotherAsyncWrite() throws Exception {
        CountDownLatch firstWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            if (writes.incrementAndGet() == 1) {
                firstWrite.countDown();
                await(releaseWrite);
            }
        }, 2);
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            await(firstWrite);
            CompletableFuture<Void> chained = first.thenCompose(ignored -> service.submit(mutation(2, true)));
            releaseWrite.countDown();
            chained.get(10, TimeUnit.SECONDS);
            Assert.assertEquals(2, writes.get());
        } finally {
            releaseWrite.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void busyCompletionPoolDoesNotRejectWritesAndShutdownDrainsCallbacks() throws Exception {
        CountDownLatch firstWrite = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        CountDownLatch callbacksEntered = new CountDownLatch(2);
        CountDownLatch releaseCallbacks = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            if (writes.incrementAndGet() == 1) {
                firstWrite.countDown();
                await(releaseWrite);
            }
        }, 2);
        service.start();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 2; i++) {
                futures.add(service.submit(mutation(i, false)).thenRun(() -> {
                    callbacksEntered.countDown();
                    try {
                        await(releaseCallbacks);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }));
            }
            await(firstWrite);
            releaseWrite.countDown();
            await(callbacksEntered);
            for (int i = 0; i < 5000; i++) {
                futures.add(service.submit(mutation(i, false)));
            }
            futures.add(service.submit(mutation(1, true)));
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(service::shutdown);
            Assert.assertFalse(shutdown.isDone());
            releaseCallbacks.countDown();
            shutdown.get(10, TimeUnit.SECONDS);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        } finally {
            releaseWrite.countDown();
            releaseCallbacks.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void shutdownDoesNotInterruptWriteAndRejectsNewRequests() throws Exception {
        CountDownLatch writeEntered = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        CountDownLatch stopping = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            writes.incrementAndGet();
            writeEntered.countDown();
            await(releaseWrite);
        }) {
            @Override
            public void wakeup() {
                super.wakeup();
                if (isStopped()) {
                    stopping.countDown();
                }
            }
        };
        service.start();
        try {
            CompletableFuture<Void> first = service.submit(mutation(1, false));
            await(writeEntered);
            CompletableFuture<Void> second = service.submit(mutation(2, true));
            CompletableFuture<Void> shutdown = CompletableFuture.runAsync(() -> service.shutdown(true));
            await(stopping);
            Assert.assertFalse(shutdown.isDone());
            Assert.assertFalse(first.isDone());
            assertFailed(service.submit(mutation(3, false)));
            releaseWrite.countDown();
            shutdown.get(10, TimeUnit.SECONDS);
            CompletableFuture.allOf(first, second).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(2, writes.get());
        } finally {
            releaseWrite.countDown();
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void idleWriterUsesServiceThreadWakeup() throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> { }) {
            @Override
            protected void waitForRunning(long interval) {
                waiting.countDown();
                // Submission and shutdown must wake the worker without waiting for this timeout.
                super.waitForRunning(TimeUnit.MINUTES.toMillis(1));
            }
        };
        service.start();
        try {
            await(waiting);
            service.start();
            service.submit(mutation(1, false)).get(10, TimeUnit.SECONDS);
        } finally {
            service.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void preparesDependentStateAfterCommitAndBatchesIndependentState() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> batchSizes = new ArrayList<>();
        AtomicInteger firstState = new AtomicInteger();
        AtomicInteger secondState = new AtomicInteger();
        PopConsumerWriteBatchService service = new PopConsumerWriteBatchService(mutations -> {
            batchSizes.add(mutations.size());
            if (batchSizes.size() == 1) {
                entered.countDown();
                await(release);
            }
        });
        service.start();
        try {
            CompletableFuture<Void> initial = service.submit(mutation(0, false));
            await(entered);
            CompletableFuture<Void> first = stateUpdate(service, 1, firstState, 0);
            CompletableFuture<Void> second = stateUpdate(service, 2, secondState, 0);
            CompletableFuture<Void> dependent = stateUpdate(service, 1, firstState, 1);
            CompletableFuture<Void> barrier = service.submit(() -> {
                Assert.assertEquals(2, firstState.get());
                Assert.assertEquals(1, secondState.get());
                return new PopConsumerWriteBatchService.PreparedWrite(Collections.emptyList(), () -> { });
            }, Collections.emptySet(), true, 0, true);
            release.countDown();
            CompletableFuture.allOf(initial, first, second, dependent, barrier).get(10, TimeUnit.SECONDS);
            Assert.assertEquals(Arrays.asList(1, 2, 1), batchSizes);
        } finally {
            release.countDown();
            service.shutdown();
        }
    }

    private CompletableFuture<Void> stateUpdate(PopConsumerWriteBatchService service, int key,
        AtomicInteger state, int expected) {
        return service.submit(() -> {
            Assert.assertEquals("PopKVGroupCommit", Thread.currentThread().getName());
            Assert.assertEquals(expected, state.get());
            return new PopConsumerWriteBatchService.PreparedWrite(mutation(key, false), () -> {
                Assert.assertEquals("PopKVGroupCommit", Thread.currentThread().getName());
                state.incrementAndGet();
            });
        }, Collections.singleton(ByteBuffer.wrap(new byte[] {(byte) key})), false, 2, true);
    }

}
