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
package org.apache.rocketmq.proxy.common;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ReceiptHandleGroupTest extends InitConfigTest {

    private static final String TOPIC = "topic";
    private static final String GROUP = "group";
    private ReceiptHandleGroup receiptHandleGroup;
    private String msgID;
    private final Random random = new Random();

    @Before
    public void before() throws Throwable {
        super.before();
        receiptHandleGroup = new ReceiptHandleGroup();
        msgID = MessageClientIDSetter.createUniqID();
    }

    protected String createHandle() {
        return ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(3000)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName("brokerName")
            .queueId(random.nextInt(10))
            .offset(random.nextInt(10))
            .commitLogOffset(0L)
            .build().encode();
    }

    @Test
    public void testAddDuplicationHandle() {
        String handle1 = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis())
            .invisibleTime(3000)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName("brokerName")
            .queueId(1)
            .offset(123)
            .commitLogOffset(0L)
            .build().encode();
        String handle2 = ReceiptHandle.builder()
            .startOffset(0L)
            .retrieveTime(System.currentTimeMillis() + 1000)
            .invisibleTime(3000)
            .reviveQueueId(1)
            .topicType(ReceiptHandle.NORMAL_TOPIC)
            .brokerName("brokerName")
            .queueId(1)
            .offset(123)
            .commitLogOffset(0L)
            .build().encode();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle2, msgID));

        assertEquals(1, receiptHandleGroup.receiptHandleMap.get(msgID).size());
    }

    @Test
    public void testGetWhenComputeIfPresent() {
        String handle1 = createHandle();
        String handle2 = createHandle();
        AtomicReference<MessageReceiptHandle> getHandleRef = new AtomicReference<>();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        CountDownLatch latch = new CountDownLatch(2);
        Thread getThread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                getHandleRef.set(receiptHandleGroup.get(msgID, handle1));
            } catch (Exception ignored) {
            }
        }, "getThread");
        Thread computeThread = new Thread(() -> {
            try {
                receiptHandleGroup.computeIfPresent(msgID, handle1, messageReceiptHandle -> {
                    try {
                        latch.countDown();
                        latch.await();
                    } catch (Exception ignored) {
                    }
                    messageReceiptHandle.updateReceiptHandle(handle2);
                    return FutureUtils.addExecutor(CompletableFuture.completedFuture(messageReceiptHandle), Executors.newCachedThreadPool());
                });
            } catch (Exception ignored) {
            }
        }, "computeThread");
        getThread.start();
        computeThread.start();

        await().atMost(Duration.ofSeconds(1)).until(() -> getHandleRef.get() != null);
        assertEquals(handle2, getHandleRef.get().getReceiptHandleStr());
        assertFalse(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testPendingComputationKeepsPermitPastTimeout() throws Exception {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        long previousTimeout = config.getLockTimeoutMsInHandleGroup();
        long timeout = 25;
        AtomicLong nanoTime = new AtomicLong();
        receiptHandleGroup = new ReceiptHandleGroup(nanoTime::get);
        String handle = createHandle();
        MessageReceiptHandle original = createMessageReceiptHandle(handle, msgID);
        MessageReceiptHandle replacement = createMessageReceiptHandle(handle, msgID);
        receiptHandleGroup.put(msgID, original);
        ReceiptHandleGroup.HandleData data = receiptHandleGroup.receiptHandleMap.get(msgID)
            .get(new ReceiptHandleGroup.HandleKey(handle));
        Semaphore semaphore = (Semaphore) FieldUtils.readField(data, "semaphore", true);

        CompletableFuture<MessageReceiptHandle> renewal = new CompletableFuture<>();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            config.setLockTimeoutMsInHandleGroup(timeout);
            receiptHandleGroup.computeIfPresent(msgID, handle, value -> renewal);
            // A computation can outlive the wait timeout without reaching its expiry threshold.
            nanoTime.set(TimeUnit.MILLISECONDS.toNanos(2 * timeout));

            CountDownLatch ready = new CountDownLatch(3);
            Future<?> get = submitContender(executor, ready, () -> receiptHandleGroup.get(msgID, handle));
            Future<?> put = submitContender(executor, ready, () -> receiptHandleGroup.put(msgID, replacement));
            Future<?> remove = submitContender(executor, ready, () -> receiptHandleGroup.remove(msgID, handle));
            for (Future<?> operation : new Future<?>[] {get, put, remove}) {
                operation.get(5, TimeUnit.SECONDS);
            }
            assertFalse(renewal.isDone());
            assertEquals(0, semaphore.availablePermits());
            assertSame(original, data.getMessageReceiptHandle());

            renewal.complete(replacement);
            assertEquals(1, semaphore.availablePermits());
            assertSame(replacement, receiptHandleGroup.get(msgID, handle));
            assertSame(replacement, receiptHandleGroup.remove(msgID, handle));
            assertNull(receiptHandleGroup.remove(msgID, handle));
            assertEquals(1, semaphore.availablePermits());
        } finally {
            renewal.completeExceptionally(new IllegalStateException("test cleanup"));
            executor.shutdownNow();
            config.setLockTimeoutMsInHandleGroup(previousTimeout);
        }
    }

    @Test
    public void testExpiredComputationCannotReleaseSuccessorPermit() throws Exception {
        ProxyConfig config = ConfigurationManager.getProxyConfig();
        long previousTimeout = config.getLockTimeoutMsInHandleGroup();
        long timeout = 25;
        AtomicLong nanoTime = new AtomicLong();
        receiptHandleGroup = new ReceiptHandleGroup(nanoTime::get);
        String handle = createHandle();
        MessageReceiptHandle original = createMessageReceiptHandle(handle, msgID);
        MessageReceiptHandle replacement = createMessageReceiptHandle(handle, msgID);
        receiptHandleGroup.put(msgID, original);
        ReceiptHandleGroup.HandleData data = receiptHandleGroup.receiptHandleMap.get(msgID)
            .get(new ReceiptHandleGroup.HandleKey(handle));
        Semaphore semaphore = (Semaphore) FieldUtils.readField(data, "semaphore", true);
        CompletableFuture<MessageReceiptHandle> stalled = new CompletableFuture<>();
        CompletableFuture<MessageReceiptHandle> successor = new CompletableFuture<>();
        AtomicReference<MessageReceiptHandle> staleSnapshot = new AtomicReference<>();

        try {
            config.setLockTimeoutMsInHandleGroup(timeout);
            receiptHandleGroup.computeIfPresent(msgID, handle, value -> {
                staleSnapshot.set(value);
                return stalled;
            }, 0);
            nanoTime.set(TimeUnit.MILLISECONDS.toNanos(4 * timeout));
            receiptHandleGroup.computeIfPresent(msgID, handle, value -> successor, 0);
            assertEquals(0, semaphore.availablePermits());

            staleSnapshot.get().updateReceiptHandle(createHandle());
            stalled.complete(null);
            assertEquals(handle, data.getMessageReceiptHandle().getReceiptHandleStr());
            assertEquals(0, semaphore.availablePermits());
            assertThrows(ProxyException.class, () -> receiptHandleGroup.computeIfPresent(msgID, handle,
                CompletableFuture::completedFuture, 0));

            successor.complete(replacement);
            assertEquals(1, semaphore.availablePermits());
            assertSame(replacement, receiptHandleGroup.remove(msgID, handle));
            assertEquals(1, semaphore.availablePermits());
        } finally {
            stalled.cancel(false);
            successor.cancel(false);
            config.setLockTimeoutMsInHandleGroup(previousTimeout);
        }
    }

    private Future<?> submitContender(ExecutorService executor, CountDownLatch ready, Runnable operation) {
        return executor.submit(() -> {
            ready.countDown();
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            ProxyException exception = assertThrows(ProxyException.class, operation::run);
            assertEquals(ProxyExceptionCode.INTERNAL_SERVER_ERROR, exception.getCode());
            return null;
        });
    }

    @Test
    public void testComputeFailureReleasesPermit() {
        String handle = createHandle();
        MessageReceiptHandle original = createMessageReceiptHandle(handle, msgID);
        receiptHandleGroup.put(msgID, original);

        assertThrows(IllegalStateException.class, () -> receiptHandleGroup.computeIfPresent(msgID, handle, value -> {
            throw new IllegalStateException("failed before returning a future");
        }, 0));

        CompletableFuture<MessageReceiptHandle> renewal = new CompletableFuture<>();
        receiptHandleGroup.computeIfPresent(msgID, handle, value -> renewal, 0);
        renewal.completeExceptionally(new IllegalStateException("failed after returning a future"));
        receiptHandleGroup.computeIfPresent(msgID, handle, value -> {
            assertEquals(original, value);
            return CompletableFuture.completedFuture(value);
        }, 0);
    }

    @Test
    public void testGetWhenComputeIfPresentReturnNull() {
        String handle1 = createHandle();
        AtomicBoolean getCalled = new AtomicBoolean(false);
        AtomicReference<MessageReceiptHandle> getHandleRef = new AtomicReference<>();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        CountDownLatch latch = new CountDownLatch(2);
        Thread getThread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                getHandleRef.set(receiptHandleGroup.get(msgID, handle1));
                getCalled.set(true);
            } catch (Exception ignored) {
            }
        }, "getThread");
        Thread computeThread = new Thread(() -> {
            try {
                receiptHandleGroup.computeIfPresent(msgID, handle1, messageReceiptHandle -> {
                    try {
                        latch.countDown();
                        latch.await();
                    } catch (Exception ignored) {
                    }
                    return FutureUtils.addExecutor(CompletableFuture.completedFuture(null), Executors.newCachedThreadPool());
                });
            } catch (Exception ignored) {
            }
        }, "computeThread");
        getThread.start();
        computeThread.start();

        await().atMost(Duration.ofSeconds(1)).until(getCalled::get);
        assertNull(getHandleRef.get());
        assertTrue(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testRemoveWhenComputeIfPresent() {
        String handle1 = createHandle();
        String handle2 = createHandle();
        AtomicReference<MessageReceiptHandle> removeHandleRef = new AtomicReference<>();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        CountDownLatch latch = new CountDownLatch(2);
        Thread removeThread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                removeHandleRef.set(receiptHandleGroup.remove(msgID, handle1));
            } catch (Exception ignored) {
            }
        }, "removeThread");
        Thread computeThread = new Thread(() -> {
            try {
                receiptHandleGroup.computeIfPresent(msgID, handle1, messageReceiptHandle -> {
                    try {
                        latch.countDown();
                        latch.await();
                    } catch (Exception ignored) {
                    }
                    messageReceiptHandle.updateReceiptHandle(handle2);
                    return FutureUtils.addExecutor(CompletableFuture.completedFuture(messageReceiptHandle), Executors.newCachedThreadPool());
                });
            } catch (Exception ignored) {
            }
        }, "computeThread");
        removeThread.start();
        computeThread.start();

        await().atMost(Duration.ofSeconds(1)).until(() -> removeHandleRef.get() != null);
        assertEquals(handle2, removeHandleRef.get().getReceiptHandleStr());
        assertTrue(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testRemoveWhenComputeIfPresentReturnNull() {
        String handle1 = createHandle();
        AtomicBoolean removeCalled = new AtomicBoolean(false);
        AtomicReference<MessageReceiptHandle> removeHandleRef = new AtomicReference<>();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        CountDownLatch latch = new CountDownLatch(2);
        Thread removeThread = new Thread(() -> {
            try {
                latch.countDown();
                latch.await();
                removeHandleRef.set(receiptHandleGroup.remove(msgID, handle1));
                removeCalled.set(true);
            } catch (Exception ignored) {
            }
        }, "removeThread");
        Thread computeThread = new Thread(() -> {
            try {
                receiptHandleGroup.computeIfPresent(msgID, handle1, messageReceiptHandle -> {
                    try {
                        latch.countDown();
                        latch.await();
                    } catch (Exception ignored) {
                    }
                    return FutureUtils.addExecutor(CompletableFuture.completedFuture(null), Executors.newCachedThreadPool());
                });
            } catch (Exception ignored) {
            }
        }, "computeThread");
        removeThread.start();
        computeThread.start();

        await().atMost(Duration.ofSeconds(1)).until(removeCalled::get);
        assertNull(removeHandleRef.get());
        assertTrue(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testRemoveMultiThread() {
        String handle1 = createHandle();
        AtomicReference<MessageReceiptHandle> removeHandleRef = new AtomicReference<>();
        AtomicInteger count = new AtomicInteger();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        int threadNum = Math.max(Runtime.getRuntime().availableProcessors(), 3);
        CountDownLatch latch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Thread thread = new Thread(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    MessageReceiptHandle handle = receiptHandleGroup.remove(msgID, handle1);
                    if (handle != null) {
                        removeHandleRef.set(handle);
                        count.incrementAndGet();
                    }
                } catch (Exception ignored) {
                }
            });
            thread.start();
        }

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(1, count.get()));
        assertEquals(handle1, removeHandleRef.get().getReceiptHandleStr());
        assertTrue(receiptHandleGroup.isEmpty());
    }

    @Test
    public void testRemoveOne() {
        String handle1 = createHandle();
        AtomicReference<MessageReceiptHandle> removeHandleRef = new AtomicReference<>();
        AtomicInteger count = new AtomicInteger();

        receiptHandleGroup.put(msgID, createMessageReceiptHandle(handle1, msgID));
        int threadNum = Math.max(Runtime.getRuntime().availableProcessors(), 3);
        CountDownLatch latch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Thread thread = new Thread(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    MessageReceiptHandle handle = receiptHandleGroup.removeOne(msgID);
                    if (handle != null) {
                        removeHandleRef.set(handle);
                        count.incrementAndGet();
                    }
                } catch (Exception ignored) {
                }
            });
            thread.start();
        }

        await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertEquals(1, count.get()));
        assertEquals(handle1, removeHandleRef.get().getReceiptHandleStr());
        assertTrue(receiptHandleGroup.isEmpty());
    }

    private MessageReceiptHandle createMessageReceiptHandle(String handle, String msgID) {
        return new MessageReceiptHandle(GROUP, TOPIC, 0, handle, msgID, 0, 0);
    }
}
