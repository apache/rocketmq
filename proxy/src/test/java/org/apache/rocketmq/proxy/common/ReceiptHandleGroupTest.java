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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.proxy.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.junit.Before;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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