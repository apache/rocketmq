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

package org.apache.rocketmq.proxy.grpc.v2.channel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class GrpcChannelManagerTest {
    private static final String CLIENT_ID = "client-id";
    private static final String OTHER_CLIENT_ID = "other-client-id";

    private GrpcChannelManager grpcChannelManager;
    private ProxyContext proxyContext;

    @Before
    public void setUp() {
        grpcChannelManager = new GrpcChannelManager(
            mock(ProxyRelayService.class), mock(GrpcClientSettingsManager.class));
        proxyContext = ProxyContext.create()
            .setRemoteAddress("10.0.0.1:8080")
            .setLocalAddress("10.0.0.2:8081");
    }

    @After
    public void tearDown() throws Exception {
        grpcChannelManager.shutdown();
    }

    @Test
    public void testOpenSessionReplacesCurrentWithNewGeneration() {
        GrpcClientChannel first = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        GrpcClientChannel second = grpcChannelManager.openSession(proxyContext, CLIENT_ID);

        assertNotSame(first, second);
        assertTrue(second.getGeneration() > first.getGeneration());
        assertSame(second, grpcChannelManager.getChannel(CLIENT_ID));
        assertEquals(first.getConnectedAtMillis(), first.getLastActiveAtMillis());
        assertEquals(second.getConnectedAtMillis(), second.getLastActiveAtMillis());
    }

    @Test
    public void testCreateChannelReusesCurrentSession() {
        GrpcClientChannel opened = grpcChannelManager.openSession(proxyContext, CLIENT_ID);

        assertSame(opened, grpcChannelManager.createChannel(proxyContext, CLIENT_ID));
    }

    @Test
    public void testRemoveChannelRequiresCurrentIdentity() {
        GrpcClientChannel oldChannel = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        GrpcClientChannel currentChannel = grpcChannelManager.openSession(proxyContext, CLIENT_ID);

        assertFalse(grpcChannelManager.removeChannel(CLIENT_ID, oldChannel));
        assertSame(currentChannel, grpcChannelManager.getChannel(CLIENT_ID));
        assertTrue(grpcChannelManager.removeChannel(CLIENT_ID, currentChannel));
        assertNull(grpcChannelManager.getChannel(CLIENT_ID));
        assertFalse(grpcChannelManager.removeChannel(CLIENT_ID, currentChannel));
    }

    @Test
    public void testTouchChannelRequiresCurrentIdentityAndIsMonotonic() {
        GrpcClientChannel oldChannel = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        GrpcClientChannel currentChannel = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        long oldActivity = oldChannel.getLastActiveAtMillis();
        long initialActivity = currentChannel.getLastActiveAtMillis();
        long laterActivity = initialActivity + 100;

        assertFalse(grpcChannelManager.touchChannel(CLIENT_ID, oldChannel, laterActivity));
        assertEquals(oldActivity, oldChannel.getLastActiveAtMillis());
        assertTrue(grpcChannelManager.touchChannel(CLIENT_ID, currentChannel, laterActivity));
        assertTrue(grpcChannelManager.touchChannel(CLIENT_ID, currentChannel, initialActivity));
        assertEquals(laterActivity, currentChannel.getLastActiveAtMillis());
    }

    @Test
    public void testConcurrentOpenLeavesGreatestGenerationCurrent() throws Exception {
        int taskCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(taskCount);
        CountDownLatch ready = new CountDownLatch(taskCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<GrpcClientChannel>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < taskCount; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    return grpcChannelManager.openSession(proxyContext, CLIENT_ID);
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            Set<Long> generations = new HashSet<>();
            long greatestGeneration = 0;
            for (Future<GrpcClientChannel> future : futures) {
                long generation = future.get(10, TimeUnit.SECONDS).getGeneration();
                generations.add(generation);
                greatestGeneration = Math.max(greatestGeneration, generation);
            }

            assertEquals(taskCount, generations.size());
            assertEquals(greatestGeneration, grpcChannelManager.getChannel(CLIENT_ID).getGeneration());
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testSameClientOpenWaitsForCurrentSessionAction() throws Exception {
        GrpcClientChannel current = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        CountDownLatch openStarted = new CountDownLatch(1);
        try {
            Future<Boolean> actionFuture = executor.submit(() ->
                grpcChannelManager.runIfCurrent(CLIENT_ID, current, () -> {
                    actionStarted.countDown();
                    await(releaseAction);
                }));
            assertTrue(actionStarted.await(10, TimeUnit.SECONDS));

            Future<GrpcClientChannel> openFuture = executor.submit(() -> {
                openStarted.countDown();
                return grpcChannelManager.openSession(proxyContext, CLIENT_ID);
            });
            assertTrue(openStarted.await(10, TimeUnit.SECONDS));
            awaitCondition(() -> grpcChannelManager.hasQueuedClientLifecycleOperation(CLIENT_ID));
            assertFalse(openFuture.isDone());
            assertSame(current, grpcChannelManager.getChannel(CLIENT_ID));

            releaseAction.countDown();
            assertTrue(actionFuture.get(10, TimeUnit.SECONDS));
            GrpcClientChannel replacement = openFuture.get(10, TimeUnit.SECONDS);
            assertSame(replacement, grpcChannelManager.getChannel(CLIENT_ID));
            assertTrue(replacement.getGeneration() > current.getGeneration());
            assertEquals(0, grpcChannelManager.clientLifecycleLockCount());
        } finally {
            releaseAction.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDifferentClientOpenDoesNotWaitForCurrentSessionAction() throws Exception {
        GrpcClientChannel current = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch actionStarted = new CountDownLatch(1);
        CountDownLatch releaseAction = new CountDownLatch(1);
        try {
            Future<Boolean> actionFuture = executor.submit(() ->
                grpcChannelManager.runIfCurrent(CLIENT_ID, current, () -> {
                    actionStarted.countDown();
                    await(releaseAction);
                }));
            assertTrue(actionStarted.await(10, TimeUnit.SECONDS));

            Future<GrpcClientChannel> openFuture = executor.submit(() ->
                grpcChannelManager.openSession(proxyContext, OTHER_CLIENT_ID));
            GrpcClientChannel other = openFuture.get(10, TimeUnit.SECONDS);
            assertSame(other, grpcChannelManager.getChannel(OTHER_CLIENT_ID));

            releaseAction.countDown();
            assertTrue(actionFuture.get(10, TimeUnit.SECONDS));
        } finally {
            releaseAction.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCurrentSessionActionExceptionDoesNotRetainLifecycleLock() {
        GrpcClientChannel current = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        RuntimeException expected = new RuntimeException("action failure");

        try {
            grpcChannelManager.runIfCurrent(CLIENT_ID, current, () -> {
                throw expected;
            });
            fail("runIfCurrent did not propagate the action exception");
        } catch (RuntimeException actual) {
            assertSame(expected, actual);
        }

        GrpcClientChannel replacement = grpcChannelManager.openSession(proxyContext, CLIENT_ID);
        assertSame(replacement, grpcChannelManager.getChannel(CLIENT_ID));
        assertTrue(replacement.getGeneration() > current.getGeneration());
    }

    @Test
    public void testLifecycleLocksAreReclaimedAfterManyClients() {
        for (int i = 0; i < 1_000; i++) {
            String clientId = "client-" + i;
            GrpcClientChannel channel = grpcChannelManager.openSession(proxyContext, clientId);
            assertTrue(grpcChannelManager.removeChannel(clientId, channel));
        }

        assertEquals(0, grpcChannelManager.clientLifecycleLockCount());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("interrupted while waiting on test latch");
        }
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }
        fail("condition was not satisfied before timeout");
    }
}
