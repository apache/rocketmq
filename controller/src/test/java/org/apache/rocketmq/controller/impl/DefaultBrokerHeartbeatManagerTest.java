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
package org.apache.rocketmq.controller.impl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.controller.BrokerHeartbeatManager;
import org.apache.rocketmq.controller.BrokerHousekeepingService;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import org.apache.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import org.apache.rocketmq.controller.impl.heartbeat.DefaultBrokerHeartbeatManager;
import org.apache.rocketmq.controller.processor.ControllerRequestProcessor;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.BrokerHeartbeatRequestHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultBrokerHeartbeatManagerTest {
    private BrokerHeartbeatManager heartbeatManager;

    @Before
    public void init() {
        final ControllerConfig config = new ControllerConfig();
        config.setScanNotActiveBrokerInterval(2000);
        this.heartbeatManager = new DefaultBrokerHeartbeatManager(config);
        this.heartbeatManager.initialize();
        this.heartbeatManager.start();
    }

    @After
    public void tearDown() {
        if (this.heartbeatManager != null) {
            this.heartbeatManager.shutdown();
        }
    }

    @Test
    public void testDetectBrokerAlive() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.heartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            latch.countDown();
        });
        this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 1L, 3000L, null,
            1, 1L, -1L, 0);
        assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
        this.heartbeatManager.shutdown();
    }

    @Test
    public void testStaleChannelCloseDoesNotEvictReRegisteredBroker() throws Exception {
        final EmbeddedChannel oldChannel = new EmbeddedChannel();
        final EmbeddedChannel newChannel = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch staleInactiveLatch = new CountDownLatch(1);
        final CountDownLatch activeCloseLatch = new CountDownLatch(1);
        this.heartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            int count = inactiveCount.incrementAndGet();
            if (count == 1) {
                staleInactiveLatch.countDown();
                activeCloseLatch.countDown();
            }
        });
        try {
            this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, oldChannel,
                1, 1L, -1L, 0);
            this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, newChannel,
                1, 2L, 1L, 0);
            this.heartbeatManager.onBrokerChannelClose(oldChannel);

            final BrokerLiveInfo liveInfo = this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
            assertNotNull("A stale channel close must not evict a broker alive on a new channel", liveInfo);
            assertSame("The live entry must be rebound to the current channel",
                newChannel, liveInfo.getChannel());
            assertTrue("Current channel must remain active", newChannel.isActive());
            assertFalse("Stale channel close must not trigger an inactive notification",
                staleInactiveLatch.await(150, TimeUnit.MILLISECONDS));
            assertEquals(0, inactiveCount.get());

            this.heartbeatManager.onBrokerChannelClose(newChannel);
            assertNull(this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L));
            assertTrue("Closing the current channel must notify broker inactive once",
                activeCloseLatch.await(3, TimeUnit.SECONDS));
            assertEquals(1, inactiveCount.get());
        } finally {
            oldChannel.finishAndReleaseAll();
            newChannel.finishAndReleaseAll();
            this.heartbeatManager.shutdown();
        }
    }

    @Test
    public void testConcurrentHeartbeatDuringStaleChannelCloseKeepsBrokerAlive() throws Exception {
        final EmbeddedChannel oldChannel = new EmbeddedChannel();
        final EmbeddedChannel newChannel = new EmbeddedChannel();
        try {
            for (int round = 0; round < 500; round++) {
                this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L,
                    oldChannel, 1, 1L, -1L, 0);

                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch finished = new CountDownLatch(2);
                final int currentRound = round;
                Thread closeThread = new Thread(() -> {
                    awaitStart(start, currentRound);
                    heartbeatManager.onBrokerChannelClose(oldChannel);
                    finished.countDown();
                });
                Thread heartbeatThread = new Thread(() -> {
                    awaitStart(start, currentRound);
                    heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L,
                        newChannel, 1, 1L, -1L, 0);
                    finished.countDown();
                });
                closeThread.start();
                heartbeatThread.start();
                start.countDown();
                assertTrue("round " + round + " did not finish", finished.await(5, TimeUnit.SECONDS));

                final BrokerLiveInfo liveInfo = this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
                assertNotNull("round " + round + ": the live entry must survive the stale close", liveInfo);
                assertEquals("round " + round + ": the live entry must carry the new channel",
                    newChannel, liveInfo.getChannel());
            }
        } finally {
            oldChannel.finishAndReleaseAll();
            newChannel.finishAndReleaseAll();
            this.heartbeatManager.shutdown();
        }
    }

    @Test
    public void testExpiryScanInterleavedWithRebindingHeartbeatKeepsNewChannelOpenAndBrokerAlive() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final EmbeddedChannel oldChannel = new EmbeddedChannel();
        final EmbeddedChannel newChannel = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        localManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        final CountDownLatch scanReadOldTimestamp = new CountDownLatch(1);
        final CountDownLatch allowScanResume = new CountDownLatch(1);
        final AtomicBoolean pauseOnce = new AtomicBoolean(false);
        final BrokerIdentityInfo identity = new BrokerIdentityInfo("cluster1", "broker1", 0L);
        final BrokerLiveInfo seededInfo = new BrokerLiveInfo("broker1", "127.0.0.1:7000", 0L,
            System.currentTimeMillis() - 10000L, 5000L, oldChannel, 1, 1L, 0) {
            @Override
            public long getLastUpdateTimestamp() {
                long captured = super.getLastUpdateTimestamp();
                if (pauseOnce.compareAndSet(false, true)) {
                    scanReadOldTimestamp.countDown();
                    try {
                        assertTrue("Timed out waiting to resume expiry scan",
                            allowScanResume.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while pausing expiry scan", e);
                    }
                }
                return captured;
            }
        };
        try {
            localManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 5000L, oldChannel,
                1, 1L, -1L, 0);
            putBrokerLiveInfo(localManager, identity, seededInfo);
            final AtomicReference<Throwable> scanError = new AtomicReference<>();
            Thread scanThread = new Thread(() -> {
                try {
                    localManager.scanNotActiveBroker();
                } catch (Throwable t) {
                    scanError.set(t);
                }
            });
            scanThread.start();
            assertTrue("Scanner did not capture old timestamp in time",
                scanReadOldTimestamp.await(5, TimeUnit.SECONDS));

            localManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 5000L, newChannel,
                1, 2L, 1L, 0);

            allowScanResume.countDown();
            scanThread.join(5000);
            assertFalse("Scanner thread did not finish", scanThread.isAlive());
            if (scanError.get() != null) {
                throw new AssertionError("Scanner thread failed", scanError.get());
            }

            assertTrue("Healthy rebound channel must remain active after interleaved scan", newChannel.isActive());
            final BrokerLiveInfo liveInfo = localManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
            assertNotNull("Live entry must not be evicted after fresh heartbeat", liveInfo);
            assertSame("Live entry must remain bound to the new channel", newChannel, liveInfo.getChannel());
            assertFalse("Interleaved fresh heartbeat must suppress expiry notification",
                inactiveLatch.await(150, TimeUnit.MILLISECONDS));
            assertEquals(0, inactiveCount.get());
        } finally {
            allowScanResume.countDown();
            oldChannel.finishAndReleaseAll();
            newChannel.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testExpiryScanRemovesTrulyExpiredBrokerClosesChannelAndNotifiesOnce() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final EmbeddedChannel expiredChannel = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        localManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        try {
            localManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 20L, expiredChannel,
                1, 1L, -1L, 0);
            Thread.sleep(60L);
            localManager.scanNotActiveBroker();

            assertNull("Expired broker entry must be removed",
                localManager.getBrokerLiveInfo("cluster1", "broker1", 0L));
            assertFalse("Expired channel must be closed", expiredChannel.isActive());
            assertTrue("Expired broker must fire inactive notification",
                inactiveLatch.await(3, TimeUnit.SECONDS));
            assertEquals(1, inactiveCount.get());
        } finally {
            expiredChannel.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testExpiryScanOnReboundSharedChannelDoesNotCloseHealthySiblingBrokerChannel() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final EmbeddedChannel oldChannelA = new EmbeddedChannel();
        final EmbeddedChannel sharedChannel = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final AtomicReference<String> lastInactiveBroker = new AtomicReference<>();
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        localManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            lastInactiveBroker.set(brokerName);
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        try {
            localManager.onBrokerHeartbeat("cluster1", "brokerA", "127.0.0.1:7000", 0L, 40L, oldChannelA,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerA", "127.0.0.1:7000", 0L, 40L, sharedChannel,
                1, 2L, 1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, sharedChannel,
                1, 1L, -1L, 0);

            Thread.sleep(80L);
            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, sharedChannel,
                1, 2L, 1L, 0);
            localManager.scanNotActiveBroker();

            assertNull("Expired brokerA must be removed",
                localManager.getBrokerLiveInfo("cluster1", "brokerA", 0L));
            assertNotNull("Healthy brokerB must remain registered",
                localManager.getBrokerLiveInfo("cluster1", "brokerB", 0L));
            assertTrue("Healthy brokerB must remain active",
                localManager.isBrokerActive("cluster1", "brokerB", 0L));
            assertTrue("Shared channel used by healthy brokerB must remain active", sharedChannel.isActive());
            assertTrue("Expired brokerA must fire inactive notification",
                inactiveLatch.await(3, TimeUnit.SECONDS));
            assertEquals(1, inactiveCount.get());
            assertEquals("brokerA", lastInactiveBroker.get());
        } finally {
            oldChannelA.finishAndReleaseAll();
            sharedChannel.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testExpiryScanConcurrentSiblingRebindToSharedChannelKeepsChannelOpen() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final EmbeddedChannel sharedChannel = new EmbeddedChannel();
        final EmbeddedChannel oldChannelB = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final AtomicReference<String> lastInactiveBroker = new AtomicReference<>();
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        localManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            lastInactiveBroker.set(brokerName);
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        final CountDownLatch scannerReadExpiredA = new CountDownLatch(1);
        final CountDownLatch allowScannerProceed = new CountDownLatch(1);
        final AtomicBoolean pauseScannerOnce = new AtomicBoolean(false);
        final BrokerIdentityInfo identityA = new BrokerIdentityInfo("cluster1", "brokerA", 0L);
        final BrokerLiveInfo seededA = new BrokerLiveInfo("brokerA", "127.0.0.1:7000", 0L,
            System.currentTimeMillis() - 10000L, 5000L, sharedChannel, 1, 1L, 0) {
            @Override
            public long getLastUpdateTimestamp() {
                long captured = super.getLastUpdateTimestamp();
                if ("rmq-scan-shared-rebind-1".equals(Thread.currentThread().getName())
                    && pauseScannerOnce.compareAndSet(false, true)) {
                    scannerReadExpiredA.countDown();
                    try {
                        assertTrue("Timed out waiting for sibling brokerB rebind during scan",
                            allowScannerProceed.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while pausing expiry scan", e);
                    }
                }
                return captured;
            }
        };

        final AtomicReference<Runnable> completeCloseRef = new AtomicReference<>();
        final EmbeddedChannel sharedChannel2 = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                if (completeCloseRef.compareAndSet(null, () -> ctx.close(promise))) {
                    return;
                }
                ctx.close(promise);
            }
        });
        final EmbeddedChannel oldChannelD = new EmbeddedChannel();

        Thread scanThread1 = null;
        try {
            localManager.onBrokerHeartbeat("cluster1", "brokerA", "127.0.0.1:7000", 0L, 5000L, sharedChannel,
                1, 1L, -1L, 0);
            putBrokerLiveInfo(localManager, identityA, seededA);
            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, oldChannelB,
                1, 1L, -1L, 0);

            final AtomicReference<Throwable> scanErr1 = new AtomicReference<>();
            scanThread1 = new Thread(() -> {
                try {
                    localManager.scanNotActiveBroker();
                } catch (Throwable t) {
                    scanErr1.set(t);
                }
            }, "rmq-scan-shared-rebind-1");
            scanThread1.start();

            assertTrue("Scanner did not capture expired brokerA timestamp in time",
                scannerReadExpiredA.await(5, TimeUnit.SECONDS));
            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, sharedChannel,
                1, 2L, 1L, 0);
            allowScannerProceed.countDown();
            scanThread1.join(5000);
            assertFalse("Scanner thread 1 did not finish", scanThread1.isAlive());
            if (scanErr1.get() != null) {
                throw new AssertionError("Scanner thread 1 failed", scanErr1.get());
            }

            assertTrue("Shared channel rebound by sibling brokerB during scan must remain active",
                sharedChannel.isActive());
            assertNull("Expired brokerA must be removed",
                localManager.getBrokerLiveInfo("cluster1", "brokerA", 0L));
            final BrokerLiveInfo liveB = localManager.getBrokerLiveInfo("cluster1", "brokerB", 0L);
            assertNotNull("Healthy brokerB must remain registered", liveB);
            assertSame("Healthy brokerB must stay bound to sharedChannel", sharedChannel, liveB.getChannel());
            assertTrue("Healthy brokerB must remain active",
                localManager.isBrokerActive("cluster1", "brokerB", 0L));
            assertTrue("Expired brokerA must fire inactive notification",
                inactiveLatch.await(3, TimeUnit.SECONDS));
            assertEquals(1, inactiveCount.get());
            assertEquals("brokerA", lastInactiveBroker.get());
            assertReverseIndexMatchesLiveTable(localManager);

            // Reverse order: scanner removes the last owner and atomically installs a closing token
            // while async close is still in flight; sibling brokerD's rebind attempt must return
            // without blocking, reject sharedChannel2, and preserve oldChannelD.
            localManager.onBrokerHeartbeat("cluster1", "brokerC", "127.0.0.1:7002", 0L, 40L, sharedChannel2,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerD", "127.0.0.1:7003", 0L, 10000L, oldChannelD,
                1, 1L, -1L, 0);
            Thread.sleep(80L);

            localManager.scanNotActiveBroker();
            assertNull(localManager.getBrokerLiveInfo("cluster1", "brokerC", 0L));
            assertNotNull("Outbound handler must hold async close of sharedChannel2", completeCloseRef.get());
            assertTrue(sharedChannel2.isActive());

            localManager.onBrokerHeartbeat("cluster1", "brokerD", "127.0.0.1:7003", 0L, 10000L, sharedChannel2,
                1, 2L, 1L, 0);
            Runnable finishClose2 = completeCloseRef.getAndSet(null);
            if (finishClose2 != null) {
                finishClose2.run();
            }

            assertFalse("Unused sharedChannel2 must be closed by scanner", sharedChannel2.isActive());
            final BrokerLiveInfo liveD = localManager.getBrokerLiveInfo("cluster1", "brokerD", 0L);
            assertNotNull("Healthy brokerD must remain registered", liveD);
            assertSame("Condemned sharedChannel2 must be rejected, keeping brokerD on oldChannelD",
                oldChannelD, liveD.getChannel());
            assertTrue(oldChannelD.isActive());
            assertTrue(localManager.isBrokerActive("cluster1", "brokerD", 0L));
            assertReverseIndexMatchesLiveTable(localManager);
        } finally {
            allowScannerProceed.countDown();
            Runnable finish = completeCloseRef.getAndSet(null);
            if (finish != null) {
                finish.run();
            }
            if (scanThread1 != null) {
                scanThread1.join(2000);
            }
            sharedChannel.finishAndReleaseAll();
            oldChannelB.finishAndReleaseAll();
            sharedChannel2.finishAndReleaseAll();
            oldChannelD.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testAsyncClosingAndFailedCloseChannelLifecycleKeepsHealthyAndRecoveringBrokers() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final AtomicReference<Runnable> completeCloseRef = new AtomicReference<>();
        final EmbeddedChannel sharedChannel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                if (completeCloseRef.compareAndSet(null, () -> ctx.close(promise))) {
                    return;
                }
                ctx.close(promise);
            }
        }) {
            @Override
            public boolean isActive() {
                boolean active = super.isActive();
                if (active && "rmq-late-shared-hb".equals(Thread.currentThread().getName())) {
                    Runnable finish = completeCloseRef.getAndSet(null);
                    if (finish != null) {
                        finish.run();
                    }
                }
                return active;
            }
        };
        final EmbeddedChannel oldChannelB = new EmbeddedChannel();

        final AtomicInteger remainingFailures = new AtomicInteger(4);
        final AtomicBoolean countCloseFutureLookups = new AtomicBoolean(false);
        final AtomicInteger closeFutureLookups = new AtomicInteger(0);
        final EmbeddedChannel flakyCloseChannel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                if (remainingFailures.getAndDecrement() > 0) {
                    promise.setFailure(new IllegalStateException("simulated close failure"));
                    return;
                }
                ctx.close(promise);
            }
        }) {
            @Override
            public ChannelFuture closeFuture() {
                if (countCloseFutureLookups.get()) {
                    closeFutureLookups.incrementAndGet();
                }
                return super.closeFuture();
            }
        };

        Thread lateHbThread = null;
        try {
            localManager.onBrokerHeartbeat("cluster1", "brokerA", "127.0.0.1:7000", 0L, 40L, sharedChannel,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, oldChannelB,
                1, 1L, -1L, 0);
            Thread.sleep(80L);
            localManager.scanNotActiveBroker();

            assertNotNull("Outbound handler must hold async close in flight", completeCloseRef.get());
            assertTrue("Transport remains active until async close completes", sharedChannel.isActive());

            localManager.onBrokerHeartbeat("cluster1", "brokerA", "127.0.0.1:7000", 0L, 10000L, sharedChannel,
                1, 2L, 1L, 0);
            assertNull("Condemned closing channel must not re-register brokerA while close is in flight",
                localManager.getBrokerLiveInfo("cluster1", "brokerA", 0L));

            final AtomicReference<Throwable> lateHbErr = new AtomicReference<>();
            lateHbThread = new Thread(() -> {
                try {
                    localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L,
                        sharedChannel, 1, 2L, 1L, 0);
                } catch (Throwable t) {
                    lateHbErr.set(t);
                }
            }, "rmq-late-shared-hb");
            lateHbThread.start();
            lateHbThread.join(5000);
            assertFalse(lateHbThread.isAlive());
            if (lateHbErr.get() != null) {
                throw new AssertionError("Late shared-channel heartbeat thread failed", lateHbErr.get());
            }

            Runnable remainingClose = completeCloseRef.getAndSet(null);
            if (remainingClose != null) {
                remainingClose.run();
            }
            assertFalse("Shared channel must be inactive once delayed close completes", sharedChannel.isActive());

            localManager.onBrokerHeartbeat("cluster1", "brokerB", "127.0.0.1:7001", 0L, 10000L, sharedChannel,
                1, 3L, 2L, 0);
            final BrokerLiveInfo liveB = localManager.getBrokerLiveInfo("cluster1", "brokerB", 0L);
            assertNotNull("Late heartbeat from closing sharedChannel must not evict healthy brokerB", liveB);
            assertSame("Healthy brokerB must remain bound to oldChannelB", oldChannelB, liveB.getChannel());
            assertTrue(oldChannelB.isActive());
            assertTrue(localManager.isBrokerActive("cluster1", "brokerB", 0L));
            assertReverseIndexMatchesLiveTable(localManager);

            // Simulate four consecutive close failures: each failure must revoke its closing token
            // without attaching lifecycle closeFuture listeners, allowing immediate heartbeat recovery.
            for (int attempt = 1; attempt <= 4; attempt++) {
                localManager.onBrokerHeartbeat("cluster1", "brokerE", "127.0.0.1:7004", 0L, 30L, flakyCloseChannel,
                    1, (long) attempt, (long) attempt - 1L, 0);
                Thread.sleep(60L);
                countCloseFutureLookups.set(true);
                try {
                    localManager.scanNotActiveBroker();
                } finally {
                    countCloseFutureLookups.set(false);
                }

                assertNull(localManager.getBrokerLiveInfo("cluster1", "brokerE", 0L));
                assertTrue("Channel must remain active when close request fails", flakyCloseChannel.isActive());
                assertFalse("closeFuture must remain incomplete when close request fails",
                    flakyCloseChannel.closeFuture().isDone());
                assertReverseIndexMatchesLiveTable(localManager);

                localManager.onBrokerHeartbeat("cluster1", "brokerE", "127.0.0.1:7004", 0L, 30L, flakyCloseChannel,
                    1, (long) attempt + 10L, (long) attempt, 0);
                final BrokerLiveInfo recoveredE = localManager.getBrokerLiveInfo("cluster1", "brokerE", 0L);
                assertNotNull("Failed close on still-active channel must clear closing mark so heartbeats recover",
                    recoveredE);
                assertSame(flakyCloseChannel, recoveredE.getChannel());
                assertTrue(localManager.isBrokerActive("cluster1", "brokerE", 0L));
                assertReverseIndexMatchesLiveTable(localManager);
            }
            assertEquals("Failed close attempts must not attach closeFuture listeners",
                0, closeFutureLookups.get());

            Thread.sleep(60L);
            localManager.scanNotActiveBroker();
            assertNull(localManager.getBrokerLiveInfo("cluster1", "brokerE", 0L));
            assertFalse("Subsequent expiry scan must close channel once transient failure clears",
                flakyCloseChannel.isActive());
            assertReverseIndexMatchesLiveTable(localManager);
        } finally {
            Runnable finish = completeCloseRef.getAndSet(null);
            if (finish != null) {
                finish.run();
            }
            if (lateHbThread != null) {
                lateHbThread.join(2000);
            }
            sharedChannel.finishAndReleaseAll();
            oldChannelB.finishAndReleaseAll();
            flakyCloseChannel.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testReverseChannelIndexConsistencyOnSwapPartialRebindAndBatchExpiry() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final EmbeddedChannel sharedCh1 = new EmbeddedChannel();
        final EmbeddedChannel sharedCh2 = new EmbeddedChannel();
        final EmbeddedChannel migratedCh = new EmbeddedChannel();
        final EmbeddedChannel swapChX = new EmbeddedChannel();
        final EmbeddedChannel swapChY = new EmbeddedChannel();
        final EmbeddedChannel healthyCh = new EmbeddedChannel();
        final List<EmbeddedChannel> batchExpiredChannels = new ArrayList<>();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final Set<String> inactiveBrokers = new HashSet<>();
        final AtomicReference<CountDownLatch> inactiveLatchRef = new AtomicReference<>(new CountDownLatch(2));
        localManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            synchronized (inactiveBrokers) {
                inactiveBrokers.add(brokerName);
            }
            inactiveCount.incrementAndGet();
            inactiveLatchRef.get().countDown();
        });
        try {
            // 1. Closing a shared channel evicts every broker bound to it and notifies each once.
            localManager.onBrokerHeartbeat("cluster1", "shared1", "127.0.0.1:7101", 0L, 10000L, sharedCh1,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "shared2", "127.0.0.1:7102", 0L, 10000L, sharedCh1,
                1, 1L, -1L, 0);
            sharedCh1.close().syncUninterruptibly();
            localManager.onBrokerChannelClose(sharedCh1);

            assertNull("Closing a shared channel must evict all brokers bound to it",
                localManager.getBrokerLiveInfo("cluster1", "shared1", 0L));
            assertNull("Closing a shared channel must evict all brokers bound to it",
                localManager.getBrokerLiveInfo("cluster1", "shared2", 0L));
            assertTrue(inactiveLatchRef.get().await(3, TimeUnit.SECONDS));
            assertEquals(2, inactiveCount.get());
            assertReverseIndexMatchesLiveTable(localManager);

            // 2. When one broker migrates off a shared channel, closing the old channel evicts only the remaining one.
            inactiveCount.set(0);
            synchronized (inactiveBrokers) {
                inactiveBrokers.clear();
            }
            inactiveLatchRef.set(new CountDownLatch(1));
            localManager.onBrokerHeartbeat("cluster1", "brokerKeep", "127.0.0.1:7103", 0L, 10000L, sharedCh2,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerDrop", "127.0.0.1:7104", 0L, 10000L, sharedCh2,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "brokerKeep", "127.0.0.1:7103", 0L, 10000L, migratedCh,
                1, 2L, 1L, 0);
            sharedCh2.close().syncUninterruptibly();
            localManager.onBrokerChannelClose(sharedCh2);

            assertNull("Remaining brokerDrop on closed sharedCh2 must be evicted",
                localManager.getBrokerLiveInfo("cluster1", "brokerDrop", 0L));
            final BrokerLiveInfo kept = localManager.getBrokerLiveInfo("cluster1", "brokerKeep", 0L);
            assertNotNull("Migrated brokerKeep must survive close of sharedCh2", kept);
            assertSame(migratedCh, kept.getChannel());
            assertTrue(inactiveLatchRef.get().await(3, TimeUnit.SECONDS));
            Thread.sleep(50L);
            assertEquals(1, inactiveCount.get());
            synchronized (inactiveBrokers) {
                assertTrue(inactiveBrokers.contains("brokerDrop"));
                assertFalse(inactiveBrokers.contains("brokerKeep"));
            }
            assertReverseIndexMatchesLiveTable(localManager);

            // 3. Concurrent opposite-direction channel swaps X<->Y must neither deadlock nor drift index state.
            localManager.onBrokerHeartbeat("cluster1", "swap1", "127.0.0.1:7105", 0L, 10000L, swapChX,
                1, 1L, -1L, 0);
            localManager.onBrokerHeartbeat("cluster1", "swap2", "127.0.0.1:7106", 0L, 10000L, swapChY,
                1, 1L, -1L, 0);
            for (int round = 0; round < 200; round++) {
                final Channel target1 = round % 2 == 0 ? swapChY : swapChX;
                final Channel target2 = round % 2 == 0 ? swapChX : swapChY;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(2);
                final int currentRound = round;
                Thread t1 = new Thread(() -> {
                    awaitStart(start, currentRound);
                    localManager.onBrokerHeartbeat("cluster1", "swap1", "127.0.0.1:7105", 0L, 10000L, target1,
                        1, (long) currentRound + 2L, (long) currentRound + 1L, 0);
                    done.countDown();
                });
                Thread t2 = new Thread(() -> {
                    awaitStart(start, currentRound);
                    localManager.onBrokerHeartbeat("cluster1", "swap2", "127.0.0.1:7106", 0L, 10000L, target2,
                        1, (long) currentRound + 2L, (long) currentRound + 1L, 0);
                    done.countDown();
                });
                t1.start();
                t2.start();
                start.countDown();
                assertTrue("Swap round " + round + " did not finish", done.await(5, TimeUnit.SECONDS));
                t1.join(2000);
                t2.join(2000);
            }
            assertReverseIndexMatchesLiveTable(localManager);

            // 4. Batch expiry of 32 distinct channels must not scan healthy broker entries via getChannel().
            final AtomicInteger healthyChannelReads = new AtomicInteger(0);
            final BrokerIdentityInfo healthyId = new BrokerIdentityInfo("cluster1", "healthySpy", 0L);
            localManager.onBrokerHeartbeat("cluster1", "healthySpy", "127.0.0.1:7107", 0L, 60000L, healthyCh,
                1, 1L, -1L, 0);
            final BrokerLiveInfo healthySpy = new BrokerLiveInfo("healthySpy", "127.0.0.1:7107", 0L,
                System.currentTimeMillis(), 60000L, healthyCh, 1, 1L, 0) {
                @Override
                public Channel getChannel() {
                    if ("rmq-batch-expiry-scan".equals(Thread.currentThread().getName())) {
                        healthyChannelReads.incrementAndGet();
                    }
                    return super.getChannel();
                }
            };
            putBrokerLiveInfo(localManager, healthyId, healthySpy);

            final int batchSize = 32;
            for (int i = 0; i < batchSize; i++) {
                EmbeddedChannel ch = new EmbeddedChannel();
                batchExpiredChannels.add(ch);
                localManager.onBrokerHeartbeat("cluster1", "batchExp-" + i, "127.0.0.1:" + (7200 + i), 0L, 30L, ch,
                    1, 1L, -1L, 0);
            }
            Thread.sleep(70L);

            final AtomicReference<Throwable> batchErr = new AtomicReference<>();
            Thread batchScanThread = new Thread(() -> {
                try {
                    localManager.scanNotActiveBroker();
                } catch (Throwable t) {
                    batchErr.set(t);
                }
            }, "rmq-batch-expiry-scan");
            batchScanThread.start();
            batchScanThread.join(5000);
            assertFalse(batchScanThread.isAlive());
            if (batchErr.get() != null) {
                throw new AssertionError("Batch expiry scan failed", batchErr.get());
            }

            assertEquals("Batch expiry of distinct channels must not scan healthy broker channels",
                0, healthyChannelReads.get());
            for (int i = 0; i < batchSize; i++) {
                assertNull(localManager.getBrokerLiveInfo("cluster1", "batchExp-" + i, 0L));
                assertFalse(batchExpiredChannels.get(i).isActive());
            }
            assertNotNull(localManager.getBrokerLiveInfo("cluster1", "healthySpy", 0L));
            assertTrue(healthyCh.isActive());
            assertReverseIndexMatchesLiveTable(localManager);
        } finally {
            sharedCh1.finishAndReleaseAll();
            sharedCh2.finishAndReleaseAll();
            migratedCh.finishAndReleaseAll();
            swapChX.finishAndReleaseAll();
            swapChY.finishAndReleaseAll();
            healthyCh.finishAndReleaseAll();
            for (EmbeddedChannel ch : batchExpiredChannels) {
                ch.finishAndReleaseAll();
            }
            localManager.shutdown();
        }
    }

    @Test
    public void testFirstHeartbeatPublishWindowCloseCleansUpZombieAndProtectsTakeover() throws Exception {
        final DefaultBrokerHeartbeatManager localManager = new DefaultBrokerHeartbeatManager(new ControllerConfig());
        localManager.initialize();
        final AtomicBoolean armFirst = new AtomicBoolean(false);
        final CountDownLatch enteredActiveCheck = new CountDownLatch(1);
        final CountDownLatch allowPublish = new CountDownLatch(1);
        final EmbeddedChannel closingChannel = new EmbeddedChannel() {
            @Override
            public boolean isActive() {
                boolean active = super.isActive();
                if (active && "rmq-hb-publish-1".equals(Thread.currentThread().getName())
                    && armFirst.compareAndSet(true, false)) {
                    enteredActiveCheck.countDown();
                    try {
                        assertTrue("Timed out waiting to publish first heartbeat",
                            allowPublish.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted during publish window", e);
                    }
                }
                return active;
            }
        };

        final AtomicBoolean armSecond = new AtomicBoolean(false);
        final AtomicInteger checkSeq = new AtomicInteger(0);
        final CountDownLatch enteredComputeCheck2 = new CountDownLatch(1);
        final CountDownLatch allowFinishCompute2 = new CountDownLatch(1);
        final CountDownLatch enteredPostCheck2 = new CountDownLatch(1);
        final CountDownLatch allowPostCleanup2 = new CountDownLatch(1);
        final EmbeddedChannel closingChannel2 = new EmbeddedChannel() {
            @Override
            public boolean isActive() {
                boolean active = super.isActive();
                if (armSecond.get() && "rmq-hb-publish-2".equals(Thread.currentThread().getName())) {
                    int seq = checkSeq.incrementAndGet();
                    if (seq == 1) {
                        enteredComputeCheck2.countDown();
                        try {
                            assertTrue("Timed out waiting to finish compute",
                                allowFinishCompute2.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("Interrupted in compute", e);
                        }
                        return active;
                    }
                    if (seq == 2) {
                        enteredPostCheck2.countDown();
                        try {
                            assertTrue("Timed out waiting to run post-compute cleanup",
                                allowPostCleanup2.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException("Interrupted before post-compute cleanup", e);
                        }
                        return super.isActive();
                    }
                }
                return active;
            }
        };
        final EmbeddedChannel takeoverChannel = new EmbeddedChannel();

        Thread hbThread1 = null;
        Thread hbThread2 = null;
        try {
            armFirst.set(true);
            final AtomicReference<Throwable> err1 = new AtomicReference<>();
            hbThread1 = new Thread(() -> {
                try {
                    localManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L,
                        closingChannel, 1, 10L, 5L, 0);
                } catch (Throwable t) {
                    err1.set(t);
                }
            }, "rmq-hb-publish-1");
            hbThread1.start();
            assertTrue("onBrokerHeartbeat must check channel liveness",
                enteredActiveCheck.await(400, TimeUnit.MILLISECONDS));
            closingChannel.close().syncUninterruptibly();
            localManager.onBrokerChannelClose(closingChannel);
            allowPublish.countDown();
            hbThread1.join(5000);
            assertFalse(hbThread1.isAlive());
            if (err1.get() != null) {
                throw new AssertionError("Heartbeat thread 1 failed", err1.get());
            }
            assertFalse(closingChannel.isActive());
            assertNull("Closed channel must not remain in brokerLiveTable after publish-window race",
                localManager.getBrokerLiveInfo("cluster1", "broker1", 0L));
            assertFalse("Broker must not remain active on closed channel",
                localManager.isBrokerActive("cluster1", "broker1", 0L));

            armSecond.set(true);
            final AtomicReference<Throwable> err2 = new AtomicReference<>();
            hbThread2 = new Thread(() -> {
                try {
                    localManager.onBrokerHeartbeat("cluster1", "broker2", "127.0.0.1:7001", 0L, 10000L,
                        closingChannel2, 1, 10L, 5L, 0);
                } catch (Throwable t) {
                    err2.set(t);
                }
            }, "rmq-hb-publish-2");
            hbThread2.start();
            assertTrue(enteredComputeCheck2.await(2, TimeUnit.SECONDS));
            closingChannel2.close().syncUninterruptibly();
            localManager.onBrokerChannelClose(closingChannel2);
            allowFinishCompute2.countDown();

            assertTrue(enteredPostCheck2.await(2, TimeUnit.SECONDS));
            localManager.onBrokerHeartbeat("cluster1", "broker2", "127.0.0.1:7001", 0L, 10000L,
                takeoverChannel, 1, 20L, 15L, 0);
            allowPostCleanup2.countDown();
            hbThread2.join(5000);
            assertFalse(hbThread2.isAlive());
            if (err2.get() != null) {
                throw new AssertionError("Heartbeat thread 2 failed", err2.get());
            }

            final BrokerLiveInfo live2 = localManager.getBrokerLiveInfo("cluster1", "broker2", 0L);
            assertNotNull("Replacement active channel must survive post-compute cleanup", live2);
            assertSame("Replacement active channel must remain bound", takeoverChannel, live2.getChannel());
            assertTrue(takeoverChannel.isActive());
            assertTrue(localManager.isBrokerActive("cluster1", "broker2", 0L));
        } finally {
            allowPublish.countDown();
            allowFinishCompute2.countDown();
            allowPostCleanup2.countDown();
            if (hbThread1 != null) {
                hbThread1.join(2000);
            }
            if (hbThread2 != null) {
                hbThread2.join(2000);
            }
            closingChannel.finishAndReleaseAll();
            closingChannel2.finishAndReleaseAll();
            takeoverChannel.finishAndReleaseAll();
            localManager.shutdown();
        }
    }

    @Test
    public void testClosedChannelRequestTaskDoesNotCreateEntryWhenTableEmpty() throws Exception {
        final ControllerManager controllerManager = new ControllerManager(
            new ControllerConfig(), new NettyServerConfig(), new NettyClientConfig());
        final BrokerHeartbeatManager localHeartbeatManager = controllerManager.getHeartbeatManager();
        localHeartbeatManager.initialize();
        final ControllerRequestProcessor processor = new ControllerRequestProcessor(controllerManager);
        final BrokerHousekeepingService housekeepingService = controllerManager.getBrokerHousekeepingService();
        final EmbeddedChannel closedChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        try {
            final RequestTask lateQueuedTask = buildHeartbeatRequestTask(processor, closedChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, 1, 10L, 5L, 0);
            closedChannel.close().syncUninterruptibly();
            assertFalse(closedChannel.isActive());
            housekeepingService.onChannelClose("127.0.0.1:7000", closedChannel);

            lateQueuedTask.run();

            assertNull("A late RequestTask from an already closed channel must not create a live entry",
                localHeartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L));
        } finally {
            closedChannel.finishAndReleaseAll();
            localHeartbeatManager.shutdown();
            shutdownControllerManagerClient(controllerManager);
        }
    }

    @Test
    public void testLateClosedChannelRequestTaskDoesNotRefreshRebindOrAllowStaleCloseEviction() throws Exception {
        final ControllerManager controllerManager = new ControllerManager(
            new ControllerConfig(), new NettyServerConfig(), new NettyClientConfig());
        final BrokerHeartbeatManager localHeartbeatManager = controllerManager.getHeartbeatManager();
        localHeartbeatManager.initialize();
        final ControllerRequestProcessor processor = new ControllerRequestProcessor(controllerManager);
        final BrokerHousekeepingService housekeepingService = controllerManager.getBrokerHousekeepingService();
        final EmbeddedChannel oldChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        final EmbeddedChannel newChannel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        localHeartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        try {
            buildHeartbeatRequestTask(processor, oldChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 3000L, 1, 10L, 5L, 2).run();

            final RequestTask lateOldTask = buildHeartbeatRequestTask(processor, oldChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 9000L, 2, 999L, 888L, 9);

            oldChannel.close().syncUninterruptibly();
            assertFalse(oldChannel.isActive());

            buildHeartbeatRequestTask(processor, newChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 4000L, 1, 20L, 15L, 1).run();

            final BrokerLiveInfo beforeLateTask = localHeartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
            assertNotNull(beforeLateTask);
            final long pinnedTimestamp = beforeLateTask.getLastUpdateTimestamp() - 500L;
            beforeLateTask.setLastUpdateTimestamp(pinnedTimestamp);

            lateOldTask.run();

            final BrokerLiveInfo afterLateTask = localHeartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
            assertNotNull(afterLateTask);
            assertEquals("Closed channel RequestTask must not refresh lastUpdateTimestamp",
                pinnedTimestamp, afterLateTask.getLastUpdateTimestamp());
            assertSame("Closed channel RequestTask must not rebind over active newChannel",
                newChannel, afterLateTask.getChannel());
            assertEquals("Closed channel RequestTask must not overwrite timeoutMillis",
                4000L, afterLateTask.getHeartbeatTimeoutMillis());
            assertEquals("Closed channel RequestTask must not overwrite electionPriority",
                1, afterLateTask.getElectionPriority().intValue());
            assertEquals("Closed channel RequestTask must not advance epoch", 1, afterLateTask.getEpoch());
            assertEquals("Closed channel RequestTask must not advance maxOffset", 20L, afterLateTask.getMaxOffset());
            assertEquals("Closed channel RequestTask must not overwrite confirmOffset",
                15L, afterLateTask.getConfirmOffset());

            housekeepingService.onChannelClose("127.0.0.1:7000", oldChannel);

            final BrokerLiveInfo afterDelayedOldClose = localHeartbeatManager.getBrokerLiveInfo(
                "cluster1", "broker1", 0L);
            assertNotNull("Delayed close of oldChannel must not evict broker active on newChannel",
                afterDelayedOldClose);
            assertSame(newChannel, afterDelayedOldClose.getChannel());
            assertTrue(newChannel.isActive());
            assertFalse("Stale close after late closed-channel heartbeat must not notify inactive",
                inactiveLatch.await(150, TimeUnit.MILLISECONDS));
            assertEquals(0, inactiveCount.get());

            buildHeartbeatRequestTask(processor, newChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 4000L, 1, 12L, 10L, 1).run();
            assertEquals(1, afterDelayedOldClose.getEpoch());
            assertEquals(20L, afterDelayedOldClose.getMaxOffset());
            assertEquals(15L, afterDelayedOldClose.getConfirmOffset());

            buildHeartbeatRequestTask(processor, newChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 4000L, 1, 30L, 25L, 1).run();
            assertEquals(1, afterDelayedOldClose.getEpoch());
            assertEquals(30L, afterDelayedOldClose.getMaxOffset());
            assertEquals(25L, afterDelayedOldClose.getConfirmOffset());

            buildHeartbeatRequestTask(processor, newChannel,
                "cluster1", "broker1", "127.0.0.1:7000", 0L, 4000L, 2, 5L, 4L, 1).run();
            assertEquals(2, afterDelayedOldClose.getEpoch());
            assertEquals(5L, afterDelayedOldClose.getMaxOffset());
            assertEquals(4L, afterDelayedOldClose.getConfirmOffset());
        } finally {
            oldChannel.finishAndReleaseAll();
            newChannel.finishAndReleaseAll();
            localHeartbeatManager.shutdown();
            shutdownControllerManagerClient(controllerManager);
        }
    }

    @Test
    public void testOverlappingActiveConnectionsFollowLatestActiveHeartbeatAndRecover() throws Exception {
        final EmbeddedChannel channelA = new EmbeddedChannel();
        final EmbeddedChannel channelB = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        this.heartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        try {
            assertTrue(channelA.isActive());
            assertTrue(channelB.isActive());

            this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, channelB,
                1, 10L, 5L, 0);
            assertSame("Initial active heartbeat must bind channelB",
                channelB, this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L).getChannel());

            this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, channelA,
                1, 10L, 5L, 0);
            assertSame("Latest active heartbeat must rebind live entry",
                channelA, this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L).getChannel());

            this.heartbeatManager.onBrokerHeartbeat("cluster1", "broker1", "127.0.0.1:7000", 0L, 10000L, channelB,
                1, 10L, 5L, 0);
            assertSame("Subsequent periodic heartbeat on active channelB must recover binding",
                channelB, this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L).getChannel());

            channelA.close().syncUninterruptibly();
            this.heartbeatManager.onBrokerChannelClose(channelA);

            final BrokerLiveInfo liveInfo = this.heartbeatManager.getBrokerLiveInfo("cluster1", "broker1", 0L);
            assertNotNull("Broker must remain registered on active channelB after channelA closes", liveInfo);
            assertSame(channelB, liveInfo.getChannel());
            assertFalse(inactiveLatch.await(150, TimeUnit.MILLISECONDS));
            assertEquals(0, inactiveCount.get());
        } finally {
            channelA.finishAndReleaseAll();
            channelB.finishAndReleaseAll();
            this.heartbeatManager.shutdown();
        }
    }

    @Test
    public void testNullChannelCloseDoesNotEvictNullChannelBroker() throws Exception {
        final EmbeddedChannel activeChannel = new EmbeddedChannel();
        final AtomicInteger inactiveCount = new AtomicInteger(0);
        final CountDownLatch inactiveLatch = new CountDownLatch(1);
        this.heartbeatManager.registerBrokerLifecycleListener((clusterName, brokerName, brokerId) -> {
            inactiveCount.incrementAndGet();
            inactiveLatch.countDown();
        });
        try {
            this.heartbeatManager.onBrokerHeartbeat("cluster1", "brokerNull", "127.0.0.1:7001", 1L, 10000L, null,
                1, 1L, -1L, 0);
            this.heartbeatManager.onBrokerChannelClose(null);
            assertNotNull("onBrokerChannelClose(null) must not evict null-channel broker entries",
                this.heartbeatManager.getBrokerLiveInfo("cluster1", "brokerNull", 1L));

            this.heartbeatManager.onBrokerHeartbeat("cluster1", "brokerBound", "127.0.0.1:7002", 2L, 10000L,
                activeChannel, 1, 1L, -1L, 0);
            this.heartbeatManager.onBrokerHeartbeat("cluster1", "brokerBound", "127.0.0.1:7002", 2L, 10000L,
                null, 1, 2L, 1L, 0);
            final BrokerLiveInfo boundInfo = this.heartbeatManager.getBrokerLiveInfo("cluster1", "brokerBound", 2L);
            assertNotNull(boundInfo);
            assertSame("Null-channel heartbeat must not clear an existing bound channel",
                activeChannel, boundInfo.getChannel());
            assertEquals(2L, boundInfo.getMaxOffset());
            assertFalse(inactiveLatch.await(150, TimeUnit.MILLISECONDS));
            assertEquals(0, inactiveCount.get());
        } finally {
            activeChannel.finishAndReleaseAll();
            this.heartbeatManager.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    private static void putBrokerLiveInfo(DefaultBrokerHeartbeatManager manager, BrokerIdentityInfo identity,
        BrokerLiveInfo liveInfo) throws Exception {
        Field field = DefaultBrokerHeartbeatManager.class.getDeclaredField("brokerLiveTable");
        field.setAccessible(true);
        Map<BrokerIdentityInfo, BrokerLiveInfo> table = (Map<BrokerIdentityInfo, BrokerLiveInfo>) field.get(manager);
        table.put(identity, liveInfo);
    }

    @SuppressWarnings("unchecked")
    private static void assertReverseIndexMatchesLiveTable(DefaultBrokerHeartbeatManager manager) throws Exception {
        Field liveField = DefaultBrokerHeartbeatManager.class.getDeclaredField("brokerLiveTable");
        liveField.setAccessible(true);
        Map<BrokerIdentityInfo, BrokerLiveInfo> liveTable =
            (Map<BrokerIdentityInfo, BrokerLiveInfo>) liveField.get(manager);

        Field reverseField;
        try {
            reverseField = DefaultBrokerHeartbeatManager.class.getDeclaredField("channelBrokerTable");
        } catch (NoSuchFieldException ignored) {
            return;
        }
        reverseField.setAccessible(true);
        Map<Channel, Object> reverseTable = (Map<Channel, Object>) reverseField.get(manager);

        Map<Channel, Set<BrokerIdentityInfo>> expected = new HashMap<>();
        for (Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> entry : liveTable.entrySet()) {
            Channel channel = entry.getValue().getChannel();
            if (channel != null) {
                expected.computeIfAbsent(channel, k -> new HashSet<>()).add(entry.getKey());
            }
        }

        assertEquals("Reverse channel index must match exact set of active bound channels",
            expected.keySet(), reverseTable.keySet());
        for (Map.Entry<Channel, Object> entry : reverseTable.entrySet()) {
            Field identitiesField = entry.getValue().getClass().getDeclaredField("brokerIdentities");
            identitiesField.setAccessible(true);
            Set<BrokerIdentityInfo> actualIdentities = (Set<BrokerIdentityInfo>) identitiesField.get(entry.getValue());
            assertNotNull("Reverse index must not retain a closing token at rest", actualIdentities);
            assertFalse("Reverse index must not retain an empty identity set", actualIdentities.isEmpty());
            assertEquals("Reverse index identities must match brokerLiveTable for channel " + entry.getKey(),
                expected.get(entry.getKey()), actualIdentities);
        }
    }

    private static void shutdownControllerManagerClient(ControllerManager controllerManager) throws Exception {
        Field field = ControllerManager.class.getDeclaredField("remotingClient");
        field.setAccessible(true);
        RemotingClient remotingClient = (RemotingClient) field.get(controllerManager);
        if (remotingClient != null) {
            remotingClient.shutdown();
        }
    }

    private static RequestTask buildHeartbeatRequestTask(ControllerRequestProcessor processor,
        EmbeddedChannel channel, String clusterName, String brokerName, String brokerAddr, Long brokerId,
        Long timeoutMillis, Integer epoch, Long maxOffset, Long confirmOffset, Integer electionPriority) {
        final BrokerHeartbeatRequestHeader header = new BrokerHeartbeatRequestHeader();
        header.setClusterName(clusterName);
        header.setBrokerName(brokerName);
        header.setBrokerAddr(brokerAddr);
        header.setBrokerId(brokerId);
        header.setHeartbeatTimeoutMills(timeoutMillis);
        header.setEpoch(epoch);
        header.setMaxOffset(maxOffset);
        header.setConfirmOffset(confirmOffset);
        header.setElectionPriority(electionPriority);
        final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.BROKER_HEARTBEAT, header);
        request.makeCustomHeaderToNet();
        final ChannelHandlerContext ctx = channel.pipeline().firstContext();
        assertNotNull("EmbeddedChannel must expose a pipeline context", ctx);
        return new RequestTask(() -> {
            try {
                RemotingCommand response = processor.processRequest(ctx, request);
                assertNotNull(response);
                assertEquals(ResponseCode.SUCCESS, response.getCode());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, channel, request);
    }

    private static void awaitStart(CountDownLatch start, int round) {
        try {
            start.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("round " + round + " interrupted", e);
        }
    }
}
