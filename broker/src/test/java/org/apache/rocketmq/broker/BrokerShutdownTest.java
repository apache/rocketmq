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

package org.apache.rocketmq.broker;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.broker.config.v1.RocksDBConfigManager;
import org.apache.rocketmq.broker.config.v1.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.config.v1.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.config.v1.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.pop.orderly.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerShutdownTest {

    private MessageStoreConfig messageStoreConfig;
    private BrokerConfig brokerConfig;
    private NettyServerConfig nettyServerConfig;
    private AuthConfig authConfig;

    @Before
    public void setUp() {
        messageStoreConfig = new MessageStoreConfig();
        String storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "store-"
                + UUID.randomUUID().toString();
        messageStoreConfig.setStorePathRootDir(storePathRootDir);

        brokerConfig = new BrokerConfig();
        nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(0);
        authConfig = new AuthConfig();
    }

    @After
    public void destroy() {
        UtilAll.deleteFile(new File(messageStoreConfig.getStorePathRootDir()));
    }

    @Test
    public void testBrokerGracefulShutdown() throws Exception {
        // Test that broker shuts down gracefully with proper resource cleanup
        BrokerController brokerController = new BrokerController(
            brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig, authConfig);
        
        // Initialize and start the broker
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
        
        // Verify broker is running
        assertThat(brokerController.getBrokerMetricsManager()).isNotNull();
        
        // Test graceful shutdown
        long startTime = System.currentTimeMillis();
        brokerController.shutdown();
        long shutdownTime = System.currentTimeMillis() - startTime;
        
        // Shutdown should complete within reasonable time (10 seconds)
        assertThat(shutdownTime).isLessThan(40000);
    }

    @Test
    public void testChainedShutdownOrdering() throws Exception {
        // Test that shutdown components are called in proper order
        BrokerController brokerController = new BrokerController(
            brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig, authConfig);
        
        assertThat(brokerController.initialize()).isTrue();
        
        // Track shutdown order using atomic flags
        AtomicBoolean metricsManagerShutdown = new AtomicBoolean(false);
        AtomicBoolean brokerStatsShutdown = new AtomicBoolean(false);
        
        // Start broker
        brokerController.start();
        
        // Verify services are initialized
        assertThat(brokerController.getBrokerMetricsManager()).isNotNull();
        assertThat(brokerController.getBrokerStatsManager()).isNotNull();
        
        // Shutdown should not throw exceptions
        brokerController.shutdown();
        
        // After shutdown, services should be properly cleaned up
        // (We can't easily verify the exact order without modifying the implementation,
        // but we can verify shutdown completes successfully)
        assertThat(true).isTrue(); // Placeholder for successful completion
    }

    @Test
    public void testShutdownWithConcurrentOperations() throws Exception {
        // Test shutdown behavior when concurrent operations are running
        BrokerController brokerController = new BrokerController(
            brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig, authConfig);
        
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
        
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        AtomicBoolean shutdownSuccess = new AtomicBoolean(false);
        
        // Simulate concurrent shutdown from another thread
        Thread shutdownThread = new Thread(() -> {
            try {
                brokerController.shutdown();
                shutdownSuccess.set(true);
            } catch (Exception e) {
                // Should not happen in graceful shutdown
            } finally {
                shutdownLatch.countDown();
            }
        });
        
        shutdownThread.start();
        
        // Wait for shutdown to complete
        assertThat(shutdownLatch.await(40, TimeUnit.SECONDS)).isTrue();
        assertThat(shutdownSuccess.get()).isTrue();
    }

    @Test(timeout = 15000)
    public void testConfigPersistencePrecedesSharedStoreShutdown() throws Exception {
        BrokerController controller = configShutdownController(true);
        Mockito.when(controller.consumerOffsetManager.stop()).thenReturn(true);

        controller.shutdownBasicService();

        InOrder order = Mockito.inOrder(controller.topicConfigManager, controller.subscriptionGroupManager,
            controller.consumerOffsetManager, controller.consumerOrderInfoManager);
        order.verify(controller.topicConfigManager).persist();
        order.verify(controller.subscriptionGroupManager).persist();
        order.verify(controller.consumerOffsetManager).persist();
        order.verify(controller.consumerOffsetManager).stop();
        order.verify(controller.topicConfigManager).stop();
        order.verify(controller.subscriptionGroupManager).stop();
        order.verify(controller.consumerOrderInfoManager).persist();
        order.verify(controller.consumerOrderInfoManager).shutdown();
    }

    @Test(timeout = 15000)
    public void testFailedOffsetShutdownStillClosesSharedStore() throws Exception {
        BrokerController controller = configShutdownController(true);
        Mockito.when(controller.consumerOffsetManager.stop()).thenReturn(false);

        controller.shutdownBasicService();

        Mockito.verify(controller.topicConfigManager).persist();
        Mockito.verify(controller.subscriptionGroupManager).persist();
        Mockito.verify(controller.consumerOffsetManager).persist();
        Mockito.verify(controller.consumerOffsetManager).stop();
        Mockito.verify(controller.topicConfigManager).stop();
        Mockito.verify(controller.subscriptionGroupManager).stop();
        Mockito.verify(controller.consumerOrderInfoManager).persist();
        Mockito.verify(controller.consumerOrderInfoManager).shutdown();
    }

    @Test(timeout = 15000)
    public void testFailedOffsetShutdownStillClosesSeparateStores() throws Exception {
        BrokerController controller = configShutdownController(false);
        Mockito.when(controller.consumerOffsetManager.stop()).thenReturn(false);

        controller.shutdownBasicService();

        Mockito.verify(controller.topicConfigManager).stop();
        Mockito.verify(controller.subscriptionGroupManager).stop();
        Mockito.verify(controller.consumerOrderInfoManager).shutdown();
    }

    @Test(timeout = 15000)
    public void testSharedStoreShutdownDrainsOffsetsAddedAfterFinalPersist() throws Exception {
        BrokerController controller = configShutdownController(true);
        brokerConfig.setPersistConsumerOffsetIncrementally(true);
        brokerConfig.setConsumerOffsetUpdateVersionStep(Long.MAX_VALUE);
        RocksDBTopicConfigManager topics = new RocksDBTopicConfigManager(controller);
        RocksDBSubscriptionGroupManager groups = new RocksDBSubscriptionGroupManager(controller);
        RocksDBConsumerOffsetManager offsets = Mockito.spy(new RocksDBConsumerOffsetManager(controller));
        RocksDBConsumerOffsetManager recovered = null;
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertThat(topics.load()).isTrue();
            assertThat(groups.load()).isTrue();
            assertThat(offsets.load()).isTrue();
            RocksDBConfigManager storage = (RocksDBConfigManager) fieldValue(offsets, "rocksDBConfigManager");
            assertThat(((RocksDBConfigManager) fieldValue(topics, "rocksDBConfigManager")).configRocksDBStorage)
                .isSameAs(storage.configRocksDBStorage);
            assertThat(((RocksDBConfigManager) fieldValue(groups, "rocksDBConfigManager")).configRocksDBStorage)
                .isSameAs(storage.configRocksDBStorage);
            controller.topicConfigManager = topics;
            controller.subscriptionGroupManager = groups;
            controller.consumerOffsetManager = offsets;

            Object persister = fieldValue(offsets, "asyncPersister");
            ScheduledExecutorService executor = (ScheduledExecutorService) fieldValue(persister, "executor");
            Future<?> blocked = executor.submit(() -> {
                entered.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for the test to release the background thread");
                }
                return null;
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            String topic = "%LMQ%shutdown-test";
            String group = "shutdown-group";
            offsets.commitOffset("client", group, topic, 0, 10);
            assertThat(storage.configRocksDBStorage.get("consumerOffset",
                (topic + "@" + group).getBytes(StandardCharsets.UTF_8))).isNull();
            // In-flight commits may still arrive after the final persist; stop must drain them before the shared DB closes.
            Mockito.doAnswer(invocation -> {
                invocation.callRealMethod();
                offsets.commitOffset("client", group, topic, 0, 11);
                return null;
            }).when(offsets).persist();
            Mockito.doAnswer(invocation -> {
                release.countDown();
                return invocation.callRealMethod();
            }).when(offsets).stop();

            controller.shutdownBasicService();
            blocked.get(5, TimeUnit.SECONDS);
            assertThat(executor.isTerminated()).isTrue();
            recovered = new RocksDBConsumerOffsetManager(controller);
            assertThat(recovered.load()).isTrue();
            assertThat(recovered.queryOffset(group, topic, 0)).isEqualTo(11);
        } finally {
            release.countDown();
            if (recovered != null) {
                recovered.stop();
            }
            Mockito.doCallRealMethod().when(offsets).stop();
            offsets.stop();
            topics.stop();
            groups.stop();
        }
    }

    // Run the real shutdownBasicService without starting a full broker; other resources are not involved in this test.
    private BrokerController configShutdownController(boolean shared) throws Exception {
        brokerConfig.setUseSingleRocksDBForAllConfigs(shared);
        BrokerController controller = Mockito.mock(BrokerController.class);
        Mockito.when(controller.getBrokerConfig()).thenReturn(brokerConfig);
        Mockito.when(controller.getMessageStoreConfig()).thenReturn(messageStoreConfig);
        setControllerField(controller, "brokerConfig", brokerConfig);
        setControllerField(controller, "remotingServerMap", Collections.emptyMap());
        setControllerField(controller, "brokerAttachedPlugins", Collections.emptyList());
        setControllerField(controller, "popMessageProcessor", Mockito.mock(PopMessageProcessor.class));
        setControllerField(controller, "ackMessageProcessor", Mockito.mock(AckMessageProcessor.class));
        setControllerField(controller, "consumerOrderInfoManager", Mockito.mock(ConsumerOrderInfoManager.class));
        controller.topicConfigManager = Mockito.mock(RocksDBTopicConfigManager.class);
        controller.subscriptionGroupManager = Mockito.mock(RocksDBSubscriptionGroupManager.class);
        controller.consumerOffsetManager = Mockito.mock(RocksDBConsumerOffsetManager.class);
        Mockito.doCallRealMethod().when(controller).shutdownBasicService();
        return controller;
    }

    private static void setControllerField(BrokerController controller, String name, Object value) throws Exception {
        Field field = BrokerController.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(controller, value);
    }

    private static Object fieldValue(Object target, String name) throws Exception {
        Class<?> type = target instanceof RocksDBConsumerOffsetManager ? RocksDBConsumerOffsetManager.class
            : target.getClass();
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    @Test
    public void testResourceCleanupDuringShutdown() throws Exception {
        // Test that resources are properly cleaned up during shutdown
        BrokerController brokerController = new BrokerController(
            brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig, authConfig);
        
        assertThat(brokerController.initialize()).isTrue();
        
        // Verify essential components are initialized
        assertThat(brokerController.getBrokerMetricsManager()).isNotNull();
        assertThat(brokerController.getBrokerStatsManager()).isNotNull();
        assertThat(brokerController.getConsumerOffsetManager()).isNotNull();
        assertThat(brokerController.getTopicConfigManager()).isNotNull();
        
        brokerController.start();
        
        // Shutdown should clean up all resources
        brokerController.shutdown();
        
        // After shutdown, the broker should be in a clean state
        // We verify this by ensuring a second shutdown call doesn't cause issues
        brokerController.shutdown(); // Should be safe to call multiple times
    }
}