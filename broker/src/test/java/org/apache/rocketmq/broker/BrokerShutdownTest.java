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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.MessageStoreStateMachine;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BrokerShutdownTest {

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 40;
    private static final long EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 5;

    private MessageStoreConfig messageStoreConfig;
    private BrokerConfig brokerConfig;
    private NettyServerConfig nettyServerConfig;
    private AuthConfig authConfig;
    private BrokerController brokerController;

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
        if (brokerController != null && brokerController.getMessageStore() != null
            && !brokerController.getMessageStore().isShutdown()) {
            brokerController.shutdown();
        }
        UtilAll.deleteFile(new File(messageStoreConfig.getStorePathRootDir()));
    }

    @Test
    public void testBrokerGracefulShutdownAndResourceCleanup() throws Exception {
        initializeAndStartBroker();

        MessageStore messageStore = brokerController.getMessageStore();
        assertThat(brokerController.getBrokerMetricsManager()).isNotNull();
        assertThat(brokerController.getBrokerStatsManager()).isNotNull();
        assertThat(brokerController.getConsumerOffsetManager()).isNotNull();
        assertThat(brokerController.getTopicConfigManager()).isNotNull();

        long startTime = System.nanoTime();
        brokerController.shutdown();
        long shutdownTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        assertThat(shutdownTime).isLessThan(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
        assertThat(messageStore.isShutdown()).isTrue();
        assertThat(messageStore.getStateMachine().getCurrentState())
            .isEqualTo(MessageStoreStateMachine.MessageStoreState.SHUTDOWN_OK);

        // Repeated shutdown should keep the broker in a clean state.
        brokerController.shutdown();
        assertThat(messageStore.getStateMachine().getCurrentState())
            .isEqualTo(MessageStoreStateMachine.MessageStoreState.SHUTDOWN_OK);
    }

    @Test
    public void testShutdownFromAnotherThread() throws Exception {
        initializeAndStartBroker();

        MessageStore messageStore = brokerController.getMessageStore();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> shutdownFuture = executor.submit(brokerController::shutdown);
            shutdownFuture.get(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        assertThat(messageStore.isShutdown()).isTrue();
        assertThat(messageStore.getStateMachine().getCurrentState())
            .isEqualTo(MessageStoreStateMachine.MessageStoreState.SHUTDOWN_OK);
    }

    private void initializeAndStartBroker() throws Exception {
        brokerController = new BrokerController(
            brokerConfig, nettyServerConfig, new NettyClientConfig(), messageStoreConfig, authConfig);
        assertThat(brokerController.initialize()).isTrue();
        brokerController.start();
    }
}
