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

package org.apache.rocketmq.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AcceleratedRecoveryTest {
    private final String storeMessage = "Once, there was a chance for me!";
    private final String messageTopic = "FooBar";
    private int queueTotal = 100;
    private AtomicInteger queueId = new AtomicInteger(0);
    private SocketAddress bornHost;
    private SocketAddress storeHost;
    private byte[] messageBody;
    private MessageStore messageStore;
    private String storePathRootDir;

    @Before
    public void init() throws Exception {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        messageBody = storeMessage.getBytes();
        
        UUID uuid = UUID.randomUUID();
        storePathRootDir = System.getProperty("java.io.tmpdir") + File.separator + "accelerated-recovery-test-" + uuid.toString();
    }

    @After
    public void destroy() {
        if (messageStore != null) {
            messageStore.shutdown();
            messageStore.destroy();
        }

        File file = new File(storePathRootDir);
        UtilAll.deleteFile(file);
    }

    @Test
    public void testAcceleratedRecoveryConfigurationEnabled() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setEnableAcceleratedRecovery(true);
        
        assertTrue("Accelerated recovery should be enabled", messageStoreConfig.isEnableAcceleratedRecovery());
    }

    @Test
    public void testAcceleratedRecoveryConfigurationDisabled() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        // Default should be false
        assertThat(messageStoreConfig.isEnableAcceleratedRecovery()).isFalse();
        
        messageStoreConfig.setEnableAcceleratedRecovery(false);
        assertThat(messageStoreConfig.isEnableAcceleratedRecovery()).isFalse();
    }

    @Test
    public void testAcceleratedRecoveryWithRocksDBStore() throws Exception {
        MessageStoreConfig messageStoreConfig = buildAcceleratedRecoveryConfig();
        messageStoreConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
        messageStoreConfig.setEnableAcceleratedRecovery(true);
        
        messageStore = new DefaultMessageStore(messageStoreConfig,
            new BrokerStatsManager("acceleratedRecoveryTest", true),
            new MyMessageArrivingListener(),
            new BrokerConfig(), new ConcurrentHashMap<>());

        boolean load = messageStore.load();
        assertTrue("Message store should load successfully", load);
        messageStore.start();

        // Put some messages
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner message = buildMessage();
            PutMessageResult result = messageStore.putMessage(message);
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
        }

        // Wait for commit log reput
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        
        // Just verify the store is working properly
        assertThat(messageStore.getMinOffsetInQueue(messageTopic, 0)).isGreaterThanOrEqualTo(0);
        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, 0)).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testAcceleratedRecoveryWithNormalStore() throws Exception {
        MessageStoreConfig messageStoreConfig = buildAcceleratedRecoveryConfig();
        messageStoreConfig.setStoreType(StoreType.DEFAULT.getStoreType());
        messageStoreConfig.setEnableAcceleratedRecovery(true);
        
        messageStore = new DefaultMessageStore(messageStoreConfig,
            new BrokerStatsManager("acceleratedRecoveryTest", true),
            new MyMessageArrivingListener(),
            new BrokerConfig(), new ConcurrentHashMap<>());

        boolean load = messageStore.load();
        assertTrue("Message store should load successfully", load);
        messageStore.start();

        // Put some messages
        for (int i = 0; i < 10; i++) {
            MessageExtBrokerInner message = buildMessage();
            PutMessageResult result = messageStore.putMessage(message);
            assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
        }

        // Wait for commit log reput
        StoreTestUtil.waitCommitLogReput((DefaultMessageStore) messageStore);
        
        // Just verify the store is working properly
        assertThat(messageStore.getMinOffsetInQueue(messageTopic, 0)).isGreaterThanOrEqualTo(0);
        assertThat(messageStore.getMaxOffsetInQueue(messageTopic, 0)).isGreaterThanOrEqualTo(0);
    }

    @Test
    public void testAcceleratedRecoveryFallbackProtection() throws Exception {
        MessageStoreConfig messageStoreConfig = buildAcceleratedRecoveryConfig();
        messageStoreConfig.setStoreType(StoreType.DEFAULT_ROCKSDB.getStoreType());
        messageStoreConfig.setEnableAcceleratedRecovery(true);
        
        messageStore = new DefaultMessageStore(messageStoreConfig,
            new BrokerStatsManager("acceleratedRecoveryTest", true),
            new MyMessageArrivingListener(),
            new BrokerConfig(), new ConcurrentHashMap<>());

        boolean load = messageStore.load();
        assertTrue("Message store should load successfully", load);
        messageStore.start();

        // Test the fallback protection logic by verifying the store can start normally
        // even with accelerated recovery enabled
        assertThat(messageStore.isOSPageCacheBusy()).isFalse();
        assertThat(messageStore.dispatchBehindBytes()).isEqualTo(0);
    }

    private MessageStoreConfig buildAcceleratedRecoveryConfig() {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        messageStoreConfig.setHaListenPort(0);
        messageStoreConfig.setStorePathRootDir(storePathRootDir);
        return messageStoreConfig;
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(messageTopic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId.getAndIncrement() % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString("TAGS=TAG1");
        return msg;
    }

    class MyMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
            long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        }
    }
}