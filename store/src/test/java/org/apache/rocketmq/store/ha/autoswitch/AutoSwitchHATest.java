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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class AutoSwitchHATest {
    private final String StoreMessage = "Once, there was a chance for me!";
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;

    private DefaultMessageStore messageStore;
    private DefaultMessageStore messageStore2;
    private DefaultMessageStore messageStore3;
    private MessageStoreConfig storeConfig1;
    private MessageStoreConfig storeConfig2;
    private MessageStoreConfig storeConfig3;
    private BrokerStatsManager brokerStatsManager = new BrokerStatsManager("simpleTest", true);
    private String storePathRootParentDir = System.getProperty("user.home") + File.separator +
        UUID.randomUUID().toString().replace("-", "");
    private String storePathRootDir = storePathRootParentDir + File.separator + "store";

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        storeConfig1 = new MessageStoreConfig();
        storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig1.setStorePathRootDir(storePathRootDir + File.separator + "broker1");
        storeConfig1.setStorePathCommitLog(storePathRootDir + File.separator + "broker1" + File.separator + "commitlog");
        storeConfig1.setStorePathEpochFile(storePathRootDir + File.separator + "broker1" + File.separator + "EpochFileCache");
        storeConfig1.setTotalReplicas(3);
        storeConfig1.setInSyncReplicas(2);
        storeConfig1.setStartupControllerMode(true);
        buildMessageStoreConfig(storeConfig1);
        storeConfig2 = new MessageStoreConfig();
        storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        storeConfig2.setStorePathRootDir(storePathRootDir + File.separator + "broker2");
        storeConfig2.setStorePathCommitLog(storePathRootDir + File.separator + "broker2" + File.separator + "commitlog");
        storeConfig2.setStorePathEpochFile(storePathRootDir + File.separator + "broker2" + File.separator + "EpochFileCache");
        storeConfig2.setHaListenPort(10943);
        storeConfig2.setTotalReplicas(3);
        storeConfig2.setInSyncReplicas(2);
        storeConfig2.setStartupControllerMode(true);
        buildMessageStoreConfig(storeConfig2);
        messageStore = buildMessageStore(storeConfig1, 0L);
        messageStore2 = buildMessageStore(storeConfig2, 1L);
        boolean load = messageStore.load();
        boolean slaveLoad = messageStore2.load();
        assertTrue(load);
        assertTrue(slaveLoad);
        messageStore.start();
        messageStore2.start();

        storeConfig3 = new MessageStoreConfig();
        storeConfig3.setBrokerRole(BrokerRole.SLAVE);
        storeConfig3.setStorePathRootDir(storePathRootDir + File.separator + "broker3");
        storeConfig3.setStorePathCommitLog(storePathRootDir + File.separator + "broker3" + File.separator + "commitlog");
        storeConfig3.setStorePathEpochFile(storePathRootDir + File.separator + "broker3" + File.separator + "EpochFileCache");
        storeConfig3.setHaListenPort(10980);
        storeConfig3.setTotalReplicas(3);
        storeConfig3.setInSyncReplicas(2);
        storeConfig3.setStartupControllerMode(true);
        buildMessageStoreConfig(storeConfig3);
        messageStore3 = buildMessageStore(storeConfig3, 3L);
        messageStore3.load();
        messageStore3.start();
    }

    public void mockData() throws InterruptedException {
        System.out.println("Begin test");
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        // First, change store1 to master, store2 to follower
        this.storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        this.messageStore.getHaService().changeToMaster(1);
        this.messageStore2.getHaService().changeToSlave("", "127.0.0.1:10912", 1);
        Thread.sleep(5000);

        for (long i = 0; i < totalMsgs; i++) {
            messageStore.putMessage(buildMessage());
        }
        System.out.println("Put log success");

        Thread.sleep(200);

        checkMessage(messageStore2, 10, 0);
    }

    private void checkMessage(final DefaultMessageStore messageStore, int totalMsgs, int startOffset) {
        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, startOffset + i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            assertTrue(GetMessageStatus.FOUND.equals(result.getStatus()));
            result.release();
        }
    }

    @Test
    public void testChangeRoleManyTimes() throws InterruptedException {
        System.out.println("Begin test");
        long totalMsgs = 10;
        QUEUE_TOTAL = 1;
        MessageBody = StoreMessage.getBytes();
        // Step1, change store1 to master, store2 to follower
        mockData();

        // Step2, change store1 to follower, store2 to master, epoch = 2
        this.storeConfig1.setBrokerRole(BrokerRole.SLAVE);
        this.storeConfig2.setBrokerRole(BrokerRole.SYNC_MASTER);
        this.messageStore.getHaService().changeToSlave("", "127.0.0.1:10943", 2);
        this.messageStore2.getHaService().changeToMaster(2);
        Thread.sleep(5000);

        for (int i = 0; i < totalMsgs; i++) {
            this.messageStore2.putMessage(buildMessage());
        }
        System.out.println("Put log success2");
        Thread.sleep(200);

        // Check slave messages (store1)
        checkMessage(messageStore, 10, 10);

        // Step3, change store2 to follower, store1 to master, epoch = 3
        this.storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        this.storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        this.messageStore.getHaService().changeToMaster(3);
        this.messageStore2.getHaService().changeToSlave("", "127.0.0.1:10912", 3);
        Thread.sleep(5000);

        for (int i = 0; i < totalMsgs; i++) {
            this.messageStore.putMessage(buildMessage());
        }
        System.out.println("Put log success3");
        Thread.sleep(200);

        // Check slave messages (store2)
        checkMessage(messageStore2, 10, 20);
    }

    @Test
    public void testAddBroker() throws Exception {
        // Step1: broker1 as leader, broker2 as follower
        mockData();

        // Step2: add new broker3, link to broker1
        messageStore3.getHaService().changeToSlave("", "127.0.0.1:10912", 1);
        Thread.sleep(5000);

        checkMessage(messageStore3, 10, 0);
    }

    @After
    public void destroy() throws Exception {
        Thread.sleep(5000L);
        messageStore2.shutdown();
        messageStore2.destroy();
        messageStore.shutdown();
        messageStore.destroy();
        messageStore3.shutdown();
        messageStore3.destroy();
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

    private DefaultMessageStore buildMessageStore(MessageStoreConfig messageStoreConfig, long brokerId) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerId(brokerId);
        return new DefaultMessageStore(messageStoreConfig, brokerStatsManager, null, brokerConfig);
    }

    private void buildMessageStoreConfig(MessageStoreConfig messageStoreConfig) {
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }
}
