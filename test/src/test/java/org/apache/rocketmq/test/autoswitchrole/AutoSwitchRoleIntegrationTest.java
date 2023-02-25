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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.autoswitchrole;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.protocol.body.SyncStateSet;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class AutoSwitchRoleIntegrationTest extends AutoSwitchRoleBase {

    private static final int DEFAULT_FILE_SIZE = 1024 * 1024;
    private static NamesrvController namesrvController;
    private static ControllerManager controllerManager;
    private static String nameserverAddress;
    private static String controllerAddress;
    private BrokerController brokerController1;
    private BrokerController brokerController2;
    private Random random = new Random();

    @BeforeClass
    public static void init() throws Exception {
        initialize();

        int controllerPort = nextPort();
        final String peers = String.format("n0-localhost:%d", controllerPort);

        final NettyServerConfig serverConfig = new NettyServerConfig();
        int namesrvPort = nextPort();
        serverConfig.setListenPort(namesrvPort);

        ControllerConfig controllerConfig = buildControllerConfig("n0", peers);
        namesrvController = new NamesrvController(new NamesrvConfig(), serverConfig, new NettyClientConfig());
        assertTrue(namesrvController.initialize());
        namesrvController.start();

        controllerManager = new ControllerManager(controllerConfig, new NettyServerConfig(), new NettyClientConfig());
        assertTrue(controllerManager.initialize());
        controllerManager.start();

        nameserverAddress = "127.0.0.1:" + namesrvPort + ";";
        controllerAddress = "127.0.0.1:" + controllerPort + ";";
    }

    public void initBroker(int mappedFileSize, String brokerName) throws Exception {

        this.brokerController1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), nextPort(), nextPort(), BrokerRole.SYNC_MASTER, mappedFileSize);
        this.brokerController2 = startBroker(nameserverAddress, controllerAddress, brokerName, 2, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, mappedFileSize);
        // Wait slave connecting to master
        assertTrue(waitSlaveReady(this.brokerController2.getMessageStore()));
        Thread.sleep(1000);
    }

    public void mockData(String topic) throws Exception {
        final MessageStore messageStore = brokerController1.getMessageStore();
        putMessage(messageStore, topic);
        // Check slave message
        checkMessage(brokerController2.getMessageStore(), topic, 10, 0);
    }

    public boolean waitSlaveReady(MessageStore messageStore) throws InterruptedException {
        int tryTimes = 0;
        while (tryTimes < 100) {
            final HAClient haClient = messageStore.getHaService().getHAClient();
            if (haClient != null && haClient.getCurrentState().equals(HAConnectionState.TRANSFER)) {
                return true;
            } else {
                Thread.sleep(2000);
                tryTimes++;
            }
        }
        return false;
    }

    @Test
    public void testCheckSyncStateSet() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);

        mockData(topic);

        // Check sync state set
        final ReplicasManager replicasManager = brokerController1.getReplicasManager();
        SyncStateSet syncStateSet = replicasManager.getSyncStateSet();
        assertEquals(2, syncStateSet.getSyncStateSet().size());

        // Shutdown controller2
        ScheduledExecutorService singleThread = Executors.newSingleThreadScheduledExecutor();
        while (!singleThread.awaitTermination(6 * 1000, TimeUnit.MILLISECONDS)) {
            this.brokerController2.shutdown();
            singleThread.shutdown();
        }

        syncStateSet = replicasManager.getSyncStateSet();
        shutdownAndClearBroker();
        assertEquals(1, syncStateSet.getSyncStateSet().size());
    }

    @Test
    public void testChangeMaster() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);
        mockData(topic);

        // Let master shutdown
        brokerController1.shutdown();
        brokerList.remove(this.brokerController1);
        Thread.sleep(6000);

        // The slave should change to master
        assertTrue(brokerController2.getReplicasManager().isMasterState());
        assertEquals(brokerController2.getReplicasManager().getMasterEpoch(), 2);

        // Restart old master, it should be slave
        brokerController1 = startBroker(nameserverAddress, controllerAddress, brokerName, 1, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, DEFAULT_FILE_SIZE);
        waitSlaveReady(brokerController1.getMessageStore());

        assertFalse(brokerController1.getReplicasManager().isMasterState());
        assertEquals(brokerController1.getReplicasManager().getMasterAddress(), brokerController2.getReplicasManager().getLocalAddress());

        // Put another batch messages
        final MessageStore messageStore = brokerController2.getMessageStore();
        putMessage(messageStore, topic);

        //Check slave message
        checkMessage(brokerController1.getMessageStore(), topic, 20, 0);
        shutdownAndClearBroker();
    }

    @Test
    public void testAddBroker() throws Exception {
        String topic = "Topic-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        initBroker(DEFAULT_FILE_SIZE, brokerName);
        mockData(topic);

        BrokerController broker3 = startBroker(nameserverAddress, controllerAddress, brokerName, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, DEFAULT_FILE_SIZE);
        waitSlaveReady(broker3.getMessageStore());

        checkMessage(broker3.getMessageStore(), topic, 10, 0);

        putMessage(this.brokerController1.getMessageStore(), topic);
        checkMessage(broker3.getMessageStore(), topic, 20, 0);
        shutdownAndClearBroker();
    }

    @Test
    public void testTruncateEpochLogAndChangeMaster() throws Exception {
        shutdownAndClearBroker();
        String topic = "FooBar";
        String brokerName = "Broker-" + AutoSwitchRoleIntegrationTest.class.getSimpleName() + random.nextInt(65535);
        // Noted that 10 msg 's total size = 1570, and if init the mappedFileSize = 1700, one file only be used to store 10 msg.
        initBroker(1700, brokerName);
        // Step1: Put message
        putMessage(this.brokerController1.getMessageStore(), topic);
        checkMessage(this.brokerController2.getMessageStore(), topic, 10, 0);

        // Step2: shutdown broker1, broker2 as master
        brokerController1.shutdown();
        brokerList.remove(brokerController1);
        Thread.sleep(5000);

        assertTrue(brokerController2.getReplicasManager().isMasterState());
        assertEquals(brokerController2.getReplicasManager().getMasterEpoch(), 2);

        // Step3: add broker3
        BrokerController broker3 = startBroker(nameserverAddress, controllerAddress, brokerName, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker3.getMessageStore());
        checkMessage(broker3.getMessageStore(), topic, 10, 0);

        // Step4: put another batch message
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        putMessage(this.brokerController2.getMessageStore(), topic);
        checkMessage(broker3.getMessageStore(), topic, 20, 0);

        // Step5: Check file position, each epoch will be stored on one file(Because fileSize = 1700, which equal to 10 msg size);
        // So epoch1 was stored in firstFile, epoch2 was stored in second file, the lastFile was empty.
        final MessageStore broker2MessageStore = this.brokerController2.getMessageStore();
        final MappedFileQueue fileQueue = broker2MessageStore.getCommitLog().getMappedFileQueue();
        assertEquals(2, fileQueue.getTotalFileSize() / 1700);

        // Truncate epoch1's log (truncateEndOffset = 1570), which means we should delete the first file directly.
        final MappedFile firstFile = broker2MessageStore.getCommitLog().getMappedFileQueue().getFirstMappedFile();
        firstFile.shutdown(1000);
        fileQueue.retryDeleteFirstFile(1000);
        assertEquals(broker2MessageStore.getCommitLog().getMinOffset(), 1700);

        final AutoSwitchHAService haService = (AutoSwitchHAService) this.brokerController2.getMessageStore().getHaService();
        haService.truncateEpochFilePrefix(1570);
        checkMessage(broker2MessageStore, topic, 10, 10);

        // Step6, start broker4, link to broker2, it should sync msg from epoch2(offset = 1700).
        BrokerController broker4 = startBroker(nameserverAddress, controllerAddress, brokerName, 4, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker4.getMessageStore());
        checkMessage(broker4.getMessageStore(), topic, 10, 10);
        shutdownAndClearBroker();
    }

    public void shutdownAndClearBroker() throws InterruptedException {
        for (BrokerController controller : brokerList) {
            controller.shutdown();
            UtilAll.deleteFile(new File(controller.getMessageStoreConfig().getStorePathRootDir()));
        }
        brokerList.clear();
    }

    @AfterClass
    public static void destroy() {
        if (namesrvController != null) {
            namesrvController.shutdown();
        }
        if (controllerManager != null) {
            controllerManager.shutdown();
        }
        File file = new File(STORE_PATH_ROOT_PARENT_DIR);
        UtilAll.deleteFile(file);
    }

}
