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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.body.SyncStateSet;
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
import org.apache.rocketmq.test.base.BaseConf;
import org.junit.After;
import org.junit.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleIntegrationTest extends AutoSwitchRoleBase {

    private final int defaultFileSize = 1024 * 1024;
    private ControllerConfig controllerConfig;
    private NamesrvController namesrvController;
    private ControllerManager controllerManager;
    private String namesrvAddress;
    private String controllerAddress;
    private BrokerController brokerController1;
    private BrokerController brokerController2;
    protected List<BrokerController> brokerControllerList;
    
    
    public void init(int mappedFileSize) throws Exception {
        super.initialize();

        // Startup namesrv
        int controllerPort = nextPort();
        final String peers = String.format("n0-localhost:%d", controllerPort);

        final NettyServerConfig serverConfig = new NettyServerConfig();
        int namesrvPort = nextPort();
        serverConfig.setListenPort(namesrvPort);

        this.controllerConfig = buildControllerConfig("n0", peers);
        this.namesrvController = new NamesrvController(new NamesrvConfig(), serverConfig, new NettyClientConfig());
        assertTrue(namesrvController.initialize());
        namesrvController.start();

        this.controllerManager = new ControllerManager(controllerConfig, new NettyServerConfig(), new NettyClientConfig());
        assertTrue(controllerManager.initialize());
        controllerManager.start();

        this.namesrvAddress = "127.0.0.1:" + namesrvPort + ";";
        this.controllerAddress = "127.0.0.1:" + controllerPort + ";";

        this.brokerController1 = startBroker(this.namesrvAddress, this.controllerAddress, 1, nextPort(), nextPort(), nextPort(), BrokerRole.SYNC_MASTER, mappedFileSize);
        this.brokerController2 = startBroker(this.namesrvAddress, this.controllerAddress, 2, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, mappedFileSize);
        this.brokerControllerList = ImmutableList.of(brokerController1, brokerController2);
    
    
        // Wait slave connecting to master
        assertTrue(waitSlaveReady(this.brokerController2.getMessageStore()));
    }

    public void mockData() throws Exception {
        System.out.println("Begin test");
        final MessageStore messageStore = brokerController1.getMessageStore();
        putMessage(messageStore);
        Thread.sleep(3000);
        // Check slave message
        checkMessage(brokerController2.getMessageStore(), 10, 0);
    }

    public boolean waitSlaveReady(MessageStore messageStore) throws InterruptedException {
        int tryTimes = 0;
        while (tryTimes < 100) {
            final HAClient haClient = messageStore.getHaService().getHAClient();
            if (haClient != null && haClient.getCurrentState().equals(HAConnectionState.TRANSFER)) {
                return true;
            } else {
                System.out.println("slave not ready");
                Thread.sleep(2000);
                tryTimes++;
            }
        }
        return false;
    }

    @Test
    public void testCheckSyncStateSet() throws Exception {
        init(defaultFileSize);
        awaitDispatchMs(6);
        mockData();

        // Check sync state set
        final ReplicasManager replicasManager = brokerController1.getReplicasManager();
        SyncStateSet syncStateSet = replicasManager.getSyncStateSet();
        assertEquals(2, syncStateSet.getSyncStateSet().size());
        
        
        // Shutdown controller2
        ScheduledExecutorService singleThread = Executors.newSingleThreadScheduledExecutor();
        while (!singleThread.awaitTermination(6* 1000, TimeUnit.MILLISECONDS)) {
            this.brokerController2.shutdown();
            singleThread.shutdown();
        }
        
        syncStateSet = replicasManager.getSyncStateSet();
        shutdown();
        assertEquals(1, syncStateSet.getSyncStateSet().size());
    }

    @Test
    public void testChangeMaster() throws Exception {
        init(defaultFileSize);
        mockData();

        // Let master shutdown
        brokerController1.shutdown();
        this.brokerList.remove(this.brokerController1);
        Thread.sleep(6000);

        // The slave should change to master
        assertTrue(brokerController2.getReplicasManager().isMasterState());
        assertEquals(brokerController2.getReplicasManager().getMasterEpoch(), 2);

        // Restart old master, it should be slave
        brokerController1 = startBroker(this.namesrvAddress, this.controllerAddress, 1, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, defaultFileSize);
        waitSlaveReady(brokerController1.getMessageStore());

        assertFalse(brokerController1.getReplicasManager().isMasterState());
        assertEquals(brokerController1.getReplicasManager().getMasterAddress(), brokerController2.getReplicasManager().getLocalAddress());

        // Put another batch messages
        final MessageStore messageStore = brokerController2.getMessageStore();
        putMessage(messageStore);

        Thread.sleep(3000);

        // Check slave message
        checkMessage(brokerController1.getMessageStore(), 20, 0);
        shutdown();
    }

    @Test
    public void testAddBroker() throws Exception {
        init(defaultFileSize);
        mockData();

        BrokerController broker3 = startBroker(this.namesrvAddress, this.controllerAddress, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, defaultFileSize);
        waitSlaveReady(broker3.getMessageStore());
        Thread.sleep(3000);

        checkMessage(broker3.getMessageStore(), 10, 0);

        putMessage(this.brokerController1.getMessageStore());
        Thread.sleep(3000);
        checkMessage(broker3.getMessageStore(), 20, 0);
        shutdown();
    }

    @Test
    public void testTruncateEpochLogAndChangeMaster() throws Exception {
        // Noted that 10 msg 's total size = 1570, and if init the mappedFileSize = 1700, one file only be used to store 10 msg.
        init(1700);
        // Step1: Put message
        putMessage(this.brokerController1.getMessageStore());
        Thread.sleep(3000);
        checkMessage(this.brokerController2.getMessageStore(), 10, 0);

        // Step2: shutdown broker1, broker2 as master
        brokerController1.shutdown();
        this.brokerList.remove(brokerController1);
        Thread.sleep(5000);

        assertTrue(brokerController2.getReplicasManager().isMasterState());
        assertEquals(brokerController2.getReplicasManager().getMasterEpoch(), 2);

        // Step3: add broker3
        BrokerController broker3 = startBroker(this.namesrvAddress, this.controllerAddress, 3, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker3.getMessageStore());
        Thread.sleep(6000);
        checkMessage(broker3.getMessageStore(), 10, 0);

        // Step4: put another batch message
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        putMessage(this.brokerController2.getMessageStore());
        Thread.sleep(2000);
        checkMessage(broker3.getMessageStore(), 20, 0);

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
        checkMessage(broker2MessageStore, 10, 10);

        // Step6, start broker4, link to broker2, it should sync msg from epoch2(offset = 1700).
        BrokerController broker4 = startBroker(this.namesrvAddress, this.controllerAddress, 4, nextPort(), nextPort(), nextPort(), BrokerRole.SLAVE, 1700);
        waitSlaveReady(broker4.getMessageStore());
        Thread.sleep(6000);
        checkMessage(broker4.getMessageStore(), 10, 10);
        shutdown();
    }
    
    public void shutdown() throws InterruptedException {
        for (BrokerController controller : this.brokerList) {
            controller.shutdown();
            System.out.println("Shutdown broker " + controller.getBrokerConfig().getListenPort());
            UtilAll.deleteFile(new File(controller.getMessageStoreConfig().getStorePathRootDir()));
        }
        if (this.namesrvController != null) {
            this.namesrvController.shutdown();
        }
        super.destroy();
    }
    
    public boolean awaitDispatchMs(long timeMs) throws Exception {
        await().atMost(Duration.ofSeconds(timeMs)).until(
            () -> {
                boolean allOk = true;
                for (BrokerController brokerController: brokerControllerList) {
                    if (brokerController.getMessageStore() == null) {
                        allOk = false;
                        break;
                    }
                }
                if (allOk) {
                    return true;
                }
                return false;
            }
        );
        return false;
    }
    
}
