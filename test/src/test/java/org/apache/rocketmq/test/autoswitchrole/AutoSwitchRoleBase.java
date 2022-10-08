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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleBase {

    private final String storePathRootParentDir = System.getProperty("user.home") + File.separator +
        UUID.randomUUID().toString().replace("-", "");
    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(35000);
    private final String storePathRootDir = storePathRootParentDir + File.separator + "store";
    private final String StoreMessage = "Once, there was a chance for me!";
    private final byte[] MessageBody = StoreMessage.getBytes();
    private final AtomicInteger QueueId = new AtomicInteger(0);
    private static final Random random = new Random();
    protected List<BrokerController> brokerList;
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private static Integer No= 0;
    
    
    protected void initialize() {
        this.brokerList = new ArrayList<>();
        try {
            StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
            BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (Exception ignored) {
        }
    }
    
    public static Integer nextPort() throws IOException {
        return nextPort(1001,9999);
    }
    
    public static Integer nextPort(Integer minPort, Integer maxPort) throws IOException  {
        Random random = new Random();
        int tempPort;
        int port;
        try{
            while (true){
                tempPort = random.nextInt(maxPort)%(maxPort-minPort+1) + minPort;
                ServerSocket serverSocket =  new ServerSocket(tempPort);
                port = serverSocket.getLocalPort();
                serverSocket.close();
                break;
            }
        }catch (Exception ignored){
            if (No>200){
                throw new IOException("This server's open ports are temporarily full!");
            }
            No++;
            port = nextPort(minPort,maxPort);
        }
        No = 0;
        return port;
    }

    public BrokerController startBroker(String namesrvAddress, String controllerAddress, int brokerId, int haPort, int brokerListenPort,
        int nettyListenPort, BrokerRole expectedRole, int mappedFileSize) throws Exception {
        final MessageStoreConfig storeConfig = buildMessageStoreConfig("broker" + brokerId, haPort, mappedFileSize);
        storeConfig.setHaMaxTimeSlaveNotCatchup(3 * 1000);
        final BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setListenPort(brokerListenPort);
        brokerConfig.setNamesrvAddr(namesrvAddress);
        brokerConfig.setControllerAddr(controllerAddress);
        brokerConfig.setSyncBrokerMetadataPeriod(2 * 1000);
        brokerConfig.setCheckSyncStateSetPeriod(2 * 1000);
        brokerConfig.setEnableControllerMode(true);

        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(nettyListenPort);

        final BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(), storeConfig);
        assertTrue(brokerController.initialize());
        brokerController.start();
        this.brokerList.add(brokerController);
        Thread.sleep(1000);
        // The first is master
        if (expectedRole == BrokerRole.SYNC_MASTER) {
            assertTrue(brokerController.getReplicasManager().isMasterState());
        } else {
            assertFalse(brokerController.getReplicasManager().isMasterState());
        }
        return brokerController;
    }

    protected MessageStoreConfig buildMessageStoreConfig(final String brokerName, final int haPort,
        final int mappedFileSize) {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setHaSendHeartbeatInterval(1000);
        storeConfig.setBrokerRole(BrokerRole.SLAVE);
        storeConfig.setHaListenPort(haPort);
        storeConfig.setStorePathRootDir(storePathRootDir + File.separator + brokerName);
        storeConfig.setStorePathCommitLog(storePathRootDir + File.separator + brokerName + File.separator + "commitlog");
        storeConfig.setStorePathEpochFile(storePathRootDir + File.separator + brokerName + File.separator + "EpochFileCache");
        storeConfig.setTotalReplicas(3);
        storeConfig.setInSyncReplicas(2);

        storeConfig.setMappedFileSizeCommitLog(mappedFileSize);
        storeConfig.setMappedFileSizeConsumeQueue(1024 * 1024);
        storeConfig.setMaxHashSlotNum(10000);
        storeConfig.setMaxIndexNum(100 * 100);
        storeConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        storeConfig.setFlushIntervalConsumeQueue(1);
        return storeConfig;
    }

    protected ControllerConfig buildControllerConfig(final String id, final String peers) {
        final ControllerConfig config = new ControllerConfig();
        config.setControllerDLegerGroup("group1");
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(id);
        config.setMappedFileSize(1024 * 1024);
        config.setControllerStorePath(storePathRootDir + File.separator + "namesrv" + id + File.separator + "DLedgerController");
        return config;
    }

    protected MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic("FooBar");
        msg.setTags("TAG1");
        msg.setBody(MessageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        int QUEUE_TOTAL = 1;
        msg.setQueueId(Math.abs(QueueId.getAndIncrement()) % QUEUE_TOTAL);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    protected void putMessage(MessageStore messageStore) throws InterruptedException {
        // Put message on master
        for (int i = 0; i < 10; i++) {
            messageStore.putMessage(buildMessage());
        }
        Thread.sleep(1000);
    }

    protected void checkMessage(final MessageStore messageStore, int totalMsgs, int startOffset) {
        for (long i = 0; i < totalMsgs; i++) {
            GetMessageResult result = messageStore.getMessage("GROUP_A", "FooBar", 0, startOffset + i, 1024 * 1024, null);
            assertThat(result).isNotNull();
            if (!GetMessageStatus.FOUND.equals(result.getStatus())) {
                System.out.println("Failed i :" + i);
            }
            assertEquals(GetMessageStatus.FOUND, result.getStatus());
            result.release();
        }
    }

    protected void destroy() {
        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

}
