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
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import static org.awaitility.Awaitility.await;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AutoSwitchRoleBase {

    protected static final String STORE_PATH_ROOT_PARENT_DIR = System.getProperty("user.home") + File.separator +
        UUID.randomUUID().toString().replace("-", "");
    private static final String STORE_PATH_ROOT_DIR = STORE_PATH_ROOT_PARENT_DIR + File.separator + "store";
    private static final String STORE_MESSAGE = "Once, there was a chance for me!";
    private static final byte[] MESSAGE_BODY = STORE_MESSAGE.getBytes();
    protected static List<BrokerController> brokerList;
    private static SocketAddress bornHost;
    private static SocketAddress storeHost;
    private static int number = 0;

    protected static void initialize() {
        brokerList = new ArrayList<>();
        try {
            storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
            bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (Exception ignored) {
        }
    }

    public static int nextPort() throws IOException {
        return nextPort(1001, 9999);
    }

    public static int nextPort(int minPort, int maxPort) throws IOException {

        Random random = new Random();
        int tempPort;
        int port;
        while (true) {
            try {
                tempPort = random.nextInt(maxPort) % (maxPort - minPort + 1) + minPort;
                ServerSocket serverSocket = new ServerSocket(tempPort);
                port = serverSocket.getLocalPort();
                serverSocket.close();
                break;
            } catch (IOException ignored) {
                if (number > 200) {
                    throw new IOException("This server's open ports are temporarily full!");
                }
                ++number;
            }
        }
        number = 0;
        return port;
    }

    public BrokerController startBroker(String namesrvAddress, String controllerAddress, String brokerName,
        int brokerId, int haPort,
        int brokerListenPort,
        int nettyListenPort, BrokerRole expectedRole, int mappedFileSize) throws Exception {
        final MessageStoreConfig storeConfig = buildMessageStoreConfig(brokerName + "#" + brokerId, haPort, mappedFileSize);
        storeConfig.setHaMaxTimeSlaveNotCatchup(3 * 1000);
        final BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setListenPort(brokerListenPort);
        brokerConfig.setNamesrvAddr(namesrvAddress);
        brokerConfig.setControllerAddr(controllerAddress);
        brokerConfig.setSyncBrokerMetadataPeriod(2 * 1000);
        brokerConfig.setCheckSyncStateSetPeriod(2 * 1000);
        brokerConfig.setBrokerName(brokerName);
        brokerConfig.setEnableControllerMode(true);

        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(nettyListenPort);

        final BrokerController brokerController = new BrokerController(brokerConfig, nettyServerConfig, new NettyClientConfig(), storeConfig);
        assertTrue(brokerController.initialize());
        brokerController.start();
        brokerList.add(brokerController);
        await().atMost(20, TimeUnit.SECONDS).until(() -> (expectedRole == BrokerRole.SYNC_MASTER) == brokerController.getReplicasManager().isMasterState());
        return brokerController;
    }

    protected MessageStoreConfig buildMessageStoreConfig(final String brokerDir, final int haPort,
        final int mappedFileSize) {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setHaSendHeartbeatInterval(1000);
        storeConfig.setBrokerRole(BrokerRole.SLAVE);
        storeConfig.setHaListenPort(haPort);
        storeConfig.setStorePathRootDir(STORE_PATH_ROOT_DIR + File.separator + brokerDir);
        storeConfig.setStorePathCommitLog(STORE_PATH_ROOT_DIR + File.separator + brokerDir + File.separator + "commitlog");
        storeConfig.setStorePathEpochFile(STORE_PATH_ROOT_DIR + File.separator + brokerDir + File.separator + "EpochFileCache");
        storeConfig.setStorePathBrokerIdentity(STORE_PATH_ROOT_DIR + File.separator + brokerDir + File.separator + "brokerIdentity");
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

    protected static ControllerConfig buildControllerConfig(final String id, final String peers) {
        final ControllerConfig config = new ControllerConfig();
        config.setControllerDLegerGroup("group1");
        config.setControllerDLegerPeers(peers);
        config.setControllerDLegerSelfId(id);
        config.setMappedFileSize(1024 * 1024);
        config.setControllerStorePath(STORE_PATH_ROOT_DIR + File.separator + "namesrv" + id + File.separator + "DLedgerController");
        return config;
    }

    protected MessageExtBrokerInner buildMessage(String topic) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setBody(MESSAGE_BODY);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    protected void putMessage(MessageStore messageStore, String topic) {
        // Put message on master
        for (int i = 0; i < 10; i++) {
            assertSame(messageStore.putMessage(buildMessage(topic)).getPutMessageStatus(), PutMessageStatus.PUT_OK);
        }
    }

    protected void checkMessage(final MessageStore messageStore, String topic, int totalNums, int startOffset) {
        await().atMost(30, TimeUnit.SECONDS)
            .until(() -> {
                GetMessageResult result = messageStore.getMessage("GROUP_A", topic, 0, startOffset, 1024, null);
//                System.out.printf(result + "%n");
//                System.out.printf("maxPhyOffset=" + messageStore.getMaxPhyOffset() + "%n");
//                System.out.printf("confirmOffset=" + messageStore.getConfirmOffset() + "%n");
                return result != null && result.getStatus() == GetMessageStatus.FOUND && result.getMessageCount() >= totalNums;
            });
    }

}
