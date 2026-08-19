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
package org.apache.rocketmq.test.dledger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.test.base.IntegrationTestBase;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;

import static org.awaitility.Awaitility.await;

public class DLedgerThreeNodeIT {
    private static final long AWAIT_SECONDS = 60;
    private static final List<String> NODE_IDS = Arrays.asList("n0", "n1", "n2");

    @Test
    public void testProduceFailoverAndRestart() throws Exception {
        NamesrvController namesrvController = null;
        DefaultMQAdminExt admin = null;
        List<NodeSpec> nodeSpecs = new ArrayList<>();
        List<BrokerController> allControllers = new ArrayList<>();
        Set<BrokerController> stopped =
            Collections.newSetFromMap(new IdentityHashMap<BrokerController, Boolean>());
        try {
            namesrvController = IntegrationTestBase.createAndStartNamesrv();
            String namesrvAddr = "127.0.0.1:"
                + namesrvController.getNettyServerConfig().getListenPort();
            admin = new DefaultMQAdminExt();
            admin.setInstanceName(UUID.randomUUID().toString());
            admin.setNamesrvAddr(namesrvAddr);
            admin.start();

            String clusterName = "DLedgerCluster-" + UUID.randomUUID();
            String brokerName = "DLedgerBroker-" + UUID.randomUUID();
            String topic = "DLedgerTopic-" + UUID.randomUUID();
            ClusterPorts clusterPorts = allocateClusterPorts(NODE_IDS.size());
            String peers = buildPeers(clusterPorts.dLedgerPorts);
            for (int i = 0; i < NODE_IDS.size(); i++) {
                nodeSpecs.add(new NodeSpec(
                    NODE_IDS.get(i), clusterPorts.dLedgerPorts.get(i),
                    clusterPorts.brokerPorts.get(i),
                    IntegrationTestBase.createBaseDir()));
            }

            List<BrokerController> active = startCluster(
                nodeSpecs, clusterName, brokerName, namesrvAddr, peers, allControllers);
            BrokerController initialLeader = awaitLeader(active);
            awaitClusterMaster(admin, brokerName, initialLeader);
            Assert.assertTrue(IntegrationTestBase.initTopic(
                topic, namesrvAddr, clusterName, 1, CQType.SimpleCQ));
            awaitTopicRouteMaster(admin, topic, brokerName, initialLeader);
            awaitTopicOnEveryNode(active, topic);

            List<String> expectedBodies = new ArrayList<>();
            expectedBodies.add("before-single");
            expectedBodies.add("before-batch-0");
            expectedBodies.add("before-batch-1");
            expectedBodies.add("before-batch-2");
            sendInitialSingleAndBatch(namesrvAddr, topic, brokerName);
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);

            stopController(initialLeader, stopped);
            active.remove(initialLeader);
            BrokerController failoverLeader = awaitLeader(active);
            Assert.assertNotSame(initialLeader, failoverLeader);
            awaitClusterMaster(admin, brokerName, failoverLeader);
            awaitTopicRouteMaster(admin, topic, brokerName, failoverLeader);
            sendOne(namesrvAddr, topic, brokerName,
                "after-failover", expectedBodies.size());
            expectedBodies.add("after-failover");
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);

            stopControllers(active, stopped);
            awaitBrokerRegistrationRemoved(admin, brokerName);
            awaitAllNodePortsAvailable(nodeSpecs);
            active = startCluster(
                nodeSpecs, clusterName, brokerName, namesrvAddr, peers, allControllers);
            BrokerController restartedLeader = awaitLeader(active);
            awaitClusterMaster(admin, brokerName, restartedLeader);
            awaitTopicRouteMaster(admin, topic, brokerName, restartedLeader);
            awaitTopicOnEveryNode(active, topic);
            awaitQueueOffset(active, topic, expectedBodies.size());

            // This pull is deliberately before the first post-restart user append.
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);
            sendOne(namesrvAddr, topic, brokerName,
                "after-restart", expectedBodies.size());
            expectedBodies.add("after-restart");
            awaitQueueOffset(active, topic, expectedBodies.size());
            assertBodies(pullExactly(
                namesrvAddr, topic, brokerName, expectedBodies.size()), expectedBodies);
        } finally {
            stopControllers(allControllers, stopped);
            if (admin != null) {
                admin.shutdown();
            }
            if (namesrvController != null) {
                namesrvController.shutdown();
            }
            for (NodeSpec nodeSpec : nodeSpecs) {
                UtilAll.deleteFile(new File(nodeSpec.storeRoot));
            }
        }
    }

    private static List<BrokerController> startCluster(List<NodeSpec> nodeSpecs,
        String clusterName, String brokerName, String namesrvAddr, String peers,
        List<BrokerController> allControllers) throws Exception {
        List<BrokerController> controllers = new ArrayList<>();
        for (NodeSpec nodeSpec : nodeSpecs) {
            BrokerController controller = startNode(
                nodeSpec, clusterName, brokerName, namesrvAddr, peers);
            controllers.add(controller);
            allControllers.add(controller);
        }
        return controllers;
    }

    private static BrokerController startNode(NodeSpec nodeSpec, String clusterName,
        String brokerName, String namesrvAddr, String peers) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerClusterName(clusterName);
        brokerConfig.setBrokerName(brokerName);
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setBrokerIP2("127.0.0.1");
        brokerConfig.setNamesrvAddr(namesrvAddr);
        brokerConfig.setRegisterNameServerPeriod(1000);
        brokerConfig.setLoadBalancePollNameServerInterval(500);

        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(nodeSpec.storeRoot);
        storeConfig.setStorePathCommitLog(
            nodeSpec.storeRoot + File.separator + "commitlog");
        storeConfig.setStorePathDLedgerCommitLog(
            nodeSpec.storeRoot + File.separator + "dledger");
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024);
        storeConfig.setMaxHashSlotNum(10_000);
        storeConfig.setMaxIndexNum(10_000);
        storeConfig.setHaListenPort(0);
        storeConfig.setEnableDLegerCommitLog(true);
        storeConfig.setdLegerGroup(brokerName);
        storeConfig.setdLegerSelfId(nodeSpec.selfId);
        storeConfig.setdLegerPeers(peers);
        storeConfig.setEnableBatchPush(true);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(nodeSpec.brokerPort);
        BrokerController controller = new BrokerController(
            brokerConfig, serverConfig, new NettyClientConfig(), storeConfig);
        try {
            Assert.assertTrue(controller.initialize());
            controller.start();
            return controller;
        } catch (Throwable t) {
            try {
                controller.shutdown();
            } catch (Throwable ignored) {
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new RuntimeException(t);
        }
    }

    private static BrokerController awaitLeader(List<BrokerController> controllers) {
        AtomicReference<BrokerController> result = new AtomicReference<>();
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                BrokerController leader = findLeader(controllers);
                if (leader == null) {
                    return false;
                }
                result.set(leader);
                return true;
            });
        return result.get();
    }

    private static BrokerController findLeader(List<BrokerController> controllers) {
        BrokerController result = null;
        for (BrokerController controller : controllers) {
            DLedgerCommitLog commitLog =
                (DLedgerCommitLog) controller.getMessageStore().getCommitLog();
            boolean dLedgerLeader =
                commitLog.getdLedgerServer().getMemberState().isLeader();
            boolean brokerMaster = controller.getMessageStoreConfig().getBrokerRole()
                == BrokerRole.SYNC_MASTER;
            boolean brokerIdIsMaster =
                controller.getBrokerConfig().getBrokerId() == MixAll.MASTER_ID;
            if (dLedgerLeader && brokerMaster && brokerIdIsMaster) {
                if (result != null) {
                    return null;
                }
                result = controller;
            }
        }
        return result;
    }

    private static void awaitClusterMaster(DefaultMQAdminExt admin, String brokerName,
        BrokerController expectedLeader) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    ClusterInfo clusterInfo = admin.examineBrokerClusterInfo();
                    BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                    if (brokerData == null) {
                        return false;
                    }
                    return expectedLeader.getBrokerAddr().equals(
                        brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
                } catch (Exception ignored) {
                    return false;
                }
            });
    }

    private static void awaitBrokerRegistrationRemoved(
        DefaultMQAdminExt admin, String brokerName) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> !admin.examineBrokerClusterInfo()
                .getBrokerAddrTable().containsKey(brokerName));
    }

    private static void awaitTopicRouteMaster(DefaultMQAdminExt admin, String topic,
        String brokerName, BrokerController expectedLeader) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    TopicRouteData route = admin.examineTopicRouteInfo(topic);
                    for (BrokerData brokerData : route.getBrokerDatas()) {
                        if (brokerName.equals(brokerData.getBrokerName())) {
                            return expectedLeader.getBrokerAddr().equals(
                                brokerData.getBrokerAddrs().get(MixAll.MASTER_ID));
                        }
                    }
                } catch (Exception ignored) {
                }
                return false;
            });
    }

    private static void awaitTopicOnEveryNode(
        List<BrokerController> controllers, String topic) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                for (BrokerController controller : controllers) {
                    if (controller.getTopicConfigManager().selectTopicConfig(topic) == null) {
                        return false;
                    }
                }
                return true;
            });
    }

    private static void awaitQueueOffset(List<BrokerController> controllers,
        String topic, long expectedOffset) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                for (BrokerController controller : controllers) {
                    if (controller.getMessageStore().getMaxOffsetInQueue(topic, 0)
                        != expectedOffset
                        || controller.getMessageStore().dispatchBehindBytes() != 0) {
                        return false;
                    }
                }
                return true;
            });
    }

    private static void sendInitialSingleAndBatch(
        String namesrvAddr, String topic, String brokerName) throws Exception {
        DefaultMQProducer producer = startProducer(namesrvAddr);
        try {
            MessageQueue queue = awaitPublishQueue(producer, topic, brokerName);
            SendResult singleResult = producer.send(new Message(
                topic, "before-single".getBytes(StandardCharsets.UTF_8)), queue);
            assertSendResult(singleResult, brokerName, 0);
            Assert.assertNotNull(singleResult.getOffsetMsgId());

            List<Message> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                batch.add(new Message(topic,
                    ("before-batch-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            SendResult batchResult = producer.send(batch, queue);
            assertSendResult(batchResult, brokerName, 1);
            Assert.assertEquals(3, batchResult.getMsgId().split(",").length);
        } finally {
            producer.shutdown();
        }
    }

    private static void sendOne(String namesrvAddr, String topic, String brokerName,
        String body, long expectedQueueOffset) throws Exception {
        DefaultMQProducer producer = startProducer(namesrvAddr);
        try {
            MessageQueue queue = awaitPublishQueue(producer, topic, brokerName);
            SendResult result = producer.send(
                new Message(topic, body.getBytes(StandardCharsets.UTF_8)), queue);
            assertSendResult(result, brokerName, expectedQueueOffset);
            Assert.assertNotNull(result.getOffsetMsgId());
        } finally {
            producer.shutdown();
        }
    }

    private static DefaultMQProducer startProducer(String namesrvAddr)
        throws Exception {
        DefaultMQProducer producer =
            new DefaultMQProducer("dledger-it-" + UUID.randomUUID());
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(namesrvAddr);
        producer.setPollNameServerInterval(500);
        producer.setSendMsgTimeout(10_000);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setVipChannelEnabled(false);
        producer.start();
        return producer;
    }

    private static MessageQueue awaitPublishQueue(DefaultMQProducer producer,
        String topic, String brokerName) {
        AtomicReference<MessageQueue> result = new AtomicReference<>();
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                try {
                    MessageQueue queue = selectQueue(
                        producer.fetchPublishMessageQueues(topic), brokerName);
                    if (queue == null) {
                        return false;
                    }
                    result.set(queue);
                    return true;
                } catch (Exception ignored) {
                    return false;
                }
            });
        return result.get();
    }

    private static List<MessageExt> pullExactly(String namesrvAddr, String topic,
        String brokerName, int expectedCount) throws Exception {
        DefaultMQPullConsumer consumer =
            new DefaultMQPullConsumer("dledger-it-" + UUID.randomUUID());
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setPollNameServerInterval(500);
        consumer.setConsumerPullTimeoutMillis(3_000);
        consumer.setVipChannelEnabled(false);
        consumer.start();
        AtomicReference<PullResult> resultRef = new AtomicReference<>();
        try {
            await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
                    try {
                        MessageQueue queue = selectQueue(
                            consumer.fetchSubscribeMessageQueues(topic), brokerName);
                        if (queue == null) {
                            return false;
                        }
                        PullResult result = consumer.pull(
                            queue, "*", 0, Math.max(32, expectedCount));
                        if (result.getPullStatus() != PullStatus.FOUND
                            || result.getMinOffset() != 0
                            || result.getMaxOffset() != expectedCount
                            || result.getMsgFoundList().size() != expectedCount) {
                            return false;
                        }
                        resultRef.set(result);
                        return true;
                    } catch (Exception ignored) {
                        return false;
                    }
                });
            return new ArrayList<>(resultRef.get().getMsgFoundList());
        } finally {
            consumer.shutdown();
        }
    }

    private static MessageQueue selectQueue(
        Iterable<MessageQueue> queues, String brokerName) {
        for (MessageQueue queue : queues) {
            if (brokerName.equals(queue.getBrokerName()) && queue.getQueueId() == 0) {
                return queue;
            }
        }
        return null;
    }

    private static void assertSendResult(
        SendResult result, String brokerName, long expectedQueueOffset) {
        Assert.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
        Assert.assertEquals(brokerName, result.getMessageQueue().getBrokerName());
        Assert.assertEquals(0, result.getMessageQueue().getQueueId());
        Assert.assertEquals(expectedQueueOffset, result.getQueueOffset());
        Assert.assertNotNull(result.getMsgId());
    }

    private static void assertBodies(
        List<MessageExt> messages, List<String> expectedBodies) {
        Assert.assertEquals(expectedBodies.size(), messages.size());
        for (int i = 0; i < expectedBodies.size(); i++) {
            MessageExt message = messages.get(i);
            Assert.assertEquals(i, message.getQueueOffset());
            Assert.assertArrayEquals(
                expectedBodies.get(i).getBytes(StandardCharsets.UTF_8), message.getBody());
        }
    }

    private static void stopControllers(List<BrokerController> controllers,
        Set<BrokerController> stopped) {
        for (BrokerController controller : new ArrayList<>(controllers)) {
            stopController(controller, stopped);
        }
    }

    private static void stopController(BrokerController controller,
        Set<BrokerController> stopped) {
        if (controller == null || !stopped.add(controller)) {
            return;
        }
        try {
            controller.shutdown();
        } catch (Throwable ignored) {
        }
    }

    private static ClusterPorts allocateClusterPorts(int count) throws IOException {
        List<ServerSocket> reservations = new ArrayList<>();
        List<Integer> dLedgerPorts = new ArrayList<>();
        List<Integer> brokerPorts = new ArrayList<>();
        try {
            InetAddress loopback = InetAddress.getByName("127.0.0.1");
            while (dLedgerPorts.size() < count) {
                ServerSocket socket = new ServerSocket(0, 50, loopback);
                if (socket.getLocalPort() <= 1024) {
                    socket.close();
                    continue;
                }
                reservations.add(socket);
                dLedgerPorts.add(socket.getLocalPort());
            }
            int attempts = 0;
            while (brokerPorts.size() < count) {
                if (++attempts > 1000) {
                    throw new IOException("Unable to reserve broker and VIP port pairs");
                }
                ServerSocket brokerSocket = new ServerSocket(0, 50, loopback);
                int brokerPort = brokerSocket.getLocalPort();
                int fastPort = brokerPort - 2;
                if (fastPort <= 1024) {
                    brokerSocket.close();
                    continue;
                }
                try {
                    ServerSocket fastSocket = new ServerSocket(fastPort, 50, loopback);
                    reservations.add(brokerSocket);
                    reservations.add(fastSocket);
                    brokerPorts.add(brokerPort);
                } catch (IOException ignored) {
                    brokerSocket.close();
                }
            }
            return new ClusterPorts(dLedgerPorts, brokerPorts);
        } finally {
            closeSockets(reservations);
        }
    }

    private static String buildPeers(List<Integer> ports) {
        StringBuilder peers = new StringBuilder();
        for (int i = 0; i < NODE_IDS.size(); i++) {
            if (i > 0) {
                peers.append(';');
            }
            peers.append(NODE_IDS.get(i)).append("-127.0.0.1:").append(ports.get(i));
        }
        return peers.toString();
    }

    private static void awaitAllNodePortsAvailable(List<NodeSpec> nodeSpecs) {
        await().atMost(AWAIT_SECONDS, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> areAllNodePortsAvailable(nodeSpecs));
    }

    private static boolean areAllNodePortsAvailable(List<NodeSpec> nodeSpecs) {
        List<ServerSocket> probes = new ArrayList<>();
        try {
            InetAddress loopback = InetAddress.getByName("127.0.0.1");
            for (NodeSpec nodeSpec : nodeSpecs) {
                probes.add(new ServerSocket(nodeSpec.dLedgerPort, 50, loopback));
                probes.add(new ServerSocket(nodeSpec.brokerPort, 50, loopback));
                probes.add(new ServerSocket(nodeSpec.brokerPort - 2, 50, loopback));
            }
            return true;
        } catch (IOException ignored) {
            return false;
        } finally {
            closeSockets(probes);
        }
    }

    private static void closeSockets(List<ServerSocket> sockets) {
        for (ServerSocket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static final class NodeSpec {
        private final String selfId;
        private final int dLedgerPort;
        private final int brokerPort;
        private final String storeRoot;

        private NodeSpec(String selfId, int dLedgerPort, int brokerPort,
            String storeRoot) {
            this.selfId = selfId;
            this.dLedgerPort = dLedgerPort;
            this.brokerPort = brokerPort;
            this.storeRoot = storeRoot;
        }
    }

    private static final class ClusterPorts {
        private final List<Integer> dLedgerPorts;
        private final List<Integer> brokerPorts;

        private ClusterPorts(
            List<Integer> dLedgerPorts, List<Integer> brokerPorts) {
            this.dLedgerPorts = dLedgerPorts;
            this.brokerPorts = brokerPorts;
        }
    }
}
