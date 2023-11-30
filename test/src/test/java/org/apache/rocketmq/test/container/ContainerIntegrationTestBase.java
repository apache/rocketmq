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

package org.apache.rocketmq.test.container;

import io.netty.channel.ChannelHandlerContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.container.BrokerContainer;
import org.apache.rocketmq.container.BrokerContainerConfig;
import org.apache.rocketmq.container.InnerSalveBrokerController;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.BeforeClass;

import static org.awaitility.Awaitility.await;

/**
 * ContainerIntegrationTestBase will setup a rocketmq ha cluster contains two broker group:
 * <li>BrokerA contains two replicas</li>
 * <li>BrokerB contains three replicas</li>
 */
public class ContainerIntegrationTestBase {
    private static final AtomicBoolean CLUSTER_SET_UP = new AtomicBoolean(false);
    private static final List<File> TMP_FILE_LIST = new ArrayList<>();
    private static final Random RANDOM = new Random();
    protected static String nsAddr;

    protected static final String THREE_REPLICAS_TOPIC = "SEND_MESSAGE_TEST_TOPIC_THREE_REPLICAS";

    protected static List<BrokerContainer> brokerContainerList = new ArrayList<>();
    protected static List<NamesrvController> namesrvControllers = new ArrayList<>();

    protected static final String BROKER_NAME_PREFIX = "TestBrokerName_";
    protected static final int COMMIT_LOG_SIZE = 128 * 1024;
    protected static final int INDEX_NUM = 1000;
    protected static final AtomicInteger BROKER_INDEX = new AtomicInteger(0);

    protected static BrokerContainer brokerContainer1;
    protected static BrokerContainer brokerContainer2;
    protected static BrokerContainer brokerContainer3;
    protected static BrokerController master1With3Replicas;
    protected static BrokerController master2With3Replicas;
    protected static BrokerController master3With3Replicas;
    protected static NamesrvController namesrvController;

    protected static DefaultMQAdminExt defaultMQAdminExt;

    private final static Logger LOG = LoggerFactory.getLogger(ContainerIntegrationTestBase.class);
    private static ConcurrentMap<BrokerConfig, MessageStoreConfig> slaveStoreConfigCache = new ConcurrentHashMap<>();

    protected static ConcurrentMap<BrokerConfigLite, BrokerController> isolatedBrokers = new ConcurrentHashMap<>();
    private static final Set<Integer> PORTS_IN_USE = new HashSet<>();

    @BeforeClass
    public static void setUp() throws Exception {
        if (CLUSTER_SET_UP.compareAndSet(false, true)) {
            System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
            System.setProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.99");
            System.setProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.99");

            setUpCluster();
            setUpTopic();
            registerCleaner();

            System.out.printf("cluster setup complete%n");
        }
    }

    private static void setUpTopic() {
        createTopic(THREE_REPLICAS_TOPIC);
    }

    private static void createTopic(String topic) {
        createTopicTo(master1With3Replicas, topic);
        createTopicTo(master2With3Replicas, topic);
        createTopicTo(master3With3Replicas, topic);
    }

    private static void setUpCluster() throws Exception {
        namesrvController = createAndStartNamesrv();
        nsAddr = "127.0.0.1:" + namesrvController.getNettyServerConfig().getListenPort();
        System.out.printf("namesrv addr: %s%n", nsAddr);

        /*
         *     BrokerContainer1      |      BrokerContainer2      |      BrokerContainer3
         *
         *   master1With3Replicas(m)      master2With3Replicas(m)      master3With3Replicas(m)
         *   master3With3Replicas(s0)     master1With3Replicas(s0)     master2With3Replicas(s0)
         *   master2With3Replicas(s1)     master3With3Replicas(s1)     master1With3Replicas(s1)
         */

        brokerContainer1 = createAndStartBrokerContainer(nsAddr);
        brokerContainer2 = createAndStartBrokerContainer(nsAddr);
        brokerContainer3 = createAndStartBrokerContainer(nsAddr);
        // Create three broker groups, two contains two replicas, another contains three replicas
        master1With3Replicas = createAndAddMaster(brokerContainer1, new BrokerGroupConfig(), BROKER_INDEX.getAndIncrement());
        master2With3Replicas = createAndAddMaster(brokerContainer2, new BrokerGroupConfig(), BROKER_INDEX.getAndIncrement());
        master3With3Replicas = createAndAddMaster(brokerContainer3, new BrokerGroupConfig(), BROKER_INDEX.getAndIncrement());

        createAndAddSlave(1, brokerContainer1, master3With3Replicas);
        createAndAddSlave(1, brokerContainer2, master1With3Replicas);
        createAndAddSlave(1, brokerContainer3, master2With3Replicas);

        createAndAddSlave(2, brokerContainer1, master2With3Replicas);
        createAndAddSlave(2, brokerContainer2, master3With3Replicas);
        createAndAddSlave(2, brokerContainer3, master1With3Replicas);

        awaitUntilSlaveOK();

        defaultMQAdminExt = new DefaultMQAdminExt("HATest_Admin_Group");
        defaultMQAdminExt.setNamesrvAddr(nsAddr);
        defaultMQAdminExt.start();
    }

    protected static void createTopicTo(BrokerController masterBroker, String topicName, int rqn, int wqn) {
        try {
            TopicConfig topicConfig = new TopicConfig(topicName, rqn, wqn, 6, 0);
            defaultMQAdminExt.createAndUpdateTopicConfig(masterBroker.getBrokerAddr(), topicConfig);
            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer1);
            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer2);
            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer3);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create topic to broker failed", e);
        }
    }

    protected static void createGroup(BrokerController masterBroker, String groupName) {
        try {
            SubscriptionGroupConfig config = new SubscriptionGroupConfig();
            config.setGroupName(groupName);

            masterBroker.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);

            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer1);
            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer2);
            triggerSlaveSync(masterBroker.getBrokerConfig().getBrokerName(), brokerContainer3);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Create group to broker failed", e);
        }
    }

    private static void triggerSlaveSync(String brokerName, BrokerContainer brokerContainer) {
        for (InnerSalveBrokerController slaveBroker : brokerContainer.getSlaveBrokers()) {
            if (slaveBroker.getBrokerConfig().getBrokerName().equals(brokerName)) {
                slaveBroker.getSlaveSynchronize().syncAll();
                slaveBroker.registerBrokerAll(true, false, true);
            }
        }
    }

    protected static void createTopicTo(BrokerController brokerController, String topicName) {
        createTopicTo(brokerController, topicName, 8, 8);
    }

    private static void registerCleaner() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (CLUSTER_SET_UP.compareAndSet(true, false)) {
                System.out.printf("clean up%n");
                defaultMQAdminExt.shutdown();

                for (final BrokerContainer brokerContainer : brokerContainerList) {
                    brokerContainer.shutdown();
                    for (BrokerController brokerController : brokerContainer.getBrokerControllers()) {
                        brokerController.getMessageStore().destroy();
                    }
                }

                for (final NamesrvController namesrvController : namesrvControllers) {
                    namesrvController.shutdown();
                }

                for (final File file : TMP_FILE_LIST) {
                    UtilAll.deleteFile(file);
                }
            }
        }));
    }

    private static File createBaseDir(String prefix) {
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
            TMP_FILE_LIST.add(file);
            System.out.printf("create file at %s%n", file.getAbsolutePath());
            return file;
        } catch (IOException e) {
            throw new RuntimeException("Couldn't create tmp folder", e);
        }
    }

    public static NamesrvController createAndStartNamesrv() {
        String baseDir = createBaseDir("test-cluster-namesrv").getAbsolutePath();
        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nameServerNettyServerConfig = new NettyServerConfig();
        namesrvConfig.setKvConfigPath(baseDir + File.separator + "namesrv" + File.separator + "kvConfig.json");
        namesrvConfig.setConfigStorePath(baseDir + File.separator + "namesrv" + File.separator + "namesrv.properties");
        namesrvConfig.setSupportActingMaster(true);
        namesrvConfig.setScanNotActiveBrokerInterval(1000);

        nameServerNettyServerConfig.setListenPort(generatePort(10000, 10000));
        NamesrvController namesrvController = new NamesrvController(namesrvConfig, nameServerNettyServerConfig);
        try {
            Assert.assertTrue(namesrvController.initialize());
            LOG.info("Name Server Start:{}", nameServerNettyServerConfig.getListenPort());
            namesrvController.start();
        } catch (Exception e) {
            LOG.info("Name Server start failed");
            e.printStackTrace();
            System.exit(1);
        }

        namesrvController.getRemotingServer().registerProcessor(RequestCode.REGISTER_BROKER, new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(final ChannelHandlerContext ctx,
                final RemotingCommand request) throws Exception {
                final RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
                final BrokerConfigLite liteConfig = new BrokerConfigLite(requestHeader.getClusterName(),
                    requestHeader.getBrokerName(),
                    requestHeader.getBrokerAddr(),
                    requestHeader.getBrokerId());
                if (isolatedBrokers.containsKey(liteConfig)) {
                    // return response with SYSTEM_ERROR
                    return RemotingCommand.createResponseCommand(null);
                }
                return namesrvController.getRemotingServer().getDefaultProcessorPair().getObject1().processRequest(ctx, request);
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        }, null);

        namesrvControllers.add(namesrvController);
        return namesrvController;

    }

    public static BrokerContainer createAndStartBrokerContainer(String nsAddr) {
        BrokerContainerConfig brokerContainerConfig = new BrokerContainerConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        brokerContainerConfig.setNamesrvAddr(nsAddr);

        nettyServerConfig.setListenPort(generatePort(20000, 10000));
        BrokerContainer brokerContainer = new BrokerContainer(brokerContainerConfig, nettyServerConfig, nettyClientConfig);
        try {
            Assert.assertTrue(brokerContainer.initialize());
            LOG.info("Broker container Start, listen on {}.", nettyServerConfig.getListenPort());
            brokerContainer.start();
        } catch (Exception e) {
            LOG.info("Broker container start failed", e);
            e.printStackTrace();
            System.exit(1);
        }
        brokerContainerList.add(brokerContainer);
        return brokerContainer;
    }

    private static int generatePort(int base, int range) {
        int result = base + RANDOM.nextInt(range);
        while (PORTS_IN_USE.contains(result) || PORTS_IN_USE.contains(result - 2)) {
            result = base + RANDOM.nextInt(range);
        }
        PORTS_IN_USE.add(result);
        PORTS_IN_USE.add(result - 2);
        return result;
    }

    public static BrokerController createAndAddMaster(BrokerContainer brokerContainer,
        BrokerGroupConfig brokerGroupConfig, int brokerIndex) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        brokerConfig.setBrokerName(BROKER_NAME_PREFIX + brokerIndex);
        brokerConfig.setBrokerIP1("127.0.0.1");
        brokerConfig.setBrokerIP2("127.0.0.1");
        brokerConfig.setBrokerId(0);
        brokerConfig.setEnablePropertyFilter(true);
        brokerConfig.setEnableSlaveActingMaster(brokerGroupConfig.enableSlaveActingMaster);
        brokerConfig.setEnableRemoteEscape(brokerGroupConfig.enableRemoteEscape);
        brokerConfig.setSlaveReadEnable(brokerGroupConfig.slaveReadEnable);
        brokerConfig.setLockInStrictMode(true);
        brokerConfig.setConsumerOffsetUpdateVersionStep(10);
        brokerConfig.setDelayOffsetUpdateVersionStep(10);
        brokerConfig.setCompatibleWithOldNameSrv(false);
        brokerConfig.setListenPort(generatePort(brokerContainer.getRemotingServer().localListenPort(), 10000));

        String baseDir = createBaseDir(brokerConfig.getBrokerName() + "_" + brokerConfig.getBrokerId()).getAbsolutePath();
        storeConfig.setStorePathRootDir(baseDir);
        storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
        storeConfig.setHaListenPort(generatePort(30000, 10000));
        storeConfig.setMappedFileSizeCommitLog(COMMIT_LOG_SIZE);
        storeConfig.setMaxIndexNum(INDEX_NUM);
        storeConfig.setMaxHashSlotNum(INDEX_NUM * 4);
        storeConfig.setTotalReplicas(brokerGroupConfig.totalReplicas);
        storeConfig.setInSyncReplicas(brokerGroupConfig.inSyncReplicas);
        storeConfig.setMinInSyncReplicas(brokerGroupConfig.minReplicas);
        storeConfig.setEnableAutoInSyncReplicas(brokerGroupConfig.autoReplicas);
        storeConfig.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig.setSyncFlushTimeout(10 * 1000);

        System.out.printf("start master %s with port %d-%d%n", brokerConfig.getCanonicalName(), brokerConfig.getListenPort(), storeConfig.getHaListenPort());
        BrokerController brokerController = null;
        try {
            brokerController = brokerContainer.addBroker(brokerConfig, storeConfig);
            Assert.assertNotNull(brokerController);
            brokerController.start();
            TMP_FILE_LIST.add(new File(brokerController.getTopicConfigManager().configFilePath()));
            TMP_FILE_LIST.add(new File(brokerController.getSubscriptionGroupManager().configFilePath()));
            LOG.info("Broker Start name:{} addr:{}", brokerConfig.getBrokerName(), brokerController.getBrokerAddr());
        } catch (Exception e) {
            LOG.info("Broker start failed", e);
            e.printStackTrace();
            System.exit(1);
        }

        return brokerController;
    }

    protected static DefaultMQProducer createProducer(String producerGroup) {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(nsAddr);
        return producer;
    }

    protected static TransactionMQProducer createTransactionProducer(String producerGroup,
        TransactionCheckListener transactionCheckListener) {
        TransactionMQProducer producer = new TransactionMQProducer(producerGroup);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(nsAddr);
        producer.setTransactionCheckListener(transactionCheckListener);
        return producer;
    }

    protected static TransactionMQProducer createTransactionProducer(String producerGroup,
        TransactionListener transactionListener) {
        TransactionMQProducer producer = new TransactionMQProducer(producerGroup);
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setNamesrvAddr(nsAddr);
        producer.setTransactionListener(transactionListener);
        return producer;
    }

    protected static DefaultMQPullConsumer createPullConsumer(String consumerGroup) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(consumerGroup);
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setNamesrvAddr(nsAddr);
        return consumer;
    }

    protected static DefaultMQPushConsumer createPushConsumer(String consumerGroup) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setNamesrvAddr(nsAddr);
        return consumer;
    }

    protected static void createAndAddSlave(int slaveBrokerId, BrokerContainer brokerContainer,
        BrokerController master) {
        BrokerConfig slaveBrokerConfig = new BrokerConfig();
        slaveBrokerConfig.setBrokerName(master.getBrokerConfig().getBrokerName());
        slaveBrokerConfig.setBrokerId(slaveBrokerId);
        slaveBrokerConfig.setBrokerClusterName(master.getBrokerConfig().getBrokerClusterName());
        slaveBrokerConfig.setCompatibleWithOldNameSrv(false);
        slaveBrokerConfig.setBrokerIP1("127.0.0.1");
        slaveBrokerConfig.setBrokerIP2("127.0.0.1");
        slaveBrokerConfig.setEnablePropertyFilter(true);
        slaveBrokerConfig.setSlaveReadEnable(true);
        slaveBrokerConfig.setEnableSlaveActingMaster(true);
        slaveBrokerConfig.setEnableRemoteEscape(true);
        slaveBrokerConfig.setLockInStrictMode(true);
        slaveBrokerConfig.setListenPort(generatePort(brokerContainer.getRemotingServer().localListenPort(), 10000));
        slaveBrokerConfig.setConsumerOffsetUpdateVersionStep(10);
        slaveBrokerConfig.setDelayOffsetUpdateVersionStep(10);

        MessageStoreConfig storeConfig = slaveStoreConfigCache.get(slaveBrokerConfig);

        if (storeConfig == null) {
            storeConfig = new MessageStoreConfig();
            String baseDir = createBaseDir(slaveBrokerConfig.getBrokerName() + "_" + slaveBrokerConfig.getBrokerId()).getAbsolutePath();
            storeConfig.setStorePathRootDir(baseDir);
            storeConfig.setStorePathCommitLog(baseDir + File.separator + "commitlog");
            storeConfig.setHaListenPort(generatePort(master.getMessageStoreConfig().getHaListenPort(), 10000));
            storeConfig.setMappedFileSizeCommitLog(COMMIT_LOG_SIZE);
            storeConfig.setMaxIndexNum(INDEX_NUM);
            storeConfig.setMaxHashSlotNum(INDEX_NUM * 4);
            storeConfig.setTotalReplicas(master.getMessageStoreConfig().getTotalReplicas());
            storeConfig.setInSyncReplicas(master.getMessageStoreConfig().getInSyncReplicas());
            storeConfig.setMinInSyncReplicas(master.getMessageStoreConfig().getMinInSyncReplicas());
            storeConfig.setBrokerRole(BrokerRole.SLAVE);
            slaveStoreConfigCache.put(slaveBrokerConfig, storeConfig);
        }

        System.out.printf("start slave %s with port %d-%d%n", slaveBrokerConfig.getCanonicalName(), slaveBrokerConfig.getListenPort(), storeConfig.getHaListenPort());

        try {
            BrokerController brokerController = brokerContainer.addBroker(slaveBrokerConfig, storeConfig);
            Assert.assertNotNull(brokerContainer);
            brokerController.start();
            TMP_FILE_LIST.add(new File(brokerController.getTopicConfigManager().configFilePath()));
            TMP_FILE_LIST.add(new File(brokerController.getSubscriptionGroupManager().configFilePath()));
            LOG.info("Add slave name:{} addr:{}", slaveBrokerConfig.getBrokerName(), brokerController.getBrokerAddr());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Couldn't add slave broker", e);
        }
    }

    protected static void removeSlaveBroker(int slaveBrokerId, BrokerContainer brokerContainer,
        BrokerController master) throws Exception {
        BrokerIdentity brokerIdentity = new BrokerIdentity(master.getBrokerConfig().getBrokerClusterName(),
            master.getBrokerConfig().getBrokerName(), slaveBrokerId);

        brokerContainer.removeBroker(brokerIdentity);
    }

    protected static void awaitUntilSlaveOK() {
        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> {
                boolean isOk = master1With3Replicas.getMessageStore().getHaService().getConnectionCount().get() == 2
                    && master1With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3;
                for (HAConnection haConnection : master1With3Replicas.getMessageStore().getHaService().getConnectionList()) {
                    isOk &= haConnection.getCurrentState().equals(HAConnectionState.TRANSFER);
                }
                return isOk;
            });

        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> {
                boolean isOk = master2With3Replicas.getMessageStore().getHaService().getConnectionCount().get() == 2
                    && master2With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3;
                for (HAConnection haConnection : master2With3Replicas.getMessageStore().getHaService().getConnectionList()) {
                    isOk &= haConnection.getCurrentState().equals(HAConnectionState.TRANSFER);
                }
                return isOk;
            });

        await().atMost(100, TimeUnit.SECONDS)
            .until(() -> {
                boolean isOk = master3With3Replicas.getMessageStore().getHaService().getConnectionCount().get() == 2
                    && master3With3Replicas.getMessageStore().getAliveReplicaNumInGroup() == 3;
                for (HAConnection haConnection : master3With3Replicas.getMessageStore().getHaService().getConnectionList()) {
                    isOk &= haConnection.getCurrentState().equals(HAConnectionState.TRANSFER);
                }
                return isOk;
            });

        try {
            Awaitility.await().pollDelay(Duration.ofMillis(2000)).until(()->true);
        } catch (ConditionTimeoutException e) {
            e.printStackTrace();
        }
    }

    protected static void isolateBroker(BrokerController brokerController) {
        final BrokerConfig config = brokerController.getBrokerConfig();

        BrokerConfigLite liteConfig = new BrokerConfigLite(config.getBrokerClusterName(),
            config.getBrokerName(),
            brokerController.getBrokerAddr(),
            config.getBrokerId());

        // Reject register requests from the specific broker
        isolatedBrokers.putIfAbsent(liteConfig, brokerController);

        // UnRegister the specific broker immediately
        namesrvController.getRouteInfoManager().unregisterBroker(liteConfig.getClusterName(),
            liteConfig.getBrokerAddr(),
            liteConfig.getBrokerName(),
            liteConfig.getBrokerId());
    }

    protected static void cancelIsolatedBroker(BrokerController brokerController) {
        final BrokerConfig config = brokerController.getBrokerConfig();

        BrokerConfigLite liteConfig = new BrokerConfigLite(config.getBrokerClusterName(),
            config.getBrokerName(),
            brokerController.getBrokerAddr(),
            config.getBrokerId());

        isolatedBrokers.remove(liteConfig);
        brokerController.registerBrokerAll(true, false, true);

        await().atMost(Duration.ofMinutes(1)).until(() -> namesrvController.getRouteInfoManager()
            .getBrokerMemberGroup(liteConfig.getClusterName(), liteConfig.brokerName).getBrokerAddrs()
            .containsKey(liteConfig.getBrokerId()));
    }

    protected static InnerSalveBrokerController getSlaveFromContainerByName(BrokerContainer brokerContainer,
        String brokerName) {
        InnerSalveBrokerController targetSlave = null;
        for (InnerSalveBrokerController slave : brokerContainer.getSlaveBrokers()) {
            if (slave.getBrokerConfig().getBrokerName().equals(brokerName)) {
                targetSlave = slave;
            }
        }

        return targetSlave;
    }

    protected static void changeCompatibleMode(boolean compatibleMode) {
        brokerContainer1.getBrokerControllers().forEach(brokerController -> brokerController.getBrokerConfig().setCompatibleWithOldNameSrv(compatibleMode));
        brokerContainer2.getBrokerControllers().forEach(brokerController -> brokerController.getBrokerConfig().setCompatibleWithOldNameSrv(compatibleMode));
        brokerContainer3.getBrokerControllers().forEach(brokerController -> brokerController.getBrokerConfig().setCompatibleWithOldNameSrv(compatibleMode));
    }

    protected static Set<MessageQueue> filterMessageQueue(Set<MessageQueue> mqSet, String topic) {
        Set<MessageQueue> targetMqSet = new HashSet<>();
        if (topic != null) {
            for (MessageQueue mq : mqSet) {
                if (mq.getTopic().equals(topic)) {
                    targetMqSet.add(mq);
                }
            }
        }

        return targetMqSet;
    }

    public static class BrokerGroupConfig {
        int totalReplicas = 3;
        int minReplicas = 1;
        int inSyncReplicas = 2;
        boolean autoReplicas = true;
        boolean enableSlaveActingMaster = true;
        boolean enableRemoteEscape = true;
        boolean slaveReadEnable = true;

        public BrokerGroupConfig() {
        }

        public BrokerGroupConfig(final int totalReplicas, final int minReplicas, final int inSyncReplicas,
            final boolean autoReplicas, boolean enableSlaveActingMaster, boolean slaveReadEnable) {
            this.totalReplicas = totalReplicas;
            this.minReplicas = minReplicas;
            this.inSyncReplicas = inSyncReplicas;
            this.autoReplicas = autoReplicas;
            this.enableSlaveActingMaster = enableSlaveActingMaster;
            this.slaveReadEnable = slaveReadEnable;
        }
    }

    static class BrokerConfigLite {
        private String clusterName;
        private String brokerName;
        private String brokerAddr;
        private long brokerId;

        public BrokerConfigLite(final String clusterName, final String brokerName, final String brokerAddr,
            final long brokerId) {
            this.clusterName = clusterName;
            this.brokerName = brokerName;
            this.brokerAddr = brokerAddr;
            this.brokerId = brokerId;
        }

        public String getClusterName() {
            return clusterName;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public String getBrokerAddr() {
            return brokerAddr;
        }

        public long getBrokerId() {
            return brokerId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            final BrokerConfigLite lite = (BrokerConfigLite) o;

            return new EqualsBuilder()
                .append(clusterName, lite.clusterName)
                .append(brokerName, lite.brokerName)
                .append(brokerAddr, lite.brokerAddr)
                .append(brokerId, lite.brokerId)
                .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                .append(clusterName)
                .append(brokerName)
                .append(brokerAddr)
                .append(brokerId)
                .toHashCode();
        }
    }
}
