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

import com.google.common.collect.Lists;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.bootstrap.BrokerNettyServer;
import org.apache.rocketmq.broker.bootstrap.BrokerScheduleService;
import org.apache.rocketmq.broker.bootstrap.BrokerMessageService;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.controller.ReplicasManager;
import org.apache.rocketmq.broker.failover.EscapeBridge;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.ConsumerOrderInfoManager;
import org.apache.rocketmq.broker.offset.LmqConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.RocksDBConsumerOffsetManager;
import org.apache.rocketmq.broker.offset.RocksDBLmqConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.RocksDBLmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.RocksDBSubscriptionGroupManager;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.RocksDBLmqTopicConfigManager;
import org.apache.rocketmq.broker.topic.RocksDBTopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.topic.TopicQueueMappingManager;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.common.utils.ServiceProvider;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.protocol.BrokerSyncInfo;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.StoreType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;

public class BrokerController {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_PROTECTION = LoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected final MessageStoreConfig messageStoreConfig;

    protected ConsumerOffsetManager consumerOffsetManager;
    protected final BroadcastOffsetManager broadcastOffsetManager;
    protected final ConsumerManager consumerManager;
    protected ConsumerFilterManager consumerFilterManager;
    protected ConsumerOrderInfoManager consumerOrderInfoManager;
    protected final PopInflightMessageCounter popInflightMessageCounter;
    protected final ProducerManager producerManager;


    protected final Broker2Client broker2Client;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final TopicRouteInfoManager topicRouteInfoManager;
    protected BrokerOuterAPI brokerOuterAPI;
    protected final SlaveSynchronize slaveSynchronize;

    protected final BrokerStatsManager brokerStatsManager;

    protected RemotingServer remotingServer;
    protected RemotingServer fastRemotingServer;
    protected TopicConfigManager topicConfigManager;
    protected SubscriptionGroupManager subscriptionGroupManager;
    protected TopicQueueMappingManager topicQueueMappingManager;

    protected boolean updateMasterHAServerAddrPeriodically = false;

    private InetSocketAddress storeHost;

    protected BrokerFastFailure brokerFastFailure;
    private Configuration configuration;
    protected TopicQueueMappingCleanService topicQueueMappingCleanService;
    protected TransactionalMessageCheckService transactionalMessageCheckService;
    protected TransactionalMessageService transactionalMessageService;
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
    protected volatile boolean shutdown = false;
    protected ShutdownHook shutdownHook;
    private volatile boolean isScheduleServiceStart = false;
    private volatile boolean isTransactionCheckServiceStart = false;
    protected volatile BrokerMemberGroup brokerMemberGroup;

    protected List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    protected volatile long shouldStartTime;
    private BrokerPreOnlineService brokerPreOnlineService;
    protected volatile boolean isIsolated = false;
    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;
    private final Lock lock = new ReentrantLock();
    protected ReplicasManager replicasManager;

    private BrokerMetricsManager brokerMetricsManager;
    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    private ColdDataCgCtrService coldDataCgCtrService;

    private final BrokerNettyServer brokerNettyServer;
    private final BrokerScheduleService brokerScheduleService;
    private BrokerMessageService brokerMessageService;

    private final SystemClock systemClock = new SystemClock();


    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig,
        final ShutdownHook shutdownHook
    ) {
        this(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        this.shutdownHook = shutdownHook;
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this(brokerConfig, null, null, messageStoreConfig);
    }

    public BrokerController(
        final BrokerConfig brokerConfig,
        final NettyServerConfig nettyServerConfig,
        final NettyClientConfig nettyClientConfig,
        final MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), getListenPort()));
        initConfiguration();
        initMetadata();

        this.brokerStatsManager = messageStoreConfig.isEnableLmq()
            ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat())
            : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());

        this.broadcastOffsetManager = new BroadcastOffsetManager(this);

        this.brokerNettyServer = new BrokerNettyServer(brokerConfig, messageStoreConfig, nettyServerConfig, this);
        this.brokerScheduleService = new BrokerScheduleService(brokerConfig, messageStoreConfig, this);

        this.consumerManager = new ConsumerManager(brokerNettyServer.getConsumerIdsChangeListener(), this.brokerStatsManager, this.brokerConfig);
        this.producerManager = new ProducerManager(this.brokerStatsManager);

        this.popInflightMessageCounter = new PopInflightMessageCounter(this);

        this.broker2Client = new Broker2Client(this);
        this.coldDataPullRequestHoldService = new ColdDataPullRequestHoldService(this);
        this.coldDataCgCtrService = new ColdDataCgCtrService(this);

        if (nettyClientConfig != null) {
            this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        }

        this.slaveSynchronize = new SlaveSynchronize(this);
        this.brokerFastFailure = new BrokerFastFailure(this);
        initProducerStateGetter();
        initConsumerStateGetter();

        this.brokerMemberGroup = new BrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
        this.brokerMemberGroup.getBrokerAddrs().put(this.brokerConfig.getBrokerId(), this.getBrokerAddr());

        this.topicRouteInfoManager = new TopicRouteInfoManager(this);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }
    }

    //************************ constructor end: depends about 100 objects ***************************************
    //************************ about 56 netty related objects which can move outside the BrokerController *******
    //************************ public methods start: 373~651, about 278 lines ***********************************
    public boolean initialize() throws CloneNotSupportedException {
        boolean result = this.loadMetadata();
        if (!result) {
            return false;
        }

        result = this.initializeMessageStore();
        if (!result) {
            return false;
        }

        return this.initAndLoadService();
    }

    public void shutdown() {
        shutdownBasicService();

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }
    }

    public void start() throws Exception {
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.registerBrokerAll(true, false, true);
        }

        this.brokerScheduleService.start();

        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    public long now() {
        return this.systemClock.now();
    }

    public synchronized void registerSingleTopicAll(final TopicConfig topicConfig) {
        TopicConfig tmpTopic = topicConfig;
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
            || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            // Copy the topic config and modify the perm
            tmpTopic = new TopicConfig(topicConfig);
            tmpTopic.setPerm(topicConfig.getPerm() & this.brokerConfig.getBrokerPermission());
        }
        this.brokerOuterAPI.registerSingleTopicAll(this.brokerConfig.getBrokerName(), tmpTopic, 3000);
    }

    public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
        this.registerIncrementBrokerData(Collections.singletonList(topicConfig), dataVersion);
    }

    public synchronized void registerIncrementBrokerData(List<TopicConfig> topicConfigList, DataVersion dataVersion) {
        if (topicConfigList == null || topicConfigList.isEmpty()) {
            return;
        }

        TopicConfigAndMappingSerializeWrapper topicConfigSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);

        ConcurrentMap<String, TopicConfig> topicConfigTable = topicConfigList.stream()
            .map(topicConfig -> {
                TopicConfig registerTopicConfig;
                if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                    || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
                    registerTopicConfig =
                        new TopicConfig(topicConfig.getTopicName(),
                            topicConfig.getReadQueueNums(),
                            topicConfig.getWriteQueueNums(),
                            this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                } else {
                    registerTopicConfig = new TopicConfig(topicConfig);
                }
                return registerTopicConfig;
            })
            .collect(Collectors.toConcurrentMap(TopicConfig::getTopicName, Function.identity()));
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = topicConfigList.stream()
            .map(TopicConfig::getTopicName)
            .map(topicName -> Optional.ofNullable(this.topicQueueMappingManager.getTopicQueueMapping(topicName))
                .map(info -> new AbstractMap.SimpleImmutableEntry<>(topicName, TopicQueueMappingDetail.cloneAsMappingInfo(info)))
                .orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!topicQueueMappingInfoMap.isEmpty()) {
            topicConfigSerializeWrapper.setTopicQueueMappingInfoMap(topicQueueMappingInfoMap);
        }

        doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {

        TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();

        topicConfigWrapper.setDataVersion(this.getTopicConfigManager().getDataVersion());
        topicConfigWrapper.setTopicConfigTable(this.getTopicConfigManager().getTopicConfigTable());

        topicConfigWrapper.setTopicQueueMappingInfoMap(this.getTopicQueueMappingManager().getTopicQueueMappingTable().entrySet().stream().map(
            entry -> new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), TopicQueueMappingDetail.cloneAsMappingInfo(entry.getValue()))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
            || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                    new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                        topicConfig.getPerm() & this.brokerConfig.getBrokerPermission(), topicConfig.getTopicSysFlag());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isInBrokerContainer())) {
            doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
        }
    }

    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.changeSpecialServiceStatus(false);
    }

    public boolean isSpecialServiceRunning() {
        if (isScheduleServiceStart() && isTransactionCheckServiceStart()) {
            return true;
        }

        AckMessageProcessor ackMessageProcessor = this.brokerNettyServer.getAckMessageProcessor();
        return ackMessageProcessor != null && ackMessageProcessor.isPopReviveServiceRunning();
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr) {
        if (brokerConfig.isEnableSlaveActingMaster() && brokerConfig.getBrokerId() != MixAll.MASTER_ID) {
            if (!lock.tryLock()) {
                return;
            }
        }

        try {
            if (minBrokerId != this.minBrokerIdInGroup) {
                String offlineBrokerAddr = null;
                if (minBrokerId > this.minBrokerIdInGroup) {
                    offlineBrokerAddr = this.minBrokerAddrInGroup;
                }
                onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, null);
            }
        } finally {
            lock.unlock();
        }
    }

    public void updateMinBroker(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr, String masterHaAddr) {
        if (!brokerConfig.isEnableSlaveActingMaster() || brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
            return;
        }

        try {
            if (!lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                return;
            }

            try {
                if (minBrokerId != this.minBrokerIdInGroup) {
                    onMinBrokerChange(minBrokerId, minBrokerAddr, offlineBrokerAddr, masterHaAddr);
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            LOG.error("Update min broker error, {}", e);
        }
    }

    public void changeSpecialServiceStatus(boolean shouldStart) {

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.statusChanged(shouldStart);
            }
        }

        changeScheduleServiceStatus(shouldStart);

        changeTransactionCheckServiceStatus(shouldStart);

        AckMessageProcessor ackMessageProcessor = this.brokerNettyServer.getAckMessageProcessor();
        if (ackMessageProcessor != null) {
            LOG.info("Set PopReviveService Status to {}", shouldStart);
            ackMessageProcessor.setPopReviveServiceStatus(shouldStart);
        }
    }

    public MessageStore getMessageStoreByBrokerName(String brokerName) {
        if (this.brokerConfig.getBrokerName().equals(brokerName)) {
            return this.getMessageStore();
        }
        return null;
    }

    public BrokerIdentity getBrokerIdentity() {
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                Integer.parseInt(messageStoreConfig.getdLegerSelfId().substring(1)), brokerConfig.isInBrokerContainer());
        } else {
            return new BrokerIdentity(
                brokerConfig.getBrokerClusterName(), brokerConfig.getBrokerName(),
                brokerConfig.getBrokerId(), brokerConfig.isInBrokerContainer());
        }
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public boolean isEnableRocksDBStore() {
        return StoreType.DEFAULT_ROCKSDB.getStoreType().equalsIgnoreCase(this.messageStoreConfig.getStoreType());
    }

    //**************************************** debug methods start ****************************************************
    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = rt == null ? 0 : this.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4SendThreadPoolQueue();
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4PullThreadPoolQueue();
    }

    public long headSlowTimeMills4LitePullThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4PullThreadPoolQueue();
    }

    public long headSlowTimeMills4QueryThreadPoolQueue() {
        return this.brokerNettyServer.headSlowTimeMills4QueryThreadPoolQueue();
    }

    public void printWaterMark() {
        this.brokerNettyServer.printWaterMark();
    }

    //**************************************** private or protected methods start ****************************************************

    private void initMetadata() {
        if (isEnableRocksDBStore()) {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqTopicConfigManager(this) : new RocksDBTopicConfigManager(this);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqSubscriptionGroupManager(this) : new RocksDBSubscriptionGroupManager(this);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new RocksDBLmqConsumerOffsetManager(this) : new RocksDBConsumerOffsetManager(this);
        } else {
            this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);
            this.subscriptionGroupManager = messageStoreConfig.isEnableLmq() ? new LmqSubscriptionGroupManager(this) : new SubscriptionGroupManager(this);
            this.consumerOffsetManager = messageStoreConfig.isEnableLmq() ? new LmqConsumerOffsetManager(this) : new ConsumerOffsetManager(this);
        }

        this.topicQueueMappingManager = new TopicQueueMappingManager(this);
        this.consumerFilterManager = new ConsumerFilterManager(this);
        this.consumerOrderInfoManager = new ConsumerOrderInfoManager(this);
    }

    protected boolean loadMetadata() {
        boolean result = this.topicConfigManager.load();
        result = result && this.topicQueueMappingManager.load();
        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();
        result = result && this.consumerFilterManager.load();
        result = result && this.consumerOrderInfoManager.load();
        return result;
    }

    public boolean initializeMessageStore() {
        this.brokerMessageService = new BrokerMessageService(this);
        return brokerMessageService.init();
    }

    public boolean initAndLoadService() throws CloneNotSupportedException {
        boolean result = true;

        if (this.brokerConfig.isEnableControllerMode()) {
            this.replicasManager = new ReplicasManager(this);
            this.replicasManager.setFenced(true);
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        if (!result) {
            return false;
        }

        this.brokerMetricsManager = new BrokerMetricsManager(this);
        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(this);

        initializeRemotingServer();
        initializeScheduledTasks();
        initialTransaction();

        return brokerNettyServer.initFileWatchService();
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        brokerNettyServer.init();
        this.remotingServer = brokerNettyServer.getRemotingServer();
        this.fastRemotingServer = brokerNettyServer.getFastRemotingServer();
    }

    protected void initializeScheduledTasks() {
        brokerScheduleService.init();
    }

    protected void initializeBrokerScheduledTasks() {
        getBrokerScheduleService().initializeBrokerScheduledTasks();
    }

    public void protectBroker() {
        if (!this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            return;
        }

        for (Map.Entry<String, MomentStatsItem> next : this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet()) {
            final long fallBehindBytes = next.getValue().getValue().get();
            if (fallBehindBytes <= this.brokerConfig.getConsumerFallbehindThreshold()) {
                continue;
            }

            final String[] split = next.getValue().getStatsKey().split("@");
            final String group = split[2];
            LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
            this.subscriptionGroupManager.disableConsume(group);
        }
    }

    private void initProducerStateGetter() {
        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
                    return getProducerManager().groupOnline(NamespaceUtil.wrapNamespace(instanceId, group));
                } else {
                    return getProducerManager().groupOnline(group);
                }
            }
        });
    }

    private void initConsumerStateGetter() {
        this.brokerStatsManager.setConsumerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                String topicFullName = NamespaceUtil.wrapNamespace(instanceId, topic);
                if (getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });
    }

    private void initConfiguration() {
        String brokerConfigPath;
        if (brokerConfig.getBrokerConfigPath() != null && !brokerConfig.getBrokerConfigPath().isEmpty()) {
            brokerConfigPath = brokerConfig.getBrokerConfigPath();
        } else {
            brokerConfigPath = BrokerPathConfigHelper.getBrokerConfigPath();
        }
        this.configuration = new Configuration(
            LOG,
            brokerConfigPath,
            this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
        );
    }

    private void initialTransaction() {
        this.transactionalMessageService = ServiceProvider.loadClass(TransactionalMessageService.class);
        if (null == this.transactionalMessageService) {
            this.transactionalMessageService = new TransactionalMessageServiceImpl(
                new TransactionalMessageBridge(this, this.getMessageStore()));
            LOG.warn("Load default transaction message hook service: {}",
                TransactionalMessageServiceImpl.class.getSimpleName());
        }
        this.transactionalMessageCheckListener = ServiceProvider.loadClass(
            AbstractTransactionalMessageCheckListener.class);
        if (null == this.transactionalMessageCheckListener) {
            this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
            LOG.warn("Load default discard message hook service: {}",
                DefaultTransactionalMessageCheckListener.class.getSimpleName());
        }
        this.transactionalMessageCheckListener.setBrokerController(this);
        this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
    }

    protected void shutdownBasicService() {
        shutdown = true;
        this.unregisterBrokerAll();

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(this);
        }
        this.getBrokerNettyServer().shutdown();
        this.brokerMessageService.shutdown();

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.shutdown();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.shutdown();
        }

        if (this.replicasManager != null) {
            this.replicasManager.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }

        if (this.consumerFilterManager != null) {
            this.consumerFilterManager.persist();
        }

        if (this.consumerOrderInfoManager != null) {
            this.consumerOrderInfoManager.persist();
        }

        if (this.transactionalMessageCheckService != null) {
            this.transactionalMessageCheckService.shutdown(false);
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.shutdown();
        }

        if (this.brokerPreOnlineService != null && !this.brokerPreOnlineService.isStopped()) {
            this.brokerPreOnlineService.shutdown();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.shutdown();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.shutdown();
        }

        if (this.topicConfigManager != null) {
            this.topicConfigManager.persist();
            this.topicConfigManager.stop();
        }

        if (this.subscriptionGroupManager != null) {
            this.subscriptionGroupManager.persist();
            this.subscriptionGroupManager.stop();
        }

        if (this.consumerOffsetManager != null) {
            this.consumerOffsetManager.persist();
            this.consumerOffsetManager.stop();
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }
    }

    protected void scheduleSendHeartbeat() {
        getBrokerScheduleService().scheduleSendHeartbeat();
    }

    protected void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId());
    }

    protected void startBasicService() throws Exception {
        if (this.replicasManager != null) {
            this.replicasManager.start();
        }

        this.storeHost = new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort());

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.start();
            }
        }

        this.brokerNettyServer.start();
        this.brokerMessageService.start();

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.start();
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }

        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.start();
        }

        if (this.topicRouteInfoManager != null) {
            this.topicRouteInfoManager.start();
        }

        if (this.brokerPreOnlineService != null) {
            this.brokerPreOnlineService.start();
        }

        if (this.coldDataPullRequestHoldService != null) {
            this.coldDataPullRequestHoldService.start();
        }

        if (this.coldDataCgCtrService != null) {
            this.coldDataCgCtrService.start();
        }
    }

    protected void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway,
        TopicConfigSerializeWrapper topicConfigWrapper) {

        if (shutdown) {
            BrokerController.LOG.info("BrokerController#doResterBrokerAll: broker has shutdown, no need to register any more.");
            return;
        }
        List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
            this.brokerConfig.getBrokerClusterName(),
            this.getBrokerAddr(),
            this.brokerConfig.getBrokerName(),
            this.brokerConfig.getBrokerId(),
            this.getHAServerAddr(),
            topicConfigWrapper,
            Lists.newArrayList(),
            oneway,
            this.brokerConfig.getRegisterBrokerTimeoutMills(),
            this.brokerConfig.isEnableSlaveActingMaster(),
            this.brokerConfig.isCompressedRegister(),
            this.brokerConfig.isEnableSlaveActingMaster() ? this.brokerConfig.getBrokerNotActiveTimeoutMillis() : null,
            this.getBrokerIdentity());

        handleRegisterBrokerResult(registerBrokerResultList, checkOrderConfig);
    }


    public void syncBrokerMemberGroup() {
        try {
            brokerMemberGroup = this.getBrokerOuterAPI()
                .syncBrokerMemberGroup(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName(), this.brokerConfig.isCompatibleWithOldNameSrv());
        } catch (Exception e) {
            BrokerController.LOG.error("syncBrokerMemberGroup from namesrv failed, ", e);
            return;
        }
        if (brokerMemberGroup == null || brokerMemberGroup.getBrokerAddrs().size() == 0) {
            BrokerController.LOG.warn("Couldn't find any broker member from namesrv in {}/{}", this.brokerConfig.getBrokerClusterName(), this.brokerConfig.getBrokerName());
            return;
        }
        brokerMessageService.getMessageStore().setAliveReplicaNumInGroup(calcAliveBrokerNumInGroup(brokerMemberGroup.getBrokerAddrs()));


        if (!this.isIsolated) {
            long minBrokerId = brokerMemberGroup.minimumBrokerId();
            this.updateMinBroker(minBrokerId, brokerMemberGroup.getBrokerAddrs().get(minBrokerId));
        }
    }

    private int calcAliveBrokerNumInGroup(Map<Long, String> brokerAddrTable) {
        if (brokerAddrTable.containsKey(this.brokerConfig.getBrokerId())) {
            return brokerAddrTable.size();
        } else {
            return brokerAddrTable.size() + 1;
        }
    }

    protected void handleRegisterBrokerResult(List<RegisterBrokerResult> registerBrokerResultList,
        boolean checkOrderConfig) {
        for (RegisterBrokerResult registerBrokerResult : registerBrokerResultList) {
            if (registerBrokerResult != null) {
                if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                    brokerMessageService.getMessageStore().updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
                    brokerMessageService.getMessageStore().updateMasterAddress(registerBrokerResult.getMasterAddr());
                }

                this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
                if (checkOrderConfig) {
                    this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
                }
                break;
            }
        }
    }

    private boolean needRegister(final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final int timeoutMills,
        final boolean isInBrokerContainer) {

        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
        List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills, isInBrokerContainer);
        boolean needRegister = false;
        for (Boolean changed : changeList) {
            if (changed) {
                needRegister = true;
                break;
            }
        }
        return needRegister;
    }

    private synchronized void changeTransactionCheckServiceStatus(boolean shouldStart) {
        if (isTransactionCheckServiceStart != shouldStart) {
            LOG.info("TransactionCheckService status changed to {}", shouldStart);
            if (shouldStart) {
                this.transactionalMessageCheckService.start();
            } else {
                this.transactionalMessageCheckService.shutdown(true);
            }
            isTransactionCheckServiceStart = shouldStart;
        }
    }

    private synchronized void changeScheduleServiceStatus(boolean shouldStart) {
        if (isScheduleServiceStart != shouldStart) {
             brokerMessageService.changeScheduleServiceStatus(shouldStart);
        }
    }

    private void onMasterOffline() {
        // close channels with master broker
        String masterAddr = this.slaveSynchronize.getMasterAddr();
        if (masterAddr != null) {
            this.brokerOuterAPI.getRemotingClient().closeChannels(
                Arrays.asList(masterAddr, MixAll.brokerVIPChannel(true, masterAddr)));
        }
        // master not available, stop sync
        this.slaveSynchronize.setMasterAddr(null);
        brokerMessageService.getMessageStore().updateHaMasterAddress(null);
    }

    private void onMasterOnline(String masterAddr, String masterHaAddr) {
        boolean needSyncMasterFlushOffset = brokerMessageService.getMessageStore().getMasterFlushedOffset() == 0
            && this.messageStoreConfig.isSyncMasterFlushOffsetWhenStartup();
        if (masterHaAddr == null || needSyncMasterFlushOffset) {
            doSyncMasterFlushOffset(masterAddr, masterHaAddr, needSyncMasterFlushOffset);
        }

        // set master HA address.
        if (masterHaAddr != null) {
            brokerMessageService.getMessageStore().updateHaMasterAddress(masterHaAddr);
        }

        // wakeup HAClient
        brokerMessageService.getMessageStore().wakeupHAClient();
    }

    private void doSyncMasterFlushOffset(String masterAddr, String masterHaAddr, boolean needSyncMasterFlushOffset) {
        try {
            BrokerSyncInfo brokerSyncInfo = this.brokerOuterAPI.retrieveBrokerHaInfo(masterAddr);

            if (needSyncMasterFlushOffset) {
                LOG.info("Set master flush offset in slave to {}", brokerSyncInfo.getMasterFlushOffset());
                brokerMessageService.getMessageStore().setMasterFlushedOffset(brokerSyncInfo.getMasterFlushOffset());
            }

            if (masterHaAddr == null) {
                brokerMessageService.getMessageStore().updateHaMasterAddress(brokerSyncInfo.getMasterHaAddress());
                brokerMessageService.getMessageStore().updateMasterAddress(brokerSyncInfo.getMasterAddress());
            }
        } catch (Exception e) {
            LOG.error("retrieve master ha info exception, {}", e);
        }
    }

    private void onMinBrokerChange(long minBrokerId, String minBrokerAddr, String offlineBrokerAddr,
        String masterHaAddr) {
        LOG.info("Min broker changed, old: {}-{}, new {}-{}",
            this.minBrokerIdInGroup, this.minBrokerAddrInGroup, minBrokerId, minBrokerAddr);

        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == this.minBrokerIdInGroup);

        if (offlineBrokerAddr != null && offlineBrokerAddr.equals(this.slaveSynchronize.getMasterAddr())) {
            // master offline
            onMasterOffline();
        }

        if (minBrokerId == MixAll.MASTER_ID && minBrokerAddr != null) {
            // master online
            onMasterOnline(minBrokerAddr, masterHaAddr);
        }

        // notify PullRequest on hold to pull from master.
        if (this.minBrokerIdInGroup == MixAll.MASTER_ID) {
            this.brokerNettyServer.getPullRequestHoldService().notifyMasterOnline();
        }
    }

    //**************************************** private or protected methods end   ****************************************************

    //**************************************** getter and setter start ****************************************************
    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    public BrokerScheduleService getBrokerScheduleService() {
        return brokerScheduleService;
    }

    public BrokerStats getBrokerStats() {
        return brokerMessageService.getBrokerStats();
    }

    public boolean isUpdateMasterHAServerAddrPeriodically() {
        return updateMasterHAServerAddrPeriodically;
    }

    public void setUpdateMasterHAServerAddrPeriodically(boolean updateMasterHAServerAddrPeriodically) {
        this.updateMasterHAServerAddrPeriodically = updateMasterHAServerAddrPeriodically;
    }

    public MessageStore getMessageStore() {
        return brokerMessageService.getMessageStore();
    }

    public void setMessageStore(MessageStore messageStore) {
        brokerMessageService.setMessageStore(messageStore);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerFilterManager getConsumerFilterManager() {
        return consumerFilterManager;
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return consumerOrderInfoManager;
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return broadcastOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public RemotingServer getFastRemotingServer() {
        return getBrokerNettyServer().getFastRemotingServer();
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public TimerMessageStore getTimerMessageStore() {
        return brokerMessageService.getTimerMessageStore();
    }


    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public String getHAServerAddr() {
        return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }


    public RemotingServer getRemotingServer() {
        return getBrokerNettyServer().getRemotingServer();
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }


    public TransactionalMessageCheckService getTransactionalMessageCheckService() {
        return transactionalMessageCheckService;
    }

    public void setTransactionalMessageCheckService(
        TransactionalMessageCheckService transactionalMessageCheckService) {
        this.transactionalMessageCheckService = transactionalMessageCheckService;
    }

    public TransactionalMessageService getTransactionalMessageService() {
        return transactionalMessageService;
    }

    public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
        this.transactionalMessageService = transactionalMessageService;
    }

    public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
        return transactionalMessageCheckListener;
    }

    public void setTransactionalMessageCheckListener(
        AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
        this.transactionalMessageCheckListener = transactionalMessageCheckListener;
    }

    public Map<Class, AccessValidator> getAccessValidatorMap() {
        return getBrokerNettyServer().getAccessValidatorMap();
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public long getMinBrokerIdInGroup() {
        return this.brokerConfig.getBrokerId();
    }

    public BrokerController peekMasterBroker() {
        return brokerConfig.getBrokerId() == MixAll.MASTER_ID ? this : null;
    }

    public BrokerMemberGroup getBrokerMemberGroup() {
        return this.brokerMemberGroup;
    }

    public int getListenPort() {
        return this.nettyServerConfig.getListenPort();
    }

    public List<BrokerAttachedPlugin> getBrokerAttachedPlugins() {
        return brokerAttachedPlugins;
    }

    public EscapeBridge getEscapeBridge() {
        return brokerMessageService.getEscapeBridge();
    }

    public long getShouldStartTime() {
        return shouldStartTime;
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }

    public boolean isScheduleServiceStart() {
        return isScheduleServiceStart;
    }

    public boolean isTransactionCheckServiceStart() {
        return isTransactionCheckServiceStart;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return brokerMessageService.getScheduleMessageService();
    }

    public ReplicasManager getReplicasManager() {
        return replicasManager;
    }

    public void setIsolated(boolean isolated) {
        isIsolated = isolated;
    }

    public boolean isIsolated() {
        return this.isIsolated;
    }

    public TimerCheckpoint getTimerCheckpoint() {
        return brokerMessageService.getTimerCheckpoint();
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return this.topicRouteInfoManager;
    }

    public ColdDataPullRequestHoldService getColdDataPullRequestHoldService() {
        return coldDataPullRequestHoldService;
    }

    public ColdDataCgCtrService getColdDataCgCtrService() {
        return coldDataCgCtrService;
    }

    public BrokerNettyServer getBrokerNettyServer() {
        return this.brokerNettyServer;
    }
    //**************************************** getter and setter end ****************************************************

}
