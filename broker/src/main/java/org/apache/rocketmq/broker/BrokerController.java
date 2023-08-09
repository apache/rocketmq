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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.bootstrap.BrokerMessageService;
import org.apache.rocketmq.broker.bootstrap.BrokerMetadataManager;
import org.apache.rocketmq.broker.bootstrap.BrokerNettyServer;
import org.apache.rocketmq.broker.bootstrap.BrokerScheduleService;
import org.apache.rocketmq.broker.bootstrap.BrokerServiceRegistry;
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
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.processor.AckMessageProcessor;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.schedule.ScheduleMessageService;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
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
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;
import org.apache.rocketmq.store.timer.TimerCheckpoint;
import org.apache.rocketmq.store.timer.TimerMessageStore;

public class BrokerController {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    protected final MessageStoreConfig messageStoreConfig;

    protected final PopInflightMessageCounter popInflightMessageCounter;
    protected final ProducerManager producerManager;
    private final ConsumerManager consumerManager;
    private final BroadcastOffsetManager broadcastOffsetManager;

    protected final Broker2Client broker2Client;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final TopicRouteInfoManager topicRouteInfoManager;
    protected final SlaveSynchronize slaveSynchronize;
    protected BrokerFastFailure brokerFastFailure;
    protected ReplicasManager replicasManager;

    protected boolean updateMasterHAServerAddrPeriodically = false;

    private InetSocketAddress storeHost;

    private Configuration configuration;
    protected TopicQueueMappingCleanService topicQueueMappingCleanService;
    protected TransactionalMessageCheckService transactionalMessageCheckService;
    protected TransactionalMessageService transactionalMessageService;
    protected AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;

    protected volatile boolean shutdown = false;
    protected ShutdownHook shutdownHook;
    private volatile boolean isScheduleServiceStart = false;
    private volatile boolean isTransactionCheckServiceStart = false;

    protected List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    protected volatile long shouldStartTime;
    protected volatile boolean isIsolated = false;
    protected volatile long minBrokerIdInGroup = 0;
    protected volatile String minBrokerAddrInGroup = null;
    private final Lock lock = new ReentrantLock();

    protected final BrokerStatsManager brokerStatsManager;
    private BrokerPreOnlineService brokerPreOnlineService;
    private BrokerMetricsManager brokerMetricsManager;
    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    private ColdDataCgCtrService coldDataCgCtrService;

    private final BrokerNettyServer brokerNettyServer;
    private final BrokerScheduleService brokerScheduleService;
    private final BrokerMetadataManager brokerMetadataManager;

    private final BrokerServiceRegistry brokerServiceRegistry;
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

        this.brokerStatsManager = messageStoreConfig.isEnableLmq()
            ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat())
            : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());

        this.broadcastOffsetManager = new BroadcastOffsetManager(this);

        this.brokerMetadataManager = new BrokerMetadataManager(this);
        this.brokerNettyServer = new BrokerNettyServer(brokerConfig, messageStoreConfig, nettyServerConfig, this);
        this.brokerScheduleService = new BrokerScheduleService(brokerConfig, messageStoreConfig, this);
        this.brokerServiceRegistry = new BrokerServiceRegistry(this);

        this.consumerManager = new ConsumerManager(brokerNettyServer.getConsumerIdsChangeListener(), this.brokerStatsManager, this.brokerConfig);
        this.producerManager = new ProducerManager(this.brokerStatsManager);

        this.popInflightMessageCounter = new PopInflightMessageCounter(this);

        this.broker2Client = new Broker2Client(this);
        this.coldDataPullRequestHoldService = new ColdDataPullRequestHoldService(this);
        this.coldDataCgCtrService = new ColdDataCgCtrService(this);

        this.slaveSynchronize = new SlaveSynchronize(this);
        this.brokerFastFailure = new BrokerFastFailure(this);
        initProducerStateGetter();
        initConsumerStateGetter();

        this.topicRouteInfoManager = new TopicRouteInfoManager(this);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(this);
        }
    }

    //************************ constructor end: depends about 100 objects ***************************************
    //************************ about 56 netty related objects which can move outside the BrokerController *******
    //************************ public methods start: 373~651, about 278 lines ***********************************
    public boolean initialize() throws CloneNotSupportedException {
        boolean result = this.brokerMetadataManager.load();
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
        this.brokerServiceRegistry.shutdown();
    }

    public void start() throws Exception {
        this.shouldStartTime = System.currentTimeMillis() + messageStoreConfig.getDisappearTimeAfterStart();

        if (messageStoreConfig.getTotalReplicas() > 1 && this.brokerConfig.isEnableSlaveActingMaster()) {
            isIsolated = true;
        }

        this.brokerServiceRegistry.start();
        startBasicService();

        if (!isIsolated && !this.messageStoreConfig.isEnableDLegerCommitLog() && !this.messageStoreConfig.isDuplicationEnable()) {
            changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
            this.brokerServiceRegistry.registerBrokerAll(true, false, true);
        }

        this.brokerScheduleService.start();
        if (brokerConfig.isSkipPreOnline()) {
            startServiceWithoutCondition();
        }
    }

    public long now() {
        return this.systemClock.now();
    }

    public void startService(long minBrokerId, String minBrokerAddr) {
        BrokerController.LOG.info("{} start service, min broker id is {}, min broker addr: {}",
            this.brokerConfig.getCanonicalName(), minBrokerId, minBrokerAddr);
        this.minBrokerIdInGroup = minBrokerId;
        this.minBrokerAddrInGroup = minBrokerAddr;

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == minBrokerId);
        this.brokerServiceRegistry.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
    }

    public void startServiceWithoutCondition() {
        BrokerController.LOG.info("{} start service", this.brokerConfig.getCanonicalName());

        this.changeSpecialServiceStatus(this.brokerConfig.getBrokerId() == MixAll.MASTER_ID);
        this.brokerServiceRegistry.registerBrokerAll(true, false, brokerConfig.isForceRegister());

        isIsolated = false;
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
    public void stopService() {
        BrokerController.LOG.info("{} stop service", this.getBrokerConfig().getCanonicalName());
        isIsolated = true;
        this.changeSpecialServiceStatus(false);
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
    }

    protected void initializeScheduledTasks() {
        brokerScheduleService.init();
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
        this.brokerServiceRegistry.unregisterBrokerAll();

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(this);
        }
        this.brokerNettyServer.shutdown();
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

        this.brokerMetadataManager.shutdown();

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
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

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }
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
            this.getBrokerOuterAPI().getRemotingClient().closeChannels(
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
            BrokerSyncInfo brokerSyncInfo = this.getBrokerOuterAPI().retrieveBrokerHaInfo(masterAddr);

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

    public BrokerServiceRegistry getBrokerServiceRegistry() {
        return brokerServiceRegistry;
    }

    public BrokerMetadataManager getBrokerMetadataManager() {
        return brokerMetadataManager;
    }

    public BrokerMessageService getBrokerMessageService() {
        return brokerMessageService;
    }

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
        return this.brokerMetadataManager.getConsumerFilterManager();
    }

    public ConsumerOrderInfoManager getConsumerOrderInfoManager() {
        return this.brokerMetadataManager.getConsumerOrderInfoManager();
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return this.brokerMetadataManager.getConsumerOffsetManager();
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

    public RemotingServer getFastRemotingServer() {
        return getBrokerNettyServer().getFastRemotingServer();
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.brokerMetadataManager.setSubscriptionGroupManager(subscriptionGroupManager);
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return this.brokerMetadataManager.getSubscriptionGroupManager();
    }

    public TimerMessageStore getTimerMessageStore() {
        return brokerMessageService.getTimerMessageStore();
    }


    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicConfigManager getTopicConfigManager() {
        return this.brokerMetadataManager.getTopicConfigManager();
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.brokerMetadataManager.setTopicConfigManager(topicConfigManager);
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return this.brokerMetadataManager.getTopicQueueMappingManager();
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
        getBrokerNettyServer().setRemotingServer(remotingServer);
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        getBrokerNettyServer().setFastRemotingServer(fastRemotingServer);
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return this.brokerServiceRegistry.getBrokerOuterAPI();
    }

    public void setBrokerOuterAPI(BrokerOuterAPI brokerOuterAPI) {
        this.brokerServiceRegistry.setBrokerOuterAPI(brokerOuterAPI);
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
        return this.getBrokerScheduleService().getBrokerMemberGroup();
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
