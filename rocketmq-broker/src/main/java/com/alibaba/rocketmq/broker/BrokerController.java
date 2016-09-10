/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.broker.client.*;
import com.alibaba.rocketmq.broker.client.net.Broker2Client;
import com.alibaba.rocketmq.broker.client.rebalance.RebalanceLockManager;
import com.alibaba.rocketmq.broker.filtersrv.FilterServerManager;
import com.alibaba.rocketmq.broker.latency.BrokerFastFailure;
import com.alibaba.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import com.alibaba.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.out.BrokerOuterAPI;
import com.alibaba.rocketmq.broker.plugin.MessageStoreFactory;
import com.alibaba.rocketmq.broker.plugin.MessageStorePluginContext;
import com.alibaba.rocketmq.broker.processor.*;
import com.alibaba.rocketmq.broker.slave.SlaveSynchronize;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.common.*;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.stats.MomentStatsItem;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.*;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageArrivingListener;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.stats.BrokerStats;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;


/**
 * @author shijia.wxr
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final Logger logProtection = LoggerFactory.getLogger(LoggerName.ProtectionLoggerName);
    private static final Logger logWaterMark = LoggerFactory.getLogger(LoggerName.WaterMarkLoggerName);
    private final BrokerConfig brokerConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private final MessageStoreConfig messageStoreConfig;
    private final DataVersion configDataVersion = new DataVersion();
    private final ConsumerOffsetManager consumerOffsetManager;
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private final ClientHousekeepingService clientHousekeepingService;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    private final MessageArrivingListener messageArrivingListener;
    private final Broker2Client broker2Client;
    private final SubscriptionGroupManager subscriptionGroupManager;
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    private final SlaveSynchronize slaveSynchronize;
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    private final FilterServerManager filterServerManager;
    private final BrokerStatsManager brokerStatsManager;
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private MessageStore messageStore;
    private RemotingServer remotingServer;
    private RemotingServer fastRemotingServer;
    private TopicConfigManager topicConfigManager;
    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService adminBrokerExecutor;
    private ExecutorService clientManageExecutor;
    private boolean updateMasterHAServerAddrPeriodically = false;
    private BrokerStats brokerStats;
    private InetSocketAddress storeHost;
    private BrokerFastFailure brokerFastFailure;

    public BrokerController(//
                            final BrokerConfig brokerConfig, //
                            final NettyServerConfig nettyServerConfig, //
                            final NettyClientConfig nettyClientConfig, //
                            final MessageStoreConfig messageStoreConfig //
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));

        this.brokerFastFailure = new BrokerFastFailure(this);
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    public boolean initialize() throws CloneNotSupportedException {
        boolean result = true;

        result = result && this.topicConfigManager.load();

        result = result && this.consumerOffsetManager.load();
        result = result && this.subscriptionGroupManager.load();

        if (result) {
            try {
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                                this.brokerConfig);
                this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                //load plugin
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                this.messageStore = MessageStoreFactory.build(context, this.messageStore);
            } catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }

        result = result && this.messageStore.load();

        if (result) {
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(//
                    this.brokerConfig.getSendMessageThreadPoolNums(),//
                    this.brokerConfig.getSendMessageThreadPoolNums(),//
                    1000 * 60,//
                    TimeUnit.MILLISECONDS,//
                    this.sendThreadPoolQueue,//
                    new ThreadFactoryImpl("SendMessageThread_"));

            this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(//
                    this.brokerConfig.getPullMessageThreadPoolNums(),//
                    this.brokerConfig.getPullMessageThreadPoolNums(),//
                    1000 * 60,//
                    TimeUnit.MILLISECONDS,//
                    this.pullThreadPoolQueue,//
                    new ThreadFactoryImpl("PullMessageThread_"));

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                            "AdminBrokerThread_"));

            this.clientManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getClientManageThreadPoolNums(), new ThreadFactoryImpl(
                            "ClientManageThread_"));

            this.registerProcessor();


            // TODO remove in future
            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    } catch (Throwable e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);


            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.protectBroker();
                    } catch (Exception e) {
                        log.error("protectBroker error.", e);
                    }
                }
            }, 3, 3, TimeUnit.MINUTES);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.printWaterMark();
                    } catch (Exception e) {
                        log.error("printWaterMark error.", e);
                    }
                }
            }, 10, 1, TimeUnit.SECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
                    } catch (Throwable e) {
                        log.error("schedule dispatchBehindBytes error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            } else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        } catch (Throwable e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        } catch (Throwable e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            } else {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }

    public void registerProcessor() {
        /**
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);


        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    public void protectBroker() {
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            final Iterator<Map.Entry<String, MomentStatsItem>> it = this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, MomentStatsItem> next = it.next();
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    logProtection.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) slowTimeMills = 0;

        return slowTimeMills;
    }

    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    public void printWaterMark() {
        logWaterMark.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        logWaterMark.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("slave fall behind master, how much, {} bytes", diff);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public String getConfigDataVersion() {
        return this.configDataVersion.toJson();
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
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

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }
    }

    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(//
                this.brokerConfig.getBrokerClusterName(), //
                this.getBrokerAddr(), //
                this.brokerConfig.getBrokerName(), //
                this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        String addr = this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
        return addr;
    }

    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        this.registerBrokerAll(true, false);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) {
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
                                this.brokerConfig.getBrokerPermission());
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
                this.brokerConfig.getBrokerClusterName(), //
                this.getBrokerAddr(), //
                this.brokerConfig.getBrokerName(), //
                this.brokerConfig.getBrokerId(), //
                this.getHAServerAddr(), //
                topicConfigWrapper,//
                this.filterServerManager.buildNewFilterServerList(),//
                oneway,//
                this.brokerConfig.getRegisterBrokerTimeoutMills());

        if (registerBrokerResult != null) {
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
        }
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    public String getHAServerAddr() {
        String addr = this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
        return addr;
    }

    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        MixAll.properties2Object(properties, messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();
    }

    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        try {
            MixAll.string2File(allConfig, BrokerPathConfigHelper.getBrokerConfigPath());
            log.info("flush broker config, {} OK", BrokerPathConfigHelper.getBrokerConfigPath());
        } catch (IOException e) {
            log.info("flush broker config Exception, " + BrokerPathConfigHelper.getBrokerConfigPath(), e);
        }
    }

    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }
        return sb.toString();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
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
}
