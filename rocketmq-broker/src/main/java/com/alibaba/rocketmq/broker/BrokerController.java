/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.client.ClientHousekeepingService;
import com.alibaba.rocketmq.broker.client.ConsumerIdsChangeListener;
import com.alibaba.rocketmq.broker.client.ConsumerManager;
import com.alibaba.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import com.alibaba.rocketmq.broker.client.ProducerManager;
import com.alibaba.rocketmq.broker.client.net.Broker2Client;
import com.alibaba.rocketmq.broker.client.rebalance.RebalanceLockManager;
import com.alibaba.rocketmq.broker.filtersrv.FilterServerManager;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.out.BrokerOuterAPI;
import com.alibaba.rocketmq.broker.processor.AdminBrokerProcessor;
import com.alibaba.rocketmq.broker.processor.ClientManageProcessor;
import com.alibaba.rocketmq.broker.processor.EndTransactionProcessor;
import com.alibaba.rocketmq.broker.processor.PullMessageProcessor;
import com.alibaba.rocketmq.broker.processor.QueryMessageProcessor;
import com.alibaba.rocketmq.broker.processor.SendMessageProcessor;
import com.alibaba.rocketmq.broker.slave.SlaveSynchronize;
import com.alibaba.rocketmq.broker.stats.BrokerStats;
import com.alibaba.rocketmq.broker.stats.BrokerStatsManager;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.broker.transaction.DefaultTransactionCheckExecuter;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;


/**
 * Broker各个服务控制器
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    // 服务器配置
    private final BrokerConfig brokerConfig;
    // 通信层配置
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    // 存储层配置
    private final MessageStoreConfig messageStoreConfig;
    // 配置文件版本号
    private final DataVersion configDataVersion = new DataVersion();
    // 消费进度存储
    private final ConsumerOffsetManager consumerOffsetManager;
    // Consumer连接、订阅关系管理
    private final ConsumerManager consumerManager;
    // Producer连接管理
    private final ProducerManager producerManager;
    // 检测所有客户端连接
    private final ClientHousekeepingService clientHousekeepingService;
    // Broker主动回查Producer事务状态
    private final DefaultTransactionCheckExecuter defaultTransactionCheckExecuter;
    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;
    // Broker主动调用Client
    private final Broker2Client broker2Client;
    // 订阅组配置管理
    private final SubscriptionGroupManager subscriptionGroupManager;
    // 订阅组内成员发生变化，立刻通知所有成员
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    // 管理队列的锁分配
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    // Broker的通信层客户端
    private final BrokerOuterAPI brokerOuterAPI;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerControllerScheduledThread"));
    // Slave定期从Master同步信息
    private final SlaveSynchronize slaveSynchronize;
    // 存储层对象
    private MessageStore messageStore;
    // 通信层对象
    private RemotingServer remotingServer;
    // Topic配置
    private TopicConfigManager topicConfigManager;
    // 处理发送消息线程池
    private ExecutorService sendMessageExecutor;
    // 处理拉取消息线程池
    private ExecutorService pullMessageExecutor;
    // 处理管理Broker线程池
    private ExecutorService adminBrokerExecutor;
    // 是否需要定期更新HA Master地址
    private boolean updateMasterHAServerAddrPeriodically = false;

    private BrokerStats brokerStats;
    // 对消息写入进行流控
    private final BlockingQueue<Runnable> sendThreadPoolQueue;

    // 对消息读取进行流控
    private final BlockingQueue<Runnable> pullThreadPoolQueue;

    // FilterServer管理
    private final FilterServerManager filterServerManager;

    private final BrokerStatsManager brokerStatsManager;


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
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.defaultTransactionCheckExecuter = new DefaultTransactionCheckExecuter(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        this.slaveSynchronize = new SlaveSynchronize(this);

        this.sendThreadPoolQueue =
                new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        this.pullThreadPoolQueue =
                new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        this.brokerStatsManager = new BrokerStatsManager();
    }


    public boolean initialize() {
        boolean result = true;

        // 加载Topic配置
        result = result && this.topicConfigManager.load();

        // 加载Consumer Offset
        result = result && this.consumerOffsetManager.load();
        // 加载Consumer subscription
        result = result && this.subscriptionGroupManager.load();

        // 初始化存储层
        if (result) {
            try {
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.defaultTransactionCheckExecuter);
            }
            catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }

        // 加载本地消息数据
        result = result && this.messageStore.load();

        if (result) {
            // 初始化通信层
            this.remotingServer =
                    new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);

            // 初始化线程池
            this.sendMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                this.brokerConfig.getSendMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.sendThreadPoolQueue,//
                new ThreadFactoryImpl("SendMessageThread_"));

            this.pullMessageExecutor = new ThreadPoolExecutor(//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                this.brokerConfig.getPullMessageThreadPoolNums(),//
                1000 * 60,//
                TimeUnit.MILLISECONDS,//
                this.pullThreadPoolQueue,//
                new ThreadFactoryImpl("PullMessageThread_"));

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(),
                        new ThreadFactoryImpl("AdminBrokerThread_"));

            this.registerProcessor();

            this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);

            // 每天凌晨00:00:00统计消息量
            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    }
                    catch (Exception e) {
                        log.error("", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            // 定时刷消费进度
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    }
                    catch (Exception e) {
                        log.error("", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            // 定时打印各个消费组的消费速度
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.recordPullTPS();
                    }
                    catch (Exception e) {
                        log.error("recordPullTPS Exception", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetHistoryInterval(), TimeUnit.MILLISECONDS);

            // 先获取Name Server地址
            if (this.brokerConfig.getNamesrvAddr() != null) {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            // 定时获取Name Server地址
            else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            // 如果是slave
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getHaMasterAddress() != null
                        && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                }
                else {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                // Slave定时从Master同步配置信息
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        }
                        catch (Exception e) {
                            log.error("ScheduledTask syncAll slave exception", e);
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
        NettyRequestProcessor sendProcessor = new SendMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor,
            this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor,
            this.sendMessageExecutor);

        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor,
            this.pullMessageExecutor);

        /**
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor,
            this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor,
            this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        NettyRequestProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor,
            this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor,
            this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor,
            this.adminBrokerExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this),
            this.sendMessageExecutor);

        /**
         * Default
         */
        this.remotingServer
            .registerDefaultProcessor(new AdminBrokerProcessor(this), this.adminBrokerExecutor);
    }


    public Broker2Client getBroker2Client() {
        return broker2Client;
    }


    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
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


    public DefaultTransactionCheckExecuter getDefaultTransactionCheckExecuter() {
        return defaultTransactionCheckExecuter;
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }


    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public ProducerManager getProducerManager() {
        return producerManager;
    }


    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }


    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
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

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
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

        // 启动时，强制注册
        this.registerBrokerAll();

        // 定时注册Broker到Name Server
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll();
                }
                catch (Exception e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }
    }


    public synchronized void registerBrokerAll() {
        TopicConfigSerializeWrapper topicConfigWrapper =
                this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(//
            this.brokerConfig.getBrokerClusterName(), //
            this.getBrokerAddr(), //
            this.brokerConfig.getBrokerName(), //
            this.brokerConfig.getBrokerId(), //
            this.getHAServerAddr(), //
            topicConfigWrapper,//
            this.filterServerManager.buildNewFilterServerList()//
            );

        if (registerBrokerResult != null) {
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
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
            MixAll.string2File(allConfig, this.brokerConfig.getBrokerConfigPath());
            log.info("flush broker config, {} OK", this.brokerConfig.getBrokerConfigPath());
        }
        catch (IOException e) {
            log.info("flush broker config Exception, " + this.brokerConfig.getBrokerConfigPath(), e);
        }
    }


    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            }
            else {
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


    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }


    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }


    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }


    public BrokerStats getBrokerStats() {
        return brokerStats;
    }


    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
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

}
