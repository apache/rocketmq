/**
 * $Id: BrokerController.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.client.ClientHousekeepingService;
import com.alibaba.rocketmq.broker.client.ConsumerManager;
import com.alibaba.rocketmq.broker.client.ProducerManager;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.processor.AdminBrokerProcessor;
import com.alibaba.rocketmq.broker.processor.ClientManageProcessor;
import com.alibaba.rocketmq.broker.processor.EndTransactionProcessor;
import com.alibaba.rocketmq.broker.processor.PullMessageProcessor;
import com.alibaba.rocketmq.broker.processor.QueryMessageProcessor;
import com.alibaba.rocketmq.broker.processor.SendMessageProcessor;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.broker.transaction.DefaultTransactionCheckExecuter;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.common.protocol.MQProtos;
import com.alibaba.rocketmq.common.protocol.MQProtosHelper;
import com.alibaba.rocketmq.remoting.RemotingServer;
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
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(MixAll.BrokerLoggerName);
    // 服务器配置
    private final BrokerConfig brokerConfig;
    // 通信层配置
    private final NettyServerConfig nettyServerConfig;
    // 存储层配置
    private final MessageStoreConfig messageStoreConfig;
    // 配置文件版本号
    private final DataVersion configDataVersion = new DataVersion();

    // 存储层对象
    private MessageStore messageStore;
    // 通信层对象
    private RemotingServer remotingServer;

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

    // Topic配置
    private TopicConfigManager topicConfigManager;
    // 处理发送消息线程池
    private ExecutorService sendMessageExecutor;
    // 处理拉取消息线程池
    private ExecutorService pullMessageExecutor;
    // 处理管理Broker线程池
    private ExecutorService adminBrokerExecutor;

    private final PullMessageProcessor pullMessageProcessor;
    private final PullRequestHoldService pullRequestHoldService;

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "BrokerControllerScheduledThread");
            }
        });


    public BrokerController(final BrokerConfig brokerConfig, final NettyServerConfig nettyServerConfig,
            final MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.consumerManager = new ConsumerManager();
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.defaultTransactionCheckExecuter = new DefaultTransactionCheckExecuter(this);
    }


    public String getBrokerAddr() {
        String addr = this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
        return addr;
    }


    private boolean registerToNameServer() {
        TopAddressing topAddressing = new TopAddressing();
        String addrs =
                (null == this.brokerConfig.getNamesrvAddr()) ? topAddressing.fetchNSAddr() : this.brokerConfig
                    .getNamesrvAddr();
        if (addrs != null) {
            String[] addrArray = addrs.split(";");

            if (addrArray != null && addrArray.length > 0) {
                Random r = new Random();
                int begin = Math.abs(r.nextInt()) % 1000000;
                for (int i = 0; i < addrArray.length; i++) {
                    String addr = addrArray[begin++ % addrArray.length];
                    boolean result =
                            MQProtosHelper.registerBrokerToNameServer(addr, this.getBrokerAddr(), 1000 * 10);
                    final String info =
                            "register broker[" + this.getBrokerAddr() + "] to name server[" + addr + "] "
                                    + (result ? " success" : " failed");
                    log.info(info);
                    System.out.println(info);
                    if (result)
                        return true;
                }
            }
        }

        return false;
    }


    public boolean initialize() {
        boolean result = true;

        // 打印服务器配置参数
        MixAll.printObjectProperties(log, this.brokerConfig);
        MixAll.printObjectProperties(log, this.nettyServerConfig);
        MixAll.printObjectProperties(log, this.messageStoreConfig);

        // 注册到Name Server
        // result = result && this.registerToNameServer();
//        registerToNameServer();
        // 加载Topic配置
        result = result && this.topicConfigManager.load();

        // 加载Consumer Offset
        result = result && this.consumerOffsetManager.load();

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
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);

            // 初始化线程池
            this.sendMessageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getSendMessageThreadPoolNums(),
                        new ThreadFactory() {

                            private AtomicInteger threadIndex = new AtomicInteger(0);


                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "SendMessageThread_" + this.threadIndex.incrementAndGet());
                            }
                        });

            this.pullMessageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getPullMessageThreadPoolNums(),
                        new ThreadFactory() {

                            private AtomicInteger threadIndex = new AtomicInteger(0);


                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "PullMessageThread_" + this.threadIndex.incrementAndGet());
                            }
                        });

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(),
                        new ThreadFactory() {

                            private AtomicInteger threadIndex = new AtomicInteger(0);


                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "AdminBrokerThread_" + this.threadIndex.incrementAndGet());
                            }
                        });

            this.registerProcessor();

            // 定时刷消费进度
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.flush();
                    }
                    catch (Exception e) {
                        log.error("", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            // 定时刷消费进度，历史记录，方便生成报表
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.flushHistory();

                        BrokerController.this.consumerOffsetManager.recordPullTPS();
                    }
                    catch (Exception e) {
                        log.error("", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetHistoryInterval(), TimeUnit.MILLISECONDS);

            // 如果是slave
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
                if (this.messageStoreConfig.getMasterAddress() != null
                        && this.messageStoreConfig.getMasterAddress().length() >= 6) {
                    this.messageStore.updateMasterAddress(this.messageStoreConfig.getMasterAddress());
                }
                else {
                    // TODO 定时去更新地址
                }
            }
        }

        return result;
    }


    public void registerProcessor() {
        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.SEND_MESSAGE_VALUE, new SendMessageProcessor(
            this), this.sendMessageExecutor);

        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.PULL_MESSAGE_VALUE,
            this.pullMessageProcessor, this.pullMessageExecutor);

        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.QUERY_MESSAGE_VALUE, queryProcessor,
            this.pullMessageExecutor);
        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.VIEW_MESSAGE_BY_ID_VALUE, queryProcessor,
            this.pullMessageExecutor);

        NettyRequestProcessor clientProcessor = new ClientManageProcessor(this);
        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.HEART_BEAT_VALUE, clientProcessor,
            this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.UNREGISTER_CLIENT_VALUE, clientProcessor,
            this.adminBrokerExecutor);

        this.remotingServer.registerProcessor(MQProtos.MQRequestCode.END_TRANSACTION_VALUE,
            new EndTransactionProcessor(this), this.sendMessageExecutor);

        this.remotingServer.registerDefaultProcessor(new AdminBrokerProcessor(this), this.adminBrokerExecutor);
    }


    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }
    }


    public void shutdown() {
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

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        this.consumerOffsetManager.flush();
    }


    public MessageStore getMessageStore() {
        return messageStore;
    }


    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }


    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }


    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }


    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }


    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();
    }


    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        boolean result = MixAll.string2File(allConfig, this.brokerConfig.getConfigFilePath());
        log.info("flush topic config, " + this.brokerConfig.getConfigFilePath() + (result ? " OK" : " Failed"));
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

        return sb.toString();
    }


    public String getConfigDataVersion() {
        return this.configDataVersion.currentVersion();
    }


    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }


    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }


    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }


    public ProducerManager getProducerManager() {
        return producerManager;
    }


    public DefaultTransactionCheckExecuter getDefaultTransactionCheckExecuter() {
        return defaultTransactionCheckExecuter;
    }
}
