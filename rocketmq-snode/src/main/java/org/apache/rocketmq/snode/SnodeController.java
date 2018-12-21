package org.apache.rocketmq.snode;/*
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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.RemotingServerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.snode.client.ClientHousekeepingService;
import org.apache.rocketmq.snode.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.snode.client.ConsumerManager;
import org.apache.rocketmq.snode.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.snode.client.ProducerManager;
import org.apache.rocketmq.snode.client.SubscriptionGroupManager;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.processor.ConsumerManageProcessor;
import org.apache.rocketmq.snode.processor.HearbeatProcessor;
import org.apache.rocketmq.snode.processor.PullMessageProcessor;
import org.apache.rocketmq.snode.processor.SendMessageProcessor;
import org.apache.rocketmq.snode.service.ScheduledService;
import org.apache.rocketmq.snode.service.SnodeOuterService;
import org.apache.rocketmq.snode.service.impl.ScheduledServiceImpl;
import org.apache.rocketmq.snode.service.impl.SnodeOuterServiceImpl;

public class SnodeController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeConfig snodeConfig;
    private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;
    private RemotingServer snodeServer;
    private ExecutorService sendMessageExcutor;
    private ExecutorService heartbeatExecutor;
    private ExecutorService pullMessageExcutor;
    private SnodeOuterService snodeOuterService;
    private ExecutorService consumerManagerExcutor;
    private ScheduledService scheduledService;
    private ProducerManager producerManager;
    private ConsumerManager consumerManager;
    private ClientHousekeepingService clientHousekeepingService;
    private SubscriptionGroupManager subscriptionGroupManager;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "SnodeControllerScheduledThread"));

    public SnodeController(NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig,
        SnodeConfig snodeConfig) {
        this.nettyClientConfig = nettyClientConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.snodeConfig = snodeConfig;
        this.snodeOuterService = SnodeOuterServiceImpl.getInstance(this);
        this.scheduledService = new ScheduledServiceImpl(this.snodeOuterService, this.snodeConfig);
        this.sendMessageExcutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodeSendMessageThread",
            false);

        this.pullMessageExcutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodePullMessageThread",
            false);

        this.heartbeatExecutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeHeartBeatCorePoolSize(),
            snodeConfig.getSnodeHeartBeatMaxPoolSize(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(snodeConfig.getSnodeHeartBeatThreadPoolQueueCapacity()),
            "SnodeHeartbeatThread",
            true);

        this.consumerManagerExcutor = ThreadUtils.newThreadPoolExecutor(
            snodeConfig.getSnodeSendMessageMinPoolSize(),
            snodeConfig.getSnodeSendMessageMaxPoolSize(),
            3000,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(snodeConfig.getSnodeSendThreadPoolQueueCapacity()),
            "SnodePullMessageThread",
            false);

        if (this.snodeConfig.getNamesrvAddr() != null) {
            this.snodeOuterService.updateNameServerAddressList(this.snodeConfig.getNamesrvAddr());
            log.info("Set user specified name server address: {}", this.snodeConfig.getNamesrvAddr());
        }

        this.producerManager = new ProducerManager();

        ConsumerIdsChangeListener consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(consumerIdsChangeListener);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.clientHousekeepingService = new ClientHousekeepingService(this.producerManager, this.consumerManager);
    }

    public SnodeConfig getSnodeConfig() {
        return snodeConfig;
    }

    public boolean initialize() {
        this.snodeServer = RemotingServerFactory.createInstance().init(this.nettyServerConfig, null);
        this.registerProcessor();
        return true;
    }

    public void registerProcessor() {
        snodeServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, new SendMessageProcessor(this), sendMessageExcutor);
        snodeServer.registerProcessor(RequestCode.HEART_BEAT, new HearbeatProcessor(this), heartbeatExecutor);
        snodeServer.registerProcessor(RequestCode.SNODE_PULL_MESSAGE, new PullMessageProcessor(this), pullMessageExcutor);
        snodeServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, new ConsumerManageProcessor(this), consumerManagerExcutor);
    }

    public void start() {
        initialize();
        this.snodeServer.start();
        this.snodeOuterService.start();
        this.scheduledService.startScheduleTask();
        this.clientHousekeepingService.start(this.snodeConfig.getHouseKeepingInterval());
    }

    public void shutdown() {
        this.sendMessageExcutor.shutdown();
        this.pullMessageExcutor.shutdown();
        this.heartbeatExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.snodeOuterService.shutdown();
        this.scheduledService.shutdown();
        this.clientHousekeepingService.shutdown();
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setProducerManager(ProducerManager producerManager) {
        this.producerManager = producerManager;
    }

    public RemotingServer getSnodeServer() {
        return snodeServer;
    }

    public void setSnodeServer(RemotingServer snodeServer) {
        this.snodeServer = snodeServer;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public void setConsumerManager(ConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void setSubscriptionGroupManager(SubscriptionGroupManager subscriptionGroupManager) {
        this.subscriptionGroupManager = subscriptionGroupManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public SnodeOuterService getSnodeOuterService() {
        return snodeOuterService;
    }

    public void setSnodeOuterService(SnodeOuterService snodeOuterService) {
        this.snodeOuterService = snodeOuterService;
    }
}
