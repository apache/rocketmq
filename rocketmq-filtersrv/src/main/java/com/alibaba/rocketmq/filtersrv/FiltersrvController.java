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
package com.alibaba.rocketmq.filtersrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import com.alibaba.rocketmq.filtersrv.filter.FilterClassManager;
import com.alibaba.rocketmq.filtersrv.processor.DefaultRequestProcessor;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;


/**
 * Name Server服务控制
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class FiltersrvController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    // Filter Server配置
    private final FiltersrvConfig filtersrvConfig;
    // 通信层配置
    private final NettyServerConfig nettyServerConfig;
    // 服务端通信层对象
    private RemotingServer remotingServer;
    // 服务端网络请求处理线程池
    private ExecutorService remotingExecutor;

    private final FilterClassManager filterClassManager = new FilterClassManager();

    // 访问Broker的api封装
    private final FilterServerOuterAPI filterServerOuterAPI = new FilterServerOuterAPI();

    private final DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(
        MixAll.FILTERSRV_CONSUMER_GROUP);

    private volatile String brokerName = null;

    // 定时线程
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NamesrvControllerScheduledThread");
            }
        });


    public FiltersrvController(FiltersrvConfig filtersrvConfig, NettyServerConfig nettyServerConfig) {
        this.filtersrvConfig = filtersrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper()
            .setConnectBrokerByUser(true);
    }


    public boolean initialize() {
        // 打印服务器配置参数
        MixAll.printObjectProperties(log, this.filtersrvConfig);

        // 初始化通信层
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

        // 初始化线程池
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "RemotingExecutorThread_" + threadIndex.incrementAndGet());
                    }
                });

        this.registerProcessor();

        // 定时向Broker注册自己
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                FiltersrvController.this.registerFilterServerToBroker();
            }
        }, 3, 10, TimeUnit.SECONDS);

        return true;
    }


    public String localAddr() {
        return String.format("%s:%d", this.filtersrvConfig.getFilterServerIP(),
            this.remotingServer.localListenPort());
    }


    public void registerFilterServerToBroker() {
        try {
            RegisterFilterServerResponseHeader responseHeader =
                    this.filterServerOuterAPI.registerFilterServerToBroker(
                        this.filtersrvConfig.getConnectWhichBroker(), this.localAddr());
            this.defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper()
                .setDefaultBrokerId(responseHeader.getBrokerId());

            if (null == this.brokerName) {
                this.brokerName = responseHeader.getBrokerName();
            }

            log.info("register filter server<%s> to broker<%s> OK, Return: %s %d", //
                this.localAddr(),//
                this.filtersrvConfig.getConnectWhichBroker(),//
                responseHeader.getBrokerName(),//
                responseHeader.getBrokerId()//
            );
        }
        catch (Exception e) {
            log.warn("register filter server Exception", e);
            // TODO 如果失败，考虑是否尝试自杀
        }
    }


    private void registerProcessor() {
        this.remotingServer
            .registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
    }


    public void start() throws Exception {
        this.defaultMQPullConsumer.start();
        this.remotingServer.start();
    }


    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.defaultMQPullConsumer.shutdown();
    }


    public RemotingServer getRemotingServer() {
        return remotingServer;
    }


    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }


    public ExecutorService getRemotingExecutor() {
        return remotingExecutor;
    }


    public void setRemotingExecutor(ExecutorService remotingExecutor) {
        this.remotingExecutor = remotingExecutor;
    }


    public FiltersrvConfig getFiltersrvConfig() {
        return filtersrvConfig;
    }


    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }


    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }


    public FilterServerOuterAPI getFilterServerOuterAPI() {
        return filterServerOuterAPI;
    }


    public FilterClassManager getFilterClassManager() {
        return filterClassManager;
    }
}
