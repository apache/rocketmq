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
package org.apache.rocketmq.filtersrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import org.apache.rocketmq.filtersrv.filter.FilterClassManager;
import org.apache.rocketmq.filtersrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.filtersrv.stats.FilterServerStatsManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FiltersrvController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);

    private final FiltersrvConfig filtersrvConfig;

    private final NettyServerConfig nettyServerConfig;
    private final FilterClassManager filterClassManager;

    private final FilterServerOuterAPI filterServerOuterAPI = new FilterServerOuterAPI();
    private final DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(
        MixAll.FILTERSRV_CONSUMER_GROUP);

    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FSScheduledThread"));
    private final FilterServerStatsManager filterServerStatsManager = new FilterServerStatsManager();

    private RemotingServer remotingServer;

    private ExecutorService remotingExecutor;
    private volatile String brokerName = null;

    public FiltersrvController(FiltersrvConfig filtersrvConfig, NettyServerConfig nettyServerConfig) {
        this.filtersrvConfig = filtersrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.filterClassManager = new FilterClassManager(this);
    }

    public boolean initialize() {

        MixAll.printObjectProperties(log, this.filtersrvConfig);

        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactoryImpl("RemotingExecutorThread_"));

        this.registerProcessor();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                FiltersrvController.this.registerFilterServerToBroker();
            }
        }, 3, 10, TimeUnit.SECONDS);

        this.defaultMQPullConsumer.setBrokerSuspendMaxTimeMillis(this.defaultMQPullConsumer
            .getBrokerSuspendMaxTimeMillis() - 1000);
        this.defaultMQPullConsumer.setConsumerTimeoutMillisWhenSuspend(this.defaultMQPullConsumer
            .getConsumerTimeoutMillisWhenSuspend() - 1000);

        this.defaultMQPullConsumer.setNamesrvAddr(this.filtersrvConfig.getNamesrvAddr());
        this.defaultMQPullConsumer.setInstanceName(String.valueOf(UtilAll.getPid()));

        return true;
    }

    private void registerProcessor() {
        this.remotingServer
            .registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
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

            log.info("register filter server<{}> to broker<{}> OK, Return: {} {}",
                this.localAddr(),
                this.filtersrvConfig.getConnectWhichBroker(),
                responseHeader.getBrokerName(),
                responseHeader.getBrokerId());
        } catch (Exception e) {
            log.warn("register filter server Exception", e);

            log.warn("access broker failed, kill oneself");
            System.exit(-1);
        }
    }

    public String localAddr() {
        return String.format("%s:%d", this.filtersrvConfig.getFilterServerIP(),
            this.remotingServer.localListenPort());
    }

    public void start() throws Exception {
        this.defaultMQPullConsumer.start();
        this.remotingServer.start();
        this.filterServerOuterAPI.start();
        this.defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper()
            .setConnectBrokerByUser(true);
        this.filterClassManager.start();
        this.filterServerStatsManager.start();
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.defaultMQPullConsumer.shutdown();
        this.filterServerOuterAPI.shutdown();
        this.filterClassManager.shutdown();
        this.filterServerStatsManager.shutdown();
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

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public FilterServerStatsManager getFilterServerStatsManager() {
        return filterServerStatsManager;
    }
}
