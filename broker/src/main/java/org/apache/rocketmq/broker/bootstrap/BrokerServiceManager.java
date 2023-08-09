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
package org.apache.rocketmq.broker.bootstrap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPreOnlineService;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class BrokerServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;
    private final BrokerConfig brokerConfig;


    private ProducerManager producerManager;
    private ConsumerManager consumerManager;
    private Broker2Client broker2Client;
    private PopInflightMessageCounter popInflightMessageCounter;


    private BroadcastOffsetManager broadcastOffsetManager;
    private TopicRouteInfoManager topicRouteInfoManager;
    private BrokerFastFailure brokerFastFailure;
    private BrokerPreOnlineService brokerPreOnlineService;


    private TopicQueueMappingCleanService topicQueueMappingCleanService;

    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    private ColdDataCgCtrService coldDataCgCtrService;


    public BrokerServiceManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();

        initContext();
        initService();
    }

    private void initContext() {
        this.consumerManager = new ConsumerManager(brokerController.getBrokerNettyServer().getConsumerIdsChangeListener(), brokerController.getBrokerStatsManager(), brokerController.getBrokerConfig());
        this.producerManager = new ProducerManager(brokerController.getBrokerStatsManager());

        this.broker2Client = new Broker2Client(brokerController);
        this.popInflightMessageCounter = new PopInflightMessageCounter(brokerController);
    }

    private void initService() {
        this.broadcastOffsetManager = new BroadcastOffsetManager(brokerController);

        this.brokerFastFailure = new BrokerFastFailure(brokerController);
        this.topicRouteInfoManager = new TopicRouteInfoManager(brokerController);

        this.coldDataPullRequestHoldService = new ColdDataPullRequestHoldService(brokerController);
        this.coldDataCgCtrService = new ColdDataCgCtrService(brokerController);

        this.topicQueueMappingCleanService = new TopicQueueMappingCleanService(brokerController);

        if (this.brokerConfig.isEnableSlaveActingMaster() && !this.brokerConfig.isSkipPreOnline()) {
            this.brokerPreOnlineService = new BrokerPreOnlineService(brokerController);
        }
    }

    public void start() {
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

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.start();
        }
    }

    public void shutdown() {
        if (this.broadcastOffsetManager != null) {
            this.broadcastOffsetManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
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

        if (this.topicQueueMappingCleanService != null) {
            this.topicQueueMappingCleanService.shutdown();
        }
    }


    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public PopInflightMessageCounter getPopInflightMessageCounter() {
        return popInflightMessageCounter;
    }

    public BroadcastOffsetManager getBroadcastOffsetManager() {
        return broadcastOffsetManager;
    }

    public TopicRouteInfoManager getTopicRouteInfoManager() {
        return topicRouteInfoManager;
    }

    public BrokerFastFailure getBrokerFastFailure() {
        return brokerFastFailure;
    }

    public BrokerPreOnlineService getBrokerPreOnlineService() {
        return brokerPreOnlineService;
    }

    public ColdDataPullRequestHoldService getColdDataPullRequestHoldService() {
        return coldDataPullRequestHoldService;
    }

    public ColdDataCgCtrService getColdDataCgCtrService() {
        return coldDataCgCtrService;
    }

    public TopicQueueMappingCleanService getTopicQueueMappingCleanService() {
        return topicQueueMappingCleanService;
    }

}
