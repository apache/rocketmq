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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPreOnlineService;
import org.apache.rocketmq.broker.ShutdownHook;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.coldctr.ColdDataCgCtrService;
import org.apache.rocketmq.broker.coldctr.ColdDataPullRequestHoldService;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.metrics.BrokerMetricsManager;
import org.apache.rocketmq.broker.offset.BroadcastOffsetManager;
import org.apache.rocketmq.broker.plugin.BrokerAttachedPlugin;
import org.apache.rocketmq.broker.processor.PopInflightMessageCounter;
import org.apache.rocketmq.broker.topic.TopicQueueMappingCleanService;
import org.apache.rocketmq.broker.topic.TopicRouteInfoManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.apache.rocketmq.store.stats.LmqBrokerStatsManager;

/**
 * manager services type like
 *      1, context style: getter and setter
 *      2, have methods: start and shutdown
 *      3, monitor service:
 *      4, broker plugins
 *      5, shutdown hook
 */
public class BrokerServiceManager {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;
    private final BrokerConfig brokerConfig;
    private final MessageStoreConfig messageStoreConfig;

    /* ServiceWithGetterAndSetter start */
    private ProducerManager producerManager;
    private ConsumerManager consumerManager;
    private Broker2Client broker2Client;
    private PopInflightMessageCounter popInflightMessageCounter;
    /* ServiceWithGetterAndSetter end */

    /* ServiceWithStartAndShutdown start */
    private BroadcastOffsetManager broadcastOffsetManager;
    private TopicRouteInfoManager topicRouteInfoManager;
    private BrokerFastFailure brokerFastFailure;
    private BrokerPreOnlineService brokerPreOnlineService;
    private TopicQueueMappingCleanService topicQueueMappingCleanService;

    private ColdDataPullRequestHoldService coldDataPullRequestHoldService;
    private ColdDataCgCtrService coldDataCgCtrService;
    /* ServiceWithStartAndShutdown start */

    /* monitor servie start */
    private BrokerStatsManager brokerStatsManager;
    private BrokerMetricsManager brokerMetricsManager;
    private BrokerStats brokerStats;
    /* monitor servie end */

    /*  broker plugin start */
    private List<BrokerAttachedPlugin> brokerAttachedPlugins = new ArrayList<>();
    /*  broker plugin end */

    private ShutdownHook shutdownHook;

    public BrokerServiceManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerController.getBrokerConfig();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();

        initMonitorService();
        initServiceWithGetterAndSetter();
        initServiceWithStartAndShutdown();
        initBrokerPlugin();
    }

    public boolean load() {
        this.brokerMetricsManager = new BrokerMetricsManager(brokerController);
        this.brokerStats = new BrokerStats(brokerController.getBrokerMessageService().getMessageStore());

        boolean result = true;
        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                result = result && brokerAttachedPlugin.load();
            }
        }

        return result;
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

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.start();
            }
        }

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
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

        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        for (BrokerAttachedPlugin brokerAttachedPlugin : brokerAttachedPlugins) {
            if (brokerAttachedPlugin != null) {
                brokerAttachedPlugin.shutdown();
            }
        }

        if (this.shutdownHook != null) {
            this.shutdownHook.beforeShutdown(brokerController);
        }
    }

    private void initServiceWithGetterAndSetter() {
        this.consumerManager = new ConsumerManager(brokerController.getBrokerNettyServer().getConsumerIdsChangeListener(), brokerStatsManager, brokerController.getBrokerConfig());
        this.producerManager = new ProducerManager(brokerStatsManager);

        this.broker2Client = new Broker2Client(brokerController);
        this.popInflightMessageCounter = new PopInflightMessageCounter(brokerController);
    }

    private void initServiceWithStartAndShutdown() {
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

    private void initMonitorService() {

        this.brokerStatsManager = messageStoreConfig.isEnableLmq()
            ? new LmqBrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat())
            : new BrokerStatsManager(this.brokerConfig.getBrokerClusterName(), this.brokerConfig.isEnableDetailStat());

        initProducerStateGetter();
        initConsumerStateGetter();
    }

    private void initProducerStateGetter() {
        this.brokerStatsManager.setProduerStateGetter(new BrokerStatsManager.StateGetter() {
            @Override
            public boolean online(String instanceId, String group, String topic) {
                if (brokerController.getTopicConfigManager().getTopicConfigTable().containsKey(NamespaceUtil.wrapNamespace(instanceId, topic))) {
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
                if (brokerController.getTopicConfigManager().getTopicConfigTable().containsKey(topicFullName)) {
                    return getConsumerManager().findSubscriptionData(NamespaceUtil.wrapNamespace(instanceId, group), topicFullName) != null;
                } else {
                    return getConsumerManager().findSubscriptionData(group, topic) != null;
                }
            }
        });
    }

    private void initBrokerPlugin() {

    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public ShutdownHook getShutdownHook() {
        return shutdownHook;
    }

    public void setShutdownHook(ShutdownHook shutdownHook) {
        this.shutdownHook = shutdownHook;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public BrokerMetricsManager getBrokerMetricsManager() {
        return brokerMetricsManager;
    }

    public List<BrokerAttachedPlugin> getBrokerAttachedPlugins() {
        return brokerAttachedPlugins;
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
