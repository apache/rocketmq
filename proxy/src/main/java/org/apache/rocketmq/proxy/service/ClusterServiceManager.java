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
package org.apache.rocketmq.proxy.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.proxy.service.message.ClusterMessageService;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.mqclient.DoNothingClientRemotingProcessor;
import org.apache.rocketmq.proxy.service.relay.ClusterProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ClusterTopicRouteService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.ClusterTransactionService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.RPCHook;

public class ClusterServiceManager extends AbstractStartAndShutdown implements ServiceManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ClusterTransactionService clusterTransactionService;
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;
    private final TopicRouteService topicRouteService;
    private final MessageService messageService;
    private final ProxyRelayService proxyRelayService;

    private final ScheduledExecutorService scheduledExecutorService;
    private final MQClientAPIFactory mqClientAPIFactory;

    public ClusterServiceManager(RPCHook rpcHook) {
        this.scheduledExecutorService = Executors.newScheduledThreadPool(3);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager(new ConsumerIdsChangeListenerImpl());

        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.mqClientAPIFactory = new MQClientAPIFactory(
            "CLUSTER_MQ_CLIENT_",
            proxyConfig.getRocketmqMQClientNum(),
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            scheduledExecutorService);

        this.topicRouteService = new ClusterTopicRouteService(rpcHook);
        this.messageService = new ClusterMessageService(this.topicRouteService, this.mqClientAPIFactory);
        this.clusterTransactionService = new ClusterTransactionService(this.topicRouteService, this.producerManager, rpcHook);
        this.proxyRelayService = new ClusterProxyRelayService();

        this.init();
    }

    protected void init() {
        this.producerManager.appendProducerChangeListener(new ProducerChangeListenerImpl());

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                producerManager.scanNotActiveChannel();
                consumerManager.scanNotActiveChannel();
            } catch (Throwable e) {
                log.error("Error occurred when scan not active client channels.", e);
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);

        this.appendShutdown(scheduledExecutorService::shutdown);
        this.appendStartAndShutdown(this.mqClientAPIFactory);
        this.appendStartAndShutdown(this.topicRouteService);
        this.appendStartAndShutdown(this.clusterTransactionService);
    }

    @Override
    public MessageService getMessageService() {
        return this.messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.producerManager;
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return this.consumerManager;
    }

    @Override
    public TransactionService getTransactionService() {
        return this.clusterTransactionService;
    }

    @Override
    public ProxyRelayService getProxyOutService() {
        return this.proxyRelayService;
    }

    protected static class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {

        }

        @Override
        public void shutdown() {

        }
    }

    protected class ProducerChangeListenerImpl implements ProducerChangeListener {
        @Override
        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
            if (event == ProducerGroupEvent.GROUP_UNREGISTER) {
                getTransactionService().unSubscribeAllTransactionTopic(group);
            }
        }
    }
}
