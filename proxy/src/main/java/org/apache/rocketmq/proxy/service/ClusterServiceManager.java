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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.client.common.NameserverAccessConfig;
import org.apache.rocketmq.client.impl.mqclient.DoNothingClientRemotingProcessor;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.ObjectCreator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.admin.DefaultAdminService;
import org.apache.rocketmq.proxy.service.client.ClusterConsumerManager;
import org.apache.rocketmq.proxy.service.client.ProxyClientRemotingProcessor;
import org.apache.rocketmq.proxy.service.message.ClusterMessageService;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.metadata.ClusterMetadataService;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ClusterProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ClusterTopicRouteService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.ClusterTransactionService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;

public class ClusterServiceManager extends AbstractStartAndShutdown implements ServiceManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected ClusterTransactionService clusterTransactionService;
    protected ProducerManager producerManager;
    protected ClusterConsumerManager consumerManager;
    protected TopicRouteService topicRouteService;
    protected MessageService messageService;
    protected ProxyRelayService proxyRelayService;
    protected ClusterMetadataService metadataService;
    protected AdminService adminService;

    protected ScheduledExecutorService scheduledExecutorService;
    protected MQClientAPIFactory messagingClientAPIFactory;
    protected MQClientAPIFactory operationClientAPIFactory;
    protected MQClientAPIFactory transactionClientAPIFactory;

    public ClusterServiceManager(RPCHook rpcHook) {
        this(rpcHook, null);
    }

    public ClusterServiceManager(RPCHook rpcHook, ObjectCreator<RemotingClient> remotingClientCreator) {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        NameserverAccessConfig nameserverAccessConfig = new NameserverAccessConfig(proxyConfig.getNamesrvAddr(),
            proxyConfig.getNamesrvDomain(), proxyConfig.getNamesrvDomainSubgroup());
        this.scheduledExecutorService = ThreadUtils.newScheduledThreadPool(3);

        this.messagingClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "ClusterMQClient_",
            proxyConfig.getRocketmqMQClientNum(),
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            scheduledExecutorService,
            remotingClientCreator
        );

        this.operationClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "OperationClient_",
            1,
            new DoNothingClientRemotingProcessor(null),
            rpcHook,
            this.scheduledExecutorService,
            remotingClientCreator
        );

        this.topicRouteService = new ClusterTopicRouteService(operationClientAPIFactory);
        this.messageService = new ClusterMessageService(this.topicRouteService, this.messagingClientAPIFactory);
        this.metadataService = new ClusterMetadataService(topicRouteService, operationClientAPIFactory);
        this.adminService = new DefaultAdminService(this.operationClientAPIFactory);

        this.producerManager = new ProducerManager();
        this.consumerManager = new ClusterConsumerManager(this.topicRouteService, this.adminService, this.operationClientAPIFactory, new ConsumerIdsChangeListenerImpl(), proxyConfig.getChannelExpiredTimeout(), rpcHook);

        this.transactionClientAPIFactory = new MQClientAPIFactory(
            nameserverAccessConfig,
            "ClusterTransaction_",
            1,
            new ProxyClientRemotingProcessor(producerManager),
            rpcHook,
            scheduledExecutorService,
            remotingClientCreator
        );

        this.clusterTransactionService = new ClusterTransactionService(this.topicRouteService, this.producerManager,
            this.transactionClientAPIFactory);
        this.proxyRelayService = new ClusterProxyRelayService(this.clusterTransactionService);

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
        this.appendStartAndShutdown(this.messagingClientAPIFactory);
        this.appendStartAndShutdown(this.operationClientAPIFactory);
        this.appendStartAndShutdown(this.transactionClientAPIFactory);
        this.appendStartAndShutdown(this.topicRouteService);
        this.appendStartAndShutdown(this.clusterTransactionService);
        this.appendStartAndShutdown(this.metadataService);
        this.appendStartAndShutdown(this.consumerManager);
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
    public ProxyRelayService getProxyRelayService() {
        return this.proxyRelayService;
    }

    @Override
    public MetadataService getMetadataService() {
        return this.metadataService;
    }

    @Override
    public AdminService getAdminService() {
        return this.adminService;
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
                getTransactionService().unSubscribeAllTransactionTopic(ProxyContext.createForInner(this.getClass()), group);
            }
        }
    }
}
