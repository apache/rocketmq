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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.service.message.LocalMessageService;
import org.apache.rocketmq.proxy.service.message.MessageService;
import org.apache.rocketmq.proxy.service.relay.LocalProxyRelayService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.LocalTopicRouteService;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.proxy.service.transaction.LocalTransactionService;
import org.apache.rocketmq.proxy.service.transaction.TransactionService;
import org.apache.rocketmq.remoting.RPCHook;

public class LocalServiceManager extends AbstractStartAndShutdown implements ServiceManager {

    private final BrokerController brokerController;
    private final TopicRouteService topicRouteService;
    private final MessageService messageService;
    private final TransactionService transactionService;
    private final ProxyRelayService proxyRelayService;

    public LocalServiceManager(BrokerController brokerController, RPCHook rpcHook) {
        this.brokerController = brokerController;
        this.messageService = new LocalMessageService(brokerController, rpcHook);
        this.topicRouteService = new LocalTopicRouteService(brokerController, rpcHook);
        this.transactionService = new LocalTransactionService();
        this.proxyRelayService = new LocalProxyRelayService(brokerController);

        this.init();
    }

    protected void init() {
        this.appendStartAndShutdown(this.topicRouteService);
    }

    @Override
    public MessageService getMessageService() {
        return this.messageService;
    }

    @Override
    public TopicRouteService getTopicRouteService() {
        return this.topicRouteService;
    }

    @Override
    public ProducerManager getProducerManager() {
        return this.brokerController.getProducerManager();
    }

    @Override
    public ConsumerManager getConsumerManager() {
        return this.brokerController.getConsumerManager();
    }

    @Override
    public TransactionService getTransactionService() {
        return this.transactionService;
    }

    @Override
    public ProxyRelayService getProxyOutService() {
        return this.proxyRelayService;
    }

}
