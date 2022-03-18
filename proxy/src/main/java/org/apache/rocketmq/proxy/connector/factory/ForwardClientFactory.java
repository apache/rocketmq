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
package org.apache.rocketmq.proxy.connector.factory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIExtImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.remoting.RPCHook;

public class ForwardClientFactory implements StartAndShutdown {

    private RPCHook rpcHook = null;

    private final MQClientFactory mqClientFactory;
    private final TransactionProducerFactory transactionalProducerFactory;

    public ForwardClientFactory(TransactionStateChecker transactionStateChecker) {
        this.init();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("ForwardClientFactoryScheduledThread" + "-%d").build()
        );
        this.mqClientFactory = new MQClientFactory(scheduledExecutorService, this.rpcHook);
        this.transactionalProducerFactory = new TransactionProducerFactory(scheduledExecutorService, this.rpcHook, transactionStateChecker);
    }

    private void init() {
        System.setProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY,
            System.getProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
        if (StringUtils.isEmpty(ConfigurationManager.getProxyConfig().getNameSrvDomain())) {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, ConfigurationManager.getProxyConfig().getNameSrvAddr());
        } else {
            System.setProperty("rocketmq.namesrv.domain", ConfigurationManager.getProxyConfig().getNameSrvDomain());
            System.setProperty("rocketmq.namesrv.domain.subgroup", ConfigurationManager.getProxyConfig().getNameSrvDomainSubgroup());
        }
    }

    public MQClientAPIExtImpl getMQClient(String instanceName, int bootstrapWorkerThreads) {
        return mqClientFactory.getOne(instanceName, bootstrapWorkerThreads);
    }

    public MQClientAPIExtImpl getTransactionalProducer(String instanceName, int bootstrapWorkerThreads) {
        return transactionalProducerFactory.getOne(instanceName, bootstrapWorkerThreads);
    }

    public void setRpcHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {
        this.mqClientFactory.shutdownAll();
        this.transactionalProducerFactory.shutdownAll();
    }
}
