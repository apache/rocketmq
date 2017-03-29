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
package org.apache.rocketmq.client.impl;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQ Client管理器
 * 维护clientId 与 MQClient对象的关系
 */
public class MQClientManager {
    private final static Logger log = ClientLogger.getLog();
    /**
     * 单例对象
     */
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    /**
     * clientId 与 MQClient对象 映射Map
     */
    private ConcurrentHashMap<String/* clientId */, MQClientInstance> factoryTable = new ConcurrentHashMap<>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 获得 MQClient 对象。如果不存在，则进行创建。
     *
     * @param clientConfig client配置
     * @param rpcHook rpcHook
     * @return MQ Client Instance
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
