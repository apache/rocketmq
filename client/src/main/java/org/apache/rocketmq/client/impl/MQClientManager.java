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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    private static MQClientManager instance;
    private AtomicInteger factoryIndexGenerator;
    private ConcurrentMap<String, MQClientInstance> factoryTable;
    private ConcurrentMap<String, Integer> nameSrvCodeTable;
    private ConcurrentMap<String, AtomicInteger> producerCodeTable;
    private ConcurrentMap<String, AtomicInteger> consumerCodeTable;
    private ConcurrentMap<String, AtomicInteger> adminCodeTable;
    private AtomicInteger nameSrvIndexGenerator;
    private String factoryIndex;

    private MQClientManager() {
        this.factoryIndexGenerator = new AtomicInteger(0);
        this.factoryTable = new ConcurrentHashMap<>();
        this.nameSrvCodeTable = new ConcurrentHashMap<>();
        this.producerCodeTable = new ConcurrentHashMap<>();
        this.consumerCodeTable = new ConcurrentHashMap<>();
        this.adminCodeTable = new ConcurrentHashMap<>();
        this.nameSrvIndexGenerator = new AtomicInteger(0);
        this.factoryIndex = UUID.randomUUID().toString();
    }

    public static MQClientManager getInstance() {
        if (instance != null) {
            return instance;
        }

        synchronized (MQClientManager.class) {
            if (instance == null) {
                instance = new MQClientManager();
            }
        }

        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        this.markIndex(clientConfig);
        MQClientInstance instance = getMQClientInstance(clientConfig);
        if (instance == null) {
            instance = createMQClientInstance(clientConfig, rpcHook);
        }

        return instance;
    }

    public MQClientInstance getMQClientInstance(final ClientConfig clientConfig) {
        String clientId = clientConfig.buildMQClientId();
        return this.factoryTable.get(clientId);
    }

    public MQClientInstance createMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = new MQClientInstance(clientConfig.cloneClientConfig(),
            this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
        MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
        if (prev != null) {
            instance = prev;
            log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
        } else {
            log.info("Created new MQClientInstance for clientId:[{}]", clientId);
        }
        return instance;
    }

    private void markIndex(final ClientConfig clientConfig) {
        if (clientConfig == null) {
            return;
        }

        if (StringUtils.isBlank(clientConfig.getFactoryIndex())) {
            clientConfig.setFactoryIndex(this.factoryIndex);
        }

        if (clientConfig.getNameSrvCode() < 0) {
            this.markNameSrvCode(clientConfig);
        }

        if (clientConfig.getGroupCode() < 0) {
            this.markGroupCode(clientConfig);
        }
    }

    private void markNameSrvCode(final ClientConfig clientConfig) {
        if (StringUtils.isBlank(clientConfig.getNamesrvAddr())) {
            return;
        }

        String namesrv = clientConfig.getNamesrvAddr();
        if (!this.nameSrvCodeTable.containsKey(namesrv)) {
            synchronized (this.nameSrvCodeTable) {
                if (!this.nameSrvCodeTable.containsKey(namesrv)) {
                    this.nameSrvCodeTable.put(namesrv, this.nameSrvIndexGenerator.getAndIncrement());
                }
            }
        }

        clientConfig.setNameSrvCode(this.nameSrvCodeTable.get(namesrv));
    }

    private void markGroupCode(final ClientConfig clientConfig) {
        if (clientConfig.clientType() == null) {
            return;
        }

        AtomicInteger indexGenerator = null;
        String key = buildGroupTableKey(clientConfig.getNameSrvCode(), clientConfig.groupName());
        switch (clientConfig.clientType()) {
            case DEFAULT_PRODUCER:
            case TRANSACTION_MQ_PRODUCER:
                indexGenerator = getGroupCodeGenerator(producerCodeTable, key);
                break;
            case DEFAULT_PUSH_CONSUMER:
            case DEFAULT_PULL_CONSUMER:
            case DEFAULT_LITE_PULL_CONSUMER:
                indexGenerator = getGroupCodeGenerator(consumerCodeTable, key);
                break;
            case DEFAULT_MQ_ADMIN_EXT:
                indexGenerator = getGroupCodeGenerator(adminCodeTable, key);
                break;
            default:
                break;
        }

        if (indexGenerator != null) {
            clientConfig.setGroupCode(indexGenerator.getAndIncrement());
        }
    }

    private AtomicInteger getGroupCodeGenerator(ConcurrentMap<String, AtomicInteger> groupCodeTable, String key) {
        if (groupCodeTable.containsKey(key)) {
            return groupCodeTable.get(key);
        }

        AtomicInteger indexGenerator = new AtomicInteger(0);

        AtomicInteger prev = groupCodeTable.putIfAbsent(key, indexGenerator);
        if (prev != null) {
            indexGenerator = prev;
        }

        return indexGenerator;
    }

    public static String buildGroupTableKey(int nameSrvCode, String group) {
        return String.format("%d@%s", nameSrvCode, group);
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
