/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.client.impl;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.remoting.RPCHook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author shijia.wxr
 */
public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<String/* clientId */, MQClientInstance> factoryTable =
            new ConcurrentHashMap<String, MQClientInstance>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

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
            } else {
                // TODO log
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
