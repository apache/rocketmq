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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;

/***
 * 主要整个应用中的MQClientInstance实例，
 * 每个ip下的每个应用实例映射一个MQClientInstance实例
 */
public class MQClientManager {
    private final static Logger log = ClientLogger.getLog();
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    //根据clientId存放客户端实例，每个ip只能有一个，不能重复创建
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /***
     * 根据clientId维护客户端实例，clienId=ip+"@"+instanceName
     * 所以同一个ip地址发起的请求基本都就共享一个 MQClientInstance
     * @param clientConfig
     * @param rpcHook
     * @return
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        String clientId = clientConfig.buildMQClientId();//根据客户端ip 创建客户端ID:ip+"@"+instanceName
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            //一个ip只能创建一个MQClientInstance实例，也就是不能重复创建
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {//表示该ip下已经有MQClientInstance，直接使用即可
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {//表示新建MQClientInstance
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
