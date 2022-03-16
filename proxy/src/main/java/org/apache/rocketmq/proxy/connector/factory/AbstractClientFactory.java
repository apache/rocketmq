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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClientFactory<T>  {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClientFactory.class);

    protected Map<String, T> cacheTable = new ConcurrentHashMap<>();
    protected RPCHook rpcHook;

    public AbstractClientFactory(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    protected abstract T newOne(String instanceName, RPCHook rpcHook, int bootstrapWorkerThreads) throws Throwable;

    protected abstract boolean tryStart(T t);

    protected abstract void shutdown(T t);

    protected static NettyClientConfig createNettyClientConfig(int bootstrapWorkerThreads) {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setBootstrapWorkerThreads(bootstrapWorkerThreads);
        nettyClientConfig.setDisableNettyWorkerGroup(true);
        nettyClientConfig.setDisableCallbackExecutor(true);
        return nettyClientConfig;
    }

//    @Override
    public T getOne(String instanceName, int bootstrapWorkerThreads) {
        if (cacheTable.containsKey(instanceName)) {
            return cacheTable.get(instanceName);
        }

        T object;
        try {
            object = this.newOne(instanceName, rpcHook, bootstrapWorkerThreads);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        T old = cacheTable.putIfAbsent(instanceName, object);
        if (old == null) {
            if (!this.tryStart(object)) {
                return null;
            }
        } else {
            object = old;
        }

        return object;
    }

//    @Override
    public void shutdownAll() {
        this.cacheTable.forEach((k, v) -> {
            try {
                this.shutdown(v);
            } catch (Exception e) {
                LOGGER.warn("RocketMQClientConstructor shutdown all err.", e);
            }
        });
    }
}
