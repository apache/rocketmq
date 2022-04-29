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
import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;

public abstract class AbstractClientManager<T>  {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final ScheduledExecutorService scheduledExecutorService;
    protected Map<String, T> cacheTable = new ConcurrentHashMap<>();
    protected RPCHook rpcHook;

    public AbstractClientManager(ScheduledExecutorService scheduledExecutorService, RPCHook rpcHook) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.rpcHook = rpcHook;
    }

    protected abstract T newOne(String instanceName, RPCHook rpcHook, int bootstrapWorkerThreads);

    protected abstract boolean tryStart(T t);

    protected abstract void shutdown(T t);

    protected static NettyClientConfig createNettyClientConfig(int bootstrapWorkerThreads) {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setBootstrapWorkerThreads(bootstrapWorkerThreads);
        nettyClientConfig.setDisableNettyWorkerGroup(true);
        nettyClientConfig.setDisableCallbackExecutor(true);
        return nettyClientConfig;
    }

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

    public void shutdownAll() {
        this.cacheTable.forEach((k, v) -> {
            try {
                this.shutdown(v);
            } catch (Exception e) {
                log.warn("try to shutdown client err.", e);
            }
        });
    }
}
