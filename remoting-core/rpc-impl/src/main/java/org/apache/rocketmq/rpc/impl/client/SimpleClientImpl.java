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

package org.apache.rocketmq.rpc.impl.client;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.RemotingService;
import org.apache.rocketmq.remoting.api.interceptor.Interceptor;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.netty.RemotingBootstrapFactory;
import org.apache.rocketmq.rpc.api.AdvancedClient;
import org.apache.rocketmq.rpc.api.SimpleClient;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;
import org.apache.rocketmq.rpc.impl.exception.ServiceExceptionManager;
import org.apache.rocketmq.rpc.impl.service.RpcConnectionListener;
import org.apache.rocketmq.rpc.impl.service.RpcInstanceAbstract;
import org.apache.rocketmq.rpc.impl.service.RpcProxyFactory;
import org.apache.rocketmq.rpc.internal.RpcErrorMapper;

public class SimpleClientImpl extends RpcInstanceAbstract implements SimpleClient {
    private RpcCommonConfig rpcCommonConfig;
    private RemotingClient remotingClient;
    private ExecutorService callServiceThreadPool;

    public SimpleClientImpl(RpcCommonConfig rpcCommonConfig) {
        this(rpcCommonConfig, RemotingBootstrapFactory.createRemotingClient(rpcCommonConfig));
    }

    private SimpleClientImpl(RpcCommonConfig rpcCommonConfig, RemotingClient remotingClient) {
        super(rpcCommonConfig);
        this.remotingClient = remotingClient;
        this.rpcCommonConfig = rpcCommonConfig;
        this.callServiceThreadPool = ThreadUtils.newFixedThreadPool(
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            rpcCommonConfig.getServiceThreadBlockQueueSize(),
            "RPC-ClientCallServiceThread", true);
    }

    public void initialize() {
        this.remotingClient.registerChannelEventListener(new RpcConnectionListener(this));
    }

    @Override
    public void start() {
        try {
            initialize();
            super.start();
            this.remotingClient.start();
        } catch (Exception e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
            ThreadUtils.shutdownGracefully(this.callServiceThreadPool, 3000, TimeUnit.MILLISECONDS);
            this.remotingClient.stop();
        } catch (Exception e) {
            throw ServiceExceptionManager.TranslateException(RpcErrorMapper.RpcErrorLatitude.LOCAL.getCode(), e);
        }
    }

    @Override
    public <T> T bind(final Class<T> service, final String address, final Properties properties) {
        return this.narrow0(service, RpcProxyFactory.createServiceProxy(service, this, remotingClient, rpcCommonConfig, address));
    }

    @Override
    public void publish(final Object service) {
        this.publishService0(service);
    }

    @Override
    public void publish(final Object service, final ExecutorService executorService) {
        this.publishService0(service, executorService);
    }

    @Override
    public AdvancedClient advancedClient() {
        return new AdvancedClientImpl(this);
    }

    @Override
    public RemotingService remotingService() {
        return this.remotingClient;
    }

    @Override
    public void registerServiceListener() {

    }

    public ExecutorService getCallServiceThreadPool() {
        return callServiceThreadPool;
    }

    public void setCallServiceThreadPool(final ExecutorService callServiceThreadPool) {
        this.callServiceThreadPool = callServiceThreadPool;
    }

    public void registerInterceptor(Interceptor interceptor) {
        if (interceptor != null)
            this.remotingClient.registerInterceptor(interceptor);
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void setRemotingClient(final RemotingClient remotingClient) {
        this.remotingClient = remotingClient;
    }
}
