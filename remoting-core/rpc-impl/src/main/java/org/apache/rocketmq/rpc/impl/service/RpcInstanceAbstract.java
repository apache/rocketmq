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

package org.apache.rocketmq.rpc.impl.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.rpc.impl.command.RpcRequestCode;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;
import org.apache.rocketmq.rpc.impl.context.RpcProviderContext;
import org.apache.rocketmq.rpc.impl.metrics.DefaultServiceAPIImpl;
import org.apache.rocketmq.rpc.impl.metrics.ThreadStats;
import org.apache.rocketmq.rpc.impl.processor.RpcRequestProcessor;

import static org.apache.rocketmq.remoting.external.ThreadUtils.newThreadFactory;

public abstract class RpcInstanceAbstract extends RpcProxyCommon {
    protected final RpcRequestProcessor rpcRequestProcessor;
    protected final ThreadLocal<RpcProviderContext> threadLocalProviderContext = new ThreadLocal<RpcProviderContext>();
    protected final RpcCommonConfig rpcCommonConfig;
    protected ThreadStats threadStats;
    private DefaultServiceAPIImpl defaultServiceAPI;
    private ThreadPoolExecutor invokeServiceThreadPool;

    public RpcInstanceAbstract(RpcCommonConfig rpcCommonConfig) {
        super(rpcCommonConfig);
        this.threadStats = new ThreadStats();
        this.rpcCommonConfig = rpcCommonConfig;
        this.rpcRequestProcessor = new RpcRequestProcessor(this.threadLocalProviderContext, this, serviceStats);

        this.invokeServiceThreadPool = new ThreadPoolExecutor(
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(rpcCommonConfig.getServiceThreadBlockQueueSize()),
            newThreadFactory("rpcInvokeServiceThread", true));

    }

    public void start() {
        this.defaultServiceAPI = new DefaultServiceAPIImpl(this.serviceStats, threadStats);
        this.serviceStats.start();
        this.publishService0(this.defaultServiceAPI);
        this.remotingService().registerRequestProcessor(RpcRequestCode.CALL_SERVICE, this.rpcRequestProcessor, this.invokeServiceThreadPool);
    }

    public void stop() {
        this.serviceStats.stop();
        ThreadUtils.shutdownGracefully(this.invokeServiceThreadPool, 3000, TimeUnit.MILLISECONDS);
    }

    protected void publishService0(Object service) {
        this.rpcRequestProcessor.putNewService(service);
    }

    protected void publishService0(Object service, ExecutorService executorService) {
        this.rpcRequestProcessor.putNewService(service, executorService);
    }

    protected <T> T narrow0(Class<T> service, RpcJdkProxy rpcJdkProxy) {
        return rpcJdkProxy.newInstance(service);
    }

    public abstract void registerServiceListener();

    public ThreadPoolExecutor getInvokeServiceThreadPool() {
        return invokeServiceThreadPool;
    }

    public void setInvokeServiceThreadPool(ThreadPoolExecutor invokeServiceThreadPool) {
        this.invokeServiceThreadPool = invokeServiceThreadPool;
    }

}
