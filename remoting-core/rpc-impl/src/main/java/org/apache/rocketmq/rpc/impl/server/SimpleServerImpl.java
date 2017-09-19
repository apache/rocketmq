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

package org.apache.rocketmq.rpc.impl.server;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.api.RemotingServer;
import org.apache.rocketmq.remoting.api.RemotingService;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.external.ThreadUtils;
import org.apache.rocketmq.remoting.impl.netty.RemotingBootstrapFactory;
import org.apache.rocketmq.rpc.api.AdvancedServer;
import org.apache.rocketmq.rpc.api.SimpleServer;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;
import org.apache.rocketmq.rpc.impl.service.RpcInstanceAbstract;
import org.apache.rocketmq.rpc.impl.service.RpcProxyFactory;

public class SimpleServerImpl extends RpcInstanceAbstract implements SimpleServer {
    private RemotingServer remotingServer;
    private ExecutorService callServiceThreadPool;
    private RpcCommonConfig rpcCommonConfig;

    public SimpleServerImpl(final RpcCommonConfig remotingConfig) {
        this(remotingConfig, RemotingBootstrapFactory.createRemotingServer(remotingConfig));
        this.rpcCommonConfig = remotingConfig;
        this.callServiceThreadPool = ThreadUtils.newThreadPoolExecutor(rpcCommonConfig.getClientAsyncCallbackExecutorThreads(),
            rpcCommonConfig.getClientAsyncCallbackExecutorThreads(), rpcCommonConfig.getServiceThreadKeepAliveTime(),
            TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(remotingConfig.getServiceThreadBlockQueueSize()),
            "serverCallServiceThread", true);
    }

    public SimpleServerImpl(final RpcCommonConfig remotingConfig, final RemotingServer remotingServer) {
        super(remotingConfig);
        this.remotingServer = remotingServer;
    }

    @Override
    public RemotingService remotingService() {
        return this.remotingServer;
    }

    @Override
    public void registerServiceListener() {

    }

    @Override
    public <T> T bind(final Class<T> service, final RemotingChannel channel, final Properties properties) {
        return this.narrow0(service, RpcProxyFactory.createServiceProxy(service, this, remotingServer, rpcCommonConfig, channel));
    }

    @Override
    public AdvancedServer advancedServer() {
        return new AdvancedServerImpl(this);
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
    public void start() {
        super.start();
        this.remotingServer.start();
    }

    @Override
    public void stop() {
        super.stop();
        ThreadUtils.shutdownGracefully(this.callServiceThreadPool, 3000, TimeUnit.MILLISECONDS);
        this.remotingServer.stop();
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(final RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }
}
