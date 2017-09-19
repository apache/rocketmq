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

import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.rpc.annotation.MethodType;
import org.apache.rocketmq.rpc.api.AdvancedServer;
import org.apache.rocketmq.rpc.api.Promise;
import org.apache.rocketmq.rpc.impl.service.RpcJdkProxy;
import org.apache.rocketmq.rpc.impl.service.RpcProxyFactory;

public class AdvancedServerImpl implements AdvancedServer {
    private final SimpleServerImpl simpleServer;

    public AdvancedServerImpl(final SimpleServerImpl simpleServer) {
        this.simpleServer = simpleServer;
    }

    @Override
    public <T> T callSync(final RemotingChannel channel, final String serviceCode, final String version,
        final Object[] parameter, final Class<T> responseType) throws Exception {
        RemotingCommand request = simpleServer.createRemoteRequest(serviceCode, version, parameter);
        RpcJdkProxy rpcJdkProxy = RpcProxyFactory.createServiceProxy(null, simpleServer, simpleServer.getRemotingServer(), simpleServer.getRpcCommonConfig(), channel);
        return (T) simpleServer.invokeRemoteMethod(rpcJdkProxy, serviceCode, request, responseType, MethodType.SYNC);
    }

    @Override
    public <T> Promise<T> callAsync(final RemotingChannel channel, final String serviceCode, final String version,
        final Object[] parameter, final Class<T> responseType) throws Exception {
        RemotingCommand request = simpleServer.createRemoteRequest(serviceCode, version, parameter);
        RpcJdkProxy rpcJdkProxy = RpcProxyFactory.createServiceProxy(null, simpleServer, simpleServer.getRemotingServer(), simpleServer.getRpcCommonConfig(), channel);
        return (Promise<T>) simpleServer.invokeRemoteMethod(rpcJdkProxy, serviceCode, request, responseType, MethodType.ASYNC);
    }

    @Override
    public void callOneway(final RemotingChannel channel, final String serviceCode, final String version,
        final Object[] parameter) throws Exception {
        RemotingCommand request = simpleServer.createRemoteRequest(serviceCode, version, parameter);
        RpcJdkProxy rpcJdkProxy = RpcProxyFactory.createServiceProxy(null, simpleServer, simpleServer.getRemotingServer(), simpleServer.getRpcCommonConfig(), channel);
        simpleServer.invokeRemoteMethod(rpcJdkProxy, serviceCode, request, Void.TYPE, MethodType.ASYNC);
    }
}
