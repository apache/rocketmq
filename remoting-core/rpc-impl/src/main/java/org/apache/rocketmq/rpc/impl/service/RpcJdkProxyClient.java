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

import org.apache.rocketmq.remoting.api.AsyncHandler;
import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.command.RemotingCommand;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;

public class RpcJdkProxyClient extends RpcJdkProxy {
    private RemotingClient remotingClient;
    private String remotingAddress;
    private RpcCommonConfig rpcCommonConfig;

    public RpcJdkProxyClient(final Class<?> service,
        final RpcProxyCommon rpcProxyCommon,
        final RemotingClient remotingClient,
        final RpcCommonConfig rpcCommonConfig,
        final String remotingAddress) {
        super(service, rpcProxyCommon);
        this.remotingClient = remotingClient;
        this.rpcCommonConfig = rpcCommonConfig;
        this.remotingAddress = remotingAddress;
    }

    @Override
    public void invokeOneWay(final RemotingCommand request) {
        this.remotingClient.invokeOneWay(remotingAddress, request, rpcCommonConfig.getServiceInvokeTimeout());
    }

    @Override
    public void invokeAsync(final RemotingCommand request, final AsyncHandler handler) {
        this.remotingClient.invokeAsync(remotingAddress, request, handler, rpcCommonConfig.getServiceInvokeTimeout());
    }
}
