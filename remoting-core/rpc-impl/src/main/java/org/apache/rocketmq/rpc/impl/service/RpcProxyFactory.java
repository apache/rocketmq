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

import org.apache.rocketmq.remoting.api.RemotingClient;
import org.apache.rocketmq.remoting.api.RemotingServer;
import org.apache.rocketmq.remoting.api.channel.RemotingChannel;
import org.apache.rocketmq.rpc.impl.config.RpcCommonConfig;

public class RpcProxyFactory {
    public static RpcJdkProxy createServiceProxy(Class<?> service,
        RpcProxyCommon rpcProxyCommon,
        RemotingClient remotingClient,
        RpcCommonConfig rpcCommonConfig,
        String remotingAddress) {
        return new RpcJdkProxyClient(service, rpcProxyCommon, remotingClient, rpcCommonConfig, remotingAddress);
    }

    public static RpcJdkProxy createServiceProxy(Class<?> service,
        RpcProxyCommon rpcProxyCommon,
        RemotingServer remotingServer,
        RpcCommonConfig rpcCommonConfig,
        RemotingChannel remotingChannel) {
        return new RpcJdkProxyServer(service, rpcProxyCommon, remotingServer, rpcCommonConfig, remotingChannel);
    }
}
