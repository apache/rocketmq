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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.MQClientAPIExt;
import org.apache.rocketmq.remoting.RPCHook;

public abstract class AbstractMQClientFactory extends AbstractClientFactory<MQClientAPIExt> {

    public AbstractMQClientFactory(ScheduledExecutorService scheduledExecutorService,
        RPCHook rpcHook) {
        super(scheduledExecutorService, rpcHook);
    }

    protected abstract ClientRemotingProcessor createClientRemotingProcessor();

    @Override
    protected MQClientAPIExt newOne(String instanceName, RPCHook rpcHook, int bootstrapWorkerThreads) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(instanceName);

        return new MQClientAPIExt(
            clientConfig,
            createNettyClientConfig(bootstrapWorkerThreads),
            createClientRemotingProcessor(),
            rpcHook
        );
    }

    @Override
    protected boolean tryStart(MQClientAPIExt client) {
        if (!client.updateNameServerAddressList()) {
            this.scheduledExecutorService.scheduleAtFixedRate(
                client::fetchNameServerAddr,
                1000 * 10,
                1000 * 60 * 2,
                TimeUnit.MILLISECONDS
            );
        }
        client.start();
        return true;
    }

    @Override
    protected void shutdown(MQClientAPIExt client) {
        client.shutdown();
    }
}
