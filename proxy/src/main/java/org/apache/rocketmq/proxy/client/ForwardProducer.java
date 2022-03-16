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
package org.apache.rocketmq.proxy.client;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.impl.MQClientAPIExtImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.proxy.client.factory.ForwardClientFactory;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ForwardProducer extends AbstractForwardClient {

    private static final String PID_PREFIX = "PID_RMQ_PROXY_PUBLISH_MESSAGE_";

    public ForwardProducer(ForwardClientFactory clientFactory) {
        super(clientFactory);
    }

    @Override
    protected int getClientNum() {
        return ConfigurationManager.getProxyConfig().getForwardProducerNum();
    }

    @Override
    protected MQClientAPIExtImpl createNewClient(ForwardClientFactory clientFactory, String name) {
        double sendClientWorkerFactor = ConfigurationManager.getProxyConfig().getForwardProducerWorkerFactor();
        final int threadCount = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * sendClientWorkerFactor);

        return clientFactory.getTransactionalProducer(name, threadCount);
    }

    @Override
    protected String getNamePrefix() {
        return PID_PREFIX;
    }

    public CompletableFuture<Integer> heartBeat(String heartbeatAddr, HeartbeatData heartbeatData, long timeout) throws Exception {
        return this.getClient().sendHeartbeat(heartbeatAddr, heartbeatData, timeout);
    }

    public CompletableFuture<SendResult> sendMessage(String address, String brokerName, Message msg,
        SendMessageRequestHeader requestHeader, long timeoutMillis) {
        return this.getClient().sendMessage(address, brokerName, msg, requestHeader, timeoutMillis);
    }

    public CompletableFuture<RemotingCommand> sendMessageBack(String brokerAddr, ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
        return this.getClient().sendMessageBack(brokerAddr, requestHeader, timeoutMillis);
    }
}
