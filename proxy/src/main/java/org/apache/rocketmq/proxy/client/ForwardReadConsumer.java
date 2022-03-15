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
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.impl.MQClientAPIExtImpl;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.proxy.client.factory.ForwardClientFactory;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;

public class ForwardReadConsumer extends AbstractForwardClient {

    private static final String CID_PREFIX = "CID_RMQ_PROXY_CONSUME_MESSAGE_";

    public ForwardReadConsumer(ForwardClientFactory clientFactory) {
        super(clientFactory);
    }

    @Override
    protected int getClientNum() {
        return ConfigurationManager.getProxyConfig().getForwardConsumerNum();
    }

    @Override
    protected MQClientAPIExtImpl createNewClient(ForwardClientFactory clientFactory, String name) {
        double workerFactor = ConfigurationManager.getProxyConfig().getForwardConsumerWorkerFactor();
        final int threadCount = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * workerFactor);

        return clientFactory.getMQClient(name, threadCount);
    }

    @Override
    protected String getNamePrefix() {
        return CID_PREFIX;
    }

    public CompletableFuture<PopResult> popMessage(String address,  String brokerName, PopMessageRequestHeader requestHeader,
        long timeoutMillis) {
        return getClient().popMessage(address, brokerName, requestHeader, timeoutMillis);
    }

    public CompletableFuture<PullResult> pullMessage(String address, PullMessageRequestHeader requestHeader,
        long timeoutMillis) {
        return getClient().pullMessage(address, requestHeader, timeoutMillis);
    }
}
