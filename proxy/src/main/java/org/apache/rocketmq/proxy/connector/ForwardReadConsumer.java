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
package org.apache.rocketmq.proxy.connector;

import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.proxy.connector.client.MQClientAPIExt;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.factory.ForwardClientManager;

public class ForwardReadConsumer extends AbstractForwardClient {

    private static final String CID_PREFIX = "CID_RMQ_PROXY_CONSUME_MESSAGE_";

    public ForwardReadConsumer(ForwardClientManager clientFactory) {
        super(clientFactory, CID_PREFIX);
    }

    @Override
    protected int getClientNum() {
        return ConfigurationManager.getProxyConfig().getForwardConsumerNum();
    }

    @Override
    protected MQClientAPIExt createNewClient(ForwardClientManager clientFactory, String name) {
        double workerFactor = ConfigurationManager.getProxyConfig().getForwardConsumerWorkerFactor();
        final int threadCount = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * workerFactor);

        return clientFactory.getMQClient(name, threadCount);
    }

    public CompletableFuture<PopResult> popMessage(Context ctx, String address, String brokerName,
        PopMessageRequestHeader requestHeader) {
        return this.popMessage(ctx, address, brokerName, requestHeader, DEFAULT_MQ_CLIENT_TIMEOUT);
    }

    public CompletableFuture<PopResult> popMessage(
        Context ctx,
        String address,
        String brokerName,
        PopMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        return this.getClient().popMessageAsync(address, brokerName, requestHeader, timeoutMillis);
    }

    public CompletableFuture<PullResult> pullMessage(Context ctx, String address, PullMessageRequestHeader requestHeader) {
        return this.pullMessage(ctx, address, requestHeader, MAX_CONSUMER_TIMEOUT_MILLIS);
    }

    public CompletableFuture<PullResult> pullMessage(Context ctx, String address, PullMessageRequestHeader requestHeader,
        long timeoutMillis) {
        return this.getClient().pullMessageAsync(address, requestHeader, timeoutMillis);
    }
}
