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
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.proxy.connector.client.MQClientAPIExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.factory.ForwardClientManager;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ForwardWriteConsumer extends AbstractForwardClient {

    private static final String CID_PREFIX = "CID_RMQ_PROXY_DELETE_MESSAGE_";

    public ForwardWriteConsumer(ForwardClientManager clientFactory) {
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

    public CompletableFuture<AckResult> ackMessage(Context ctx, String address, String messageId, AckMessageRequestHeader requestHeader) {
        return this.ackMessage(ctx, address, messageId, requestHeader, DEFAULT_MQ_CLIENT_TIMEOUT);
    }

    public CompletableFuture<AckResult> ackMessage(
        Context ctx,
        String address,
        String messageId,
        AckMessageRequestHeader requestHeader,
        long timeoutMillis
    ) {
        return this.getClient().ackMessageAsync(address, requestHeader, timeoutMillis);
    }

    public CompletableFuture<AckResult> changeInvisibleTimeAsync(
        Context ctx,
        String address,
        String brokerName,
        String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader
    ) {
        return this.changeInvisibleTimeAsync(ctx, address, brokerName, messageId, requestHeader, DEFAULT_MQ_CLIENT_TIMEOUT);
    }

    public CompletableFuture<AckResult> changeInvisibleTimeAsync(
        Context ctx,
        String address,
        String brokerName,
        String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader,
        long timeoutMillis
    ) {
        return this.getClient().changeInvisibleTimeAsync(address, brokerName, requestHeader, timeoutMillis);
    }

    public void updateConsumerOffsetOneWay(
        Context ctx,
        String brokerAddr,
        UpdateConsumerOffsetRequestHeader header,
        long timeoutMillis
    ) throws RemotingException, InterruptedException {
        this.getClient().updateConsumerOffsetOneWay(brokerAddr, header, timeoutMillis);
    }
}
