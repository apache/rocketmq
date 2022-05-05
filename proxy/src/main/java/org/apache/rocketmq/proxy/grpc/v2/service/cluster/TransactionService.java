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
package org.apache.rocketmq.proxy.grpc.v2.service.cluster;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateCheckRequest;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.service.BaseService;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TransactionService extends BaseService implements TransactionStateChecker {

    protected final ChannelManager channelManager;
    protected final ForwardProducer forwardProducer;

    private volatile ResponseHook<TransactionStateCheckRequest, TelemetryCommand> checkTransactionStateHook;
    private volatile ResponseHook<EndTransactionRequest, EndTransactionResponse> endTransactionHook;

    public TransactionService(ConnectorManager connectorManager, ChannelManager channelManager) {
        super(connectorManager);
        this.forwardProducer = connectorManager.getForwardProducer();
        this.channelManager = channelManager;
    }

    @Override
    public void checkTransactionState(TransactionStateCheckRequest checkData) {
        Context ctx = Context.current();
        try {
            List<String> clientIdList = this.channelManager.getClientIdList(checkData.getGroupId());
            if (CollectionUtils.isEmpty(clientIdList)) {
                return;
            }

            String clientId = clientIdList.get(ThreadLocalRandom.current().nextInt(clientIdList.size()));
            GrpcClientChannel channel = GrpcClientChannel.getChannel(this.channelManager, checkData.getGroupId(), clientId);

            String transactionId = checkData.getTransactionId().getProxyTransactionId();
            MessageExt messageExt = checkData.getMessageExt();
            Message message = GrpcConverter.buildMessage(messageExt);
            TelemetryCommand response = TelemetryCommand.newBuilder()
                .setRecoverOrphanedTransactionCommand(
                    RecoverOrphanedTransactionCommand.newBuilder()
                        .setOrphanedTransactionalMessage(message)
                        .setTransactionId(transactionId)
                        .setMessageQueue(GrpcConverter.buildMessageQueue(messageExt, checkData.getBrokerName()))
                        .build()
                ).build();

            channel.writeAndFlush(response);
            if (this.checkTransactionStateHook != null) {
                this.checkTransactionStateHook.beforeResponse(ctx, checkData, response, null);
            }
        } catch (Throwable t) {
            if (this.checkTransactionStateHook != null) {
                this.checkTransactionStateHook.beforeResponse(ctx, checkData, null, t);
            }
        }
    }

    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        CompletableFuture<EndTransactionResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (endTransactionHook != null) {
                endTransactionHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            TransactionId handle = TransactionId.decode(request.getTransactionId());
            String brokerAddr = RemotingHelper.parseSocketAddressAddr(handle.getBrokerAddr());
            EndTransactionRequestHeader requestHeader = this.toEndTransactionRequestHeader(ctx, request);
            this.forwardProducer.endTransaction(ctx, brokerAddr, requestHeader);
            future.complete(EndTransactionResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected EndTransactionRequestHeader toEndTransactionRequestHeader(Context ctx, EndTransactionRequest request) {
        String topic = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        // use topic name as producerGroup
        return GrpcConverter.buildEndTransactionRequestHeader(request, topic);
    }

    public ResponseHook<TransactionStateCheckRequest, TelemetryCommand> getCheckTransactionStateHook() {
        return checkTransactionStateHook;
    }

    public void setCheckTransactionStateHook(
        ResponseHook<TransactionStateCheckRequest, TelemetryCommand> checkTransactionStateHook) {
        this.checkTransactionStateHook = checkTransactionStateHook;
    }

    public ResponseHook<EndTransactionRequest, EndTransactionResponse> getEndTransactionHook() {
        return endTransactionHook;
    }

    public void setEndTransactionHook(
        ResponseHook<EndTransactionRequest, EndTransactionResponse> endTransactionHook) {
        this.endTransactionHook = endTransactionHook;
    }
}
