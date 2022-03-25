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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.RecoverOrphanedTransactionCommand;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.transaction.TransactionId;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateCheckRequest;
import org.apache.rocketmq.proxy.connector.transaction.TransactionStateChecker;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseHook;

public class TransactionService extends BaseService implements TransactionStateChecker {

    private final ChannelManager channelManager;
    private final ForwardProducer forwardProducer;

    private volatile ResponseHook<TransactionStateCheckRequest, PollCommandResponse> checkTransactionStateHook = null;
    private volatile ResponseHook<EndTransactionRequest, EndTransactionResponse> endTransactionHook = null;

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

            Message message = GrpcConverter.buildMessage(checkData.getMessageExt());
            PollCommandResponse response = PollCommandResponse.newBuilder()
                .setRecoverOrphanedTransactionCommand(
                    RecoverOrphanedTransactionCommand.newBuilder()
                        .setOrphanedTransactionalMessage(message)
                        .setTransactionId(transactionId)
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
            EndTransactionRequestHeader requestHeader = this.toEndTransactionRequestHeader(ctx, request);
            this.forwardProducer.endTransaction(
                RemotingHelper.parseSocketAddressAddr(handle.getBrokerAddr()),
                requestHeader, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            future.complete(EndTransactionResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected EndTransactionRequestHeader toEndTransactionRequestHeader(Context ctx, EndTransactionRequest request) {
        return GrpcConverter.buildEndTransactionRequestHeader(request);
    }

    public void setCheckTransactionStateHook(
        ResponseHook<TransactionStateCheckRequest, PollCommandResponse> checkTransactionStateHook) {
        this.checkTransactionStateHook = checkTransactionStateHook;
    }

    public void setEndTransactionHook(
        ResponseHook<EndTransactionRequest, EndTransactionResponse> endTransactionHook) {
        this.endTransactionHook = endTransactionHook;
    }
}
