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
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import com.google.protobuf.util.Timestamps;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.DefaultForwardClient;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;

public class PullMessageService extends BaseService {
    private final DefaultForwardClient forwardClient;
    private final ForwardReadConsumer readConsumer;

    private volatile ResponseHook<QueryOffsetRequest, QueryOffsetResponse> queryOffsetHook;
    private volatile ResponseHook<PullMessageRequest, PullMessageResponse> pullMessageHook;

    public PullMessageService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.forwardClient = connectorManager.getDefaultForwardClient();
        this.readConsumer = connectorManager.getForwardReadConsumer();
    }

    public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        CompletableFuture<QueryOffsetResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryOffsetHook != null) {
                queryOffsetHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            MessageQueue messageQueue = request.getMessageQueue();
            String topic = GrpcConverter.wrapResourceWithNamespace(messageQueue.getTopic());
            String brokerName = messageQueue.getBroker().getName();
            int queueId = messageQueue.getId();

            CompletableFuture<Long> offsetFuture;
            switch (request.getPolicy()) {
                case BEGINNING:
                    offsetFuture = CompletableFuture.completedFuture(0L);
                    break;
                case END:
                    offsetFuture = this.forwardClient.getMaxOffset(this.getBrokerAddr(ctx, brokerName), topic, queueId);
                    break;
                default:
                    long timestamp = Timestamps.toMillis(request.getTimePoint());
                    offsetFuture = this.forwardClient.searchOffset(this.getBrokerAddr(ctx, brokerName), topic, queueId, timestamp);
            }

            offsetFuture
                .thenAccept(result -> future.complete(
                    QueryOffsetResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                        .setOffset(result)
                        .build()))
                .exceptionally(throwable -> {
                    future.completeExceptionally(throwable);
                    return null;
                });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        CompletableFuture<PullMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (pullMessageHook != null) {
                pullMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            PullMessageRequestHeader requestHeader = this.buildPullMessageRequestHeader(ctx, request);

            String brokerName = request.getMessageQueue().getBroker().getName();
            String brokerAddr = this.getBrokerAddr(ctx, brokerName);

            CompletableFuture<PullResult> pullResultFuture = this.readConsumer.pullMessage(brokerAddr, requestHeader);
            pullResultFuture
                .thenAccept(pullResult -> {
                    try {
                        future.complete(convertToPullMessageResponse(ctx, request, pullResult));
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                })
                .exceptionally(throwable -> {
                    future.completeExceptionally(throwable);
                    return null;
                });

        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected PullMessageRequestHeader buildPullMessageRequestHeader(Context ctx, PullMessageRequest request) {
        checkSubscriptionData(request.getMessageQueue().getTopic(), request.getFilterExpression());
        return GrpcConverter.buildPullMessageRequestHeader(request, GrpcConverter.buildPollTimeFromContext(ctx));
    }

    protected PullMessageResponse convertToPullMessageResponse(Context ctx, PullMessageRequest request, PullResult result) {
        PullMessageResponse.Builder responseBuilder = PullMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
            .setMinOffset(result.getMinOffset())
            .setMaxOffset(result.getMaxOffset())
            .setNextOffset(result.getNextBeginOffset());

        SubscriptionData subscriptionData =  GrpcConverter.buildSubscriptionData(
            GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic()), request.getFilterExpression());

        PullStatus status = result.getPullStatus();
        if (status.equals(PullStatus.FOUND)) {
            List<Message> messageList = result.getMsgFoundList().stream()
                .filter(msg -> FilterUtils.isTagMatched(subscriptionData.getTagsSet(), msg.getTags())) // only return tag matched messages.
                .map(GrpcConverter::buildMessage)
                .collect(Collectors.toList());

            return responseBuilder.addAllMessages(messageList).build();
        } else {
            return responseBuilder.build();
        }
    }
}
