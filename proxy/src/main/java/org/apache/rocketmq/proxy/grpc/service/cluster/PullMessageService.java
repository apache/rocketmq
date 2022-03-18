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

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryOffsetPolicy;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtil;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.DefaultForwardClient;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;

public class PullMessageService extends BaseService {

    private final DefaultForwardClient defaultForwardClient;

    private volatile ResponseHook<QueryOffsetRequest, QueryOffsetResponse> queryOffsetHook = null;

    private volatile ResponseHook<PullMessageRequest, PullMessageResponse> pullMessageHook = null;

    public PullMessageService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.defaultForwardClient = connectorManager.getDefaultForwardClient();
    }

    public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        CompletableFuture<QueryOffsetResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (queryOffsetHook != null) {
                queryOffsetHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            Partition partition = request.getPartition();
            String topic = Converter.getResourceNameWithNamespace(partition.getTopic());

            String brokerName = partition.getBroker().getName();
            int queueId = partition.getId();
            CompletableFuture<Long> offsetFuture;
            if (request.getPolicy() == QueryOffsetPolicy.BEGINNING) {
                offsetFuture = CompletableFuture.completedFuture(0L);
            } else if (request.getPolicy() == QueryOffsetPolicy.END) {
                String brokerAddr = this.getBrokerAddr(ctx, brokerName);
                offsetFuture = this.defaultForwardClient.getMaxOffset(brokerAddr, topic, queueId, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            } else {
                long timestamp = Timestamps.toMillis(request.getTimePoint());
                String brokerAddr = this.getBrokerAddr(ctx, brokerName);
                offsetFuture = this.defaultForwardClient.searchOffset(brokerAddr, topic, queueId, timestamp, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            }
            offsetFuture.thenAccept(result -> future.complete(
                    QueryOffsetResponse.newBuilder()
                        .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
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
                pullMessageHook.beforeResponse(request, response, throwable);
            }
        });

        try {
            PullMessageRequestHeader requestHeader = this.convertToPullMessageRequestHeader(ctx, request);

            String brokerName = request.getPartition().getBroker().getName();
            String brokerAddr = this.getBrokerAddr(ctx, brokerName);

            CompletableFuture<PullResult> pullResultFuture = this.connectorManager.getForwardReadConsumer()
                .pullMessage(brokerAddr, requestHeader, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
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

    protected PullMessageRequestHeader convertToPullMessageRequestHeader(Context ctx, PullMessageRequest request) {
        // check filterExpression is correct or not
        Converter.buildSubscriptionData(Converter.getResourceNameWithNamespace(request.getPartition().getTopic()), request.getFilterExpression());

        long pollTime = ctx.getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS) - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            throw new ProxyException(Code.DEADLINE_EXCEEDED, "request has been canceled due to timeout");
        }
        return Converter.buildPullMessageRequestHeader(request, pollTime);
    }

    protected PullMessageResponse convertToPullMessageResponse(Context ctx, PullMessageRequest request, PullResult result) {
        PullMessageResponse.Builder responseBuilder = PullMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .setMinOffset(result.getMinOffset())
            .setMaxOffset(result.getMaxOffset())
            .setNextOffset(result.getNextBeginOffset());

        SubscriptionData subscriptionData =  Converter.buildSubscriptionData(
            Converter.getResourceNameWithNamespace(request.getPartition().getTopic()), request.getFilterExpression());

        PullStatus status = result.getPullStatus();
        if (status.equals(PullStatus.FOUND)) {
            List<Message> messageList = result.getMsgFoundList().stream()
                .filter(msg -> FilterUtil.isTagMatched(subscriptionData.getTagsSet(), msg.getTags())) // only return tag matched messages.
                .map(Converter::buildMessage)
                .collect(Collectors.toList());

            return responseBuilder.addAllMessages(messageList).build();
        } else {
            return responseBuilder.build();
        }
    }
}
