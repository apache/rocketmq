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

package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.UpdateOffsetRequest;
import apache.rocketmq.v2.UpdateOffsetResponse;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcValidator;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class PullMessageActivity extends AbstractMessingActivity {

    public PullMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public void pullMessage(ProxyContext ctx, PullMessageRequest request,
        StreamObserver<PullMessageResponse> responseObserver) {
        PullMessageResponseStreamWriter writer = createWriter(ctx, responseObserver);
        try {
            long pollingTime = GrpcValidator.getInstance().reasonableLongPollingTimeout(
                Durations.toMillis(request.getLongPollingTimeout()),
                ctx.getRemainingMs(),
                ctx.getClientVersion()
            );

            Broker broker = request.getMessageQueue().getBroker();
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());
            SubscriptionData subscriptionData = GrpcConverter.getInstance().buildSubscriptionData(topic, request.getFilterExpression());

            this.messagingProcessor.pullMessage(
                ctx,
                new MessageQueue(topic, broker.getName(), request.getMessageQueue().getId()),
                group,
                request.getOffset(),
                request.getBatchSize(),
                PullSysFlag.buildSysFlag(false, true, true, false),
                0,
                pollingTime,
                subscriptionData,
                ctx.getRemainingMs()
            )
                .thenAccept(pullResult -> writer.writeAndComplete(ctx, pullResult))
                .exceptionally(t -> {
                    writer.writeAndComplete(ctx, t);
                    return null;
                });
        } catch (Throwable t) {
            writer.writeAndComplete(ctx, t);
        }
    }

    protected PullMessageResponseStreamWriter createWriter(ProxyContext ctx,
        StreamObserver<PullMessageResponse> responseObserver) {
        return new PullMessageResponseStreamWriter(responseObserver);
    }

    public CompletableFuture<UpdateOffsetResponse> updateOffset(ProxyContext ctx, UpdateOffsetRequest request) {
        CompletableFuture<UpdateOffsetResponse> future = new CompletableFuture<>();
        try {
            Broker broker = request.getMessageQueue().getBroker();
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());

            return this.messagingProcessor.updateConsumerOffset(
                ctx,
                new MessageQueue(topic, broker.getName(), request.getMessageQueue().getId()),
                group,
                request.getOffset(),
                false
            ).thenApply(r -> UpdateOffsetResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<GetOffsetResponse> getOffset(ProxyContext ctx, GetOffsetRequest request) {
        CompletableFuture<GetOffsetResponse> future = new CompletableFuture<>();
        try {
            Broker broker = request.getMessageQueue().getBroker();
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());

            return this.messagingProcessor.queryConsumerOffset(
                ctx,
                new MessageQueue(topic, broker.getName(), request.getMessageQueue().getId()),
                group
            ).thenApply(offset -> GetOffsetResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .setOffset(offset)
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<QueryOffsetResponse> queryOffset(ProxyContext ctx, QueryOffsetRequest request) {
        CompletableFuture<QueryOffsetResponse> future = new CompletableFuture<>();
        try {
            Broker broker = request.getMessageQueue().getBroker();
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getMessageQueue().getTopic());
            MessageQueue messageQueue = new MessageQueue(topic, broker.getName(), request.getMessageQueue().getId());

            QueryOffsetPolicy queryOffsetPolicy = request.getQueryOffsetPolicy();
            CompletableFuture<Long> responseFuture;
            switch (queryOffsetPolicy) {
                case BEGINNING:
                    responseFuture = this.messagingProcessor.getMinOffset(ctx, messageQueue);
                    break;
                case END:
                    responseFuture = this.messagingProcessor.getMaxOffset(ctx, messageQueue);
                    break;
                case TIMESTAMP:
                    if (!request.hasTimestamp()) {
                        throw new GrpcProxyException(Code.BAD_REQUEST, "timestamp is required when queryOffsetPolicy is TIMESTAMP");
                    }
                    responseFuture = this.messagingProcessor.searchOffset(ctx, messageQueue, Timestamps.toMillis(request.getTimestamp()));
                    break;
                default:
                    throw new GrpcProxyException(Code.BAD_REQUEST, "unrecognized queryOffsetPolicy");
            }
            return responseFuture.thenApply(offset -> QueryOffsetResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .setOffset(offset).build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
