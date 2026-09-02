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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.GetOffsetRequest;
import apache.rocketmq.v2.GetOffsetResponse;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import com.google.protobuf.util.Timestamps;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class OffsetActivity extends AbstractMessagingActivity {

    public OffsetActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<GetOffsetResponse> getOffset(ProxyContext ctx, GetOffsetRequest request) {
        try {
            validateTopicAndConsumerGroup(request.getMessageQueue().getTopic(), request.getGroup());
            org.apache.rocketmq.common.message.MessageQueue messageQueue = convertMessageQueue(request.getMessageQueue());
            return this.messagingProcessor.queryConsumerOffset(
                ctx,
                messageQueue,
                request.getGroup().getName(),
                MessagingProcessor.DEFAULT_TIMEOUT_MILLS
            ).thenApply(this::convertToGetOffsetResponse);
        } catch (Throwable t) {
            return FutureUtils.completeExceptionally(t);
        }
    }

    public CompletableFuture<QueryOffsetResponse> queryOffset(ProxyContext ctx, QueryOffsetRequest request) {
        try {
            validateTopic(request.getMessageQueue().getTopic());
            return queryOffset0(ctx, request).thenApply(this::convertToQueryOffsetResponse);
        } catch (Throwable t) {
            return FutureUtils.completeExceptionally(t);
        }
    }

    protected CompletableFuture<Long> queryOffset0(ProxyContext ctx, QueryOffsetRequest request) {
        QueryOffsetPolicy queryOffsetPolicy = request.getQueryOffsetPolicy();
        org.apache.rocketmq.common.message.MessageQueue messageQueue = convertMessageQueue(request.getMessageQueue());
        switch (queryOffsetPolicy) {
            case BEGINNING:
                return this.messagingProcessor.getMinOffset(
                    ctx,
                    messageQueue,
                    MessagingProcessor.DEFAULT_TIMEOUT_MILLS
                );
            case END:
                return this.messagingProcessor.getMaxOffset(
                    ctx,
                    messageQueue,
                    MessagingProcessor.DEFAULT_TIMEOUT_MILLS
                );
            case TIMESTAMP:
                if (!request.hasTimestamp()) {
                    throw new GrpcProxyException(Code.BAD_REQUEST, "timestamp is required for TIMESTAMP query offset policy");
                }
                return this.messagingProcessor.searchOffset(
                    ctx,
                    messageQueue,
                    Timestamps.toMillis(request.getTimestamp()),
                    MessagingProcessor.DEFAULT_TIMEOUT_MILLS
                );
            default:
                throw new GrpcProxyException(Code.BAD_REQUEST, "unsupported query offset policy: " + queryOffsetPolicy);
        }
    }

    protected org.apache.rocketmq.common.message.MessageQueue convertMessageQueue(
        apache.rocketmq.v2.MessageQueue messageQueue) {
        return new org.apache.rocketmq.common.message.MessageQueue(
            messageQueue.getTopic().getName(),
            messageQueue.getBroker().getName(),
            messageQueue.getId()
        );
    }

    protected GetOffsetResponse convertToGetOffsetResponse(long offset) {
        return GetOffsetResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .setOffset(offset)
            .build();
    }

    protected QueryOffsetResponse convertToQueryOffsetResponse(long offset) {
        return QueryOffsetResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .setOffset(offset)
            .build();
    }
}
