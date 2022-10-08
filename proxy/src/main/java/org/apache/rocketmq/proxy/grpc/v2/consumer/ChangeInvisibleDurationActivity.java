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

import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import com.google.protobuf.util.Durations;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;

public class ChangeInvisibleDurationActivity extends AbstractMessingActivity {
    protected ReceiptHandleProcessor receiptHandleProcessor;

    public ChangeInvisibleDurationActivity(MessagingProcessor messagingProcessor,
        ReceiptHandleProcessor receiptHandleProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.receiptHandleProcessor = receiptHandleProcessor;
    }

    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        CompletableFuture<ChangeInvisibleDurationResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            validateInvisibleTime(Durations.toMillis(request.getInvisibleDuration()));

            ReceiptHandle receiptHandle = ReceiptHandle.decode(request.getReceiptHandle());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());

            MessageReceiptHandle messageReceiptHandle = receiptHandleProcessor.removeReceiptHandle(ctx.getClientID(), group, request.getMessageId(), receiptHandle.getReceiptHandle());
            if (messageReceiptHandle != null) {
                receiptHandle = ReceiptHandle.decode(messageReceiptHandle.getReceiptHandleStr());
            }
            return this.messagingProcessor.changeInvisibleTime(
                ctx,
                receiptHandle,
                request.getMessageId(),
                group,
                GrpcConverter.getInstance().wrapResourceWithNamespace(request.getTopic()),
                Durations.toMillis(request.getInvisibleDuration())
            ).thenApply(ackResult -> convertToChangeInvisibleDurationResponse(ctx, request, ackResult));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ChangeInvisibleDurationResponse convertToChangeInvisibleDurationResponse(ProxyContext ctx,
        ChangeInvisibleDurationRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return ChangeInvisibleDurationResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .setReceiptHandle(ackResult.getExtraInfo())
                .build();
        }
        return ChangeInvisibleDurationResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "changeInvisibleDuration failed: status is abnormal"))
            .build();
    }
}
