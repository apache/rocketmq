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
package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import java.util.concurrent.CompletableFuture;
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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ForwardMessageToDLQActivity extends AbstractMessingActivity {
    protected ReceiptHandleProcessor receiptHandleProcessor;

    public ForwardMessageToDLQActivity(MessagingProcessor messagingProcessor, ReceiptHandleProcessor receiptHandleProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.receiptHandleProcessor = receiptHandleProcessor;
    }

    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());

            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());
            String handleString = request.getReceiptHandle();
            MessageReceiptHandle messageReceiptHandle = receiptHandleProcessor.removeReceiptHandle(ctx, grpcChannelManager.getChannel(ctx.getClientID()), group, request.getMessageId(), request.getReceiptHandle());
            if (messageReceiptHandle != null) {
                handleString = messageReceiptHandle.getReceiptHandleStr();
            }
            ReceiptHandle receiptHandle = ReceiptHandle.decode(handleString);

            return this.messagingProcessor.forwardMessageToDeadLetterQueue(
                ctx,
                receiptHandle,
                request.getMessageId(),
                GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup()),
                GrpcConverter.getInstance().wrapResourceWithNamespace(request.getTopic())
            ).thenApply(result -> convertToForwardMessageToDeadLetterQueueResponse(ctx, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ForwardMessageToDeadLetterQueueResponse convertToForwardMessageToDeadLetterQueueResponse(ProxyContext ctx,
        RemotingCommand result) {
        return ForwardMessageToDeadLetterQueueResponse.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(result.getCode(), result.getRemark()))
            .build();
    }
}
