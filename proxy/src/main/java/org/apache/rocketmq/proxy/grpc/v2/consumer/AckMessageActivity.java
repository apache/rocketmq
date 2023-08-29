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

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.Code;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.BatchAckResult;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;

public class AckMessageActivity extends AbstractMessingActivity {

    public AckMessageActivity(MessagingProcessor messagingProcessor, GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            String group = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getGroup());
            String topic = GrpcConverter.getInstance().wrapResourceWithNamespace(request.getTopic());
            if (ConfigurationManager.getProxyConfig().isEnableBatchAck()) {
                future = ackMessageInBatch(ctx, group, topic, request);
            } else {
                future = ackMessageOneByOne(ctx, group, topic, request);
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected CompletableFuture<AckMessageResponse> ackMessageInBatch(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        List<ReceiptHandleMessage> handleMessageList = new ArrayList<>(request.getEntriesCount());

        for (AckMessageEntry ackMessageEntry : request.getEntriesList()) {
            String handleString = getHandleString(ctx, group, request, ackMessageEntry);
            handleMessageList.add(new ReceiptHandleMessage(ReceiptHandle.decode(handleString), ackMessageEntry.getMessageId()));
        }
        return this.messagingProcessor.batchAckMessage(ctx, handleMessageList, group, topic)
            .thenApply(batchAckResultList -> {
                AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder();
                Set<Code> responseCodes = new HashSet<>();
                for (BatchAckResult batchAckResult : batchAckResultList) {
                    AckMessageResultEntry entry = convertToAckMessageResultEntry(batchAckResult);
                    responseBuilder.addEntries(entry);
                    responseCodes.add(entry.getStatus().getCode());
                }
                setAckResponseStatus(responseBuilder, responseCodes);
                return responseBuilder.build();
            });
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(BatchAckResult batchAckResult) {
        ReceiptHandleMessage handleMessage = batchAckResult.getReceiptHandleMessage();
        AckMessageResultEntry.Builder resultBuilder = AckMessageResultEntry.newBuilder()
            .setMessageId(handleMessage.getMessageId())
            .setReceiptHandle(handleMessage.getReceiptHandle().getReceiptHandle());
        if (batchAckResult.getProxyException() != null) {
            resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(batchAckResult.getProxyException()));
        } else {
            AckResult ackResult = batchAckResult.getAckResult();
            if (AckStatus.OK.equals(ackResult.getStatus())) {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()));
            } else {
                resultBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"));
            }
        }
        return resultBuilder.build();
    }

    protected CompletableFuture<AckMessageResponse> ackMessageOneByOne(ProxyContext ctx, String group, String topic, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> resultFuture = new CompletableFuture<>();
        CompletableFuture<AckMessageResultEntry>[] futures = new CompletableFuture[request.getEntriesCount()];
        for (int i = 0; i < request.getEntriesCount(); i++) {
            futures[i] = processAckMessage(ctx, group, topic, request, request.getEntries(i));
        }
        CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
                return;
            }

            Set<Code> responseCodes = new HashSet<>();
            List<AckMessageResultEntry> entryList = new ArrayList<>();
            for (CompletableFuture<AckMessageResultEntry> entryFuture : futures) {
                AckMessageResultEntry entryResult = entryFuture.join();
                responseCodes.add(entryResult.getStatus().getCode());
                entryList.add(entryResult);
            }
            AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder()
                .addAllEntries(entryList);
            setAckResponseStatus(responseBuilder, responseCodes);
            resultFuture.complete(responseBuilder.build());
        });
        return resultFuture;
    }

    protected CompletableFuture<AckMessageResultEntry> processAckMessage(ProxyContext ctx, String group, String topic, AckMessageRequest request,
        AckMessageEntry ackMessageEntry) {
        CompletableFuture<AckMessageResultEntry> future = new CompletableFuture<>();

        try {
            String handleString = this.getHandleString(ctx, group, request, ackMessageEntry);
            CompletableFuture<AckResult> ackResultFuture = this.messagingProcessor.ackMessage(
                ctx,
                ReceiptHandle.decode(handleString),
                ackMessageEntry.getMessageId(),
                group,
                topic
            );
            ackResultFuture.thenAccept(result -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, result));
            }).exceptionally(t -> {
                future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
                return null;
            });
        } catch (Throwable t) {
            future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, t));
        }
        return future;
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry, Throwable throwable) {
        return AckMessageResultEntry.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(throwable))
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .build();
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry,
        AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResultEntry.newBuilder()
                .setMessageId(ackMessageEntry.getMessageId())
                .setReceiptHandle(ackMessageEntry.getReceiptHandle())
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResultEntry.newBuilder()
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"))
            .build();
    }

    protected void setAckResponseStatus(AckMessageResponse.Builder responseBuilder, Set<Code> responseCodes) {
        if (responseCodes.size() > 1) {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.MULTIPLE_RESULTS, Code.MULTIPLE_RESULTS.name()));
        } else if (responseCodes.size() == 1) {
            Code code = responseCodes.stream().findAny().get();
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(code, code.name()));
        } else {
            responseBuilder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "ack message result is empty"));
        }
    }

    protected String getHandleString(ProxyContext ctx, String group, AckMessageRequest request, AckMessageEntry ackMessageEntry) {
        String handleString = ackMessageEntry.getReceiptHandle();

        MessageReceiptHandle messageReceiptHandle = messagingProcessor.removeReceiptHandle(ctx, grpcChannelManager.getChannel(ctx.getClientID()), group, ackMessageEntry.getMessageId(), ackMessageEntry.getReceiptHandle());
        if (messageReceiptHandle != null) {
            handleString = messageReceiptHandle.getReceiptHandleStr();
        }
        return handleString;
    }
}
