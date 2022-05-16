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
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;

public class AckMessageActivity extends AbstractMessingActivity {

    public AckMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        super(messagingProcessor, grpcClientSettingsManager);
    }

    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        ProxyContext proxyContext = createContext(ctx);
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();

        try {
            CompletableFuture<AckMessageResultEntry>[] futures = new CompletableFuture[request.getEntriesCount()];
            for (int i = 0; i < request.getEntriesCount(); i++) {
                futures[i] = processAckMessage(proxyContext, request, request.getEntries(i));
            }
            CompletableFuture.allOf(futures).whenComplete((val, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                List<AckMessageResultEntry> entryList = new ArrayList<>();
                for (CompletableFuture<AckMessageResultEntry> entryFuture : futures) {
                    entryFuture.thenAccept(entryList::add);
                }
                AckMessageResponse.Builder responseBuilder = AckMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                    .addAllEntries(entryList);
                future.complete(responseBuilder.build());
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected CompletableFuture<AckMessageResultEntry> processAckMessage(ProxyContext ctx, AckMessageRequest request,
        AckMessageEntry ackMessageEntry) {
        CompletableFuture<AckMessageResultEntry> future = new CompletableFuture<>();
        AckMessageResultEntry.Builder failResult = AckMessageResultEntry.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "ack message failed"))
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle());

        try {
            ReceiptHandle receiptHandle = ReceiptHandle.decode(ackMessageEntry.getReceiptHandle());

            CompletableFuture<AckResult> ackResultFuture = this.messagingProcessor.ackMessage(
                ctx,
                receiptHandle,
                ackMessageEntry.getMessageId(),
                GrpcConverter.wrapResourceWithNamespace(request.getGroup()),
                GrpcConverter.wrapResourceWithNamespace(request.getTopic()));
            ackResultFuture
                .thenAccept(result -> future.complete(convertToAckMessageResultEntry(ctx, ackMessageEntry, result)))
                .exceptionally(throwable -> {
                    future.complete(failResult.setStatus(ResponseBuilder.buildStatus(throwable)).build());
                    return null;
                });
        } catch (Throwable t) {
            future.complete(failResult.setStatus(ResponseBuilder.buildStatus(t)).build());
        }
        return future;
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(ProxyContext ctx, AckMessageEntry ackMessageEntry,
        AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResultEntry.newBuilder()
                .setMessageId(ackMessageEntry.getMessageId())
                .setReceiptHandle(ackMessageEntry.getReceiptHandle())
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResultEntry.newBuilder()
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle())
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"))
            .build();
    }
}
