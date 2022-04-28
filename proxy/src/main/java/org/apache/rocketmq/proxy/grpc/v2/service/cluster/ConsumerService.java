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

import apache.rocketmq.v2.AckMessageEntry;
import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.AckMessageResultEntry;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.BaseService;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerService extends BaseService {
    protected final ForwardReadConsumer readConsumer;
    protected final ForwardWriteConsumer writeConsumer;
    /**
     * For sending messages back to broker.
     */
    protected final ForwardProducer producer;
    protected final GrpcClientManager grpcClientManager;

    private volatile ReadQueueSelector readQueueSelector;
    private volatile ReceiveMessageResponseStreamWriter.Builder receiveMessageWriterBuilder;

    private volatile ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;
    private volatile ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook;
    private volatile ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook;
    private volatile ResponseHook<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> changeInvisibleDurationHook;

    public ConsumerService(ConnectorManager connectorManager, GrpcClientManager grpcClientManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();
        this.producer = connectorManager.getForwardProducer();
        this.grpcClientManager = grpcClientManager;

        this.readQueueSelector = new DefaultReadQueueSelector(connectorManager.getTopicRouteCache());
        this.receiveMessageWriterBuilder = (observer, hook) -> new DefaultReceiveMessageResponseStreamWriter(
            observer,
            hook,
            writeConsumer,
            connectorManager.getTopicRouteCache(),
            new DefaultReceiveMessageResultFilter(
                producer, writeConsumer, grpcClientManager, connectorManager.getTopicRouteCache())
        );
    }

    public void receiveMessage(Context ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        ReceiveMessageResponseStreamWriter writer = receiveMessageWriterBuilder.build(responseObserver, receiveMessageHook);
        try {
            PopMessageRequestHeader requestHeader = this.buildPopMessageRequestHeader(ctx, request);
            SelectableMessageQueue messageQueue = this.readQueueSelector.select(ctx, request, requestHeader);

            if (messageQueue == null) {
                throw new ProxyException(Code.FORBIDDEN, "no readable topic route for topic " + requestHeader.getTopic());
            }

            this.readConsumer.popMessage(
                ctx,
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                requestHeader,
                requestHeader.getPollTime())
                .thenAccept(result -> writer.write(ctx, request, result.getPopStatus(), result.getMsgFoundList()))
                .exceptionally(e -> {
                    writer.write(ctx, request, e);
                    return null;
                });
        } catch (Throwable t) {
            writer.write(ctx, request, t);
        }
    }

    protected PopMessageRequestHeader buildPopMessageRequestHeader(Context ctx, ReceiveMessageRequest request) {
        checkSubscriptionData(request.getMessageQueue().getTopic(), request.getFilterExpression());
        boolean fifo = grpcClientManager.getClientSettings(ctx).getSubscription().getFifo();
        return GrpcConverter.buildPopMessageRequestHeader(request, GrpcConverter.buildPollTimeFromContext(ctx), fifo);
    }

    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (ackMessageHook != null) {
                ackMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            CompletableFuture<AckMessageResultEntry>[] futures = new CompletableFuture[request.getEntriesCount()];
            for (int i = 0; i < request.getEntriesCount(); i++) {
                futures[i] = processAckMessage(ctx, request, request.getEntries(i));
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

    protected CompletableFuture<AckMessageResultEntry> processAckMessage(Context ctx, AckMessageRequest request,
        AckMessageEntry ackMessageEntry) {
        CompletableFuture<AckMessageResultEntry> future = new CompletableFuture<>();
        AckMessageResultEntry.Builder failResult = AckMessageResultEntry.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "ack message failed"))
            .setMessageId(ackMessageEntry.getMessageId())
            .setReceiptHandle(ackMessageEntry.getReceiptHandle());

        try {
            ReceiptHandle receiptHandle = resolveReceiptHandle(ctx, ackMessageEntry.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            AckMessageRequestHeader requestHeader = this.buildAckMessageRequestHeader(ctx, request, receiptHandle);
            CompletableFuture<AckResult> ackResultFuture = this.writeConsumer.ackMessage(ctx, brokerAddr, ackMessageEntry.getMessageId(), requestHeader);
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

    protected AckMessageRequestHeader buildAckMessageRequestHeader(Context ctx, AckMessageRequest request,
        ReceiptHandle handle) {
        return GrpcConverter.buildAckMessageRequestHeader(request, handle);
    }

    protected AckMessageResultEntry convertToAckMessageResultEntry(Context ctx, AckMessageEntry ackMessageEntry,
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

    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        try {
            ReceiptHandle receiptHandle = resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            Settings settings = grpcClientManager.getClientSettings(ctx);
            int maxDeliveryAttempts = settings.getBackoffPolicy().getMaxAttempts();
            if (request.getDeliveryAttempt() >= maxDeliveryAttempts) {
                future = this.producer.sendMessageBackThenAckOrg(
                    ctx,
                    brokerAddr,
                    this.buildConsumerSendMsgBackToDLQRequestHeader(ctx, request, maxDeliveryAttempts),
                    this.buildAckMessageRequestHeader(ctx, request)
                ).thenApply(result -> convertToNackMessageResponse(ctx, request, result));
            } else {
                ChangeInvisibleTimeRequestHeader requestHeader = this.buildChangeInvisibleTimeRequestHeader(ctx, request);
                future = this.writeConsumer.changeInvisibleTimeAsync(ctx, brokerAddr, receiptHandle.getBrokerName(), request.getMessageId(), requestHeader)
                    .thenApply(result -> convertToNackMessageResponse(ctx, request, result));
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        future.whenComplete((response, throwable) -> {
            if (nackMessageHook != null) {
                nackMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        return future;
    }

    protected ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(Context ctx,
        NackMessageRequest request) {
        RetryPolicy retryPolicy = grpcClientManager.getClientSettings(ctx).getBackoffPolicy();
        return GrpcConverter.buildChangeInvisibleTimeRequestHeader(request, retryPolicy);
    }

    protected AckMessageRequestHeader buildAckMessageRequestHeader(Context ctx, NackMessageRequest request) {
        return GrpcConverter.buildAckMessageRequestHeader(request);
    }

    protected ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackToDLQRequestHeader(Context ctx,
        NackMessageRequest request,
        int maxReconsumeTimes) {
        return GrpcConverter.buildConsumerSendMsgBackToDLQRequestHeader(request, maxReconsumeTimes);
    }

    protected NackMessageResponse convertToNackMessageResponse(Context ctx, NackMessageRequest request,
        AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return NackMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return NackMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "nack failed: status is abnormal"))
            .build();
    }

    protected NackMessageResponse convertToNackMessageResponse(Context ctx, NackMessageRequest request,
        RemotingCommand sendMsgBackToDLQResult) {
        return NackMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(sendMsgBackToDLQResult.getCode(), sendMsgBackToDLQResult.getRemark()))
            .build();
    }

    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        CompletableFuture<ChangeInvisibleDurationResponse> future = new CompletableFuture<>();

        try {
            ReceiptHandle receiptHandle = resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            ChangeInvisibleTimeRequestHeader requestHeader = convertToChangeInvisibleTimeRequestHeader(ctx, request);
            future = this.writeConsumer.changeInvisibleTimeAsync(ctx, brokerAddr, receiptHandle.getBrokerName(), "", requestHeader)
                .thenApply(result -> convertToChangeInvisibleDurationResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        future.whenComplete((response, throwable) -> {
            if (changeInvisibleDurationHook != null) {
                changeInvisibleDurationHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        return future;
    }

    protected ChangeInvisibleTimeRequestHeader convertToChangeInvisibleTimeRequestHeader(Context ctx,
        ChangeInvisibleDurationRequest request) {
        return GrpcConverter.buildChangeInvisibleTimeRequestHeader(request);
    }

    protected ChangeInvisibleDurationResponse convertToChangeInvisibleDurationResponse(Context ctx,
        ChangeInvisibleDurationRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return ChangeInvisibleDurationResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .setReceiptHandle(ackResult.getExtraInfo())
                .build();
        }
        return ChangeInvisibleDurationResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "changeInvisibleDuration failed: status is abnormal"))
            .build();
    }

    public ReadQueueSelector getReadQueueSelector() {
        return readQueueSelector;
    }

    public void setReadQueueSelector(ReadQueueSelector readQueueSelector) {
        this.readQueueSelector = readQueueSelector;
    }

    public ReceiveMessageResponseStreamWriter.Builder getReceiveMessageWriterBuilder() {
        return receiveMessageWriterBuilder;
    }

    public void setReceiveMessageWriterBuilder(
        ReceiveMessageResponseStreamWriter.Builder receiveMessageWriterBuilder) {
        this.receiveMessageWriterBuilder = receiveMessageWriterBuilder;
    }

    public ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> getReceiveMessageHook() {
        return receiveMessageHook;
    }

    public void setReceiveMessageHook(
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
    }

    public ResponseHook<AckMessageRequest, AckMessageResponse> getAckMessageHook() {
        return ackMessageHook;
    }

    public void setAckMessageHook(
        ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook) {
        this.ackMessageHook = ackMessageHook;
    }

    public ResponseHook<NackMessageRequest, NackMessageResponse> getNackMessageHook() {
        return nackMessageHook;
    }

    public void setNackMessageHook(
        ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook) {
        this.nackMessageHook = nackMessageHook;
    }

    public ResponseHook<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> getChangeInvisibleDurationHook() {
        return changeInvisibleDurationHook;
    }

    public void setChangeInvisibleDurationHook(
        ResponseHook<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> changeInvisibleDurationHook) {
        this.changeInvisibleDurationHook = changeInvisibleDurationHook;
    }
}
