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
import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerService extends BaseService {
    private final ForwardReadConsumer readConsumer;
    private final ForwardWriteConsumer writeConsumer;
    /**
     * For sending messages back to broker.
     */
    private final ForwardProducer producer;

    private volatile ReadQueueSelector readQueueSelector;
    private volatile ResponseHook<ReceiveMessageRequest, List<ReceiveMessageResponse>> receiveMessageHook;
    private volatile ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook;
    private volatile ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> forwardToDLQInRecvMessageHook;
    private volatile ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook;
    private volatile ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook;
    private volatile ResponseHook<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> changeInvisibleDurationHook;

    private final GrpcClientManager grpcClientManager;

    public ConsumerService(ConnectorManager connectorManager, GrpcClientManager grpcClientManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();
        this.producer = connectorManager.getForwardProducer();

        this.readQueueSelector = new DefaultReadQueueSelector(connectorManager.getTopicRouteCache());

        this.grpcClientManager = grpcClientManager;
    }

    public CompletableFuture<List<ReceiveMessageResponse>> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        CompletableFuture<List<ReceiveMessageResponse>> future = new CompletableFuture<>();

        try {
            PopMessageRequestHeader requestHeader = this.buildPopMessageRequestHeader(ctx, request);
            SelectableMessageQueue messageQueue = this.readQueueSelector.select(ctx, request, requestHeader);

            if (messageQueue == null) {
                throw new ProxyException(Code.FORBIDDEN, "no readable topic route for topic " + requestHeader.getTopic());
            }

            future = this.readConsumer.popMessage(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                requestHeader,
                requestHeader.getPollTime())
                .thenApply(result -> convertToReceiveMessageResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        future.whenComplete((response, throwable) -> {
            if (receiveMessageHook != null) {
                receiveMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        return future;
    }

    protected PopMessageRequestHeader buildPopMessageRequestHeader(Context ctx, ReceiveMessageRequest request) {
        checkSubscriptionData(request.getMessageQueue().getTopic(), request.getFilterExpression());
        boolean fifo = grpcClientManager.getClientSettings(ctx).getSubscription().getFifo();
        return GrpcConverter.buildPopMessageRequestHeader(request, GrpcConverter.buildPollTimeFromContext(ctx), fifo);
    }

    protected List<ReceiveMessageResponse> convertToReceiveMessageResponse(Context ctx, ReceiveMessageRequest request,
        PopResult result) {
        List<ReceiveMessageResponse> responseList = new ArrayList<>();
        PopStatus status = result.getPopStatus();
        switch (status) {
            case FOUND:
                List<Message> messageList = filterMessage(ctx, request, result.getMsgFoundList());
                if (messageList.isEmpty()) {
                    responseList.add(ReceiveMessageResponse.newBuilder()
                        .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                        .build());
                } else {
                    for (Message message : messageList) {
                        responseList.add(ReceiveMessageResponse.newBuilder()
                            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                            .setMessage(message)
                            .build());
                    }
                }
                break;
            case POLLING_FULL:
                responseList.add(ReceiveMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.TOO_MANY_REQUESTS, "polling full"))
                    .build());
                break;
            case NO_NEW_MSG:
            case POLLING_NOT_FOUND:
            default:
                responseList.add(ReceiveMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.OK, "no new message"))
                    .build());
                break;
        }
        return responseList;
    }

    protected List<Message> filterMessage(Context ctx, ReceiveMessageRequest request, List<MessageExt> messageExtList) {
        if (messageExtList == null || messageExtList.isEmpty()) {
            return Collections.emptyList();
        }
        Settings settings = grpcClientManager.getClientSettings(ctx);
        ClientType clientType = settings.getClientType();
        int maxAttempts = settings.getSubscription().getBackoffPolicy().getMaxAttempts();
        Resource topic = request.getMessageQueue().getTopic();
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        SubscriptionData subscriptionData = GrpcConverter.buildSubscriptionData(topicName, request.getFilterExpression());

        List<Message> resMessageList = new ArrayList<>();
        for (MessageExt messageExt : messageExtList) {
            if (ClientType.SIMPLE_CONSUMER.equals(clientType) && messageExt.getReconsumeTimes() >= maxAttempts) {
                forwardMessageToDLQ(ctx, request, messageExt, maxAttempts);
                continue;
            }
            if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
                this.ackNoMatchedMessage(ctx, request, messageExt);
                continue;
            }
            resMessageList.add(GrpcConverter.buildMessage(messageExt));
        }
        return resMessageList;
    }

    protected void forwardMessageToDLQ(Context ctx, ReceiveMessageRequest request, MessageExt messageExt,
        int maxReconsumeTimes) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();

        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            String brokerAddr = this.getBrokerAddr(ctx, handle.getBrokerName());
            Resource topic = request.getMessageQueue().getTopic();
            Resource group = request.getGroup();
            ConsumerSendMsgBackRequestHeader sendMsgBackRequestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(
                topic,
                group,
                handle,
                messageExt.getMsgId(),
                maxReconsumeTimes);
            AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(
                topic,
                group,
                handle);

            future = this.producer.sendMessageBackThenAckOrg(brokerAddr, sendMsgBackRequestHeader, ackMessageRequestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((result, throwable) -> {
            if (forwardToDLQInRecvMessageHook != null) {
                forwardToDLQInRecvMessageHook.beforeResponse(ctx, consumerSendMsgBackRequestHeader, result, throwable);
            }
        });
    }

    protected void ackNoMatchedMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();

        AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            String brokerAddr = this.getBrokerAddr(ctx, handle.getBrokerName());
            ackMessageRequestHeader.setConsumerGroup(GrpcConverter.wrapResourceWithNamespace(request.getGroup()));
            ackMessageRequestHeader.setTopic(messageExt.getTopic());
            ackMessageRequestHeader.setQueueId(handle.getQueueId());
            ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
            ackMessageRequestHeader.setOffset(handle.getOffset());

            future = this.writeConsumer.ackMessage(brokerAddr, ackMessageRequestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((ackResult, throwable) -> {
            if (ackNoMatchedMessageHook != null) {
                ackNoMatchedMessageHook.beforeResponse(ctx, ackMessageRequestHeader, ackResult, throwable);
            }
        });

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
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, ackMessageEntry.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            AckMessageRequestHeader requestHeader = this.buildAckMessageRequestHeader(ctx, request, receiptHandle);
            CompletableFuture<AckResult> ackResultFuture = this.writeConsumer.ackMessage(brokerAddr, requestHeader);
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
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            Settings settings = grpcClientManager.getClientSettings(ctx);
            int maxDeliveryAttempts = settings.getSubscription().getBackoffPolicy().getMaxAttempts();
            if (request.getDeliveryAttempt() >= maxDeliveryAttempts) {
                future = this.producer.sendMessageBack(
                    brokerAddr,
                    this.buildConsumerSendMsgBackToDLQRequestHeader(ctx, request, maxDeliveryAttempts)
                ).thenApply(result -> {
                    if (result.getCode() == ResponseCode.SUCCESS) {
                        writeConsumer.ackMessage(
                            brokerAddr,
                            this.buildAckMessageRequestHeader(ctx, request));
                    }
                    return convertToNackMessageResponse(ctx, request, result);
                });
            } else {
                ChangeInvisibleTimeRequestHeader requestHeader = this.buildChangeInvisibleTimeRequestHeader(ctx, request);
                future = this.writeConsumer.changeInvisibleTimeAsync(brokerAddr, receiptHandle.getBrokerName(), requestHeader)
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
        RetryPolicy retryPolicy = grpcClientManager.getClientSettings(ctx).getSubscription().getBackoffPolicy();
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
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            ChangeInvisibleTimeRequestHeader requestHeader = convertToChangeInvisibleTimeRequestHeader(ctx, request);
            future = this.writeConsumer.changeInvisibleTimeAsync(brokerAddr, receiptHandle.getBrokerName(), requestHeader)
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

    public ResponseHook<ReceiveMessageRequest, List<ReceiveMessageResponse>> getReceiveMessageHook() {
        return receiveMessageHook;
    }

    public void setReceiveMessageHook(
        ResponseHook<ReceiveMessageRequest, List<ReceiveMessageResponse>> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
    }

    public ResponseHook<AckMessageRequestHeader, AckResult> getAckNoMatchedMessageHook() {
        return ackNoMatchedMessageHook;
    }

    public void setAckNoMatchedMessageHook(
        ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook) {
        this.ackNoMatchedMessageHook = ackNoMatchedMessageHook;
    }

    public ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> getForwardToDLQInRecvMessageHook() {
        return forwardToDLQInRecvMessageHook;
    }

    public void setForwardToDLQInRecvMessageHook(
        ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> forwardToDLQInRecvMessageHook) {
        this.forwardToDLQInRecvMessageHook = forwardToDLQInRecvMessageHook;
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
