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

import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ProducerService extends BaseService {

    private final ForwardProducer producer;
    private volatile WriteQueueSelector writeQueueSelector;
    private volatile ResponseHook<SendMessageRequest, SendMessageResponse> sendMessageHook;
    private volatile ResponseHook<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> forwardMessageToDLQHook;


    public ProducerService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.producer = connectorManager.getForwardProducer();
        writeQueueSelector = new DefaultWriteQueueSelector(this.connectorManager.getTopicRouteCache());
    }

    public void setSendMessageHook(ResponseHook<SendMessageRequest, SendMessageResponse> sendMessageHook) {
        this.sendMessageHook = sendMessageHook;
    }

    public void setWriteQueueSelector(WriteQueueSelector writeQueueSelector) {
        this.writeQueueSelector = writeQueueSelector;
    }

    public void setForwardMessageToDLQHook(
        ResponseHook<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> forwardMessageToDLQHook) {
        this.forwardMessageToDLQHook = forwardMessageToDLQHook;
    }

    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (sendMessageHook != null) {
                sendMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            Pair<SendMessageRequestHeader, org.apache.rocketmq.common.message.Message> requestPair = this.buildSendMessageRequest(ctx, request);
            SendMessageRequestHeader requestHeader = requestPair.getLeft();
            org.apache.rocketmq.common.message.Message message = requestPair.getRight();
            SelectableMessageQueue selectableMessageQueue = writeQueueSelector.selectQueue(ctx, request, requestHeader, message);

            String topic = requestHeader.getTopic();
            if (selectableMessageQueue == null) {
                throw new ProxyException(Code.NOT_FOUND, "no writeable topic route for topic: " + topic);
            }

            // send message to broker.
            CompletableFuture<SendResult> sendResultCompletableFuture = this.producer.sendMessage(
                selectableMessageQueue.getBrokerAddr(),
                selectableMessageQueue.getBrokerName(),
                message,
                requestHeader
            );

            sendResultCompletableFuture
                .thenAccept(result -> {
                    try {
                        future.complete(convertToSendMessageResponse(ctx, request, result));
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                })
                .exceptionally(e -> {
                    future.completeExceptionally(e);
                    return null;
                });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected Pair<SendMessageRequestHeader, org.apache.rocketmq.common.message.Message> buildSendMessageRequest(
        Context ctx, SendMessageRequest request) {
        SendMessageRequestHeader requestHeader = GrpcConverter.buildSendMessageRequestHeader(request);
        org.apache.rocketmq.common.message.Message message = GrpcConverter.buildMessage(request.getMessage());
        return Pair.of(requestHeader, message);
    }

    protected SendMessageResponse convertToSendMessageResponse(Context ctx, SendMessageRequest request, SendResult result) {
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            return SendMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "send message failed, sendStatus=" + result.getSendStatus()))
                .build();
        }

        if (StringUtils.isNotBlank(result.getTransactionId())) {
            Message message = request.getMessage();
            String group = GrpcConverter.wrapResourceWithNamespace(message.getSystemAttribute().getProducerGroup());
            String topic = GrpcConverter.wrapResourceWithNamespace(message.getTopic());
            this.connectorManager.getTransactionHeartbeatRegisterService().addProducerGroup(group, topic);
        }

        return SendMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .setMessageId(StringUtils.defaultString(result.getMsgId()))
            .setTransactionId(StringUtils.defaultString(result.getTransactionId())) // use "" if transactionID is null.
            .build();
    }

    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (forwardMessageToDLQHook != null) {
                forwardMessageToDLQHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());
            ConsumerSendMsgBackRequestHeader requestHeader = this.buildConsumerSendMsgBackRequestHeader(ctx, request);
            CompletableFuture<RemotingCommand> resultFuture = this.producer.sendMessageBack(brokerAddr, requestHeader);
            resultFuture
                .thenAccept(result ->
                    future.complete(
                        ForwardMessageToDeadLetterQueueResponse.newBuilder()
                            .setCommon(ResponseBuilder.buildCommon(result.getCode(), result.getRemark()))
                            .build()
                    )
                )
                .exceptionally(throwable -> {
                    future.completeExceptionally(throwable);
                    return null;
                });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return GrpcConverter.buildConsumerSendMsgBackRequestHeader(request);
    }
}
