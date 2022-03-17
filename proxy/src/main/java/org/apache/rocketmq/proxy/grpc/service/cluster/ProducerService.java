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
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ProducerService extends BaseService {

    private volatile ProducerQueueSelector messageQueueSelector;
    private volatile ResponseHook<SendMessageRequest, SendMessageResponse> sendMessageHook = null;
    private volatile ResponseHook<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> forwardMessageToDLQHook = null;

    public ProducerService(ConnectorManager connectorManager) {
        super(connectorManager);
        messageQueueSelector = new DefaultProducerQueueSelector(this.connectorManager.getTopicRouteCache());
    }

    public void setSendMessageHook(ResponseHook<SendMessageRequest, SendMessageResponse> sendMessageHook) {
        this.sendMessageHook = sendMessageHook;
    }

    public void setMessageQueueSelector(ProducerQueueSelector messageQueueSelector) {
        this.messageQueueSelector = messageQueueSelector;
    }

    public void setForwardMessageToDLQHook(
        ResponseHook<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> forwardMessageToDLQHook) {
        this.forwardMessageToDLQHook = forwardMessageToDLQHook;
    }

    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (sendMessageHook != null) {
                sendMessageHook.beforeResponse(request, response, throwable);
            }
        });

        try {
            Pair<SendMessageRequestHeader, org.apache.rocketmq.common.message.Message> requestPair = this.convertSendMessageRequest(ctx, request);
            SendMessageRequestHeader requestHeader = requestPair.getLeft();
            org.apache.rocketmq.common.message.Message message = requestPair.getRight();
            SelectableMessageQueue addressableMessageQueue = messageQueueSelector.selectQueue(ctx, request, requestHeader, message);

            String topic = requestHeader.getTopic();
            if (addressableMessageQueue == null) {
                throw new ProxyException(Code.NOT_FOUND, "no writeable topic route for topic " + topic);
            }

            CompletableFuture<SendResult> sendResultCompletableFuture = this.connectorManager.getForwardProducer().sendMessage(
                addressableMessageQueue.getBrokerAddr(),
                addressableMessageQueue.getBrokerName(),
                message,
                requestHeader,
                ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT
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

    protected Pair<SendMessageRequestHeader, org.apache.rocketmq.common.message.Message> convertSendMessageRequest(
        Context ctx, SendMessageRequest request) {
        return Pair.of(Converter.buildSendMessageRequestHeader(request), Converter.buildMessage(request.getMessage()));
    }

    protected SendMessageResponse convertToSendMessageResponse(Context ctx, SendMessageRequest request,
        SendResult sendResult) {
        if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
            return SendMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "send message failed, sendStatus=" + sendResult.getSendStatus()))
                .build();
        }

        if (StringUtils.isNotBlank(sendResult.getTransactionId())) {
            Message message = request.getMessage();
            String group = Converter.getResourceNameWithNamespace(message.getSystemAttribute().getProducerGroup());
            String topic = Converter.getResourceNameWithNamespace(message.getTopic());
            this.connectorManager.getTransactionHeartbeatRegisterService().addProducerGroup(group, topic);
        }

        return SendMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .setMessageId(StringUtils.defaultString(sendResult.getMsgId()))
            .setTransactionId(StringUtils.defaultString(sendResult.getTransactionId()))
            .build();
    }

    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (forwardMessageToDLQHook != null) {
                forwardMessageToDLQHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());
            ConsumerSendMsgBackRequestHeader requestHeader = this.convertToConsumerSendMsgBackRequestHeader(ctx, request);
            CompletableFuture<RemotingCommand> resultFuture = this.connectorManager.getForwardProducer()
                .sendMessageBack(brokerAddr, requestHeader, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            resultFuture.thenAccept(result -> {
                future.complete(ForwardMessageToDeadLetterQueueResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(result.getCode(), result.getRemark()))
                    .build());
            }).exceptionally(throwable -> {
                future.completeExceptionally(throwable);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ConsumerSendMsgBackRequestHeader convertToConsumerSendMsgBackRequestHeader(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return Converter.buildConsumerSendMsgBackRequestHeader(request);
    }
}
