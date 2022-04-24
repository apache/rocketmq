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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendReceipt;
import com.beust.jcommander.internal.Lists;
import io.grpc.Context;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
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

        try {
            SelectableMessageQueue selectableMessageQueue = writeQueueSelector.selectQueue(ctx, request);
            if (selectableMessageQueue == null) {
                throw new ProxyException(Code.FORBIDDEN, "no writeable topic route");
            }

            // send message to broker.
            future = this.producer.sendMessage(
                selectableMessageQueue.getBrokerAddr(),
                selectableMessageQueue.getBrokerName(),
                convertToMessageList(ctx, request),
                convertToSendMessageRequestHeader(ctx, request, selectableMessageQueue)
            ).thenApply(result -> convertToSendMessageResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((response, throwable) -> {
            if (sendMessageHook != null) {
                sendMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        return future;
    }

    protected SendMessageRequestHeader convertToSendMessageRequestHeader(Context ctx, SendMessageRequest request, SelectableMessageQueue selectableMessageQueue) {
        // use topic name as group
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessages(0).getTopic());
        return GrpcConverter.buildSendMessageRequestHeader(request, topicName,
            selectableMessageQueue.getQueueId());
    }

    protected List<org.apache.rocketmq.common.message.Message> convertToMessageList(Context ctx, SendMessageRequest request) {
        // use topic name as group
        Resource topic = request.getMessages(0).getTopic();
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        return GrpcConverter.buildMessage(request.getMessagesList(), topic);
    }

    protected SendMessageResponse convertToSendMessageResponse(Context ctx, SendMessageRequest request,
        SendResult result) {
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            return SendMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "send message failed, sendStatus=" + result.getSendStatus()))
                .build();
        }

        List<SendReceipt> sendReceiptList = Lists.newArrayList();
        sendReceiptList.add(SendReceipt.newBuilder()
            .setMessageId(StringUtils.defaultString(result.getMsgId()))
            .setTransactionId(StringUtils.defaultString(result.getTransactionId()))
            .build());
        return SendMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
            .addAllReceipts(sendReceiptList)
            .build();
    }

    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();

        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());
            ConsumerSendMsgBackRequestHeader sendMsgBackRequestHeader = this.buildConsumerSendMsgBackRequestHeader(ctx, request);
            AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(
                request.getTopic(), request.getGroup(), receiptHandle);

            future = this.producer.sendMessageBackThenAckOrg(brokerAddr, sendMsgBackRequestHeader, ackMessageRequestHeader)
                .thenApply(result -> convertToForwardMessageToDeadLetterQueueResponse(ctx, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        future.whenComplete((response, throwable) -> {
            if (forwardMessageToDLQHook != null) {
                forwardMessageToDLQHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        return future;
    }

    protected ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackRequestHeader(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return GrpcConverter.buildConsumerSendMsgBackRequestHeader(request);
    }

    protected ForwardMessageToDeadLetterQueueResponse convertToForwardMessageToDeadLetterQueueResponse(Context ctx,
        RemotingCommand result) {
        return ForwardMessageToDeadLetterQueueResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(result.getCode(), result.getRemark()))
            .build();
    }
}
