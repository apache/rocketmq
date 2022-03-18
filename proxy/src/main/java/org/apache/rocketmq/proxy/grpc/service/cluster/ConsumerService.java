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

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.DelayPolicy;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ConsumerService extends BaseService {

    private final ForwardReadConsumer readConsumer;
    private final ForwardWriteConsumer writeConsumer;

    private volatile ReadQueueSelector readQueueSelector;
    private volatile ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook = null;
    private volatile ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook = null;
    private volatile ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook = null;

    private final DelayPolicy delayPolicy;

    public ConsumerService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();

        this.readQueueSelector = new DefaultReadQueueSelector(connectorManager.getTopicRouteCache());
        this.delayPolicy = DelayPolicy.build(ConfigurationManager.getProxyConfig().getMessageDelayLevel());
    }

    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        CompletableFuture<ReceiveMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (receiveMessageHook != null) {
                receiveMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            PopMessageRequestHeader requestHeader = this.convertToPopMessageRequestHeader(ctx, request);
            SelectableMessageQueue messageQueue = this.readQueueSelector.select(ctx, request, requestHeader);

            CompletableFuture<PopResult> popResultFuture = this.readConsumer.popMessage(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                requestHeader,
                requestHeader.getPollTime());
            popResultFuture.thenAccept(result -> {
                try {
                    future.complete(convertToReceiveMessageResponse(ctx, request, result));
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected PopMessageRequestHeader convertToPopMessageRequestHeader(Context ctx, ReceiveMessageRequest request) {
        long timeRemaining = ctx.getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }

        return Converter.buildPopMessageRequestHeader(request, pollTime);
    }

    protected ReceiveMessageResponse convertToReceiveMessageResponse(Context ctx, ReceiveMessageRequest request, PopResult result) {
        PopStatus status = result.getPopStatus();
        switch (status) {
            case FOUND:
                break;
            case POLLING_FULL:
                return ReceiveMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.RESOURCE_EXHAUSTED, "polling full"))
                    .build();
            case NO_NEW_MSG:
            case POLLING_NOT_FOUND:
            default:
                return ReceiveMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.OK, "no new message"))
                    .build();
        }

        List<Message> messages = new ArrayList<>();
        for (MessageExt messageExt : result.getMsgFoundList()) {
            messages.add(Converter.buildMessage(messageExt));
        }

        return ReceiveMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
            .addAllMessages(messages)
            .build();
    }

    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (ackMessageHook != null) {
                ackMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            AckMessageRequestHeader requestHeader = this.convertToAckMessageRequestHeader(ctx, request);
            CompletableFuture<AckResult> ackResultFuture = this.writeConsumer.ackMessage(brokerAddr, requestHeader, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            ackResultFuture
                    .thenAccept(result -> {
                        try {
                            future.complete(convertToAckMessageResponse(ctx, request, result));
                        } catch (Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }
                    })
                    .exceptionally(throwable -> {
                        future.completeExceptionally(throwable);
                        return null;
                    });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected AckMessageRequestHeader convertToAckMessageRequestHeader(Context ctx, AckMessageRequest request) {
        return Converter.buildAckMessageRequestHeader(request);
    }

    protected AckMessageResponse convertToAckMessageResponse(Context ctx, AckMessageRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "ack failed: status is abnormal"))
            .build();
    }

    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (nackMessageHook != null) {
                nackMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            ChangeInvisibleTimeRequestHeader requestHeader = this.convertToChangeInvisibleTimeRequestHeader(ctx, request);
            CompletableFuture<AckResult> resultFuture = this.writeConsumer.changeInvisibleTimeAsync(brokerAddr, receiptHandle.getBrokerName(), requestHeader,
                ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            resultFuture
                    .thenAccept(result -> {
                        try {
                            future.complete(convertToNackMessageResponse(ctx, request, result));
                        } catch (Throwable throwable) {
                            future.completeExceptionally(throwable);
                        }
                    })
                    .exceptionally(throwable -> {
                        future.completeExceptionally(throwable);
                        return null;
                    });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ChangeInvisibleTimeRequestHeader convertToChangeInvisibleTimeRequestHeader(Context ctx, NackMessageRequest request) {
        return Converter.buildChangeInvisibleTimeRequestHeader(request, delayPolicy);
    }

    protected NackMessageResponse convertToNackMessageResponse(Context ctx, NackMessageRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return NackMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.OK, Code.OK.name()))
                .build();
        }
        return NackMessageResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "nack failed: status is abnormal"))
            .build();
    }

    public void setReadQueueSelector(
        ReadQueueSelector readQueueSelector) {
        this.readQueueSelector = readQueueSelector;
    }

    public void setReceiveMessageHook(
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
    }

    public void setAckMessageHook(
        ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook) {
        this.ackMessageHook = ackMessageHook;
    }

    public void setNackMessageHook(
        ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook) {
        this.nackMessageHook = nackMessageHook;
    }
}
