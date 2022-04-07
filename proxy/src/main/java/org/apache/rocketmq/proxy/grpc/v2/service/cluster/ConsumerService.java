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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Settings;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.common.DelayPolicy;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerService extends BaseService {
    private final DelayPolicy delayPolicy;
    private final ForwardReadConsumer readConsumer;
    private final ForwardWriteConsumer writeConsumer;
    /**
     * For sending messages back to broker.
     */
    private final ForwardProducer producer;

    private volatile ReadQueueSelector readQueueSelector;
    private volatile ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;
    private volatile ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook;
    private volatile ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook;
    private volatile ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageResponseResponseHook;

    public ConsumerService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();
        this.producer = connectorManager.getForwardProducer();

        this.readQueueSelector = new DefaultReadQueueSelector(connectorManager.getTopicRouteCache());
        this.delayPolicy = DelayPolicy.build(ConfigurationManager.getProxyConfig().getMessageDelayLevel());
    }

    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        CompletableFuture<ReceiveMessageResponse> future = new CompletableFuture<>();
        // register hook.
        future.whenComplete((response, throwable) -> {
            if (receiveMessageHook != null) {
                receiveMessageHook.beforeResponse(ctx, request, response, throwable);
            }
        });

        try {
            PopMessageRequestHeader requestHeader = this.buildPopMessageRequestHeader(ctx, request);
            SelectableMessageQueue messageQueue = this.readQueueSelector.select(ctx, request, requestHeader);

            if (messageQueue == null) {
                throw new ProxyException(Code.FORBIDDEN, "no readable topic route for topic " + requestHeader.getTopic());
            }

            CompletableFuture<PopResult> popResultFuture = this.readConsumer.popMessage(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                requestHeader,
                requestHeader.getPollTime());
            popResultFuture
                .thenAccept(result -> {
                    try {
                        future.complete(convertToReceiveMessageResponse(ctx, request, result));
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

    protected PopMessageRequestHeader buildPopMessageRequestHeader(Context ctx, ReceiveMessageRequest request) {
        checkSubscriptionData(request.getMessageQueue().getTopic(), request.getFilterExpression());
        return GrpcConverter.buildPopMessageRequestHeader(request, GrpcConverter.buildPollTimeFromContext(ctx));
    }

    protected ReceiveMessageResponse convertToReceiveMessageResponse(Context ctx, ReceiveMessageRequest request, PopResult result) {
        PopStatus status = result.getPopStatus();
        switch (status) {
            case FOUND:
                return ReceiveMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                    .addAllMessages(checkAndGetMessagesFromPopResult(ctx, request, result))
                    .build();
            case POLLING_FULL:
                return ReceiveMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.TOO_MANY_REQUESTS, "polling full"))
                    .build();
            case NO_NEW_MSG:
            case POLLING_NOT_FOUND:
            default:
                return ReceiveMessageResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.OK, "no new message"))
                    .build();
        }
    }

    protected List<Message> checkAndGetMessagesFromPopResult(Context ctx, ReceiveMessageRequest request, PopResult result) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
        SubscriptionData subscriptionData = GrpcConverter.buildSubscriptionData(topicName, request.getFilterExpression());

        List<Message> messages = new ArrayList<>();
        for (MessageExt messageExt : result.getMsgFoundList()) {
            if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
                this.ackNoMatchedMessage(ctx, request, messageExt);
                continue;
            }
            messages.add(GrpcConverter.buildMessage(messageExt));
        }

        return messages;
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
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            AckMessageRequestHeader requestHeader = this.buildAckMessageRequestHeader(ctx, request);
            CompletableFuture<AckResult> ackResultFuture = this.writeConsumer.ackMessage(brokerAddr, requestHeader);
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

    protected AckMessageRequestHeader buildAckMessageRequestHeader(Context ctx, AckMessageRequest request) {
        return GrpcConverter.buildAckMessageRequestHeader(request);
    }

    protected AckMessageResponse convertToAckMessageResponse(Context ctx, AckMessageRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return AckMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return AckMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "ack failed: status is abnormal"))
            .build();
    }

    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (nackMessageResponseResponseHook != null) {
                nackMessageResponseResponseHook.beforeResponse(ctx, request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = this.resolveReceiptHandle(ctx, request.getReceiptHandle());
            String brokerAddr = this.getBrokerAddr(ctx, receiptHandle.getBrokerName());

            Settings settings = GrpcClientManager.getClientSettings(ctx);
            int maxDeliveryAttempts = settings.getSubscription().getDeadLetterPolicy().getMaxDeliveryAttempts();
            if (request.getDeliveryAttempt() >= maxDeliveryAttempts) {
                CompletableFuture<RemotingCommand> resultFuture = this.producer.sendMessageBack(
                    brokerAddr,
                    this.buildConsumerSendMsgBackToDLQRequestHeader(ctx, request, maxDeliveryAttempts)
                );

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
            } else {
                ChangeInvisibleTimeRequestHeader requestHeader = this.buildChangeInvisibleTimeRequestHeader(ctx, request);
                CompletableFuture<AckResult> resultFuture = this.writeConsumer.changeInvisibleTimeAsync(brokerAddr, receiptHandle.getBrokerName(), requestHeader);
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
            }
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected ChangeInvisibleTimeRequestHeader buildChangeInvisibleTimeRequestHeader(Context ctx, NackMessageRequest request) {
        return GrpcConverter.buildChangeInvisibleTimeRequestHeader(request, this.delayPolicy);
    }

    protected ConsumerSendMsgBackRequestHeader buildConsumerSendMsgBackToDLQRequestHeader(Context ctx, NackMessageRequest request,
        int maxReconsumeTimes) {
        return GrpcConverter.buildConsumerSendMsgBackToDLQRequestHeader(request, maxReconsumeTimes);
    }

    protected NackMessageResponse convertToNackMessageResponse(Context ctx, NackMessageRequest request, AckResult ackResult) {
        if (AckStatus.OK.equals(ackResult.getStatus())) {
            return NackMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build();
        }
        return NackMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "nack failed: status is abnormal"))
            .build();
    }

    protected NackMessageResponse convertToNackMessageResponse(Context ctx, NackMessageRequest request, RemotingCommand sendMsgBackToDLQResult) {
        return NackMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(sendMsgBackToDLQResult.getCode(), sendMsgBackToDLQResult.getRemark()))
            .build();
    }

    public void setReadQueueSelector(ReadQueueSelector readQueueSelector) {
        this.readQueueSelector = readQueueSelector;
    }

    public void setReceiveMessageHook(ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
    }

    public void setAckNoMatchedMessageHook(ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook) {
        this.ackNoMatchedMessageHook = ackNoMatchedMessageHook;
    }

    public void setAckMessageHook(ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook) {
        this.ackMessageHook = ackMessageHook;
    }
}
