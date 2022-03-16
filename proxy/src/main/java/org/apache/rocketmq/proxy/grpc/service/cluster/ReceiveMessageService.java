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
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ParameterConverter;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;

public class ReceiveMessageService extends BaseService {

    private final ForwardReadConsumer readConsumer;
    private final ForwardWriteConsumer writeConsumer;
    private final TopicRouteCache topicRouteCache;

    private ParameterConverter<String /* addr */, String /* brokerName */> brokerAddrConverter;
    private ParameterConverter<String, ReceiptHandle> receiptHandleConverter;

    private ParameterConverter<PopResult, ReceiveMessageResponse> popResultResponseParameterConverter;
    private ReceiveMessageQueueSelector receiveMessageQueueSelector;
    private ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook = null;

    private ParameterConverter<AckMessageRequest, AckMessageRequestHeader> ackMessageRequestConverter;
    private ParameterConverter<AckResult, AckMessageResponse> ackMessageResponseConverter;
    private ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook = null;

    private ParameterConverter<NackMessageRequest, ChangeInvisibleTimeRequestHeader> nackMessageRequestConverter;
    private ParameterConverter<AckResult, NackMessageResponse> nackMessageResponseConverter;
    private ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook = null;

    public ReceiveMessageService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();
        this.topicRouteCache = connectorManager.getTopicRouteCache();

        this.brokerAddrConverter = (ctx, brokerName) -> this.topicRouteCache.getBrokerAddr(brokerName);
        this.receiptHandleConverter = (ctx, handleStr) -> ReceiptHandle.decode(handleStr);

        this.popResultResponseParameterConverter = new DefaultPopResultResponseParameterConverter();
        this.receiveMessageQueueSelector = new DefaultReceiveMessageQueueSelector(connectorManager.getTopicRouteCache());

        this.ackMessageRequestConverter = (ctx, request) -> Converter.buildAckMessageRequestHeader(request);
        this.ackMessageResponseConverter = new DefaultAckMessageResponseConverter();

        this.nackMessageRequestConverter = (ctx, request) -> Converter.buildChangeInvisibleTimeRequestHeader(request);
        this.nackMessageResponseConverter = new DefaultNackMessageResponseConverter();
    }

    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        CompletableFuture<ReceiveMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (receiveMessageHook != null) {
                receiveMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            long timeRemaining = Context.current()
                .getDeadline()
                .timeRemaining(TimeUnit.MILLISECONDS);
            long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
            if (pollTime <= 0) {
                pollTime = timeRemaining;
            }

            PopMessageRequestHeader requestHeader = Converter.buildPopMessageRequestHeader(request, pollTime);
            SelectableMessageQueue messageQueue = this.receiveMessageQueueSelector.select(ctx, request, requestHeader);

            CompletableFuture<PopResult> popResultFuture = this.readConsumer.popMessage(
                messageQueue.getBrokerAddr(),
                messageQueue.getBrokerName(),
                requestHeader,
                pollTime);
            popResultFuture.thenAccept(result -> {
                try {
                    future.complete(popResultResponseParameterConverter.convert(ctx, result));
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (ackMessageHook != null) {
                ackMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = receiptHandleConverter.convert(ctx, request.getReceiptHandle());
            if (receiptHandle.isExpired()) {
                future.complete(AckMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "handle has expired"))
                    .build());
                return future;
            }

            String brokerAddr = brokerAddrConverter.convert(ctx, receiptHandle.getBrokerName());
            AckMessageRequestHeader requestHeader = this.ackMessageRequestConverter.convert(ctx, request);
            CompletableFuture<AckResult> ackResultFuture = this.writeConsumer.ackMessage(brokerAddr, requestHeader, ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            ackResultFuture.thenAccept(result -> {
                try {
                    future.complete(ackMessageResponseConverter.convert(ctx, result));
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            }).exceptionally(throwable -> {
                future.completeExceptionally(throwable);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (nackMessageHook != null) {
                nackMessageHook.beforeResponse(request, response, throwable);
            }
        });
        try {
            ReceiptHandle receiptHandle = this.receiptHandleConverter.convert(ctx, request.getReceiptHandle());
            if (receiptHandle.isExpired()) {
                future.complete(NackMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "handle has expired"))
                    .build());
                return future;
            }

            String brokerAddr = brokerAddrConverter.convert(ctx, receiptHandle.getBrokerName());
            ChangeInvisibleTimeRequestHeader requestHeader = this.nackMessageRequestConverter.convert(ctx, request);
            CompletableFuture<AckResult> resultFuture = this.writeConsumer.changeInvisibleTimeAsync(brokerAddr, receiptHandle.getBrokerName(), requestHeader,
                ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT);
            resultFuture.thenAccept(result -> {
                try {
                    future.complete(nackMessageResponseConverter.convert(ctx, result));
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            }).exceptionally(throwable -> {
                future.completeExceptionally(throwable);
                return null;
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public void setBrokerAddrConverter(
        ParameterConverter<String, String> brokerAddrConverter) {
        this.brokerAddrConverter = brokerAddrConverter;
    }

    public void setReceiptHandleConverter(
        ParameterConverter<String, ReceiptHandle> receiptHandleConverter) {
        this.receiptHandleConverter = receiptHandleConverter;
    }

    public void setPopResultResponseParameterConverter(
        ParameterConverter<PopResult, ReceiveMessageResponse> popResultResponseParameterConverter) {
        this.popResultResponseParameterConverter = popResultResponseParameterConverter;
    }

    public void setReceiveMessageQueueSelector(
        ReceiveMessageQueueSelector receiveMessageQueueSelector) {
        this.receiveMessageQueueSelector = receiveMessageQueueSelector;
    }

    public void setReceiveMessageHook(
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
    }

    public void setAckMessageRequestConverter(
        ParameterConverter<AckMessageRequest, AckMessageRequestHeader> ackMessageRequestConverter) {
        this.ackMessageRequestConverter = ackMessageRequestConverter;
    }

    public void setAckMessageResponseConverter(
        ParameterConverter<AckResult, AckMessageResponse> ackMessageResponseConverter) {
        this.ackMessageResponseConverter = ackMessageResponseConverter;
    }

    public void setAckMessageHook(
        ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook) {
        this.ackMessageHook = ackMessageHook;
    }

    public void setNackMessageRequestConverter(
        ParameterConverter<NackMessageRequest, ChangeInvisibleTimeRequestHeader> nackMessageRequestConverter) {
        this.nackMessageRequestConverter = nackMessageRequestConverter;
    }

    public void setNackMessageResponseConverter(
        ParameterConverter<AckResult, NackMessageResponse> nackMessageResponseConverter) {
        this.nackMessageResponseConverter = nackMessageResponseConverter;
    }

    public void setNackMessageHook(
        ResponseHook<NackMessageRequest, NackMessageResponse> nackMessageHook) {
        this.nackMessageHook = nackMessageHook;
    }
}
