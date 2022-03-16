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
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.ForwardReadConsumer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ParameterConverter;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;

public class ReceiveMessageService extends BaseService {

    private final ForwardReadConsumer readConsumer;
    private final ForwardWriteConsumer writeConsumer;
    private final TopicRouteCache topicRouteCache;

    private ParameterConverter<PopResult, ReceiveMessageResponse> popResultResponseParameterConverter;
    private ReceiveMessageQueueSelector receiveMessageQueueSelector;
    private ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook = null;

    private ResponseHook<AckMessageRequest, AckMessageResponse> ackMessageHook = null;

    public ReceiveMessageService(ConnectorManager connectorManager) {
        super(connectorManager);
        this.readConsumer = connectorManager.getForwardReadConsumer();
        this.writeConsumer = connectorManager.getForwardWriteConsumer();
        this.topicRouteCache = connectorManager.getTopicRouteCache();

        this.popResultResponseParameterConverter = new DefaultPopResultResponseParameterConverter();
        this.receiveMessageQueueSelector = new DefaultReceiveMessageQueueSelector(connectorManager.getTopicRouteCache());
    }

    public void setPopResultResponseParameterConverter(
        ParameterConverter<PopResult, ReceiveMessageResponse> popResultResponseParameterConverter) {
        this.popResultResponseParameterConverter = popResultResponseParameterConverter;
    }

    public void setReceiveMessageQueueSelector(ReceiveMessageQueueSelector receiveMessageQueueSelector) {
        this.receiveMessageQueueSelector = receiveMessageQueueSelector;
    }

    public void setReceiveMessageHook(ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook) {
        this.receiveMessageHook = receiveMessageHook;
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
}
