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

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.grpc.common.ParameterConverter;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;
import org.apache.rocketmq.proxy.grpc.common.ProxyResponseCode;
import org.apache.rocketmq.proxy.grpc.common.ResponseHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private volatile ParameterConverter<SendMessageRequest, SendMessageRequestHeader> parameterConverter;
    private volatile ParameterConverter<Message, org.apache.rocketmq.common.message.Message> messageConverter;
    private volatile ParameterConverter<SendResult, SendMessageResponse> responseConverter;
    private volatile ProducerQueueSelector messageQueueSelector;
    private volatile ResponseHook<SendMessageRequest, SendMessageResponse> producerServiceHook = null;

    public ProducerService(ConnectorManager connectorManager) {
        super(connectorManager);

        parameterConverter = new DefaultProducerRequestConverter();
        messageConverter = new DefaultProducerMessageConverter();
        responseConverter = new DefaultProducerResponseConverter();
        messageQueueSelector = new DefaultProducerQueueSelector(this.connectorManager.getTopicRouteCache());
    }

    public void setParameterConverter(
        ParameterConverter<SendMessageRequest, SendMessageRequestHeader> parameterConverter) {
        this.parameterConverter = parameterConverter;
    }

    public void setMessageConverter(
        ParameterConverter<Message, org.apache.rocketmq.common.message.Message> messageConverter) {
        this.messageConverter = messageConverter;
    }

    public void setResponseConverter(ParameterConverter<SendResult, SendMessageResponse> responseConverter) {
        this.responseConverter = responseConverter;
    }

    public void setProducerServiceHook(ResponseHook<SendMessageRequest, SendMessageResponse> producerServiceHook) {
        this.producerServiceHook = producerServiceHook;
    }

    public void setMessageQueueSelector(ProducerQueueSelector messageQueueSelector) {
        this.messageQueueSelector = messageQueueSelector;
    }

    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        future.whenComplete((response, throwable) -> {
            if (producerServiceHook != null) {
                producerServiceHook.beforeResponse(request, response, throwable);
            }
        });

        try {
            org.apache.rocketmq.common.message.Message message = messageConverter.convert(ctx, request.getMessage());
            SendMessageRequestHeader requestHeader = this.parameterConverter.convert(ctx, request);
            SelectableMessageQueue addressableMessageQueue = messageQueueSelector.selectQueue(ctx, request, requestHeader, message);

            String topic = requestHeader.getTopic();
            if (addressableMessageQueue == null) {
                throw new ProxyException(ProxyResponseCode.NO_TOPIC_ROUTE,
                    "no writeable topic route for topic " + topic);
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
                        future.complete(this.responseConverter.convert(ctx, result));
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
}
