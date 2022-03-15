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

import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.proxy.client.ClientManager;
import org.apache.rocketmq.proxy.client.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;
import org.apache.rocketmq.proxy.grpc.common.ProxyResponseCode;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(ProducerService.class);

    private volatile ProducerServiceHook producerServiceHook = null;
    private volatile MessageQueueSelector messageQueueSelector = new DefaultMessageQueueSelector();

    public ProducerService(ClientManager clientManager) {
        super(clientManager);
    }

    public interface MessageQueueSelector {
        AddressableMessageQueue selectQueue(Context ctx, SendMessageRequest request, SendMessageRequestHeader requestHeader,
            org.apache.rocketmq.common.message.Message message);
    }

    public class DefaultMessageQueueSelector implements MessageQueueSelector {

        @Override
        public AddressableMessageQueue selectQueue(Context ctx, SendMessageRequest request, SendMessageRequestHeader requestHeader,
            org.apache.rocketmq.common.message.Message message) {
            try {
                String topic = requestHeader.getTopic();
                String brokerName = "";
                if (request.hasPartition()) {
                    brokerName = request.getPartition().getBroker().getName();
                }
                Integer queueId = requestHeader.getQueueId();
                String shardingKey = message.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
                AddressableMessageQueue addressableMessageQueue;
                if (!StringUtils.isBlank(brokerName) && queueId != null) {
                    // Grpc client sendSelect situation
                    addressableMessageQueue = selectTargetQueue(topic, brokerName, queueId);
                } else if (shardingKey != null) {
                    // With shardingKey
                    addressableMessageQueue = selectOrderQueue(topic, shardingKey);
                } else {
                    addressableMessageQueue = selectNormalQueue(topic);
                }
                return addressableMessageQueue;
            } catch (Exception e) {
                log.error("error when select queue in DefaultMessageQueueSelector. request: {}", request, e);
                return null;
            }
        }

        protected AddressableMessageQueue selectNormalQueue(String topic) throws Exception {
            return clientManager.getTopicRouteCache().selectOneWriteQueue(topic, null);
        }

        protected AddressableMessageQueue selectTargetQueue(String topic, String brokerName, int queueId) throws Exception {
            return clientManager.getTopicRouteCache().selectOneWriteQueue(topic, brokerName, queueId);
        }

        protected AddressableMessageQueue selectOrderQueue(String topic, String shardingKey) throws Exception {
            return clientManager.getTopicRouteCache().selectOneWriteQueueByKey(topic, shardingKey, null);
        }
    }

    public interface ProducerServiceHook {

        void beforeSend(Context ctx, AddressableMessageQueue addressableMessageQueue, Message msg, SendMessageRequestHeader requestHeader);

        void afterSend(Context ctx, AddressableMessageQueue addressableMessageQueue, Message msg, SendMessageRequestHeader requestHeader,
            SendResult sendResult);
    }

    public void setProducerServiceHook(ProducerServiceHook hook) {
        this.producerServiceHook = hook;
    }

    public void setMessageQueueSelector(MessageQueueSelector messageQueueSelector) {
        this.messageQueueSelector = messageQueueSelector;
    }

    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        org.apache.rocketmq.common.message.Message message = Converter.buildMessage(request.getMessage());
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();

        try {
            SendMessageRequestHeader requestHeader = Converter.buildSendMessageRequestHeader(request);
            AddressableMessageQueue addressableMessageQueue = messageQueueSelector.selectQueue(ctx, request, requestHeader, message);

            String topic = requestHeader.getTopic();
            if (addressableMessageQueue == null) {
                throw new ProxyException(ProxyResponseCode.NO_TOPIC_ROUTE,
                    "no writeable topic route for topic " + topic);
            }

            if (producerServiceHook != null) {
                producerServiceHook.beforeSend(ctx, addressableMessageQueue, message, requestHeader);
            }
            CompletableFuture<SendResult> sendResultCompletableFuture = this.clientManager.getProducerClient().sendMessage(
                addressableMessageQueue.getBrokerAddr(),
                addressableMessageQueue.getBrokerName(),
                message,
                requestHeader,
                ProxyUtils.DEFAULT_MQ_CLIENT_TIMEOUT
            );
            sendResultCompletableFuture
                .thenAccept(result -> {
                    if (producerServiceHook != null) {
                        producerServiceHook.afterSend(ctx, addressableMessageQueue, message, requestHeader, result);
                    }
                    future.complete(ResponseBuilder.buildSendMessageResponse(result));
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
