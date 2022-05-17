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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import com.google.protobuf.util.Durations;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.ConsumeInitMode;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.GrpcContextConstants;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;

public class ReceiveMessageActivity extends AbstractMessingActivity {

    public ReceiveMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        super(messagingProcessor, grpcClientSettingsManager);
    }

    public void receiveMessage(Context ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        ProxyContext proxyContext = createContext(ctx);
        boolean fifo = false;

        ReceiveMessageResponseStreamWriter writer = new ReceiveMessageResponseStreamWriter(
            this.messagingProcessor,
            responseObserver
        );

        long timeRemaining = ctx.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }
        if (pollTime <= 0) {
            writer.write(proxyContext, Code.MESSAGE_NOT_FOUND, "time remaining is too small");
            return;
        }

        long invisibleTime = Durations.toMillis(request.getInvisibleDuration());
        if (request.getAutoRenew()) {
            invisibleTime = Durations.toMillis(
                this.grpcClientSettingsManager.getClientSettings(proxyContext.getVal(GrpcContextConstants.CLIENT_ID))
                    .getSubscription().getLongPollingTimeout()
            );
        }

        String topic = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
        String group = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
        FilterExpression filterExpression = request.getFilterExpression();
        SubscriptionData subscriptionData;
        try {
            subscriptionData = FilterAPI.build(topic, filterExpression.getExpression(),
                GrpcConverter.buildExpressionType(filterExpression.getType()));
        } catch (Exception e) {
            writer.write(proxyContext, Code.ILLEGAL_FILTER_EXPRESSION, e.getMessage());
            return;
        }

        this.messagingProcessor.popMessage(
            proxyContext,
            new ReceiveMessageQueueSelector(
                request.getMessageQueue().getBroker().getName()
            ),
            group,
            topic,
            request.getBatchSize(),
            invisibleTime,
            pollTime,
            ConsumeInitMode.MAX,
            subscriptionData,
            fifo,
            new PopMessageResultFilterImpl(grpcClientSettingsManager),
            timeRemaining
        ).thenAccept(popResult -> writer.write(proxyContext, request, popResult))
            .exceptionally(t -> {
                writer.write(proxyContext, request, t);
                return null;
            });
    }

    protected static class ReceiveMessageQueueSelector implements QueueSelector {

        private final String brokerName;

        public ReceiveMessageQueueSelector(String brokerName) {
            this.brokerName = brokerName;
        }

        @Override
        public SelectableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                SelectableMessageQueue selectableMessageQueue = null;
                MessageQueueSelector messageQueueSelector = messageQueueView.getReadSelector();

                if (StringUtils.isNotBlank(brokerName)) {
                    selectableMessageQueue = messageQueueSelector.getQueueByBrokerName(brokerName);
                }

                if (selectableMessageQueue == null) {
                    selectableMessageQueue = messageQueueSelector.selectOne(true);
                }
                return selectableMessageQueue;
            } catch (Throwable t) {
                return null;
            }
        }
    }
}
