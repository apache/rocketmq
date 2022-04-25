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

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.proxy.grpc.v2.service.cluster.BaseService.getBrokerAddr;

public class DefaultReceiveMessageResultFilter implements ReceiveMessageResultFilter {

    protected final ForwardProducer producer;
    protected final ForwardWriteConsumer writeConsumer;
    protected final GrpcClientManager grpcClientManager;
    protected final TopicRouteCache topicRouteCache;

    private volatile ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook;
    private volatile ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> forwardToDLQInRecvMessageHook;

    public DefaultReceiveMessageResultFilter(ForwardProducer producer, ForwardWriteConsumer writeConsumer,
        GrpcClientManager grpcClientManager, TopicRouteCache topicRouteCache) {
        this.producer = producer;
        this.writeConsumer = writeConsumer;
        this.grpcClientManager = grpcClientManager;
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    public List<Message> filterMessage(Context ctx, ReceiveMessageRequest request, List<MessageExt> messageExtList) {
        if (messageExtList == null || messageExtList.isEmpty()) {
            return Collections.emptyList();
        }
        Settings settings = grpcClientManager.getClientSettings(ctx);
        ClientType clientType = settings.getClientType();
        int maxAttempts = settings.getSubscription().getBackoffPolicy().getMaxAttempts();
        Resource topic = request.getMessageQueue().getTopic();
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        SubscriptionData subscriptionData = GrpcConverter.buildSubscriptionData(topicName, request.getFilterExpression());

        List<Message> resMessageList = new ArrayList<>();
        for (MessageExt messageExt : messageExtList) {
            if (ClientType.SIMPLE_CONSUMER.equals(clientType) && messageExt.getReconsumeTimes() >= maxAttempts) {
                forwardMessageToDLQ(ctx, request, messageExt, maxAttempts);
                continue;
            }
            if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
                this.ackNoMatchedMessage(ctx, request, messageExt);
                continue;
            }
            resMessageList.add(GrpcConverter.buildMessage(messageExt));
        }
        return resMessageList;
    }

    protected void forwardMessageToDLQ(Context ctx, ReceiveMessageRequest request, MessageExt messageExt,
        int maxReconsumeTimes) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();

        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            String brokerAddr = getBrokerAddr(ctx, topicRouteCache, handle.getBrokerName());
            Resource topic = request.getMessageQueue().getTopic();
            Resource group = request.getGroup();
            ConsumerSendMsgBackRequestHeader sendMsgBackRequestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(
                topic,
                group,
                handle,
                messageExt.getMsgId(),
                maxReconsumeTimes);
            AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(
                topic,
                group,
                handle);

            future = this.producer.sendMessageBackThenAckOrg(ctx, brokerAddr, sendMsgBackRequestHeader, ackMessageRequestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((result, throwable) -> {
            if (forwardToDLQInRecvMessageHook != null) {
                forwardToDLQInRecvMessageHook.beforeResponse(ctx, consumerSendMsgBackRequestHeader, result, throwable);
            }
        });
    }

    protected void ackNoMatchedMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();

        AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            String brokerAddr = getBrokerAddr(ctx, topicRouteCache, handle.getBrokerName());
            ackMessageRequestHeader.setConsumerGroup(GrpcConverter.wrapResourceWithNamespace(request.getGroup()));
            ackMessageRequestHeader.setTopic(messageExt.getTopic());
            ackMessageRequestHeader.setQueueId(handle.getQueueId());
            ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
            ackMessageRequestHeader.setOffset(handle.getOffset());

            future = this.writeConsumer.ackMessage(ctx, brokerAddr, ackMessageRequestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((ackResult, throwable) -> {
            if (ackNoMatchedMessageHook != null) {
                ackNoMatchedMessageHook.beforeResponse(ctx, ackMessageRequestHeader, ackResult, throwable);
            }
        });
    }

    public ResponseHook<AckMessageRequestHeader, AckResult> getAckNoMatchedMessageHook() {
        return ackNoMatchedMessageHook;
    }

    public void setAckNoMatchedMessageHook(
        ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook) {
        this.ackNoMatchedMessageHook = ackNoMatchedMessageHook;
    }

    public ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> getForwardToDLQInRecvMessageHook() {
        return forwardToDLQInRecvMessageHook;
    }

    public void setForwardToDLQInRecvMessageHook(
        ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> forwardToDLQInRecvMessageHook) {
        this.forwardToDLQInRecvMessageHook = forwardToDLQInRecvMessageHook;
    }
}
