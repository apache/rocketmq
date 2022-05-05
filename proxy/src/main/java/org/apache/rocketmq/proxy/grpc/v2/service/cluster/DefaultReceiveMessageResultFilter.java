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

import apache.rocketmq.v2.ReceiveMessageRequest;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.proxy.connector.ForwardProducer;
import org.apache.rocketmq.proxy.connector.ForwardWriteConsumer;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.BaseReceiveMessageResultFilter;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import static org.apache.rocketmq.proxy.grpc.v2.service.BaseService.getBrokerAddr;

public class DefaultReceiveMessageResultFilter extends BaseReceiveMessageResultFilter {

    protected final ForwardProducer producer;
    protected final ForwardWriteConsumer writeConsumer;
    protected final TopicRouteCache topicRouteCache;

    private volatile ResponseHook<AckMessageRequestHeader, AckResult> ackNoMatchedMessageHook;
    private volatile ResponseHook<ConsumerSendMsgBackRequestHeader, RemotingCommand> forwardToDLQInRecvMessageHook;

    public DefaultReceiveMessageResultFilter(ForwardProducer producer, ForwardWriteConsumer writeConsumer,
        GrpcClientManager grpcClientManager, TopicRouteCache topicRouteCache) {
        super(grpcClientManager);
        this.producer = producer;
        this.writeConsumer = writeConsumer;
        this.topicRouteCache = topicRouteCache;
    }

    @Override
    protected void processNoMatchMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();

        ReceiptHandle handle = ReceiptHandle.create(messageExt);
        if (handle == null) {
            return;
        }
        AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(request, handle);
        try {
            String brokerAddr = getBrokerAddr(ctx, topicRouteCache, handle.getBrokerName());
            future = this.writeConsumer.ackMessage(ctx, brokerAddr, messageExt.getMsgId(), ackMessageRequestHeader);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }

        future.whenComplete((ackResult, throwable) -> {
            if (ackNoMatchedMessageHook != null) {
                ackNoMatchedMessageHook.beforeResponse(ctx, ackMessageRequestHeader, ackResult, throwable);
            }
        });
    }

    @Override
    protected void processExceedMaxAttemptsMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt,
        int maxAttempts) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();

        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            String brokerAddr = getBrokerAddr(ctx, topicRouteCache, handle.getBrokerName());
            ConsumerSendMsgBackRequestHeader sendMsgBackRequestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(
                request,
                handle,
                messageExt.getMsgId(),
                maxAttempts);
            AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(request, handle);

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
