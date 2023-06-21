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
package org.apache.rocketmq.proxy.grpc.v2;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.client.ClientActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.consumer.AckMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ChangeInvisibleDurationActivity;
import org.apache.rocketmq.proxy.grpc.v2.consumer.ReceiveMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.ForwardMessageToDLQActivity;
import org.apache.rocketmq.proxy.grpc.v2.producer.SendMessageActivity;
import org.apache.rocketmq.proxy.grpc.v2.route.RouteActivity;
import org.apache.rocketmq.proxy.grpc.v2.transaction.EndTransactionActivity;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;

public class DefaultGrpcMessingActivity extends AbstractStartAndShutdown implements GrpcMessingActivity {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected GrpcClientSettingsManager grpcClientSettingsManager;
    protected GrpcChannelManager grpcChannelManager;
    protected ReceiptHandleProcessor receiptHandleProcessor;
    protected ReceiveMessageActivity receiveMessageActivity;
    protected AckMessageActivity ackMessageActivity;
    protected ChangeInvisibleDurationActivity changeInvisibleDurationActivity;
    protected SendMessageActivity sendMessageActivity;
    protected ForwardMessageToDLQActivity forwardMessageToDLQActivity;
    protected EndTransactionActivity endTransactionActivity;
    protected RouteActivity routeActivity;
    protected ClientActivity clientActivity;

    protected DefaultGrpcMessingActivity(MessagingProcessor messagingProcessor) {
        this.init(messagingProcessor);
    }

    protected void init(MessagingProcessor messagingProcessor) {
        this.grpcClientSettingsManager = new GrpcClientSettingsManager(messagingProcessor);
        this.grpcChannelManager = new GrpcChannelManager(messagingProcessor.getProxyRelayService(), this.grpcClientSettingsManager);
        this.receiptHandleProcessor = new ReceiptHandleProcessor(messagingProcessor);

        this.receiveMessageActivity = new ReceiveMessageActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.ackMessageActivity = new AckMessageActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.changeInvisibleDurationActivity = new ChangeInvisibleDurationActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.sendMessageActivity = new SendMessageActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.forwardMessageToDLQActivity = new ForwardMessageToDLQActivity(messagingProcessor, receiptHandleProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.endTransactionActivity = new EndTransactionActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.routeActivity = new RouteActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.clientActivity = new ClientActivity(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);

        this.appendStartAndShutdown(this.receiptHandleProcessor);
        this.appendStartAndShutdown(this.grpcClientSettingsManager);
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        return this.routeActivity.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        return this.clientActivity.heartbeat(ctx, request);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        return this.sendMessageActivity.sendMessage(ctx, request);
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx,
        QueryAssignmentRequest request) {
        return this.routeActivity.queryAssignment(ctx, request);
    }

    @Override
    public void receiveMessage(ProxyContext ctx, ReceiveMessageRequest request,
        StreamObserver<ReceiveMessageResponse> responseObserver) {
        this.receiveMessageActivity.receiveMessage(ctx, request, responseObserver);
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(ProxyContext ctx, AckMessageRequest request) {
        return this.ackMessageActivity.ackMessage(ctx, request);
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(ProxyContext ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return this.forwardMessageToDLQActivity.forwardMessageToDeadLetterQueue(ctx, request);
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(ProxyContext ctx, EndTransactionRequest request) {
        return this.endTransactionActivity.endTransaction(ctx, request);
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        return this.clientActivity.notifyClientTermination(ctx, request);
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(ProxyContext ctx,
        ChangeInvisibleDurationRequest request) {
        return this.changeInvisibleDurationActivity.changeInvisibleDuration(ctx, request);
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(ProxyContext ctx,
        StreamObserver<TelemetryCommand> responseObserver) {
        return this.clientActivity.telemetry(ctx, responseObserver);
    }
}
