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

package org.apache.rocketmq.proxy.grpc.service;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.ChangeInvisibleDurationRequest;
import apache.rocketmq.v1.ChangeInvisibleDurationResponse;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.QueryRouteRequest;
import apache.rocketmq.v1.QueryRouteResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.ReportMessageConsumptionResultRequest;
import apache.rocketmq.v1.ReportMessageConsumptionResultResponse;
import apache.rocketmq.v1.ReportThreadStackTraceRequest;
import apache.rocketmq.v1.ReportThreadStackTraceResponse;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.rpc.Code;
import io.grpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannel;
import org.apache.rocketmq.proxy.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.configuration.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.adapter.InvocationContext;
import org.apache.rocketmq.proxy.grpc.adapter.channel.ReceiveMessageChannel;
import org.apache.rocketmq.proxy.grpc.adapter.channel.SendMessageChannel;
import org.apache.rocketmq.proxy.grpc.adapter.handler.ReceiveMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.adapter.handler.SendMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalGrpcService implements GrpcForwardService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final BrokerController brokerController;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("LocalGrpcServiceScheduledThread"));
    private final ChannelManager channelManager;

    public LocalGrpcService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.channelManager = new ChannelManager();
    }

    @Override public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return null;
    }

    @Override public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        LanguageCode languageCode;
        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
        languageCode = LanguageCode.valueOf(language);
        HeartbeatData heartbeatData = Converter.buildHeartbeatData(request);

        SimpleChannel channel = channelManager.createChannel();
        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        command.setLanguage(languageCode);
        command.setVersion(MQVersion.Version.V5_0_0.ordinal());
        command.setBody(heartbeatData.encode());
        command.makeCustomHeaderToNet();

        RemotingCommand response = this.brokerController.getClientManageProcessor()
            .heartBeat(simpleChannelHandlerContext, command);
        HeartbeatResponse heartbeatResponse = ResponseBuilder.buildHeartbeatResponse(response);
        return CompletableFuture.completedFuture(heartbeatResponse);
    }

    @Override public CompletableFuture<HealthCheckResponse> healthCheck(Context ctx, HealthCheckRequest request) {
        LOGGER.trace("Received health check request from client: {}", request.getClientHost());
        final HealthCheckResponse response = HealthCheckResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"))
            .build();
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        SendMessageRequestHeader requestHeader = Converter.buildSendMessageRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        Message message = request.getMessage();
        command.setBody(message.getBody().toByteArray());
        command.makeCustomHeaderToNet();

        SendMessageResponseHandler handler = new SendMessageResponseHandler(message.getSystemAttribute().getMessageId());
        SendMessageChannel channel = SendMessageChannel.create(channelManager.createChannel(), handler);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        InvocationContext<SendMessageRequest, SendMessageResponse> context
            = new InvocationContext<>(request, future);
        channel.registerInvocationContext(command.getOpaque(), context);
        try {
            CompletableFuture<RemotingCommand> processorFuture = brokerController.getSendMessageProcessor()
                .asyncProcessRequest(channelHandlerContext, command);
            processorFuture.thenAccept(r -> {
                handler.handle(r, context);
                channel.eraseInvocationContext(command.getOpaque());
            });
        } catch (final Exception e) {
            LOGGER.error("Failed to process send message command", e);
            channel.eraseInvocationContext(command.getOpaque());
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        long timeRemaining = Context.current()
            .getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeMill();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }
        PopMessageRequestHeader requestHeader = Converter.buildPopMessageRequestHeader(request, pollTime);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        ReceiveMessageResponseHandler handler = new ReceiveMessageResponseHandler();
        ReceiveMessageChannel channel = ReceiveMessageChannel.create(channelManager.createChannel(), handler);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        CompletableFuture<ReceiveMessageResponse> future = new CompletableFuture<>();
        InvocationContext<ReceiveMessageRequest, ReceiveMessageResponse> context
            = new InvocationContext<>(request, future);
        channel.registerInvocationContext(command.getOpaque(), context);
        try {
            RemotingCommand response = brokerController.getPopMessageProcessor().processRequest(channelHandlerContext, command);
            if (response != null) {
                handler.handle(response, context);
                channel.eraseInvocationContext(command.getOpaque());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to process pop message command", e);
            channel.eraseInvocationContext(command.getOpaque());
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        return null;
    }

    @Override public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        return null;
    }

    @Override public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        return null;
    }

    @Override public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        return null;
    }

    @Override public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        return null;
    }

    @Override public CompletableFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        return null;
    }

    @Override public CompletableFuture<ReportThreadStackTraceResponse> reportThreadStackTrace(Context ctx,
        ReportThreadStackTraceRequest request) {
        return null;
    }

    @Override public CompletableFuture<ReportMessageConsumptionResultResponse> reportMessageConsumptionResult(Context ctx,
        ReportMessageConsumptionResultRequest request) {
        return null;
    }

    @Override public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        return null;
    }

    @Override public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        return null;
    }

    @Override public void start() throws Exception {
        this.brokerController.start();
        this.scheduledExecutorService.scheduleWithFixedDelay(this::scanAndCleanChannels, 5, 5, TimeUnit.MINUTES);
    }

    @Override public void shutdown() throws Exception {
        this.scheduledExecutorService.shutdown();
        this.brokerController.shutdown();
    }

    private void scanAndCleanChannels() {
        this.channelManager.scanAndCleanChannels();
    }
}
