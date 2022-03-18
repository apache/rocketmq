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
import apache.rocketmq.v1.NoopCommand;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetPolicy;
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
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Context;
import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannel;
import org.apache.rocketmq.proxy.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.grpc.adapter.InvocationContext;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.adapter.channel.ReceiveMessageChannel;
import org.apache.rocketmq.proxy.grpc.adapter.channel.SendMessageChannel;
import org.apache.rocketmq.proxy.grpc.adapter.channel.PullMessageChannel;
import org.apache.rocketmq.proxy.grpc.adapter.handler.PullMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.adapter.handler.ReceiveMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.adapter.handler.SendMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.DelayPolicy;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.common.PollCommandResponseFuture;
import org.apache.rocketmq.proxy.grpc.common.PollCommandResponseManager;
import org.apache.rocketmq.proxy.grpc.common.ProxyMode;
import org.apache.rocketmq.proxy.grpc.common.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.service.cluster.RouteService;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
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
    private final PollCommandResponseManager pollCommandResponseManager;
    private final RouteService routeService;
    private final DelayPolicy delayPolicy;

    public LocalGrpcService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.channelManager = new ChannelManager();
        // TransactionStateChecker is not used in Local mode.
        ConnectorManager connectorManager = new ConnectorManager(null);
        this.pollCommandResponseManager = new PollCommandResponseManager();
        this.routeService = new RouteService(ProxyMode.LOCAL, connectorManager);
        this.delayPolicy = DelayPolicy.build(brokerController.getMessageStoreConfig().getMessageDelayLevel());
    }

    @Override public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return this.routeService.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        LanguageCode languageCode;
        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
        languageCode = LanguageCode.valueOf(language);
        HeartbeatData heartbeatData = Converter.buildHeartbeatData(request);

        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        String groupName;
        switch (request.getClientDataCase()) {
            case PRODUCER_DATA: {
                groupName = Converter.getResourceNameWithNamespace(request.getProducerData().getGroup());
                break;
            }
            case CONSUMER_DATA: {
                groupName = Converter.getResourceNameWithNamespace(request.getConsumerData().getGroup());
                break;
            }
            default: {
                future.completeExceptionally(new IllegalArgumentException("Wrong client data type"));
                return future;
            }
        }

        GrpcClientChannel channel = GrpcClientChannel.create(channelManager, groupName, request.getClientId(), pollCommandResponseManager);
        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        command.setLanguage(languageCode);
        command.setVersion(MQVersion.Version.V5_0_0.ordinal());
        command.setBody(heartbeatData.encode());
        command.makeCustomHeaderToNet();

        RemotingCommand response = this.brokerController.getClientManageProcessor()
            .heartBeat(simpleChannelHandlerContext, command);
        HeartbeatResponse heartbeatResponse = ResponseBuilder.buildHeartbeatResponse(response);
        future.complete(heartbeatResponse);
        return future;
    }

    @Override
    public CompletableFuture<HealthCheckResponse> healthCheck(Context ctx, HealthCheckRequest request) {
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
        SendMessageChannel channel = channelManager.createChannel(() -> new SendMessageChannel(handler), SendMessageChannel.class);
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
        return this.routeService.queryAssignment(ctx, request);
    }

    @Override
    public CompletableFuture<ReceiveMessageResponse> receiveMessage(Context ctx, ReceiveMessageRequest request) {
        long timeRemaining = Context.current()
            .getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }
        PopMessageRequestHeader requestHeader = Converter.buildPopMessageRequestHeader(request, pollTime);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        ReceiveMessageResponseHandler handler = new ReceiveMessageResponseHandler();
        ReceiveMessageChannel channel = channelManager.createChannel(() -> new ReceiveMessageChannel(handler), ReceiveMessageChannel.class);
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
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        AckMessageRequestHeader requestHeader = Converter.buildAckMessageRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            AckMessageResponse.Builder builder = AckMessageResponse.newBuilder();
            if (null != responseCommand) {
                builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            } else {
                builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
            }
            AckMessageResponse response = builder.build();
            future.complete(response);
        } catch (Exception e) {
            LOGGER.error("Exception raised when ack message", e);
        }
        return future;
    }

    @Override public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ChangeInvisibleTimeRequestHeader requestHeader = Converter.buildChangeInvisibleTimeRequestHeader(request, delayPolicy);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            NackMessageResponse response = NackMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()))
                .build();
            future.complete(response);
        } catch (Exception e) {
            LOGGER.error("Exception raised while nackMessage", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        SimpleChannel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ConsumerSendMsgBackRequestHeader requestHeader = Converter.buildConsumerSendMsgBackRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        try {
            CompletableFuture<RemotingCommand> processorFuture = brokerController.getSendMessageProcessor()
                .asyncProcessRequest(channelHandlerContext, command);
            processorFuture.thenAccept(r -> {
                ForwardMessageToDeadLetterQueueResponse.Builder builder = ForwardMessageToDeadLetterQueueResponse.newBuilder();
                if (null != r) {
                    builder.setCommon(ResponseBuilder.buildCommon(r.getCode(), r.getRemark()));
                } else {
                    builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
                }
                ForwardMessageToDeadLetterQueueResponse response = builder.build();
                future.complete(response);
            });
        } catch (Exception e) {
            LOGGER.error("Exception raised when forwardMessageToDeadLetterQueue", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        EndTransactionRequestHeader requestHeader = Converter.buildEndTransactionRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<EndTransactionResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getEndTransactionProcessor()
                .processRequest(channelHandlerContext, command);
            EndTransactionResponse.Builder builder = EndTransactionResponse.newBuilder();
            if (null != responseCommand) {
                builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            } else {
                builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
            }
            EndTransactionResponse response = builder.build();
            future.complete(response);
        } catch (Exception e) {
            LOGGER.error("Exception raised while endTransaction", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        Partition partition = request.getPartition();
        String topicName = Converter.getResourceNameWithNamespace(partition.getTopic());
        int queueId = partition.getId();

        long offset;
        if (request.getPolicy() == QueryOffsetPolicy.BEGINNING) {
            offset = 0L;
        } else if (request.getPolicy() == QueryOffsetPolicy.END) {
            offset = brokerController.getMessageStore()
                .getMaxOffsetInQueue(topicName, queueId);
        } else {
            long timestamp = Timestamps.toMillis(request.getTimePoint());
            offset = brokerController.getMessageStore()
                .getOffsetInQueueByTime(topicName, queueId, timestamp);
        }
        return CompletableFuture.completedFuture(QueryOffsetResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"))
            .setOffset(offset)
            .build());
    }

    @Override public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        long timeRemaining = Context.current()
            .getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS);
        long pollTime = timeRemaining - ConfigurationManager.getProxyConfig().getLongPollingReserveTimeInMillis();
        if (pollTime <= 0) {
            pollTime = timeRemaining;
        }
        PullMessageRequestHeader requestHeader = Converter.buildPullMessageRequestHeader(request, pollTime);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        PullMessageResponseHandler handler = new PullMessageResponseHandler();
        PullMessageChannel channel = channelManager.createChannel(() -> new PullMessageChannel(handler), PullMessageChannel.class);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        CompletableFuture<PullMessageResponse> future = new CompletableFuture<>();
        InvocationContext<PullMessageRequest, PullMessageResponse> context = new InvocationContext<>(request, future);
        channel.registerInvocationContext(command.getOpaque(), context);
        try {
            RemotingCommand response = brokerController.getPullMessageProcessor()
                .processRequest(channelHandlerContext, command);
            if (response != null) {
                handler.handle(response, context);
                channel.eraseInvocationContext(command.getOpaque());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to process pull message command", e);
            channel.eraseInvocationContext(command.getOpaque());
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override public CompletableFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        String clientId = request.getClientId();
        CompletableFuture<PollCommandResponse> future = new CompletableFuture<>();
        switch (request.getGroupCase()) {
            case PRODUCER_GROUP:
                Resource producerGroup = request.getProducerGroup();
                String producerGroupName = Converter.getResourceNameWithNamespace(producerGroup);
                GrpcClientChannel producerChannel = GrpcClientChannel.getChannel(channelManager, producerGroupName, clientId);
                if (producerChannel == null) {
                    future.complete(PollCommandResponse.newBuilder()
                        .setNoopCommand(NoopCommand.newBuilder().build())
                        .build());
                    break;
                }
                producerChannel.addClientObserver(future);
                break;
            case CONSUMER_GROUP:
                Resource consumerGroup = request.getConsumerGroup();
                String consumerGroupName = Converter.getResourceNameWithNamespace(consumerGroup);
                GrpcClientChannel consumerChannel = GrpcClientChannel.getChannel(channelManager, consumerGroupName, clientId);
                if (consumerChannel == null) {
                    future.complete(PollCommandResponse.newBuilder()
                        .setNoopCommand(NoopCommand.newBuilder().build())
                        .build());
                    break;
                }
                consumerChannel.addClientObserver(future);
                break;
            default:
                break;
        }
        return future;
    }

    @Override
    public CompletableFuture<ReportThreadStackTraceResponse> reportThreadStackTrace(Context ctx,
        ReportThreadStackTraceRequest request) {
        String commandId = request.getCommandId();
        String threadStack = request.getThreadStackTrace();
        PollCommandResponseFuture pollCommandResponseFuture = pollCommandResponseManager.getResponse(commandId);
        if (pollCommandResponseFuture != null) {
            RemotingServer remotingServer = this.brokerController.getRemotingServer();
            if (remotingServer instanceof NettyRemotingAbstract) {
                NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
                RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "From gRPC client");
                remotingCommand.setOpaque(pollCommandResponseFuture.getOpaque());
                ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
                runningInfo.setJstack(threadStack);
                remotingCommand.setBody(runningInfo.encode());
                nettyRemotingAbstract.processResponseCommand(new SimpleChannelHandlerContext(channelManager.createChannel()), remotingCommand);
            }
        }
        return CompletableFuture.completedFuture(ReportThreadStackTraceResponse.newBuilder()
            .setCommon(ResponseBuilder.buildSuccessCommon())
            .build());
    }

    @Override
    public CompletableFuture<ReportMessageConsumptionResultResponse> reportMessageConsumptionResult(Context ctx,
        ReportMessageConsumptionResultRequest request) {
        String commandId = request.getCommandId();
        PollCommandResponseFuture pollCommandResponseFuture = pollCommandResponseManager.getResponse(commandId);
        if (pollCommandResponseFuture != null) {
            RemotingServer remotingServer = this.brokerController.getRemotingServer();
            if (remotingServer instanceof NettyRemotingAbstract) {
                NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
                RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "From gRPC client");
                remotingCommand.setOpaque(pollCommandResponseFuture.getOpaque());
                ConsumeMessageDirectlyResult result = Converter.buildConsumeMessageDirectlyResult(request);
                remotingCommand.setBody(result.encode());
                nettyRemotingAbstract.processResponseCommand(new SimpleChannelHandlerContext(channelManager.createChannel()), remotingCommand);
            }
        }
        return CompletableFuture.completedFuture(ReportMessageConsumptionResultResponse.newBuilder()
            .setCommon(ResponseBuilder.buildSuccessCommon())
            .build());
    }

    @Override public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
        UnregisterClientRequestHeader header = Converter.buildUnregisterClientRequestHeader(request);

        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, header);
        remotingCommand.makeCustomHeaderToNet();
        try {
            this.brokerController.getClientManageProcessor().unregisterClient(simpleChannelHandlerContext, remotingCommand);
        } catch (Exception ignored) {
        }
        return new CompletableFuture<>();
    }

    @Override public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ChangeInvisibleTimeRequestHeader requestHeader = Converter.buildChangeInvisibleTimeRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<ChangeInvisibleDurationResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) responseCommand.readCustomHeader();
            ChangeInvisibleDurationResponse.Builder builder = ChangeInvisibleDurationResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            if (responseCommand.getCode() == ResponseCode.SUCCESS) {
                builder.setReceiptHandle(ReceiptHandle.builder()
                    .startOffset(requestHeader.getOffset())
                    .retrieveTime(responseHeader.getPopTime())
                    .invisibleTime(responseHeader.getInvisibleTime())
                    .reviveQueueId(responseHeader.getReviveQid())
                    .topic(Converter.getResourceNameWithNamespace(request.getTopic()))
                    .brokerName(brokerController.getBrokerConfig().getBrokerName())
                    .queueId(requestHeader.getQueueId())
                    .offset(requestHeader.getOffset())
                    .build()
                    .encode());
            }

            future.complete(builder.build());
        } catch (Exception e) {
            LOGGER.error("Exception raised while changeInvisibleDuration", e);
            future.completeExceptionally(e);
        }
        return future;
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
