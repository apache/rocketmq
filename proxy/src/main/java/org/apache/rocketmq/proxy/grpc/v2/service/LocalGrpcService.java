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

package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.ClientOverwrittenSettings;
import apache.rocketmq.v2.ClientSettings;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Direction;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NackMessageRequest;
import apache.rocketmq.v2.NackMessageResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.PullMessageRequest;
import apache.rocketmq.v2.PullMessageResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryOffsetPolicy;
import apache.rocketmq.v2.QueryOffsetRequest;
import apache.rocketmq.v2.QueryOffsetResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.protobuf.util.Timestamps;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
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
import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.common.DelayPolicy;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.channel.InvocationContext;
import org.apache.rocketmq.proxy.common.TelemetryCommandRecord;
import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyMode;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.PullMessageChannel;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.ReceiveMessageChannel;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.SendMessageChannel;
import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.PullMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.ReceiveMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.SendMessageResponseHandler;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.service.cluster.RouteService;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalGrpcService extends AbstractStartAndShutdown implements GrpcForwardService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final BrokerController brokerController;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryImpl("LocalGrpcServiceScheduledThread"));
    private final ChannelManager channelManager;
    private final TelemetryCommandManager telemetryCommandManager;
    private final GrpcClientManager grpcClientManager;
    private final RouteService routeService;
    private final DelayPolicy delayPolicy;

    public LocalGrpcService(BrokerController brokerController) {
        this(brokerController, new TelemetryCommandManager());
    }

    /**
     * For unit test
     * @param brokerController BrokerController works in local mode
     * @param telemetryCommandManager Used to manage telemetry command
     */
    LocalGrpcService(BrokerController brokerController, TelemetryCommandManager telemetryCommandManager) {
        this.brokerController = brokerController;
        this.channelManager = new ChannelManager();
        // TransactionStateChecker is not used in Local mode.
        ConnectorManager connectorManager = new ConnectorManager(null);
        this.telemetryCommandManager = telemetryCommandManager;
        this.grpcClientManager = new GrpcClientManager();
        this.routeService = new RouteService(ProxyMode.LOCAL, connectorManager, grpcClientManager);
        this.delayPolicy = DelayPolicy.build(brokerController.getMessageStoreConfig().getMessageDelayLevel());
        this.appendStartAndShutdown(connectorManager);
        this.appendStartAndShutdown(new LocalGrpcServiceStartAndShutdown());
    }

    @Override
    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
        return this.routeService.queryRoute(ctx, request);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        LanguageCode languageCode;
        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        languageCode = LanguageCode.valueOf(language);

        ClientSettings clientSettings = grpcClientManager.getClientSettings(clientId);
        HeartbeatData heartbeatData = GrpcConverter.buildHeartbeatData(clientId, request, clientSettings);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        command.setLanguage(languageCode);
        command.setVersion(MQVersion.Version.V5_0_0.ordinal());
        command.setBody(heartbeatData.encode());
        command.makeCustomHeaderToNet();

        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
        switch (clientSettings.getClientType()) {
            case PRODUCER: {
                for (Resource topic : clientSettings.getSettings().getPublishing().getTopicsList()) {
                    String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
                    GrpcClientChannel channel = GrpcClientChannel.create(channelManager, topicName, clientId, telemetryCommandManager);
                    SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);

                    this.brokerController.getClientManageProcessor()
                        .heartBeat(simpleChannelHandlerContext, command);
                }
                HeartbeatResponse heartbeatResponse = HeartbeatResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(Code.OK, "Producer heartbeat"))
                    .build();
                future.complete(heartbeatResponse);
                break;
            }
            case PULL_CONSUMER:
            case PUSH_CONSUMER:
            case SIMPLE_CONSUMER: {
                String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
                GrpcClientChannel channel = GrpcClientChannel.create(channelManager, groupName, clientId, telemetryCommandManager);
                SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);

                RemotingCommand response = this.brokerController.getClientManageProcessor()
                    .heartBeat(simpleChannelHandlerContext, command);
                HeartbeatResponse heartbeatResponse = HeartbeatResponse.newBuilder()
                    .setStatus(ResponseBuilder.buildStatus(response.getCode(), response.getRemark()))
                    .build();
                future.complete(heartbeatResponse);
                break;
            }
            default: {
                throw new IllegalArgumentException("ClientType not exist " + clientSettings.getClientType());
            }
        }

        return future;
    }

    @Override
    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
        SendMessageRequestHeader requestHeader = GrpcConverter.buildSendMessageRequestHeader(request, topicName);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        List<org.apache.rocketmq.common.message.Message> messageList = GrpcConverter.buildMessage(request.getMessagesList(), topicName);
        MessageBatch messageBatch = MessageBatch.generateFromList(messageList);
        MessageClientIDSetter.setUniqID(messageBatch);
        messageBatch.setBody(messageBatch.encode());
        command.setBody(messageBatch.encode());
        command.makeCustomHeaderToNet();

        SendMessageResponseHandler handler = new SendMessageResponseHandler();
        SendMessageChannel channel = channelManager.createChannel(() -> new SendMessageChannel(handler), SendMessageChannel.class);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
        InvocationContext<SendMessageRequest, SendMessageResponse> context
            = new InvocationContext<>(request, future);
        channel.registerInvocationContext(command.getOpaque(), context);
        try {
            RemotingCommand response = brokerController.getSendMessageProcessor()
                .processRequest(channelHandlerContext, command);
            if (response != null) {
                handler.handle(response, context);
                channel.eraseInvocationContext(command.getOpaque());
            }
        } catch (final Exception e) {
            log.error("Failed to process send message command", e);
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
        long pollTime = GrpcConverter.buildPollTimeFromContext(ctx);
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        ClientSettings clientSettings = grpcClientManager.getClientSettings(clientId);
        PopMessageRequestHeader requestHeader = GrpcConverter.buildPopMessageRequestHeader(request, pollTime,
            clientSettings.getSettings().getSubscription().getFifo());
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        ReceiveMessageResponseHandler handler = new ReceiveMessageResponseHandler(clientSettings.getSettings().getSubscription().getFifo());
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
            log.error("Failed to process pop message command", e);
            channel.eraseInvocationContext(command.getOpaque());
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
        AckMessageRequestHeader requestHeader = GrpcConverter.buildAckMessageRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            AckMessageResponse.Builder builder = AckMessageResponse.newBuilder();
            builder.setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()));
            AckMessageResponse response = builder.build();
            future.complete(response);
        } catch (Exception e) {
            log.error("Exception raised when ack message", e);
        }
        return future;
    }

    @Override
    public CompletableFuture<NackMessageResponse> nackMessage(Context ctx, NackMessageRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ChangeInvisibleTimeRequestHeader requestHeader = GrpcConverter.buildChangeInvisibleTimeRequestHeader(request, delayPolicy);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<NackMessageResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            NackMessageResponse response = NackMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()))
                .build();
            future.complete(response);
        } catch (Exception e) {
            log.error("Exception raised while nackMessage", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
        ForwardMessageToDeadLetterQueueRequest request) {
        SimpleChannel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ConsumerSendMsgBackRequestHeader requestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand response = brokerController.getSendMessageProcessor()
                .processRequest(channelHandlerContext, command);

            future.complete(ForwardMessageToDeadLetterQueueResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(response.getCode(), response.getRemark()))
                .build());
        } catch (Exception e) {
            log.error("Exception raised when forwardMessageToDeadLetterQueue", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
        EndTransactionRequestHeader requestHeader = GrpcConverter.buildEndTransactionRequestHeader(request, topicName);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<EndTransactionResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getEndTransactionProcessor()
                .processRequest(channelHandlerContext, command);
            EndTransactionResponse.Builder builder = EndTransactionResponse.newBuilder();
            if (null != responseCommand) {
                builder.setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()));
            } else {
                builder.setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "Response command is null"));
            }
            EndTransactionResponse response = builder.build();
            future.complete(response);
        } catch (Exception e) {
            log.error("Exception raised while endTransaction", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<QueryOffsetResponse> queryOffset(Context ctx, QueryOffsetRequest request) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getMessageQueue().getTopic());
        int queueId = request.getMessageQueue().getId();

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
            .setStatus(ResponseBuilder.buildStatus(Code.OK, "ok"))
            .setOffset(offset)
            .build());
    }

    @Override
    public CompletableFuture<PullMessageResponse> pullMessage(Context ctx, PullMessageRequest request) {
        long pollTime = org.apache.rocketmq.proxy.grpc.v1.adapter.GrpcConverter.buildPollTimeFromContext(ctx);
        PullMessageRequestHeader requestHeader = GrpcConverter.buildPullMessageRequestHeader(request, pollTime);
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
            log.error("Failed to process pull message command", e);
            channel.eraseInvocationContext(command.getOpaque());
            future.completeExceptionally(e);
        }
        return future;
    }

    public void reportThreadStackTrace(ThreadStackTrace request) {
        String nonce = request.getNonce();
        String threadStack = request.getThreadStackTrace();
        TelemetryCommandRecord pollCommandResponseFuture = telemetryCommandManager.getCommand(nonce);
        if (pollCommandResponseFuture != null) {
            Integer opaque = pollCommandResponseFuture.getOpaque();
            if (opaque != null) {
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
        }
    }

    public void reportVerifyMessageResult(VerifyMessageResult request) {
        String nonce = request.getNonce();
        TelemetryCommandRecord pollCommandResponseFuture = telemetryCommandManager.getCommand(nonce);
        if (pollCommandResponseFuture != null) {
            Integer opaque = pollCommandResponseFuture.getOpaque();
            if (opaque != null) {
                RemotingServer remotingServer = this.brokerController.getRemotingServer();
                if (remotingServer instanceof NettyRemotingAbstract) {
                    NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
                    RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "From gRPC client");
                    remotingCommand.setOpaque(pollCommandResponseFuture.getOpaque());
                    ConsumeMessageDirectlyResult result = GrpcConverter.buildConsumeMessageDirectlyResult(request);
                    remotingCommand.setBody(result.encode());
                    nettyRemotingAbstract.processResponseCommand(new SimpleChannelHandlerContext(channelManager.createChannel()), remotingCommand);
                }
            }
        }
    }

    @Override
    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
        NotifyClientTerminationRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        ClientSettings clientSettings = grpcClientManager.getClientSettings(clientId);
        UnregisterClientRequestHeader header = GrpcConverter.buildUnregisterClientRequestHeader(clientId, clientSettings.getClientType(), request);

        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, header);
        remotingCommand.makeCustomHeaderToNet();
        try {
            this.brokerController.getClientManageProcessor().unregisterClient(simpleChannelHandlerContext, remotingCommand);
        } catch (Exception ignored) {
        }
        return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
        ChangeInvisibleDurationRequest request) {
        Channel channel = channelManager.createChannel();
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        ChangeInvisibleTimeRequestHeader requestHeader = GrpcConverter.buildChangeInvisibleTimeRequestHeader(request);
        ReceiptHandle receiptHandle = ReceiptHandle.decode(request.getReceiptHandle());
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        command.makeCustomHeaderToNet();

        CompletableFuture<ChangeInvisibleDurationResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand responseCommand = brokerController.getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) responseCommand.readCustomHeader();
            ChangeInvisibleDurationResponse.Builder builder = ChangeInvisibleDurationResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()));
            if (responseCommand.getCode() == ResponseCode.SUCCESS) {
                builder.setReceiptHandle(ReceiptHandle.builder()
                    .startOffset(requestHeader.getOffset())
                    .retrieveTime(responseHeader.getPopTime())
                    .invisibleTime(responseHeader.getInvisibleTime())
                    .reviveQueueId(responseHeader.getReviveQid())
                    .topicType(receiptHandle.getTopicType())
                    .brokerName(brokerController.getBrokerConfig().getBrokerName())
                    .queueId(requestHeader.getQueueId())
                    .offset(requestHeader.getOffset())
                    .build()
                    .encode());
            }

            future.complete(builder.build());
        } catch (Exception e) {
            log.error("Exception raised while changeInvisibleDuration", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver) {
        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand request) {
                switch (request.getCommandCase()) {
                    case CLIENT_SETTINGS: {
                        ClientSettings clientSettings = request.getClientSettings();
                        grpcClientManager.updateClientSettings(clientId, clientSettings);
                        Settings settings = clientSettings.getSettings();
                        if (settings.hasPublishing()) {
                            Publishing publishing = settings.getPublishing();
                            for (Resource topic : publishing.getTopicsList()) {
                                String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
                                GrpcClientChannel producerChannel = GrpcClientChannel.create(channelManager, topicName, clientId, telemetryCommandManager);
                                producerChannel.setClientObserver(responseObserver);
                            }
                        }
                        if (settings.hasSubscription()) {
                            Subscription subscription = settings.getSubscription();
                            String groupName = GrpcConverter.wrapResourceWithNamespace(subscription.getGroup());
                            GrpcClientChannel consumerChannel = GrpcClientChannel.create(channelManager, groupName, clientId, telemetryCommandManager);
                            consumerChannel.setClientObserver(responseObserver);
                        }
                        responseObserver.onNext(TelemetryCommand.newBuilder()
                            .setClientOverwrittenSettings(ClientOverwrittenSettings.newBuilder()
                                .setNonce(clientSettings.getNonce())
                                .setDirection(Direction.RESPONSE)
                                .setSettings(settings)
                                .build())
                            .build());
                        break;
                    }
                    case THREAD_STACK_TRACE: {
                        reportThreadStackTrace(request.getThreadStackTrace());
                        break;
                    }
                    case VERIFY_MESSAGE_RESULT: {
                        reportVerifyMessageResult(request.getVerifyMessageResult());
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Request type is illegal");
                    }
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private class LocalGrpcServiceStartAndShutdown implements StartAndShutdown {
        @Override public void start() throws Exception {
            LocalGrpcService.this.scheduledExecutorService.scheduleWithFixedDelay(LocalGrpcService.this::scanAndCleanChannels, 5, 5, TimeUnit.MINUTES);
        }

        @Override public void shutdown() throws Exception {
            LocalGrpcService.this.scheduledExecutorService.shutdown();
        }
    }

    private void scanAndCleanChannels() {
        this.channelManager.scanAndCleanChannels();
    }
}
