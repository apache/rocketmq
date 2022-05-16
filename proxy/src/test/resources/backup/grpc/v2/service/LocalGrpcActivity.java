///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.rocketmq.proxy.grpc.v2.service;
//
//import apache.rocketmq.v2.AckMessageEntry;
//import apache.rocketmq.v2.AckMessageRequest;
//import apache.rocketmq.v2.AckMessageResponse;
//import apache.rocketmq.v2.AckMessageResultEntry;
//import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
//import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
//import apache.rocketmq.v2.Code;
//import apache.rocketmq.v2.EndTransactionRequest;
//import apache.rocketmq.v2.EndTransactionResponse;
//import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
//import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
//import apache.rocketmq.v2.HeartbeatRequest;
//import apache.rocketmq.v2.HeartbeatResponse;
//import apache.rocketmq.v2.NotifyClientTerminationRequest;
//import apache.rocketmq.v2.NotifyClientTerminationResponse;
//import apache.rocketmq.v2.QueryAssignmentRequest;
//import apache.rocketmq.v2.QueryAssignmentResponse;
//import apache.rocketmq.v2.QueryRouteRequest;
//import apache.rocketmq.v2.QueryRouteResponse;
//import apache.rocketmq.v2.ReceiveMessageRequest;
//import apache.rocketmq.v2.ReceiveMessageResponse;
//import apache.rocketmq.v2.Resource;
//import apache.rocketmq.v2.SendMessageRequest;
//import apache.rocketmq.v2.SendMessageResponse;
//import apache.rocketmq.v2.Settings;
//import apache.rocketmq.v2.TelemetryCommand;
//import apache.rocketmq.v2.ThreadStackTrace;
//import apache.rocketmq.v2.VerifyMessageResult;
//import io.grpc.Context;
//import io.grpc.stub.StreamObserver;
//import io.netty.channel.Channel;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import org.apache.rocketmq.broker.BrokerController;
//import org.apache.rocketmq.broker.client.ClientChannelInfo;
//import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
//import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
//import org.apache.rocketmq.broker.client.ProducerChangeListener;
//import org.apache.rocketmq.broker.client.ProducerGroupEvent;
//import org.apache.rocketmq.client.consumer.PopStatus;
//import org.apache.rocketmq.common.MQVersion;
//import org.apache.rocketmq.common.constant.LoggerName;
//import org.apache.rocketmq.common.consumer.ReceiptHandle;
//import org.apache.rocketmq.common.message.MessageBatch;
//import org.apache.rocketmq.common.message.MessageClientIDSetter;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.message.MessageQueue;
//import org.apache.rocketmq.common.protocol.RequestCode;
//import org.apache.rocketmq.common.protocol.ResponseCode;
//import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
//import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
//import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
//import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeResponseHeader;
//import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
//import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
//import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
//import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
//import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
//import org.apache.rocketmq.logging.InternalLogger;
//import org.apache.rocketmq.logging.InternalLoggerFactory;
//import org.apache.rocketmq.proxy.grpc.v2.common.ChannelManager;
//import org.apache.rocketmq.proxy.channel.InvocationContext;
//import org.apache.rocketmq.proxy.channel.SimpleChannel;
//import org.apache.rocketmq.proxy.channel.SimpleChannelHandlerContext;
//import org.apache.rocketmq.proxy.common.AbstractStartAndShutdown;
//import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
//import org.apache.rocketmq.proxy.common.TelemetryCommandRecord;
//import org.apache.rocketmq.proxy.service.ServiceManager;
//import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.ReceiveMessageChannel;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.SendMessageChannel;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.ReceiveMessageResponseHandler;
//import org.apache.rocketmq.proxy.grpc.v2.adapter.handler.SendMessageResponseHandler;
//import org.apache.rocketmq.proxy.grpc.v2.service.local.LocalReceiveMessageResponseStreamWriter;
//import org.apache.rocketmq.proxy.grpc.v2.service.local.LocalReceiveMessageResultFilter;
//import org.apache.rocketmq.proxy.grpc.v2.service.local.LocalWriteQueueSelector;
//import org.apache.rocketmq.proxy.grpc.v2.service.local.RouteService;
//import org.apache.rocketmq.proxy.grpc2.v2.GrpcMessingActivity;
//import org.apache.rocketmq.remoting.RemotingServer;
//import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
//import org.apache.rocketmq.remoting.protocol.LanguageCode;
//import org.apache.rocketmq.remoting.protocol.RemotingCommand;
//
//public class LocalGrpcActivity extends AbstractStartAndShutdown implements GrpcMessingActivity {
//    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
//
//    private final BrokerController brokerController;
//    private final ChannelManager channelManager;
//    private final TelemetryCommandManager telemetryCommandManager;
//    private final GrpcClientManager grpcClientManager;
//    private final RouteService routeService;
//    private final ClientSettingsService clientSettingsService;
//    private final LocalWriteQueueSelector localWriteQueueSelector;
//    private final BaseReceiveMessageResponseStreamWriter.Builder streamWriterBuilder;
//
//    private volatile ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> receiveMessageHook;
//
//    public LocalGrpcActivity(BrokerController brokerController) {
//        this(brokerController, new TelemetryCommandManager());
//    }
//
//    /**
//     * For unit test
//     * @param brokerController BrokerController works in local mode
//     * @param telemetryCommandManager Used to manage telemetry command
//     */
//    LocalGrpcActivity(BrokerController brokerController, TelemetryCommandManager telemetryCommandManager) {
//        this.brokerController = brokerController;
//        this.channelManager = new ChannelManager();
//        // TransactionStateChecker is not used in Local mode.
//        ServiceManager serviceManager = new ServiceManager(null);
//        this.telemetryCommandManager = telemetryCommandManager;
//        this.grpcClientManager = new GrpcClientManager();
//        this.routeService = new RouteService(serviceManager, grpcClientManager);
//        this.clientSettingsService = new ClientSettingsService(this.channelManager, this.grpcClientManager, this.telemetryCommandManager);
//        this.localWriteQueueSelector = new LocalWriteQueueSelector(brokerController.getBrokerConfig().getBrokerName(),
//            brokerController.getTopicConfigManager(), serviceManager.getTopicRouteService());
//
//        this.brokerController.getConsumerManager().appendConsumerIdsChangeListener(new ConsumerIdsChangeListenerImpl());
//        this.brokerController.getProducerManager().appendProducerChangeListener(new ProducerChangeListenerImpl());
//
//        this.streamWriterBuilder = (observer, hook) -> new LocalReceiveMessageResponseStreamWriter(
//            observer,
//            hook,
//            channelManager,
//            brokerController,
//            new LocalReceiveMessageResultFilter(channelManager, brokerController, grpcClientManager)
//        );
//
//        this.appendStartAndShutdown(serviceManager);
//    }
//
//    @Override
//    public CompletableFuture<QueryRouteResponse> queryRoute(Context ctx, QueryRouteRequest request) {
//        return this.routeService.queryRoute(ctx, request);
//    }
//
//    @Override
//    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
//        LanguageCode languageCode;
//        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
//        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
//        languageCode = LanguageCode.valueOf(language);
//
//        Settings clientSettings = grpcClientManager.getClientSettings(clientId);
//        HeartbeatData heartbeatData = GrpcConverter.buildHeartbeatData(clientId, request, clientSettings);
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
//        command.setLanguage(languageCode);
//        command.setVersion(MQVersion.Version.V5_0_0.ordinal());
//        command.setBody(heartbeatData.encode());
//        command.makeCustomHeaderToNet();
//
//        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
//        switch (clientSettings.getClientType()) {
//            case PRODUCER: {
//                for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
//                    String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
//                    GrpcClientChannel channel = GrpcClientChannel.create(channelManager, topicName, clientId, telemetryCommandManager);
//                    SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
//
//                    this.brokerController.getClientManageProcessor()
//                        .heartBeat(simpleChannelHandlerContext, command);
//                }
//                HeartbeatResponse heartbeatResponse = HeartbeatResponse.newBuilder()
//                    .setStatus(ResponseBuilder.buildStatus(Code.OK, "Producer heartbeat"))
//                    .build();
//                future.complete(heartbeatResponse);
//                break;
//            }
//            case PUSH_CONSUMER:
//            case SIMPLE_CONSUMER: {
//                String groupName = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
//                GrpcClientChannel channel = GrpcClientChannel.create(channelManager, groupName, clientId, telemetryCommandManager);
//                SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
//
//                RemotingCommand response = this.brokerController.getClientManageProcessor()
//                    .heartBeat(simpleChannelHandlerContext, command);
//                HeartbeatResponse heartbeatResponse = HeartbeatResponse.newBuilder()
//                    .setStatus(ResponseBuilder.buildStatus(response.getCode(), response.getRemark()))
//                    .build();
//                future.complete(heartbeatResponse);
//                break;
//            }
//            default: {
//                throw new IllegalArgumentException("ClientType not exist " + clientSettings.getClientType());
//            }
//        }
//
//        return future;
//    }
//
//    @Override
//    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
//        MessageQueue messageQueue = localWriteQueueSelector.selectQueue(ctx, request).getMessageQueue();
//        String topicName = messageQueue.getTopic();
//        SendMessageRequestHeader requestHeader = GrpcConverter.buildSendMessageRequestHeader(request, topicName, messageQueue.getQueueId());
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
//        List<org.apache.rocketmq.common.message.Message> messageList = GrpcConverter.buildMessage(request.getMessagesList(), request.getMessages(0).getTopic());
//        String messageId;
//        if (messageList.size() == 1) {
//            org.apache.rocketmq.common.message.Message message = messageList.get(0);
//            command.setBody(message.getBody());
//            messageId = MessageClientIDSetter.getUniqID(message);
//        } else {
//            MessageBatch messageBatch = MessageBatch.generateFromList(messageList);
//            MessageClientIDSetter.setUniqID(messageBatch);
//            messageBatch.setBody(messageBatch.encode());
//            command.setBody(messageBatch.encode());
//            messageId = MessageClientIDSetter.getUniqID(messageBatch);
//        }
//        command.makeCustomHeaderToNet();
//
//        SendMessageResponseHandler handler = new SendMessageResponseHandler(messageId, requestHeader.getSysFlag(), brokerController.getBrokerAddr());
//        SendMessageChannel channel = channelManager.createChannel(ctx, context -> new SendMessageChannel(context, handler), SendMessageChannel.class);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();
//        InvocationContext<SendMessageRequest, SendMessageResponse> context
//            = new InvocationContext<>(request, future);
//        channel.registerInvocationContext(command.getOpaque(), context);
//        try {
//            RemotingCommand response = brokerController.getSendMessageProcessor()
//                .processRequest(channelHandlerContext, command);
//            if (response != null) {
//                handler.handle(response, context);
//                channel.eraseInvocationContext(command.getOpaque());
//            }
//        } catch (final Exception e) {
//            log.error("Failed to process send message command", e);
//            channel.eraseInvocationContext(command.getOpaque());
//            future.completeExceptionally(e);
//        }
//        return future;
//    }
//
//    @Override
//    public CompletableFuture<QueryAssignmentResponse> queryAssignment(Context ctx, QueryAssignmentRequest request) {
//        return this.routeService.queryAssignment(ctx, request);
//    }
//
//    @Override
//    public void receiveMessage(Context ctx, ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
//        long pollTime = GrpcConverter.buildPollTimeFromContext(ctx);
//        // TODO: get fifo config from subscriptionGroupManager
//        boolean fifo = false;
//        BaseReceiveMessageResponseStreamWriter writer = streamWriterBuilder.build(responseObserver, receiveMessageHook);
//        ReceiveMessageResponseHandler handler = new ReceiveMessageResponseHandler(brokerController.getBrokerConfig().getBrokerName(), fifo);
//        ReceiveMessageChannel channel = channelManager.createChannel(ctx, context -> new ReceiveMessageChannel(context, handler), ReceiveMessageChannel.class);
//        CompletableFuture<List<MessageExt>> future = new CompletableFuture<>();
//        InvocationContext<ReceiveMessageRequest, List<MessageExt>> context
//            = new InvocationContext<>(request, future);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//        PopMessageRequestHeader requestHeader = GrpcConverter.buildPopMessageRequestHeader(request, pollTime, fifo);
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
//        command.makeCustomHeaderToNet();
//        channel.registerInvocationContext(command.getOpaque(), context);
//        try {
//            RemotingCommand response = brokerController.getPopMessageProcessor().processRequest(channelHandlerContext, command);
//            if (response != null) {
//                handler.handle(response, context);
//                channel.eraseInvocationContext(command.getOpaque());
//            }
//        } catch (Exception e) {
//            log.error("Failed to process pop message command", e);
//            channel.eraseInvocationContext(command.getOpaque());
//            future.completeExceptionally(e);
//        }
//        future.thenAccept(r -> writer.write(ctx, request, PopStatus.FOUND, r))
//            .exceptionally(e -> {
//                writer.write(ctx, request, e);
//                return null;
//            });
//    }
//
//    @Override
//    public CompletableFuture<AckMessageResponse> ackMessage(Context ctx, AckMessageRequest request) {
//        Channel channel = channelManager.createChannel(ctx);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//        CompletableFuture<AckMessageResponse> future = new CompletableFuture<>();
//        List<AckMessageResultEntry> ackMessageResultEntryList = new ArrayList<>();
//        for (AckMessageEntry entry : request.getEntriesList()) {
//            ReceiptHandle receiptHandle = ReceiptHandle.decode(entry.getReceiptHandle());
//            if (receiptHandle.isExpired()) {
//                ackMessageResultEntryList.add(AckMessageResultEntry.newBuilder()
//                    .setReceiptHandle(entry.getReceiptHandle())
//                    .setMessageId(entry.getMessageId())
//                    .setStatus(ResponseBuilder.buildStatus(Code.RECEIPT_HANDLE_EXPIRED, "expired"))
//                    .build());
//                continue;
//            }
//            AckMessageRequestHeader requestHeader = GrpcConverter.buildAckMessageRequestHeader(request, receiptHandle);
//            RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
//            command.makeCustomHeaderToNet();
//
//            try {
//                RemotingCommand responseCommand = brokerController.getAckMessageProcessor()
//                    .processRequest(channelHandlerContext, command);
//                ackMessageResultEntryList.add(AckMessageResultEntry.newBuilder()
//                    .setReceiptHandle(entry.getReceiptHandle())
//                    .setMessageId(entry.getMessageId())
//                    .setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()))
//                    .build());
//            } catch (Exception e) {
//                log.error("Exception raised when ack message", e);
//                ackMessageResultEntryList.add(AckMessageResultEntry.newBuilder()
//                    .setReceiptHandle(entry.getReceiptHandle())
//                    .setMessageId(entry.getMessageId())
//                    .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, e.getMessage()))
//                    .build());
//            }
//        }
//        AckMessageResponse response = AckMessageResponse.newBuilder()
//            .setStatus(ResponseBuilder.buildStatus(ResponseCode.SUCCESS, "ok"))
//            .addAllEntries(ackMessageResultEntryList)
//            .build();
//        future.complete(response);
//        return future;
//    }
//
//    @Override
//    public CompletableFuture<ForwardMessageToDeadLetterQueueResponse> forwardMessageToDeadLetterQueue(Context ctx,
//        ForwardMessageToDeadLetterQueueRequest request) {
//        SimpleChannel channel = channelManager.createChannel(ctx);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//
//        ConsumerSendMsgBackRequestHeader requestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(request);
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
//        command.makeCustomHeaderToNet();
//
//        CompletableFuture<ForwardMessageToDeadLetterQueueResponse> future = new CompletableFuture<>();
//        try {
//            RemotingCommand response = brokerController.getSendMessageProcessor()
//                .processRequest(channelHandlerContext, command);
//
//            future.complete(ForwardMessageToDeadLetterQueueResponse.newBuilder()
//                .setStatus(ResponseBuilder.buildStatus(response.getCode(), response.getRemark()))
//                .build());
//        } catch (Exception e) {
//            log.error("Exception raised when forwardMessageToDeadLetterQueue", e);
//            future.completeExceptionally(e);
//        }
//        return future;
//    }
//
//    @Override
//    public CompletableFuture<EndTransactionResponse> endTransaction(Context ctx, EndTransactionRequest request) {
//        Channel channel = channelManager.createChannel(ctx);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//
//        String topicName = GrpcConverter.wrapResourceWithNamespace(request.getTopic());
//        EndTransactionRequestHeader requestHeader = GrpcConverter.buildEndTransactionRequestHeader(request, topicName);
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
//        command.makeCustomHeaderToNet();
//
//        CompletableFuture<EndTransactionResponse> future = new CompletableFuture<>();
//        try {
//            RemotingCommand responseCommand = brokerController.getEndTransactionProcessor()
//                .processRequest(channelHandlerContext, command);
//            EndTransactionResponse.Builder builder = EndTransactionResponse.newBuilder();
//            if (null != responseCommand) {
//                builder.setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()));
//            } else {
//                builder.setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "Response command is null"));
//            }
//            EndTransactionResponse response = builder.build();
//            future.complete(response);
//        } catch (Exception e) {
//            log.error("Exception raised while endTransaction", e);
//            future.completeExceptionally(e);
//        }
//        return future;
//    }
//
//    public void reportThreadStackTrace(Context ctx, ThreadStackTrace request) {
//        String nonce = request.getNonce();
//        String threadStack = request.getThreadStackTrace();
//        TelemetryCommandRecord pollCommandResponseFuture = telemetryCommandManager.getCommand(nonce);
//        if (pollCommandResponseFuture != null) {
//            Integer opaque = pollCommandResponseFuture.getOpaque();
//            if (opaque != null) {
//                RemotingServer remotingServer = this.brokerController.getRemotingServer();
//                if (remotingServer instanceof NettyRemotingAbstract) {
//                    NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
//                    RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "From gRPC client");
//                    remotingCommand.setOpaque(pollCommandResponseFuture.getOpaque());
//                    ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
//                    runningInfo.setJstack(threadStack);
//                    remotingCommand.setBody(runningInfo.encode());
//                    nettyRemotingAbstract.processResponseCommand(new SimpleChannelHandlerContext(channelManager.createChannel(ctx)), remotingCommand);
//                }
//            }
//        }
//    }
//
//    public void reportVerifyMessageResult(Context ctx, VerifyMessageResult request) {
//        String nonce = request.getNonce();
//        TelemetryCommandRecord pollCommandResponseFuture = telemetryCommandManager.getCommand(nonce);
//        if (pollCommandResponseFuture != null) {
//            Integer opaque = pollCommandResponseFuture.getOpaque();
//            if (opaque != null) {
//                RemotingServer remotingServer = this.brokerController.getRemotingServer();
//                if (remotingServer instanceof NettyRemotingAbstract) {
//                    NettyRemotingAbstract nettyRemotingAbstract = (NettyRemotingAbstract) remotingServer;
//                    RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, "From gRPC client");
//                    remotingCommand.setOpaque(pollCommandResponseFuture.getOpaque());
//                    ConsumeMessageDirectlyResult result = GrpcConverter.buildConsumeMessageDirectlyResult(request);
//                    remotingCommand.setBody(result.encode());
//                    nettyRemotingAbstract.processResponseCommand(new SimpleChannelHandlerContext(channelManager.createChannel(ctx)), remotingCommand);
//                }
//            }
//        }
//    }
//
//    @Override
//    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx,
//        NotifyClientTerminationRequest request) {
//        Channel channel = channelManager.createChannel(ctx);
//        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
//        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
//        Settings clientSettings = grpcClientManager.getClientSettings(clientId);
//        UnregisterClientRequestHeader header = GrpcConverter.buildUnregisterClientRequestHeader(clientId, clientSettings.getClientType(), request);
//
//        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, header);
//        remotingCommand.makeCustomHeaderToNet();
//        try {
//            this.brokerController.getClientManageProcessor().unregisterClient(simpleChannelHandlerContext, remotingCommand);
//        } catch (Exception ignored) {
//        }
//        return new CompletableFuture<>();
//    }
//
//    @Override
//    public CompletableFuture<ChangeInvisibleDurationResponse> changeInvisibleDuration(Context ctx,
//        ChangeInvisibleDurationRequest request) {
//        Channel channel = channelManager.createChannel(ctx);
//        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);
//
//        ChangeInvisibleTimeRequestHeader requestHeader = GrpcConverter.buildChangeInvisibleTimeRequestHeader(request);
//        ReceiptHandle receiptHandle = ReceiptHandle.decode(request.getReceiptHandle());
//        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
//        command.makeCustomHeaderToNet();
//
//        CompletableFuture<ChangeInvisibleDurationResponse> future = new CompletableFuture<>();
//        try {
//            RemotingCommand responseCommand = brokerController.getChangeInvisibleTimeProcessor()
//                .processRequest(channelHandlerContext, command);
//            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) responseCommand.readCustomHeader();
//            ChangeInvisibleDurationResponse.Builder builder = ChangeInvisibleDurationResponse.newBuilder()
//                .setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()));
//            if (responseCommand.getCode() == ResponseCode.SUCCESS) {
//                builder.setReceiptHandle(ReceiptHandle.builder()
//                    .startOffset(requestHeader.getOffset())
//                    .retrieveTime(responseHeader.getPopTime())
//                    .invisibleTime(responseHeader.getInvisibleTime())
//                    .reviveQueueId(responseHeader.getReviveQid())
//                    .topicType(receiptHandle.getTopicType())
//                    .brokerName(brokerController.getBrokerConfig().getBrokerName())
//                    .queueId(requestHeader.getQueueId())
//                    .offset(requestHeader.getOffset())
//                    .build()
//                    .encode());
//            }
//
//            future.complete(builder.build());
//        } catch (Exception e) {
//            log.error("Exception raised while changeInvisibleDuration", e);
//            future.completeExceptionally(e);
//        }
//        return future;
//    }
//
//    @Override
//    public StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver) {
//        String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
//        return new StreamObserver<TelemetryCommand>() {
//            @Override
//            public void onNext(TelemetryCommand request) {
//                switch (request.getCommandCase()) {
//                    case SETTINGS: {
//                        responseObserver.onNext(clientSettingsService.processClientSettings(ctx, request, responseObserver));
//                        break;
//                    }
//                    case THREAD_STACK_TRACE: {
//                        reportThreadStackTrace(ctx, request.getThreadStackTrace());
//                        break;
//                    }
//                    case VERIFY_MESSAGE_RESULT: {
//                        reportVerifyMessageResult(ctx, request.getVerifyMessageResult());
//                        break;
//                    }
//                    default: {
//                        throw new IllegalArgumentException("Request type is illegal");
//                    }
//                }
//            }
//
//            @Override
//            public void onError(Throwable t) {
//
//            }
//
//            @Override
//            public void onCompleted() {
//                responseObserver.onCompleted();
//            }
//        };
//    }
//
//    protected class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {
//
//        @Override
//        public void handle(ConsumerGroupEvent event, String group, Object... args) {
//            if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
//                if (args == null || args.length < 1) {
//                    return;
//                }
//                if (args[0] instanceof ClientChannelInfo) {
//                    ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
//                    channelManager.onClientOffline(clientChannelInfo.getClientId());
//                    grpcClientManager.removeClientSettings(clientChannelInfo.getClientId());
//                }
//            }
//        }
//
//        @Override
//        public void shutdown() {
//
//        }
//    }
//
//    protected class ProducerChangeListenerImpl implements ProducerChangeListener {
//
//        @Override
//        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
//            if (event == ProducerGroupEvent.CLIENT_UNREGISTER) {
//                channelManager.onClientOffline(clientChannelInfo.getClientId());
//                grpcClientManager.removeClientSettings(clientChannelInfo.getClientId());
//            }
//        }
//    }
//}
