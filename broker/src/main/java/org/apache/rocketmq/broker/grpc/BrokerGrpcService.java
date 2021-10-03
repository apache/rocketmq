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

package org.apache.rocketmq.broker.grpc;

import apache.rocketmq.v1.AckMessageRequest;
import apache.rocketmq.v1.AckMessageResponse;
import apache.rocketmq.v1.Address;
import apache.rocketmq.v1.AddressScheme;
import apache.rocketmq.v1.Assignment;
import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.ConsumePolicy;
import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.EndTransactionRequest;
import apache.rocketmq.v1.EndTransactionResponse;
import apache.rocketmq.v1.Endpoints;
import apache.rocketmq.v1.FilterExpression;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v1.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v1.GenericPollingRequest;
import apache.rocketmq.v1.HealthCheckRequest;
import apache.rocketmq.v1.HealthCheckResponse;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.HeartbeatResponse;
import apache.rocketmq.v1.MessagingServiceGrpc;
import apache.rocketmq.v1.MultiplexingRequest;
import apache.rocketmq.v1.MultiplexingResponse;
import apache.rocketmq.v1.NackMessageRequest;
import apache.rocketmq.v1.NackMessageResponse;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.NotifyClientTerminationResponse;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.ProducerData;
import apache.rocketmq.v1.PullMessageRequest;
import apache.rocketmq.v1.PullMessageResponse;
import apache.rocketmq.v1.QueryAssignmentRequest;
import apache.rocketmq.v1.QueryAssignmentResponse;
import apache.rocketmq.v1.QueryOffsetPolicy;
import apache.rocketmq.v1.QueryOffsetRequest;
import apache.rocketmq.v1.QueryOffsetResponse;
import apache.rocketmq.v1.ReceiveMessageRequest;
import apache.rocketmq.v1.ReceiveMessageResponse;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SendMessageResponse;
import apache.rocketmq.v1.SubscriptionEntry;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.grpc.adapter.InvocationContext;
import org.apache.rocketmq.broker.grpc.adapter.PullMessageChannel;
import org.apache.rocketmq.broker.grpc.adapter.ReceiveMessageChannel;
import org.apache.rocketmq.broker.grpc.adapter.SimpleChannel;
import org.apache.rocketmq.broker.grpc.adapter.SimpleChannelHandlerContext;
import org.apache.rocketmq.broker.loadbalance.AssignmentManager;
import org.apache.rocketmq.broker.stat.Histogram;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.AddressableMessageQueue;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.grpc.channel.GrpcClientChannelManager;
import org.apache.rocketmq.grpc.channel.GrpcClientObserver;
import org.apache.rocketmq.grpc.common.Converter;
import org.apache.rocketmq.grpc.common.DelayPolicy;
import org.apache.rocketmq.grpc.common.InterceptorConstants;
import org.apache.rocketmq.grpc.common.ResponseBuilder;
import org.apache.rocketmq.grpc.common.ResponseWriter;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerGrpcService extends MessagingServiceGrpc.MessagingServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.GRPC_LOGGER_NAME);

    private final BrokerController controller;

    private final ConcurrentHashMap<String, SimpleChannel> clientIdChannelMap = new ConcurrentHashMap<>();

    private final GrpcClientChannelManager clientChannelManager = new GrpcClientChannelManager();

    private final Histogram messageLatency;

    private final DelayPolicy delayPolicy;

    public BrokerGrpcService(BrokerController controller) {
        this.controller = controller;
        int latencyStatsGrade = 11;
        this.messageLatency = new Histogram("Message-Latency", latencyStatsGrade);
        this.delayPolicy = DelayPolicy.build(controller.getMessageStoreConfig().getMessageDelayLevel());
        int i = 0;
        messageLatency.getLabels().add(i++, "[00ms~10ms): ");
        messageLatency.getLabels().add(i++, "[10ms~20ms): ");
        messageLatency.getLabels().add(i++, "[20ms~30ms): ");
        messageLatency.getLabels().add(i++, "[30ms~40ms): ");
        messageLatency.getLabels().add(i++, "[40ms~50ms): ");
        messageLatency.getLabels().add(i++, "[50ms~60ms): ");
        messageLatency.getLabels().add(i++, "[60ms~70ms): ");
        messageLatency.getLabels().add(i++, "[70ms~80ms): ");
        messageLatency.getLabels().add(i++, "[80ms~90ms): ");
        messageLatency.getLabels().add(i++, "[90ms~100ms): ");
        messageLatency.getLabels().add(i, "[1000ms~inf): ");
    }

    /**
     * Scan and remove inactive mocking channels;
     * Scan and clean expired requests;
     */
    public void scanAndCleanChannels() {
        try {
            Iterator<Map.Entry<String, SimpleChannel>> iterator = clientIdChannelMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleChannel> entry = iterator.next();
                if (!entry.getValue().isActive()) {
                    iterator.remove();
                } else {
                    entry.getValue().cleanExpiredRequests();
                }
            }
        } catch (Throwable e) {
            LOGGER.error("[BUG] Unexpected exception", e);
        }
    }

    public void logLatencyStats() {
        try {
            LOGGER.info("Histogram: {}", messageLatency.reportAndReset());
        } catch (Throwable e) {
            LOGGER.error("[BUG] Unexpected exception", e);
        }
    }

    @Override
    public void healthCheck(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
        LOGGER.trace("Received health check request from client: {}", request.getClientHost());
        final HealthCheckResponse response = HealthCheckResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"))
            .build();
        ResponseWriter.write(responseObserver, response);
    }

    public void heartbeat(HeartbeatRequest request,StreamObserver<HeartbeatResponse> responseObserver) {
        SimpleChannel channel = createChannel(request.getClientId());
        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
        String version = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.CLIENT_VERSION);
        LanguageCode languageCode = LanguageCode.valueOf(language);
        String clientId = request.getClientId();
        if (version == null) {
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.INVALID_ARGUMENT, "version not set"))
                .build();
            ResponseWriter.write(responseObserver, response);
            return;
        }
        int versionCode = Integer.parseInt(version);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            channel,
            clientId,
            languageCode,
            versionCode
        );

        if (request.hasProducerData()) {
            ProducerData producerData = request.getProducerData();
            String groupName = Converter.getResourceNameWithNamespace(producerData.getGroup());
            controller.getProducerManager().registerProducer(groupName, clientChannelInfo);
        }

        if (request.hasConsumerData()) {
            ConsumerData consumerData = request.getConsumerData();
            String groupName = Converter.getResourceNameWithNamespace(consumerData.getGroup());
            Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
            for (SubscriptionEntry sub : consumerData.getSubscriptionsList()) {
                String topicName = Converter.getResourceNameWithNamespace(sub.getTopic());
                FilterExpression filterExpression = sub.getExpression();
                String expression = filterExpression.getExpression();
                String expressionType = Converter.buildExpressionType(filterExpression.getType());
                try {
                    SubscriptionData subscriptionData = FilterAPI.build(topicName, expression, expressionType);
                    subscriptionDataSet.add(subscriptionData);
                } catch (Exception e) {
                    LOGGER.error("Build subscription failed when apply heartbeat", e);
                    ResponseWriter.writeException(responseObserver, e);
                }
            }
            controller.getConsumerManager().registerConsumer(
                groupName,
                clientChannelInfo,
                Converter.buildConsumeType(consumerData.getConsumeType()),
                Converter.buildMessageModel(consumerData.getConsumeModel()),
                Converter.buildConsumeFromWhere(consumerData.getConsumePolicy()),
                subscriptionDataSet,
                false
            );
        }

        HeartbeatResponse.Builder builder = HeartbeatResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"));

        final HeartbeatResponse response = builder.build();
        ResponseWriter.write(responseObserver, response);
    }

    @Override
    public void sendMessage(SendMessageRequest request, StreamObserver<SendMessageResponse> responseObserver) {
        SimpleChannel channel = createChannel(anonymousChannelId());
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        SendMessageRequestHeader requestHeader = Converter.buildSendMessageRequestHeader(request);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        command.setBody(request.getMessage().getBody().toByteArray());
        command.makeCustomHeaderToNet();
        SendMessageResponse response = null;
        try {
            RemotingCommand responseCommand = controller.getSendMessageProcessor()
                .processRequest(channelHandlerContext, command);
            boolean processed = false;
            if (null != responseCommand) {
                processed = true;
                response = ResponseBuilder.buildSendMessageResponse(responseCommand);
            }

            if (!processed) {
                responseCommand = channelHandlerContext.getCommand();
                if (null != responseCommand) {
                    processed = true;
                    response = ResponseBuilder.buildSendMessageResponse(responseCommand);
                }
            }

            if (!processed) {
                LOGGER.error("Unexpected error. Cannot find send message response command");
                response = SendMessageResponse.newBuilder()
                    .setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Failed to acquire send-message remoting command"))
                    .build();
            }
            ResponseWriter.write(responseObserver, response);
        } catch (final RemotingCommandException e) {
            LOGGER.error("Failed to process send message command", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    @Override
    public void queryAssignment(QueryAssignmentRequest request, StreamObserver<QueryAssignmentResponse> responseObserver) {
        String topicName = Converter.getResourceNameWithNamespace(request.getTopic());

        AssignmentManager assignmentManager = controller.getAssignmentManager();
        Set<AddressableMessageQueue> addressableMessageQueueSet = assignmentManager.getAddressableMessageQueueSet(topicName);

        try {
            QueryAssignmentResponse.Builder builder = QueryAssignmentResponse.newBuilder();
                if (null != addressableMessageQueueSet && !addressableMessageQueueSet.isEmpty()) {
                    for (AddressableMessageQueue assignment : addressableMessageQueueSet) {
                        String brokerAddress = assignment.getAddress();
                        HostAndPort hostAndPort = HostAndPort.fromString(brokerAddress);
                        builder.addAssignments(Assignment.newBuilder()
                            .setPartition(Partition.newBuilder()
                                .setTopic(request.getTopic())
                                .setPermission(Permission.READ)
                                .setBroker(Broker.newBuilder()
                                    .setName(assignment.getBrokerName())
                                    .setId(assignment.getBrokerId())
                                    .setEndpoints(Endpoints.newBuilder()
                                        .setScheme(AddressScheme.IPv4)
                                        .addAddresses(Address.newBuilder()
                                            .setHost(hostAndPort.getHostText())
                                            .setPort(hostAndPort.getPort())
                                            .build())
                                        .build())
                                    .build())
                                .build()));
                    }
                }


            QueryAssignmentResponse response = builder.build();
            ResponseWriter.write(responseObserver, response);
        } catch (Exception e) {
            LOGGER.error("QueryAssignment raised an exception", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    @Override
    public void receiveMessage(ReceiveMessageRequest request, StreamObserver<ReceiveMessageResponse> responseObserver) {
        LOGGER.trace("Receive message request: {}", request);
        SimpleChannel channel = createReceiveMessageChannel(anonymousChannelId());
        channel.setController(controller);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        Resource group = request.getGroup();
        String groupName = Converter.getResourceNameWithNamespace(group);
        Partition partition = request.getPartition();
        Resource topic = partition.getTopic();
        String topicName = Converter.getResourceNameWithNamespace(topic);
        int queueId = partition.getId();
        int maxMessageNumbers = request.getBatchSize();
        long invisibleTime = Durations.toMillis(request.getInvisibleDuration());
        long bornTime = Timestamps.toMillis(request.getInitializationTimestamp());
        ConsumePolicy policy = request.getConsumePolicy();
        int initMode = Converter.buildConsumeInitMode(policy);

        FilterExpression filterExpression = request.getFilterExpression();
        String expression = filterExpression.getExpression();
        String expressionType = Converter.buildExpressionType(filterExpression.getType());

        long rpcTimeout = Context.current()
            .getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS) - controller.getGrpcServerConfig().getRpcRoadReserveTimeMs();
        if (rpcTimeout <= 0) {
            ResponseWriter.write(responseObserver, ReceiveMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.DEADLINE_EXCEEDED, "request has been canceled due to timeout"))
                .build());
            return;
        }

        PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
        requestHeader.setConsumerGroup(groupName);
        requestHeader.setTopic(topicName);
        requestHeader.setQueueId(queueId);
        requestHeader.setMaxMsgNums(maxMessageNumbers);
        requestHeader.setInvisibleTime(invisibleTime);
        requestHeader.setPollTime(rpcTimeout);
        requestHeader.setBornTime(bornTime);
        requestHeader.setInitMode(initMode);
        requestHeader.setExpType(expressionType);
        requestHeader.setExp(expression);
        requestHeader.setOrder(request.getFifoFlag());
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();
        InvocationContext<ReceiveMessageRequest, ReceiveMessageResponse> context
            = new InvocationContext<>(request, (ServerCallStreamObserver<ReceiveMessageResponse>) responseObserver);
        channel.registerInvocationContext(command.getOpaque(), context);
        try {
            RemotingCommand responseCommand = controller.getPopMessageProcessor().processRequest(channelHandlerContext, command);
            if (null != responseCommand) {
                handlePopResponseCommand(responseCommand, context);
                channel.eraseInvocationContext(command.getOpaque());
            }
        } catch (RemotingCommandException e) {
            LOGGER.error("Pop message failed", e);
            channel.eraseInvocationContext(command.getOpaque());
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void ackMessage(AckMessageRequest request, StreamObserver<AckMessageResponse> responseObserver) {
        Channel channel = createChannel(anonymousChannelId());
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        String groupName = Converter.getResourceNameWithNamespace(request.getGroup());
        String topicName = Converter.getResourceNameWithNamespace(request.getTopic());
        String receiptHandle = request.getReceiptHandle();
        String[] extraInfos = ExtraInfoUtil.split(receiptHandle);
        int queueId = ExtraInfoUtil.getQueueId(extraInfos);
        long offset = ExtraInfoUtil.getQueueOffset(extraInfos);

        AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
        ackMessageRequestHeader.setConsumerGroup(groupName);
        ackMessageRequestHeader.setTopic(topicName);
        ackMessageRequestHeader.setQueueId(queueId);
        ackMessageRequestHeader.setExtraInfo(receiptHandle);
        ackMessageRequestHeader.setOffset(offset);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, ackMessageRequestHeader);
        command.makeCustomHeaderToNet();

        try {
            RemotingCommand responseCommand = controller.getAckMessageProcessor().processRequest(channelHandlerContext,
                command);
            AckMessageResponse.Builder builder = AckMessageResponse.newBuilder();
            if (null != responseCommand) {
                builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            } else {
                builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
            }
            AckMessageResponse response = builder.build();
            ResponseWriter.write(responseObserver, response);
        } catch (RemotingCommandException e) {
            LOGGER.error("Exception raised when ack message", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void nackMessage(NackMessageRequest request, StreamObserver<NackMessageResponse> responseObserver) {
        Channel channel = createChannel(anonymousChannelId());
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        String groupName = Converter.getResourceNameWithNamespace(request.getGroup());
        String topicName = Converter.getResourceNameWithNamespace(request.getTopic());
        String receiptHandle = request.getReceiptHandle();
        String[] extraInfos = ExtraInfoUtil.split(receiptHandle);
        int queueId = ExtraInfoUtil.getQueueId(extraInfos);
        long offset = ExtraInfoUtil.getQueueOffset(extraInfos);
        int deliveryAttempt = request.getDeliveryAttempt();

        int retryDelayLevelDelta = controller.getBrokerConfig().getRetryDelayLevelDelta();
        long invisibleTime = delayPolicy.getDelayInterval(retryDelayLevelDelta + deliveryAttempt);

        ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
        changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
        changeInvisibleTimeRequestHeader.setTopic(topicName);
        changeInvisibleTimeRequestHeader.setQueueId(queueId);
        changeInvisibleTimeRequestHeader.setExtraInfo(receiptHandle);
        changeInvisibleTimeRequestHeader.setOffset(offset);
        changeInvisibleTimeRequestHeader.setInvisibleTime(invisibleTime);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME,
            changeInvisibleTimeRequestHeader);
        command.makeCustomHeaderToNet();

        try {
            RemotingCommand responseCommand = controller.getChangeInvisibleTimeProcessor()
                .processRequest(channelHandlerContext, command);
            NackMessageResponse response = NackMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()))
                .build();
            ResponseWriter.write(responseObserver, response);
        } catch (RemotingCommandException e) {
            LOGGER.error("Exception raised while changeInvisibleTime", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void forwardMessageToDeadLetterQueue(ForwardMessageToDeadLetterQueueRequest request,
        StreamObserver<ForwardMessageToDeadLetterQueueResponse> responseObserver) {
        Channel channel = createChannel(anonymousChannelId());
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        String groupName = Converter.getResourceNameWithNamespace(request.getGroup());
        String topicName = Converter.getResourceNameWithNamespace(request.getTopic());
        String receiptHandle = request.getReceiptHandle();
        String[] extraInfos = ExtraInfoUtil.split(receiptHandle);
        long offset = ExtraInfoUtil.getQueueOffset(extraInfos);

        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        requestHeader.setGroup(groupName);
        requestHeader.setOffset(offset);
        requestHeader.setDelayLevel(-1);
        requestHeader.setOriginMsgId(request.getMessageId());
        requestHeader.setOriginTopic(topicName);
        requestHeader.setMaxReconsumeTimes(requestHeader.getMaxReconsumeTimes());

        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
        command.makeCustomHeaderToNet();

        try {
            CompletableFuture<RemotingCommand> future = controller.getSendMessageProcessor().asyncProcessRequest(channelHandlerContext, command);
            future.thenAccept(r -> {
                ForwardMessageToDeadLetterQueueResponse.Builder builder = ForwardMessageToDeadLetterQueueResponse.newBuilder();
                if (null != r) {
                    builder.setCommon(ResponseBuilder.buildCommon(r.getCode(), r.getRemark()));
                } else {
                    builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
                }
                ForwardMessageToDeadLetterQueueResponse response = builder.build();
                ResponseWriter.write(responseObserver, response);
            });
        } catch (Exception e) {
            LOGGER.error("Exception raised when forwardMessageToDeadLetterQueue", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void endTransaction(EndTransactionRequest request, StreamObserver<EndTransactionResponse> responseObserver) {
        Channel channel = createChannel(anonymousChannelId());
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        String groupName = Converter.getResourceNameWithNamespace(request.getGroup());
        String messageId = request.getMessageId();
        String transactionId = request.getTransactionId();
        long transactionStateTableOffset = request.getTransactionStateTableOffset();
        long commitLogOffset = request.getCommitLogOffset();
        boolean fromTransactionCheck = request.getSource() == EndTransactionRequest.Source.SERVER_CHECK;
        int commitOrRollback = Converter.buildTransactionCommitOrRollback(request.getResolution());

        EndTransactionRequestHeader endTransactionRequestHeader = new EndTransactionRequestHeader();
        endTransactionRequestHeader.setProducerGroup(groupName);
        endTransactionRequestHeader.setMsgId(messageId);
        endTransactionRequestHeader.setTransactionId(transactionId);
        endTransactionRequestHeader.setTranStateTableOffset(transactionStateTableOffset);
        endTransactionRequestHeader.setCommitLogOffset(commitLogOffset);
        endTransactionRequestHeader.setCommitOrRollback(commitOrRollback);
        endTransactionRequestHeader.setFromTransactionCheck(fromTransactionCheck);

        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, endTransactionRequestHeader);
        command.makeCustomHeaderToNet();

        try {
            RemotingCommand responseCommand = controller.getEndTransactionProcessor().processRequest(channelHandlerContext,
                command);
            EndTransactionResponse.Builder builder = EndTransactionResponse.newBuilder();
            if (null != responseCommand) {
                ResponseWriter.write(responseObserver, builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()))
                    .build());
            } else {
                builder.setCommon(ResponseBuilder.buildCommon(Code.INTERNAL, "Response command is null"));
            }
        } catch (Exception e) {
            LOGGER.error("Exception raised while endTransaction", e);
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void queryOffset(QueryOffsetRequest request, StreamObserver<QueryOffsetResponse> responseObserver) {
        Partition partition = request.getPartition();
        String topicName = Converter.getResourceNameWithNamespace(partition.getTopic());
        int queueId = partition.getId();

        long offset;
        if (request.getPolicy() == QueryOffsetPolicy.BEGINNING) {
            offset = 0L;
        } else if (request.getPolicy() == QueryOffsetPolicy.END) {
            offset = controller.getMessageStore().getMaxOffsetInQueue(topicName, queueId);
        } else {
            long timestamp = Timestamps.toMillis(request.getTimePoint());
            offset = controller.getMessageStore().getOffsetInQueueByTime(topicName, queueId, timestamp);
        }
        ResponseWriter.write(responseObserver, QueryOffsetResponse.newBuilder()
            .setCommon(ResponseBuilder.buildCommon(Code.OK, "ok"))
            .setOffset(offset)
            .build());
    }

    public void pullMessage(PullMessageRequest request, StreamObserver<PullMessageResponse> responseObserver) {
        SimpleChannel channel = createPullMessageChannel(anonymousChannelId());
        channel.setController(controller);
        SimpleChannelHandlerContext channelHandlerContext = new SimpleChannelHandlerContext(channel);

        long rpcTimeout = Context.current()
            .getDeadline()
            .timeRemaining(TimeUnit.MILLISECONDS) - controller.getGrpcServerConfig().getRpcRoadReserveTimeMs();
        if (rpcTimeout <= 0) {
            ResponseWriter.write(responseObserver, PullMessageResponse.newBuilder()
                .setCommon(ResponseBuilder.buildCommon(Code.DEADLINE_EXCEEDED, "request has been canceled due to timeout"))
                .build());
            return;
        }

        Partition partition = request.getPartition();
        String groupName = Converter.getResourceNameWithNamespace(request.getGroup());
        String topicName = Converter.getResourceNameWithNamespace(partition.getTopic());

        int queueId = partition.getId();
        int sysFlag = PullSysFlag.buildSysFlag(false, true, true, false, false);
        String expression = request.getFilterExpression().getExpression();
        String expressionType = Converter.buildExpressionType(request.getFilterExpression().getType());

        PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
        requestHeader.setConsumerGroup(groupName);
        requestHeader.setTopic(topicName);
        requestHeader.setQueueId(queueId);
        requestHeader.setQueueOffset(request.getOffset());
        requestHeader.setMaxMsgNums(request.getBatchSize());
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setCommitOffset(0L);
        requestHeader.setSuspendTimeoutMillis(rpcTimeout);
        requestHeader.setSubscription(expression);
        requestHeader.setSubVersion(0L);
        requestHeader.setExpressionType(expressionType);

        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();
        InvocationContext<PullMessageRequest, PullMessageResponse> context
            = new InvocationContext<>(request, (ServerCallStreamObserver<PullMessageResponse>) responseObserver);
        try {
            RemotingCommand responseCommand = controller.getPopMessageProcessor().processRequest(channelHandlerContext,
                command);
            if (null != responseCommand) {
                handlePullResponseCommand(responseCommand, context);
                channel.eraseInvocationContext(command.getOpaque());
            }
        } catch (RemotingCommandException e) {
            LOGGER.error("Pull message failed", e);
            channel.eraseInvocationContext(command.getOpaque());
            ResponseWriter.writeException(responseObserver, e);
        }
    }

    public void multiplexingCall(MultiplexingRequest request, StreamObserver<MultiplexingResponse> responseObserver) {
        String mid;
        switch (request.getTypeCase()) {
            case POLLING_REQUEST:
                GenericPollingRequest pollingRequest = request.getPollingRequest();
                String clientId = pollingRequest.getClientId();

                switch (pollingRequest.getGroupCase()) {
                    case PRODUCER_GROUP:
                        Resource producerGroup = pollingRequest.getProducerGroup();
                        String producerGroupName = Converter.getResourceNameWithNamespace(producerGroup);
                        this.clientChannelManager.add(producerGroupName, clientId, request, responseObserver);
                        break;
                    case CONSUMER_GROUP:
                        Resource consumerGroup = pollingRequest.getConsumerGroup();
                        String consumerGroupName = Converter.getResourceNameWithNamespace(consumerGroup);
                        this.clientChannelManager.add(consumerGroupName, clientId, request, responseObserver);
                        break;
                    default:
                        break;
                }
                return;
            case PRINT_THREAD_STACK_RESPONSE:
                mid = request.getPrintThreadStackResponse().getMid();
                break;
            case VERIFY_MESSAGE_CONSUMPTION_RESPONSE:
                mid = request.getVerifyMessageConsumptionResponse().getMid();
                break;
            default:
                return;
        }

        GrpcClientObserver clientObserver = this.clientChannelManager.getByMid(mid);
        if (clientObserver == null) {
            LOGGER.error("gRPC client channel is not present. mid: {}", mid);
            return;
        }

        clientObserver.result(request, responseObserver);
    }

    public void notifyClientTermination(NotifyClientTerminationRequest request,
        StreamObserver<NotifyClientTerminationResponse> responseObserver) {
        String clientId = request.getClientId();
        SimpleChannel channel = createChannel(request.getClientId());
        String language = StringUtils.defaultString(InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE));
        String version = StringUtils.defaultString(InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.CLIENT_VERSION));
        LanguageCode languageCode = LanguageCode.valueOf(language);
        int versionCode = Integer.parseInt(version);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            channel,
            clientId,
            languageCode,
            versionCode);

        if (request.hasProducerGroup()) {
            String groupName = Converter.getResourceNameWithNamespace(request.getProducerGroup());
            if (groupName != null) {
                controller.getProducerManager()
                    .unregisterProducer(groupName, clientChannelInfo);
            }
        }
        if (request.hasConsumerGroup()) {
            String groupName = Converter.getResourceNameWithNamespace(request.getConsumerGroup());
            if (groupName != null) {
                controller.getConsumerManager().unregisterConsumer(groupName, clientChannelInfo, false);
            }
        }
    }

    private SimpleChannel createChannel(final String clientId) {
        if (Strings.isNullOrEmpty(clientId)) {
            LOGGER.warn("ClientId is unexpected null or empty");
            return createChannel();
        }

        if (!clientIdChannelMap.containsKey(clientId)) {
            clientIdChannelMap.putIfAbsent(clientId, createChannel());
        }

        return clientIdChannelMap.get(clientId).updateLastAccessTime();
    }

    private SimpleChannel createReceiveMessageChannel(final String clientId) {
        if (Strings.isNullOrEmpty(clientId)) {
            LOGGER.warn("ClientId is unexpected null or empty");
            return createReceiveMessageChannel();
        }

        if (!clientIdChannelMap.containsKey(clientId)) {
            clientIdChannelMap.putIfAbsent(clientId, createReceiveMessageChannel());
        }

        return clientIdChannelMap.get(clientId).updateLastAccessTime();
    }

    private SimpleChannel createPullMessageChannel(final String clientId) {
        if (Strings.isNullOrEmpty(clientId)) {
            LOGGER.warn("ClientId is unexpected null or empty");
            return createPullMessageChannel();
        }

        if (!clientIdChannelMap.containsKey(clientId)) {
            clientIdChannelMap.putIfAbsent(clientId, createPullMessageChannel());
        }

        return clientIdChannelMap.get(clientId).updateLastAccessTime();
    }

    public void handlePullResponseCommand(RemotingCommand responseCommand,
        InvocationContext<PullMessageRequest, PullMessageResponse> context) {
        try {
            PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) responseCommand.readCustomHeader();
            PullMessageResponse.Builder builder = PullMessageResponse.newBuilder();
            if (responseCommand.getCode() == RemotingSysResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(responseCommand.getBody());
                List<MessageExt> msgFoundList = MessageDecoder.decodes(byteBuffer);
                for (MessageExt messageExt : msgFoundList) {
                    builder.addMessages(Converter.buildMessage(messageExt));
                }
            }
            PullMessageResponse response = builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()))
                .setMinOffset(responseHeader.getMinOffset())
                .setNextOffset(responseHeader.getNextBeginOffset())
                .setMaxOffset(responseHeader.getMaxOffset())
                .build();
            ResponseWriter.write(context.getStreamObserver(), response);
        } catch (Exception e) {
            ResponseWriter.writeException(context.getStreamObserver(), e);
        }
    }

    public void handlePopResponseCommand(RemotingCommand responseCommand,
        InvocationContext<ReceiveMessageRequest, ReceiveMessageResponse> context) {
        long currentTimeInMillis = System.currentTimeMillis();
        long popCosts = currentTimeInMillis - context.getTimestamp();
        try {
            Stopwatch stopWatch = Stopwatch.createStarted();
            ReceiveMessageResponse.Builder builder = ReceiveMessageResponse.newBuilder();
            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) responseCommand.readCustomHeader();
            builder.setCommon(ResponseBuilder.buildCommon(responseCommand.getCode(), responseCommand.getRemark()));
            builder.setInvisibleDuration(Durations.fromMillis(responseHeader.getInvisibleTime()))
                .setDeliveryTimestamp(Timestamps.fromMillis(responseHeader.getPopTime()));

            ReceiveMessageResponse response;
            if (responseCommand.getCode() == RemotingSysResponseCode.SUCCESS) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(responseCommand.getBody());
                List<MessageExt> msgFoundList = MessageDecoder.decodes(byteBuffer);

                Map<String, Long> startOffsetInfo;
                Map<String, List<Long>> msgOffsetInfo;
                Map<String, Integer> orderCountInfo;
                startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
                msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
                orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
                Map<String/*topicMark@queueId*/, List<Long>/*msg queueOffset*/> sortMap = new HashMap<>(16);
                String brokerName = controller.getBrokerConfig().getBrokerName();
                for (MessageExt messageExt : msgFoundList) {
                    String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                    if (!sortMap.containsKey(key)) {
                        sortMap.put(key, new ArrayList<>(4));
                    }
                    sortMap.get(key).add(messageExt.getQueueOffset());
                }
                Map<String, String> map = new HashMap<>(5);
                for (MessageExt messageExt : msgFoundList) {
                    if (startOffsetInfo == null) {
                        // we should set the check point info to extraInfo field , if the command is popMsg
                        // find pop ck offset
                        String key = messageExt.getTopic() + messageExt.getQueueId();
                        if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
                            String extraInfo = ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(),
                                responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                responseHeader.getReviveQid(), messageExt.getTopic(), brokerName,
                                messageExt.getQueueId());
                            map.put(key, extraInfo);
                        }
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                            map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
                    } else {
                        String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(),
                            messageExt.getQueueId());
                        int index = sortMap.get(key).indexOf(messageExt.getQueueOffset());
                        Long msgQueueOffset = msgOffsetInfo.get(key).get(index);
                        if (msgQueueOffset != messageExt.getQueueOffset()) {
                            LOGGER.warn("Queue offset[{}] of msg is strange, not equal to the stored in msg, {}",
                                msgQueueOffset, messageExt);
                        }
                        String extraInfo = ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(key),
                            responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                            responseHeader.getReviveQid(), messageExt.getTopic(),
                            brokerName, messageExt.getQueueId(), msgQueueOffset);
                        messageExt.setQueueOffset(msgQueueOffset);
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, extraInfo);
                        if (context.getRequest().getFifoFlag() && orderCountInfo != null) {
                            Integer count = orderCountInfo.get(key);
                            if (count != null && count > 0) {
                                messageExt.setReconsumeTimes(count);
                            }
                        }
                    }
                    messageExt.setBrokerName(controller.getBrokerConfig().getBrokerName());
                    messageExt.getProperties().computeIfAbsent(MessageConst.PROPERTY_FIRST_POP_TIME,
                        k -> String.valueOf(responseHeader.getPopTime()));
                }

                for (MessageExt messageExt : msgFoundList) {
                    long latency = currentTimeInMillis - messageExt.getStoreTimestamp();
                    messageLatency.countIn((int) (latency / 10));
                    builder.addMessages(Converter.buildMessage(messageExt));
                }
            }
            response = builder.build();
            long elapsed = stopWatch.stop().elapsed(TimeUnit.MILLISECONDS);
            LOGGER.debug("Translating remoting response to gRPC response costs {}ms. Duration request received: {}",
                elapsed, popCosts);
            ResponseWriter.write(context.getStreamObserver(), response);
        } catch (Exception e) {
            LOGGER.error("Unexpected exception raised when handle pop remoting command", e);
            ResponseWriter.writeException(context.getStreamObserver(), e);
        }
    }

    private String anonymousChannelId() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LOCAL_ADDRESS);
        return clientHost + "@" + localAddress;
    }

    private SimpleChannel createChannel() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LOCAL_ADDRESS);
        return new SimpleChannel(null, clientHost, localAddress);
    }

    private SimpleChannel createReceiveMessageChannel() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LOCAL_ADDRESS);
        return new ReceiveMessageChannel(null, clientHost, localAddress);
    }

    private SimpleChannel createPullMessageChannel() {
        final String clientHost = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.REMOTE_ADDRESS);
        final String localAddress = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LOCAL_ADDRESS);
        return new PullMessageChannel(null, clientHost, localAddress);
    }
}
