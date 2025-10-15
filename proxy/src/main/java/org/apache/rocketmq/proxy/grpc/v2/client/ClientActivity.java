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
package org.apache.rocketmq.proxy.grpc.v2.client;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.SubscriptionEntry;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageResult;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.channel.ChannelHelper;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.ContextStreamObserver;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.CMResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.protocol.filter.FilterAPI;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class ClientActivity extends AbstractMessingActivity {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public ClientActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager,
        GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
        this.init();
    }

    protected void init() {
        this.messagingProcessor.registerConsumerListener(new ConsumerIdsChangeListenerImpl());
        this.messagingProcessor.registerProducerListener(new ProducerChangeListenerImpl());
    }

    public CompletableFuture<HeartbeatResponse> heartbeat(ProxyContext ctx, HeartbeatRequest request) {
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();

        try {
            Settings clientSettings = grpcClientSettingsManager.getClientSettings(ctx);
            if (clientSettings == null) {
                future.complete(HeartbeatResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.UNRECOGNIZED_CLIENT_TYPE, "cannot find client settings for this client"))
                    .build());
                return future;
            }
            switch (clientSettings.getClientType()) {
                case PRODUCER: {
                    for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
                        String topicName = topic.getName();
                        this.registerProducer(ctx, topicName);
                    }
                    break;
                }
                case PUSH_CONSUMER:
                case SIMPLE_CONSUMER: {
                    validateConsumerGroup(request.getGroup());
                    String consumerGroup = request.getGroup().getName();
                    this.registerConsumer(ctx, consumerGroup, clientSettings.getClientType(), clientSettings.getSubscription().getSubscriptionsList(), false);
                    break;
                }
                default: {
                    future.complete(HeartbeatResponse.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.UNRECOGNIZED_CLIENT_TYPE, clientSettings.getClientType().name()))
                        .build());
                    return future;
                }
            }
            future.complete(HeartbeatResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build());
            return future;
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(ProxyContext ctx,
        NotifyClientTerminationRequest request) {
        CompletableFuture<NotifyClientTerminationResponse> future = new CompletableFuture<>();

        try {
            String clientId = ctx.getClientID();
            LanguageCode languageCode = LanguageCode.valueOf(ctx.getLanguage());
            Settings clientSettings = grpcClientSettingsManager.removeAndGetClientSettings(ctx);
            if (clientSettings == null) {
                future.complete(NotifyClientTerminationResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.UNRECOGNIZED_CLIENT_TYPE, "cannot find client settings for this client"))
                    .build());
                return future;
            }

            switch (clientSettings.getClientType()) {
                case PRODUCER:
                    for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
                        String topicName = topic.getName();
                        GrpcClientChannel channel = this.grpcChannelManager.removeChannel(clientId);
                        if (channel != null) {
                            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());
                            this.messagingProcessor.unRegisterProducer(ctx, topicName, clientChannelInfo);
                        }
                    }
                    break;
                case PUSH_CONSUMER:
                case SIMPLE_CONSUMER:
                    validateConsumerGroup(request.getGroup());
                    String consumerGroup = request.getGroup().getName();
                    GrpcClientChannel channel = this.grpcChannelManager.removeChannel(clientId);
                    if (channel != null) {
                        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());
                        this.messagingProcessor.unRegisterConsumer(ctx, consumerGroup, clientChannelInfo);
                    }
                    break;
                default:
                    future.complete(NotifyClientTerminationResponse.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.UNRECOGNIZED_CLIENT_TYPE, clientSettings.getClientType().name()))
                        .build());
                    return future;
            }
            future.complete(NotifyClientTerminationResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public ContextStreamObserver<TelemetryCommand> telemetry(StreamObserver<TelemetryCommand> responseObserver) {
        return new ContextStreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(ProxyContext ctx, TelemetryCommand request) {
                try {
                    switch (request.getCommandCase()) {
                        case SETTINGS: {
                            processAndWriteClientSettings(ctx, request, responseObserver);
                            break;
                        }
                        case THREAD_STACK_TRACE: {
                            reportThreadStackTrace(ctx, request.getStatus(), request.getThreadStackTrace());
                            break;
                        }
                        case VERIFY_MESSAGE_RESULT: {
                            reportVerifyMessageResult(ctx, request.getStatus(), request.getVerifyMessageResult());
                            break;
                        }
                    }
                } catch (Throwable t) {
                    processTelemetryException(request, t, responseObserver);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("telemetry on error", t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    protected void processTelemetryException(TelemetryCommand request, Throwable t,
        StreamObserver<TelemetryCommand> responseObserver) {
        StatusRuntimeException exception = io.grpc.Status.INTERNAL
            .withDescription("process client telemetryCommand failed. " + t.getMessage())
            .withCause(t)
            .asRuntimeException();
        if (t instanceof GrpcProxyException) {
            GrpcProxyException proxyException = (GrpcProxyException) t;
            if (proxyException.getCode().getNumber() < Code.INTERNAL_ERROR_VALUE &&
                proxyException.getCode().getNumber() >= Code.BAD_REQUEST_VALUE) {
                exception = io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("process client telemetryCommand failed. " + t.getMessage())
                    .withCause(t)
                    .asRuntimeException();
            }
        }
        if (exception.getStatus().getCode().equals(io.grpc.Status.Code.INTERNAL)) {
            log.warn("process client telemetryCommand failed. request:{}", request, t);
        }
        responseObserver.onError(exception);
    }

    protected void processAndWriteClientSettings(ProxyContext ctx, TelemetryCommand request,
        StreamObserver<TelemetryCommand> responseObserver) {
        GrpcClientChannel grpcClientChannel = null;
        Settings settings = request.getSettings();
        switch (settings.getPubSubCase()) {
            case PUBLISHING:
                for (Resource topic : settings.getPublishing().getTopicsList()) {
                    validateTopic(topic);
                    String topicName = topic.getName();
                    grpcClientChannel = registerProducer(ctx, topicName);
                    grpcClientChannel.setClientObserver(responseObserver);
                }
                break;
            case SUBSCRIPTION:
                validateConsumerGroup(settings.getSubscription().getGroup());
                String groupName = settings.getSubscription().getGroup().getName();
                grpcClientChannel = registerConsumer(ctx, groupName, settings.getClientType(), settings.getSubscription().getSubscriptionsList(), true);
                grpcClientChannel.setClientObserver(responseObserver);
                break;
            default:
                break;
        }
        if (Settings.PubSubCase.PUBSUB_NOT_SET.equals(settings.getPubSubCase())) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                .withDescription("there is no publishing or subscription data in settings")
                .asRuntimeException());
            return;
        }
        TelemetryCommand command = processClientSettings(ctx, request);
        if (grpcClientChannel != null) {
            grpcClientChannel.writeTelemetryCommand(command);
        } else {
            responseObserver.onNext(command);
        }
    }

    protected TelemetryCommand processClientSettings(ProxyContext ctx, TelemetryCommand request) {
        String clientId = ctx.getClientID();
        grpcClientSettingsManager.updateClientSettings(ctx, clientId, request.getSettings());
        Settings settings = grpcClientSettingsManager.getClientSettings(ctx);
        return TelemetryCommand.newBuilder()
            .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
            .setSettings(settings)
            .build();
    }

    protected GrpcClientChannel registerProducer(ProxyContext ctx, String topicName) {
        String clientId = ctx.getClientID();
        LanguageCode languageCode = LanguageCode.valueOf(ctx.getLanguage());

        GrpcClientChannel channel = this.grpcChannelManager.createChannel(ctx, clientId);
        // use topic name as producer group
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, parseClientVersion(ctx.getClientVersion()));
        this.messagingProcessor.registerProducer(ctx, topicName, clientChannelInfo);
        TopicMessageType topicMessageType = this.messagingProcessor.getMetadataService().getTopicMessageType(ctx, topicName);
        if (TopicMessageType.TRANSACTION.equals(topicMessageType)) {
            this.messagingProcessor.addTransactionSubscription(ctx, topicName, topicName);
        }
        return channel;
    }

    protected GrpcClientChannel registerConsumer(ProxyContext ctx, String consumerGroup, ClientType clientType,
        List<SubscriptionEntry> subscriptionEntryList, boolean updateSubscription) {
        String clientId = ctx.getClientID();
        LanguageCode languageCode = LanguageCode.valueOf(ctx.getLanguage());

        GrpcClientChannel channel = this.grpcChannelManager.createChannel(ctx, clientId);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, parseClientVersion(ctx.getClientVersion()));

        this.messagingProcessor.registerConsumer(
            ctx,
            consumerGroup,
            clientChannelInfo,
            this.buildConsumeType(clientType),
            MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            this.buildSubscriptionDataSet(subscriptionEntryList),
            updateSubscription
        );
        return channel;
    }

    private int parseClientVersion(String clientVersionStr) {
        int clientVersion = MQVersion.CURRENT_VERSION;
        if (!StringUtils.isEmpty(clientVersionStr)) {
            try {
                String tmp = StringUtils.upperCase(clientVersionStr);
                clientVersion = MQVersion.Version.valueOf(tmp).ordinal();
            } catch (Exception ignored) {
            }
        }
        return clientVersion;
    }

    protected void reportThreadStackTrace(ProxyContext ctx, Status status, ThreadStackTrace request) {
        String nonce = request.getNonce();
        String threadStack = request.getThreadStackTrace();
        CompletableFuture<ProxyRelayResult<ConsumerRunningInfo>> responseFuture = this.grpcChannelManager.getAndRemoveResponseFuture(nonce);
        if (responseFuture != null) {
            try {
                if (status.getCode().equals(Code.OK)) {
                    ConsumerRunningInfo runningInfo = new ConsumerRunningInfo();
                    runningInfo.setJstack(threadStack);
                    responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", runningInfo));
                } else if (status.getCode().equals(Code.VERIFY_FIFO_MESSAGE_UNSUPPORTED)) {
                    responseFuture.complete(new ProxyRelayResult<>(ResponseCode.NO_PERMISSION, "forbidden to verify message", null));
                } else {
                    responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SYSTEM_ERROR, "verify message failed", null));
                }
            } catch (Throwable t) {
                responseFuture.completeExceptionally(t);
            }
        }
    }

    protected void reportVerifyMessageResult(ProxyContext ctx, Status status, VerifyMessageResult request) {
        String nonce = request.getNonce();
        CompletableFuture<ProxyRelayResult<ConsumeMessageDirectlyResult>> responseFuture = this.grpcChannelManager.getAndRemoveResponseFuture(nonce);
        if (responseFuture != null) {
            try {
                ConsumeMessageDirectlyResult result = this.buildConsumeMessageDirectlyResult(status, request);
                responseFuture.complete(new ProxyRelayResult<>(ResponseCode.SUCCESS, "", result));
            } catch (Throwable t) {
                responseFuture.completeExceptionally(t);
            }
        }
    }

    protected ConsumeMessageDirectlyResult buildConsumeMessageDirectlyResult(Status status,
        VerifyMessageResult request) {
        ConsumeMessageDirectlyResult consumeMessageDirectlyResult = new ConsumeMessageDirectlyResult();
        switch (status.getCode().getNumber()) {
            case Code.OK_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_SUCCESS);
                break;
            }
            case Code.FAILED_TO_CONSUME_MESSAGE_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_LATER);
                break;
            }
            case Code.MESSAGE_CORRUPTED_VALUE: {
                consumeMessageDirectlyResult.setConsumeResult(CMResult.CR_RETURN_NULL);
                break;
            }
        }
        consumeMessageDirectlyResult.setRemark("from gRPC client");
        return consumeMessageDirectlyResult;
    }

    protected ConsumeType buildConsumeType(ClientType clientType) {
        switch (clientType) {
            case SIMPLE_CONSUMER:
                return ConsumeType.CONSUME_ACTIVELY;
            case PUSH_CONSUMER:
                return ConsumeType.CONSUME_PASSIVELY;
            default:
                throw new IllegalArgumentException("Client type is not consumer, type: " + clientType);
        }
    }

    protected Set<SubscriptionData> buildSubscriptionDataSet(List<SubscriptionEntry> subscriptionEntryList) {
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        for (SubscriptionEntry sub : subscriptionEntryList) {
            String topicName = sub.getTopic().getName();
            FilterExpression filterExpression = sub.getExpression();
            subscriptionDataSet.add(buildSubscriptionData(topicName, filterExpression));
        }
        return subscriptionDataSet;
    }

    protected SubscriptionData buildSubscriptionData(String topicName, FilterExpression filterExpression) {
        String expression = filterExpression.getExpression();
        String expressionType = GrpcConverter.getInstance().buildExpressionType(filterExpression.getType());
        try {
            return FilterAPI.build(topicName, expression, expressionType);
        } catch (Exception e) {
            throw new GrpcProxyException(Code.ILLEGAL_FILTER_EXPRESSION, "expression format is not correct", e);
        }
    }

    protected class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
            switch (event) {
                case CLIENT_UNREGISTER:
                    processClientUnregister(group, args);
                    break;
                case REGISTER:
                    processClientRegister(group, args);
                    break;
                default:
                    break;
            }
        }

        protected void processClientUnregister(String group, Object... args) {
            if (args == null || args.length < 1) {
                return;
            }
            if (args[0] instanceof ClientChannelInfo) {
                ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                if (ChannelHelper.isRemote(clientChannelInfo.getChannel())) {
                    return;
                }
                GrpcClientChannel removedChannel = grpcChannelManager.removeChannel(clientChannelInfo.getClientId());
                log.info("remove grpc channel when client unregister. group:{}, clientChannelInfo:{}, removed:{}",
                    group, clientChannelInfo, removedChannel != null);
            }
        }

        protected void processClientRegister(String group, Object... args) {
            if (args == null || args.length < 2) {
                return;
            }
            if (args[1] instanceof ClientChannelInfo) {
                ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[1];
                Channel channel = clientChannelInfo.getChannel();
                if (ChannelHelper.isRemote(channel)) {
                    // save settings from channel sync from other proxy
                    Settings settings = GrpcClientChannel.parseChannelExtendAttribute(channel);
                    log.debug("save client settings sync from other proxy. group:{}, channelInfo:{}, settings:{}", group, clientChannelInfo, settings);
                    if (settings == null) {
                        return;
                    }
                    grpcClientSettingsManager.updateClientSettings(
                        ProxyContext.createForInner(this.getClass()),
                        clientChannelInfo.getClientId(),
                        settings
                    );
                }
            }
        }

        @Override
        public void shutdown() {

        }
    }

    protected class ProducerChangeListenerImpl implements ProducerChangeListener {

        @Override
        public void handle(ProducerGroupEvent event, String group, ClientChannelInfo clientChannelInfo) {
            if (event == ProducerGroupEvent.CLIENT_UNREGISTER) {
                grpcChannelManager.removeChannel(clientChannelInfo.getClientId());
                grpcClientSettingsManager.removeAndGetRawClientSettings(clientChannelInfo.getClientId());
            }
        }
    }
}
