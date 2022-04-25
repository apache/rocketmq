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

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.TelemetryCommand;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.broker.client.ProducerGroupEvent;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.common.TelemetryCommandManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.proxy.grpc.v2.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.proxy.grpc.v2.service.ClientSettingsService;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class ForwardClientService extends BaseService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected final ChannelManager channelManager;
    protected final GrpcClientManager grpcClientManager;
    protected final TelemetryCommandManager telemetryCommandManager;

    protected ConsumerManager consumerManager;
    protected ProducerManager producerManager;
    protected ClientSettingsService clientSettingsService;

    public ForwardClientService(
        ConnectorManager connectorManager,
        ScheduledExecutorService scheduledExecutorService,
        ChannelManager channelManager,
        GrpcClientManager grpcClientManager,
        TelemetryCommandManager telemetryCommandManager
    ) {
        super(connectorManager);
        scheduledExecutorService.scheduleWithFixedDelay(
            this::scanNotActiveChannel,
            Duration.ofSeconds(10).toMillis(),
            Duration.ofSeconds(10).toMillis(),
            TimeUnit.MILLISECONDS);
        this.channelManager = channelManager;
        this.grpcClientManager = grpcClientManager;
        this.telemetryCommandManager = telemetryCommandManager;
    }

    @Override
    public void start() throws Exception {
        this.clientSettingsService = new ClientSettingsService(this.channelManager, this.grpcClientManager, this.telemetryCommandManager);
        this.consumerManager = new ConsumerManager(new ConsumerIdsChangeListenerImpl());
        this.producerManager = new ProducerManager();
        this.producerManager.appendProducerChangeListener(new ProducerChangeListenerImpl());
    }

    protected class ConsumerIdsChangeListenerImpl implements ConsumerIdsChangeListener {

        @Override
        public void handle(ConsumerGroupEvent event, String group, Object... args) {
            if (event == ConsumerGroupEvent.CLIENT_UNREGISTER) {
                if (args == null || args.length < 1) {
                    return;
                }
                if (args[0] instanceof ClientChannelInfo) {
                    ClientChannelInfo clientChannelInfo = (ClientChannelInfo) args[0];
                    channelManager.onClientOffline(clientChannelInfo.getClientId());
                    grpcClientManager.removeClientSettings(clientChannelInfo.getClientId());
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
            switch (event) {
                case GROUP_UNREGISTER:
                    connectorManager.getTransactionHeartbeatRegisterService().onProducerGroupOffline(group);
                    break;
                case CLIENT_UNREGISTER:
                    channelManager.onClientOffline(clientChannelInfo.getClientId());
                    grpcClientManager.removeClientSettings(clientChannelInfo.getClientId());
                    break;
                default:
                    break;
            }
        }
    }

    public CompletableFuture<HeartbeatResponse> heartbeat(Context ctx, HeartbeatRequest request) {
        CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();

        try {
            String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
            String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
            LanguageCode languageCode = LanguageCode.valueOf(language);

            Settings clientSettings = grpcClientManager.getClientSettings(clientId);
            switch (clientSettings.getClientType()) {
                case PRODUCER: {
                    for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
                        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
                        GrpcClientChannel channel = GrpcClientChannel.create(channelManager, topicName, clientId, telemetryCommandManager);
                        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());
                        // use topic name as producer group
                        producerManager.registerProducer(topicName, clientChannelInfo);
                        connectorManager.getTransactionHeartbeatRegisterService().addProducerGroup(topicName, topicName);
                    }
                    break;
                }
                case PUSH_CONSUMER:
                case SIMPLE_CONSUMER: {
                    if (!request.hasGroup()) {
                        throw new ProxyException(Code.ILLEGAL_CONSUMER_GROUP, "group cannot be empty for consumer");
                    }
                    String consumerGroup = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
                    GrpcClientChannel channel = GrpcClientChannel.create(ctx, channelManager, consumerGroup, clientId, telemetryCommandManager);
                    ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());

                    consumerManager.registerConsumer(
                        consumerGroup,
                        clientChannelInfo,
                        GrpcConverter.buildConsumeType(clientSettings.getClientType()),
                        MessageModel.CLUSTERING,
                        ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
                        GrpcConverter.buildSubscriptionDataSet(clientSettings.getSubscription().getSubscriptionsList()),
                        false
                    );
                    break;
                }
                default: {
                    throw new IllegalArgumentException("ClientType not exist " + clientSettings.getClientType());
                }
            }
            future.complete(HeartbeatResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build());
            return future;
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<NotifyClientTerminationResponse> notifyClientTermination(Context ctx, NotifyClientTerminationRequest request) {
        CompletableFuture<NotifyClientTerminationResponse> future = new CompletableFuture<>();

        try {
            String clientId = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.CLIENT_ID);
            Settings clientSettings = grpcClientManager.getClientSettings(clientId);

            switch (clientSettings.getClientType()) {
                case PRODUCER:
                    for (Resource topic : clientSettings.getPublishing().getTopicsList()) {
                        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
                        // user topic name as producer group
                        GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, topicName, clientId);
                        if (channel != null) {
                            producerManager.doChannelCloseEvent(topicName, channel);
                        }
                    }
                    break;
                case PUSH_CONSUMER:
                case SIMPLE_CONSUMER:
                    if (!request.hasGroup()) {
                        throw new ProxyException(Code.ILLEGAL_CONSUMER_GROUP, "group cannot be empty for consumer");
                    }
                    String consumerGroup = GrpcConverter.wrapResourceWithNamespace(request.getGroup());
                    GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, consumerGroup, clientId);
                    if (channel != null) {
                        consumerManager.doChannelCloseEvent(consumerGroup, channel);
                    }
                    break;
                default:
                    break;
            }
            future.complete(NotifyClientTerminationResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
                .build());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public StreamObserver<TelemetryCommand> telemetry(Context ctx, StreamObserver<TelemetryCommand> responseObserver) {
        return new StreamObserver<TelemetryCommand>() {
            @Override
            public void onNext(TelemetryCommand request) {
                if (request.getCommandCase() == TelemetryCommand.CommandCase.SETTINGS) {
                    responseObserver.onNext(clientSettingsService.processClientSettings(ctx, request, responseObserver));
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

    private void scanNotActiveChannel() {
        try {
            this.consumerManager.scanNotActiveChannel();
            this.producerManager.scanNotActiveChannel();
        } catch (Exception e) {
            log.error("error occurred when scan not active client channels.", e);
        }
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }
}
