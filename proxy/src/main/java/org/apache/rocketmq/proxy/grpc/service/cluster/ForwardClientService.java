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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import apache.rocketmq.v1.ConsumerData;
import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NoopCommand;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PollCommandRequest;
import apache.rocketmq.v1.PollCommandResponse;
import apache.rocketmq.v1.Resource;
import io.grpc.Context;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.grpc.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.adapter.PollResponseManager;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForwardClientService extends BaseService {
    private static final Logger log = LoggerFactory.getLogger(ForwardClientService.class);

    private final ChannelManager channelManager;
    private final ConsumerManager consumerManager;
    private final ProducerManager producerManager;
    private final PollResponseManager pollCommandResponseManager;

    public ForwardClientService(
        ConnectorManager connectorManager,
        ScheduledExecutorService scheduledExecutorService,
        ChannelManager channelManager,
        PollResponseManager pollCommandResponseManager
    ) {
        super(connectorManager);
        scheduledExecutorService.scheduleWithFixedDelay(
            this::scanNotActiveChannel,
            Duration.ofSeconds(10).toMillis(),
            Duration.ofSeconds(10).toMillis(),
            TimeUnit.MILLISECONDS);
        this.channelManager = channelManager;
        this.pollCommandResponseManager = pollCommandResponseManager;

        this.consumerManager = new ConsumerManager(new ConsumerIdsChangeListener() {
            @Override public void handle(ConsumerGroupEvent event, String group, Object... args) {
            }

            @Override public void shutdown() {
            }
        });
        this.producerManager = new ProducerManager();
        this.producerManager.setProducerOfflineListener(connectorManager.getTransactionHeartbeatRegisterService()::onProducerGroupOffline);
    }

    public void heartbeat(Context ctx, HeartbeatRequest request) {
        String language = InterceptorConstants.METADATA.get(ctx).get(InterceptorConstants.LANGUAGE);
        LanguageCode languageCode = LanguageCode.valueOf(language);
        String clientId = request.getClientId();

        if (request.hasProducerData()) {
            String producerGroup = GrpcConverter.wrapResourceWithNamespace(request.getProducerData().getGroup());
            GrpcClientChannel channel = GrpcClientChannel.create(ctx, channelManager, producerGroup, clientId, pollCommandResponseManager);
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());
            producerManager.registerProducer(producerGroup, clientChannelInfo);
        }

        if (request.hasConsumerData()) {
            ConsumerData consumerData = request.getConsumerData();
            String consumerGroup = GrpcConverter.wrapResourceWithNamespace(consumerData.getGroup());
            GrpcClientChannel channel = GrpcClientChannel.create(ctx, channelManager, consumerGroup, clientId, pollCommandResponseManager);
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());

            consumerManager.registerConsumer(
                consumerGroup,
                clientChannelInfo,
                GrpcConverter.buildConsumeType(consumerData.getConsumeType()),
                GrpcConverter.buildMessageModel(consumerData.getConsumeModel()),
                GrpcConverter.buildConsumeFromWhere(consumerData.getConsumePolicy()),
                GrpcConverter.buildSubscriptionDataSet(consumerData.getSubscriptionsList()),
                false
            );
        }
    }

    public void unregister(Context ctx, NotifyClientTerminationRequest request) {
        String clientId = request.getClientId();

        if (request.hasProducerGroup()) {
            String producerGroup = GrpcConverter.wrapResourceWithNamespace(request.getProducerGroup());
            GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, producerGroup, clientId);
            if (channel != null) {
                producerManager.doChannelCloseEvent(producerGroup, channel);
            }
        }

        if (request.hasConsumerGroup()) {
            String consumerGroup = GrpcConverter.wrapResourceWithNamespace(request.getConsumerGroup());
            GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, consumerGroup, clientId);
            if (channel != null) {
                consumerManager.doChannelCloseEvent(consumerGroup, channel);
            }
        }
    }

    public CompletableFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        CompletableFuture<PollCommandResponse> future = new CompletableFuture<>();
        PollCommandResponse noopCommandResponse = PollCommandResponse.newBuilder().setNoopCommand(
            NoopCommand.newBuilder().build()
        ).build();

        String clientId = request.getClientId();
        switch (request.getGroupCase()) {
            case PRODUCER_GROUP:
                Resource producerGroup = request.getProducerGroup();
                String producerGroupName = GrpcConverter.wrapResourceWithNamespace(producerGroup);
                GrpcClientChannel producerChannel = GrpcClientChannel.getChannel(this.channelManager, producerGroupName, clientId);
                if (producerChannel == null) {
                    future.complete(noopCommandResponse);
                } else {
                    producerChannel.addClientObserver(future);
                }
                break;
            case CONSUMER_GROUP:
                Resource consumerGroup = request.getConsumerGroup();
                String consumerGroupName = GrpcConverter.wrapResourceWithNamespace(consumerGroup);
                GrpcClientChannel consumerChannel = GrpcClientChannel.getChannel(this.channelManager, consumerGroupName, clientId);
                if (consumerChannel == null) {
                    future.complete(noopCommandResponse);
                } else {
                    consumerChannel.addClientObserver(future);
                }
                break;
            default:
                break;
        }
        return future;
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
