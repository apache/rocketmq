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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.transaction.TransactionHeartbeatRegisterService;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private final ChannelManager channelManager;
    private final ConsumerManager consumerManager = new ConsumerManager((event, group, args) -> {
    });
    private final ProducerManager producerManager;

    public ClientService(ConnectorManager connectorManager, ScheduledExecutorService scheduledExecutorService, ChannelManager channelManager) {
        super(connectorManager);
        scheduledExecutorService.scheduleWithFixedDelay(this::scanNotActiveChannel, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
        this.channelManager = channelManager;

        this.producerManager = new ProducerManager();
        this.producerManager.setProducerOfflineListener(connectorManager.getTransactionHeartbeatRegisterService()::onProducerGroupOffline);
    }

    public void heartbeat(Context ctx, HeartbeatRequest request, ChannelManager channelManager) {
        String language = InterceptorConstants.METADATA.get(Context.current()).get(InterceptorConstants.LANGUAGE);
        LanguageCode languageCode = LanguageCode.valueOf(language);
        String clientId = request.getClientId();

        if (request.hasProducerData()) {
            String producerGroup = Converter.getResourceNameWithNamespace(request.getProducerData().getGroup());
            GrpcClientChannel channel = GrpcClientChannel.create(channelManager, producerGroup, clientId);
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());
            producerManager.registerProducer(producerGroup, clientChannelInfo);
        }

        if (request.hasConsumerData()) {
            ConsumerData consumerData = request.getConsumerData();
            String consumerGroup = Converter.getResourceNameWithNamespace(consumerData.getGroup());
            GrpcClientChannel channel = GrpcClientChannel.create(channelManager, consumerGroup, clientId);
            ClientChannelInfo clientChannelInfo = new ClientChannelInfo(channel, clientId, languageCode, MQVersion.Version.V5_0_0.ordinal());

            consumerManager.registerConsumer(
                consumerGroup,
                clientChannelInfo,
                Converter.buildConsumeType(consumerData.getConsumeType()),
                Converter.buildMessageModel(consumerData.getConsumeModel()),
                Converter.buildConsumeFromWhere(consumerData.getConsumePolicy()),
                Converter.buildSubscriptionDataSet(consumerData.getSubscriptionsList()),
                false
            );
        }
    }

    public void unregister(Context ctx, NotifyClientTerminationRequest request, ChannelManager channelManager) {
        String clientId = request.getClientId();

        if (request.hasProducerGroup()) {
            String producerGroup = Converter.getResourceNameWithNamespace(request.getProducerGroup());
            GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, producerGroup, clientId);
            if (channel != null) {
                producerManager.doChannelCloseEvent(producerGroup, channel);
            }
        }

        if (request.hasConsumerGroup()) {
            String consumerGroup = Converter.getResourceNameWithNamespace(request.getConsumerGroup());
            GrpcClientChannel channel = GrpcClientChannel.removeChannel(channelManager, consumerGroup, clientId);
            if (channel != null) {
                consumerManager.doChannelCloseEvent(consumerGroup, channel);
            }
        }
    }

    public CompletableFuture<PollCommandResponse> pollCommand(Context ctx, PollCommandRequest request) {
        CompletableFuture<PollCommandResponse> future = new CompletableFuture<>();
        String clientId = request.getClientId();
        PollCommandResponse noopCommandResponse = PollCommandResponse.newBuilder().setNoopCommand(NoopCommand.newBuilder().build()).build();

        switch (request.getGroupCase()) {
            case PRODUCER_GROUP:
                Resource producerGroup = request.getProducerGroup();
                String producerGroupName = Converter.getResourceNameWithNamespace(producerGroup);
                GrpcClientChannel producerChannel = GrpcClientChannel.getChannel(this.channelManager, producerGroupName, clientId);
                if (producerChannel == null) {
                    future.complete(noopCommandResponse);
                } else {
                    producerChannel.addClientObserver(future);
                }
                break;
            case CONSUMER_GROUP:
                Resource consumerGroup = request.getConsumerGroup();
                String consumerGroupName = Converter.getResourceNameWithNamespace(consumerGroup);
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
}
