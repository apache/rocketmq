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
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import io.grpc.Context;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.grpc.adapter.channel.GrpcClientChannel;
import org.apache.rocketmq.proxy.grpc.common.Converter;
import org.apache.rocketmq.proxy.grpc.common.InterceptorConstants;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientService {

    private static final Logger log = LoggerFactory.getLogger(ClientService.class);

    private final ConsumerManager consumerManager = new ConsumerManager((event, group, args) -> {
    });
    private final ProducerManager producerManager = new ProducerManager();

    public ClientService(ScheduledExecutorService scheduledExecutorService) {
        scheduledExecutorService.scheduleWithFixedDelay(this::scanNotActiveChannel, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
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

    private void scanNotActiveChannel() {
        try {
            this.consumerManager.scanNotActiveChannel();
            this.producerManager.scanNotActiveChannel();
        } catch (Exception e) {
            log.error("error occurred when scan not active client channels.", e);
        }
    }
}
