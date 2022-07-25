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
package org.apache.rocketmq.proxy.processor;

import io.netty.channel.Channel;
import java.util.Set;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.ServiceManager;

public class ClientProcessor extends AbstractProcessor {

    public ClientProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
    }

    public void registerProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    ) {
        this.serviceManager.getProducerManager().registerProducer(producerGroup, clientChannelInfo);
    }

    public void unRegisterProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    ) {
        this.serviceManager.getProducerManager().unregisterProducer(producerGroup, clientChannelInfo);
    }

    public Channel findProducerChannel(
        ProxyContext ctx,
        String producerGroup,
        String clientId
    ) {
        return this.serviceManager.getProducerManager().findChannel(clientId);
    }

    public void registerProducerChangeListener(ProducerChangeListener listener) {
        this.serviceManager.getProducerManager().appendProducerChangeListener(listener);
    }

    public void registerConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType,
        MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList,
        boolean updateSubscription
    ) {
        this.serviceManager.getConsumerManager().registerConsumer(
            consumerGroup,
            clientChannelInfo,
            consumeType,
            messageModel,
            consumeFromWhere,
            subList,
            false,
            updateSubscription);
    }

    public ClientChannelInfo findConsumerChannel(
        ProxyContext ctx,
        String consumerGroup,
        String clientId
    ) {
        return this.serviceManager.getConsumerManager().findChannel(consumerGroup, clientId);
    }

    public void unRegisterConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo
    ) {
        this.serviceManager.getConsumerManager().unregisterConsumer(consumerGroup, clientChannelInfo, false);
    }

    public void registerConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        this.serviceManager.getConsumerManager().appendConsumerIdsChangeListener(listener);
    }

    public ConsumerGroupInfo getConsumerGroupInfo(String consumerGroup) {
        return this.serviceManager.getConsumerManager().getConsumerGroupInfo(consumerGroup);
    }
}
