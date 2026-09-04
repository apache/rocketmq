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

import apache.rocketmq.v2.Code;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.Channel;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.lite.LiteSubscriptionAction;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

@SuppressWarnings("UnstableApiUsage")
public class ClientProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final RateLimiter syncLiteSubscriptionRateLimiter;

    public ClientProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);

        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        this.syncLiteSubscriptionRateLimiter = RateLimiter.create(proxyConfig.getMaxSyncLiteSubscriptionRate());
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
        validateLiteMode(ctx, consumerGroup, messageModel);
        if (MessageModel.LITE_SELECTIVE == messageModel) {
            validateLiteSubTopic(ctx, consumerGroup, subList);
        }
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

    public CompletableFuture<Void> syncLiteSubscription(ProxyContext ctx,
        LiteSubscriptionDTO liteSubscriptionDTO, long timeoutMillis
    ) {
        try {
            validateLiteBindTopic(ctx, liteSubscriptionDTO.getGroup(), liteSubscriptionDTO.getTopic());
            if (CollectionUtils.isNotEmpty(liteSubscriptionDTO.getLiteTopicSet())) {
                validateLiteSubscriptionQuota(ctx, liteSubscriptionDTO.getGroup(), liteSubscriptionDTO.getLiteTopicSet().size());
            }

            if (LiteSubscriptionAction.PARTIAL_ADD == liteSubscriptionDTO.getAction()) {
                if (!syncLiteSubscriptionRateLimiter.tryAcquire()) {
                    String msg = String.format("Too many syncLiteSubscription requests, topic=%s, group=%s, clientId=%s",
                        liteSubscriptionDTO.getTopic(), liteSubscriptionDTO.getGroup(), ctx.getClientID());
                    log.warn(msg);
                    throw new GrpcProxyException(Code.TOO_MANY_REQUESTS, msg);
                }
            }

            return this.serviceManager
                .getLiteSubscriptionService()
                .syncLiteSubscription(ctx, liteSubscriptionDTO, timeoutMillis);
        } catch (Throwable t) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(t);
            return future;
        }
    }

    public ClientChannelInfo findConsumerChannel(
        ProxyContext ctx,
        String consumerGroup,
        Channel channel
    ) {
        return this.serviceManager.getConsumerManager().findChannel(consumerGroup, channel);
    }

    public void unRegisterConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo
    ) {
        this.serviceManager.getConsumerManager().unregisterConsumer(consumerGroup, clientChannelInfo, false);
    }

    public void doChannelCloseEvent(String remoteAddr, Channel channel) {
        this.serviceManager.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.serviceManager.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    public void registerConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        this.serviceManager.getConsumerManager().appendConsumerIdsChangeListener(listener);
    }

    public ConsumerGroupInfo getConsumerGroupInfo(ProxyContext ctx, String consumerGroup) {
        return this.serviceManager.getConsumerManager().getConsumerGroupInfo(consumerGroup);
    }

    /**
     * Validates the message model for a given consumer group.
     * Ensures that regular groups do not use LITE mode and LITE groups use LITE mode.
     *
     * @param ctx          the proxy context
     * @param group        the consumer group name
     * @param messageModel the message model to validate
     */
    protected void validateLiteMode(ProxyContext ctx, String group, MessageModel messageModel) {
        String bindTopic = getGroupOrException(ctx, group).getLiteBindTopic();
        if (StringUtils.isEmpty(bindTopic)) {
            // regular group
            if (MessageModel.LITE_SELECTIVE == messageModel) {
                throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP,
                    "regular group cannot use LITE mode: " + group);
            }
        } else {
            // lite group
            if (MessageModel.LITE_SELECTIVE != messageModel) {
                throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP,
                    "lite group must use LITE mode: " + group);
            }
        }
    }

    protected void validateLiteSubTopic(ProxyContext ctx, String group, Set<SubscriptionData> subList) {
        if (CollectionUtils.isEmpty(subList)) {
            return;
        }
        // check bindTopic for sub list
        validateLiteBindTopic(ctx, group, subList.iterator().next().getTopic());
    }

    protected void validateLiteBindTopic(ProxyContext ctx, String group, String bindTopic) {
        String expectedBindTopic = getGroupOrException(ctx, group).getLiteBindTopic();
        if (!Objects.equals(expectedBindTopic, bindTopic)) {
            throw new GrpcProxyException(Code.ILLEGAL_TOPIC,
                String.format("lite group %s is expected to bind topic %s, but actual is %s",
                    group, expectedBindTopic, bindTopic));
        }
    }

    protected void validateLiteSubscriptionQuota(ProxyContext ctx, String group, int actual) {
        int quota = getGroupOrException(ctx, group).getLiteSubClientQuota();
        int quotaBuffer = 300;
        if (actual > quota + quotaBuffer) {
            throw new GrpcProxyException(Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED,
                "lite subscription quota exceeded: " + quota);
        }
    }

    protected SubscriptionGroupConfig getGroupOrException(ProxyContext ctx, String group) {
        SubscriptionGroupConfig groupConfig = this.messagingProcessor.getSubscriptionGroupConfig(ctx, group);
        if (groupConfig == null) {
            throw new GrpcProxyException(Code.ILLEGAL_CONSUMER_GROUP, "group not found: " + group);
        }
        return groupConfig;
    }
}
