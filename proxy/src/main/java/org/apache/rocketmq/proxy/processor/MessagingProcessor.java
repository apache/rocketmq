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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerChangeListener;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.MessageReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

public interface MessagingProcessor extends StartAndShutdown {

    long DEFAULT_TIMEOUT_MILLS = Duration.ofSeconds(2).toMillis();

    SubscriptionGroupConfig getSubscriptionGroupConfig(
        ProxyContext ctx,
        String consumerGroupName
    );

    ProxyTopicRouteData getTopicRouteDataForProxy(
        ProxyContext ctx,
        List<Address> requestHostAndPortList,
        String topicName
    ) throws Exception;

    default CompletableFuture<List<SendResult>> sendMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String producerGroup,
        int sysFlag,
        List<Message> msg
    ) {
        return sendMessage(ctx, queueSelector, producerGroup, sysFlag, msg, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<List<SendResult>> sendMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String producerGroup,
        int sysFlag,
        List<Message> msg,
        long timeoutMillis
    );

    default CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName
    ) {
        return forwardMessageToDeadLetterQueue(ctx, handle, messageId, groupName, topicName, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long timeoutMillis
    );

    default CompletableFuture<Void> endTransaction(
        ProxyContext ctx,
        String transactionId,
        String messageId,
        String producerGroup,
        TransactionStatus transactionStatus,
        boolean fromTransactionCheck
    ) {
        return endTransaction(ctx, transactionId, messageId, producerGroup, transactionStatus, fromTransactionCheck, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<Void> endTransaction(
        ProxyContext ctx,
        String transactionId,
        String messageId,
        String producerGroup,
        TransactionStatus transactionStatus,
        boolean fromTransactionCheck,
        long timeoutMillis
    );

    CompletableFuture<PopResult> popMessage(
        ProxyContext ctx,
        QueueSelector queueSelector,
        String consumerGroup,
        String topic,
        int maxMsgNums,
        long invisibleTime,
        long pollTime,
        int initMode,
        SubscriptionData subscriptionData,
        boolean fifo,
        PopMessageResultFilter popMessageResultFilter,
        String attemptId,
        long timeoutMillis
    );

    default CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic
    ) {
        return ackMessage(ctx, handle, messageId, consumerGroup, topic, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic,
        long timeoutMillis
    );

    default CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic
    ) {
        return batchAckMessage(ctx, handleMessageList, consumerGroup, topic, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<List<BatchAckResult>> batchAckMessage(
        ProxyContext ctx,
        List<ReceiptHandleMessage> handleMessageList,
        String consumerGroup,
        String topic,
        long timeoutMillis
    );

    default CompletableFuture<AckResult> changeInvisibleTime(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long invisibleTime
    ) {
        return changeInvisibleTime(ctx, handle, messageId, groupName, topicName, invisibleTime, DEFAULT_TIMEOUT_MILLS);
    }

    CompletableFuture<AckResult> changeInvisibleTime(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String groupName,
        String topicName,
        long invisibleTime,
        long timeoutMillis
    );

    CompletableFuture<PullResult> pullMessage(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long queueOffset,
        int maxMsgNums,
        int sysFlag,
        long commitOffset,
        long suspendTimeoutMillis,
        SubscriptionData subscriptionData,
        long timeoutMillis
    );

    CompletableFuture<Void> updateConsumerOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long commitOffset,
        long timeoutMillis
    );

    CompletableFuture<Long> queryConsumerOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        String consumerGroup,
        long timeoutMillis
    );

    CompletableFuture<Set<MessageQueue>> lockBatchMQ(
        ProxyContext ctx,
        Set<MessageQueue> mqSet,
        String consumerGroup,
        String clientId,
        long timeoutMillis
    );

    CompletableFuture<Void> unlockBatchMQ(
        ProxyContext ctx,
        Set<MessageQueue> mqSet,
        String consumerGroup,
        String clientId,
        long timeoutMillis
    );

    CompletableFuture<Long> getMaxOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        long timeoutMillis
    );

    CompletableFuture<Long> getMinOffset(
        ProxyContext ctx,
        MessageQueue messageQueue,
        long timeoutMillis
    );

    CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);

    CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis);

    void registerProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    );

    void unRegisterProducer(
        ProxyContext ctx,
        String producerGroup,
        ClientChannelInfo clientChannelInfo
    );

    Channel findProducerChannel(
        ProxyContext ctx,
        String producerGroup,
        String clientId
    );

    void registerProducerListener(
        ProducerChangeListener producerChangeListener
    );

    void registerConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType,
        MessageModel messageModel,
        ConsumeFromWhere consumeFromWhere,
        Set<SubscriptionData> subList,
        boolean updateSubscription
    );

    ClientChannelInfo findConsumerChannel(
        ProxyContext ctx,
        String consumerGroup,
        Channel channel
    );

    void unRegisterConsumer(
        ProxyContext ctx,
        String consumerGroup,
        ClientChannelInfo clientChannelInfo
    );

    void registerConsumerListener(
        ConsumerIdsChangeListener consumerIdsChangeListener
    );

    void doChannelCloseEvent(String remoteAddr, Channel channel);

    ConsumerGroupInfo getConsumerGroupInfo(ProxyContext ctx, String consumerGroup);

    void addTransactionSubscription(
        ProxyContext ctx,
        String producerGroup,
        String topic
    );

    ProxyRelayService getProxyRelayService();

    MetadataService getMetadataService();

    void addReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, MessageReceiptHandle messageReceiptHandle);

    MessageReceiptHandle removeReceiptHandle(ProxyContext ctx, Channel channel, String group, String msgID, String receiptHandle);
}
