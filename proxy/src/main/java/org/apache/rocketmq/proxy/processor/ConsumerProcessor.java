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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;

public class ConsumerProcessor extends AbstractProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    public ConsumerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        super(messagingProcessor, serviceManager);
    }

    public CompletableFuture<PopResult> popMessage(
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
        long timeoutMillis
    ) {
        CompletableFuture<PopResult> future = new CompletableFuture<>();
        try {
            SelectableMessageQueue messageQueue = queueSelector.select(ctx, this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no readable queue");
            }

            if (maxMsgNums > ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST) {
                log.warn("change maxNums from {} to {} for pop request, with info: topic:{}, group:{}",
                    maxMsgNums, ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST, topic, consumerGroup);
                maxMsgNums = ProxyUtils.MAX_MSG_NUMS_FOR_POP_REQUEST;
            }

            PopMessageRequestHeader requestHeader = new PopMessageRequestHeader();
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setTopic(topic);
            requestHeader.setQueueId(messageQueue.getQueueId());
            requestHeader.setMaxMsgNums(maxMsgNums);
            requestHeader.setInvisibleTime(invisibleTime);
            requestHeader.setPollTime(pollTime);
            requestHeader.setInitMode(initMode);
            requestHeader.setExpType(subscriptionData.getExpressionType());
            requestHeader.setExp(subscriptionData.getSubString());
            requestHeader.setOrder(fifo);

            return this.serviceManager.getMessageService().popMessage(
                ctx,
                messageQueue,
                requestHeader,
                timeoutMillis)
                .thenApply(popResult -> {
                    if (PopStatus.FOUND.equals(popResult.getPopStatus()) &&
                        popResult.getMsgFoundList() != null &&
                        !popResult.getMsgFoundList().isEmpty() &&
                        popMessageResultFilter != null) {

                        List<MessageExt> messageExtList = new ArrayList<>();
                        for (MessageExt messageExt : popResult.getMsgFoundList()) {
                            try {
                                PopMessageResultFilter.FilterResult filterResult =
                                    popMessageResultFilter.filterMessage(ctx, consumerGroup, subscriptionData, messageExt);
                                switch (filterResult) {
                                    case NO_MATCH:
                                        this.messagingProcessor.ackMessage(
                                            ctx,
                                            ReceiptHandle.create(messageExt),
                                            messageExt.getMsgId(),
                                            consumerGroup,
                                            topic,
                                            MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                                        break;
                                    case TO_DLQ:
                                        this.messagingProcessor.forwardMessageToDeadLetterQueue(
                                            ctx,
                                            ReceiptHandle.create(messageExt),
                                            messageExt.getMsgId(),
                                            consumerGroup,
                                            topic,
                                            MessagingProcessor.DEFAULT_TIMEOUT_MILLS);
                                        break;
                                    case MATCH:
                                    default:
                                        messageExtList.add(messageExt);
                                        break;
                                }
                            } catch (Throwable t) {
                                log.error("process filterMessage failed. requestHeader:{}, msg:{}", requestHeader, messageExt, t);
                                messageExtList.add(messageExt);
                            }
                        }
                        popResult.setMsgFoundList(messageExtList);
                    }
                    return popResult;
                });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> ackMessage(
        ProxyContext ctx,
        ReceiptHandle handle,
        String messageId,
        String consumerGroup,
        String topic,
        long timeoutMillis
    ) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.checkReceiptHandle(handle);

            AckMessageRequestHeader ackMessageRequestHeader = new AckMessageRequestHeader();
            ackMessageRequestHeader.setConsumerGroup(consumerGroup);
            ackMessageRequestHeader.setTopic(handle.getRealTopic(topic, consumerGroup));
            ackMessageRequestHeader.setQueueId(handle.getQueueId());
            ackMessageRequestHeader.setExtraInfo(handle.getReceiptHandle());
            ackMessageRequestHeader.setOffset(handle.getOffset());

            return this.serviceManager.getMessageService().ackMessage(
                ctx,
                handle,
                messageId,
                ackMessageRequestHeader,
                timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long invisibleTime, long timeoutMillis) {
        CompletableFuture<AckResult> future = new CompletableFuture<>();
        try {
            this.checkReceiptHandle(handle);

            ChangeInvisibleTimeRequestHeader changeInvisibleTimeRequestHeader = new ChangeInvisibleTimeRequestHeader();
            changeInvisibleTimeRequestHeader.setConsumerGroup(groupName);
            changeInvisibleTimeRequestHeader.setTopic(handle.getRealTopic(topicName, groupName));
            changeInvisibleTimeRequestHeader.setQueueId(handle.getQueueId());
            changeInvisibleTimeRequestHeader.setExtraInfo(handle.getReceiptHandle());
            changeInvisibleTimeRequestHeader.setOffset(handle.getOffset());
            changeInvisibleTimeRequestHeader.setInvisibleTime(invisibleTime);

            return this.serviceManager.getMessageService().changeInvisibleTime(
                ctx,
                handle,
                messageId,
                changeInvisibleTimeRequestHeader,
                timeoutMillis);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }
}
