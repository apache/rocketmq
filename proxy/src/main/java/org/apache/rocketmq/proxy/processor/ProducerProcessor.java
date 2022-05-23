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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ProducerProcessor extends AbstractProcessor {

    private final ExecutorService executor;

    public ProducerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager, ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
    }

    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, List<MessageExt> messageExtList, long timeoutMillis) {
        CompletableFuture<List<SendResult>> future = new CompletableFuture<>();
        try {
            String topic = messageExtList.get(0).getTopic();
            SelectableMessageQueue messageQueue = queueSelector.select(ctx,
                this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no writable queue");
            }

            SendMessageRequestHeader requestHeader = buildSendMessageRequestHeader(messageExtList, producerGroup, messageQueue.getQueueId());

            future = this.serviceManager.getMessageService().sendMessage(
                ctx,
                messageQueue,
                messageExtList,
                requestHeader,
                timeoutMillis)
            .thenApplyAsync(sendResultList -> {
                for (SendResult sendResult : sendResultList) {
                    int tranType = MessageSysFlag.getTransactionValue(requestHeader.getSysFlag());
                    if (SendStatus.SEND_OK.equals(sendResult.getSendStatus()) &&
                        tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE &&
                        StringUtils.isNotBlank(sendResult.getTransactionId())) {
                        TransactionId transactionId = TransactionId.genByBrokerTransactionId(messageQueue.getBrokerName(), sendResult);
                        sendResult.setTransactionId(transactionId.getProxyTransactionId());
                    }
                }
                return sendResultList;
            }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected SendMessageRequestHeader buildSendMessageRequestHeader(List<MessageExt> messageExtList,
        String producerGroup, int queueId) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        MessageExt message = messageExtList.get(0);

        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setDefaultTopic("");
        requestHeader.setDefaultTopicQueueNums(0);
        requestHeader.setQueueId(queueId);
        requestHeader.setSysFlag(message.getSysFlag());
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setReconsumeTimes(0);
        if (messageExtList.size() > 1) {
            requestHeader.setBatch(true);
        }
        if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            String reconsumeTimes = MessageAccessor.getReconsumeTime(message);
            if (reconsumeTimes != null) {
                requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_RECONSUME_TIME);
            }

            String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(message);
            if (maxReconsumeTimes != null) {
                requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                MessageAccessor.clearProperty(message, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
            }
        }

        return requestHeader;
    }

    public CompletableFuture<RemotingCommand> forwardMessageToDeadLetterQueue(ProxyContext ctx, ReceiptHandle handle,
        String messageId, String groupName, String topicName, long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            if (handle.getCommitLogOffset() < 0) {
                throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "commit log offset is empty");
            }

            ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
            consumerSendMsgBackRequestHeader.setOffset(handle.getCommitLogOffset());
            consumerSendMsgBackRequestHeader.setGroup(groupName);
            consumerSendMsgBackRequestHeader.setDelayLevel(-1);
            consumerSendMsgBackRequestHeader.setOriginMsgId(messageId);
            consumerSendMsgBackRequestHeader.setOriginTopic(handle.getRealTopic(topicName, groupName));
            consumerSendMsgBackRequestHeader.setMaxReconsumeTimes(0);

            future = this.serviceManager.getMessageService().sendMessageBack(
                ctx,
                handle,
                messageId,
                consumerSendMsgBackRequestHeader,
                timeoutMillis
            ).whenCompleteAsync((remotingCommand, t) -> {
                if (t == null && remotingCommand.getCode() == ResponseCode.SUCCESS) {
                    this.messagingProcessor.ackMessage(ctx, handle, messageId,
                        groupName, topicName, timeoutMillis);
                }
            }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

}
