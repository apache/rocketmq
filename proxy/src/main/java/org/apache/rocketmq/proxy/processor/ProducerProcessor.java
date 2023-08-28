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
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.common.utils.FutureUtils;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.validator.DefaultTopicMessageTypeValidator;
import org.apache.rocketmq.proxy.processor.validator.TopicMessageTypeValidator;
import org.apache.rocketmq.proxy.service.ServiceManager;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

public class ProducerProcessor extends AbstractProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ExecutorService executor;
    private final TopicMessageTypeValidator topicMessageTypeValidator;

    public ProducerProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager, ExecutorService executor) {
        super(messagingProcessor, serviceManager);
        this.executor = executor;
        this.topicMessageTypeValidator = new DefaultTopicMessageTypeValidator();
    }

    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, QueueSelector queueSelector,
        String producerGroup, int sysFlag, List<Message> messageList, long timeoutMillis) {
        CompletableFuture<List<SendResult>> future = new CompletableFuture<>();
        try {
            Message message = messageList.get(0);
            String topic = message.getTopic();
            if (ConfigurationManager.getProxyConfig().isEnableTopicMessageTypeCheck()) {
                if (topicMessageTypeValidator != null) {
                    // Do not check retry or dlq topic
                    if (!NamespaceUtil.isRetryTopic(topic) && !NamespaceUtil.isDLQTopic(topic)) {
                        TopicMessageType topicMessageType = serviceManager.getMetadataService().getTopicMessageType(ctx, topic);
                        TopicMessageType messageType = TopicMessageType.parseFromMessageProperty(message.getProperties());
                        topicMessageTypeValidator.validate(topicMessageType, messageType);
                    }
                }
            }
            AddressableMessageQueue messageQueue = queueSelector.select(ctx,
                this.serviceManager.getTopicRouteService().getCurrentMessageQueueView(ctx, topic));
            if (messageQueue == null) {
                throw new ProxyException(ProxyExceptionCode.FORBIDDEN, "no writable queue");
            }

            for (Message msg : messageList) {
                MessageClientIDSetter.setUniqID(msg);
            }
            SendMessageRequestHeader requestHeader = buildSendMessageRequestHeader(messageList, producerGroup, sysFlag, messageQueue.getQueueId());

            future = this.serviceManager.getMessageService().sendMessage(
                ctx,
                messageQueue,
                messageList,
                requestHeader,
                timeoutMillis)
                .thenApplyAsync(sendResultList -> {
                    for (SendResult sendResult : sendResultList) {
                        int tranType = MessageSysFlag.getTransactionValue(requestHeader.getSysFlag());
                        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus()) &&
                            tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE &&
                            StringUtils.isNotBlank(sendResult.getTransactionId())) {
                            fillTransactionData(ctx, producerGroup, messageQueue, sendResult, messageList);
                        }
                    }
                    return sendResultList;
                }, this.executor);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return FutureUtils.addExecutor(future, this.executor);
    }

    protected void fillTransactionData(ProxyContext ctx, String producerGroup, AddressableMessageQueue messageQueue, SendResult sendResult, List<Message> messageList) {
        try {
            MessageId id;
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
            this.serviceManager.getTransactionService().addTransactionDataByBrokerName(
                ctx,
                messageQueue.getBrokerName(),
                producerGroup,
                sendResult.getQueueOffset(),
                id.getOffset(),
                sendResult.getTransactionId(),
                messageList.get(0)
            );
        } catch (Throwable t) {
            log.warn("fillTransactionData failed. messageQueue: {}, sendResult: {}", messageQueue, sendResult, t);
        }
    }

    protected SendMessageRequestHeader buildSendMessageRequestHeader(List<Message> messageList,
        String producerGroup, int sysFlag, int queueId) {
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();

        Message message = messageList.get(0);

        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setDefaultTopic(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC);
        requestHeader.setDefaultTopicQueueNums(4);
        requestHeader.setQueueId(queueId);
        requestHeader.setSysFlag(sysFlag);
        /*
        In RocketMQ 4.0, org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader.bornTimestamp
        represents the timestamp when the message was born. In RocketMQ 5.0, the bornTimestamp of the message
        is a message attribute, that is, the timestamp when message was constructed, and there is no
        bornTimestamp in the SendMessageRequest of RocketMQ 5.0.
        Note: When using grpc sendMessage to send multiple messages, the bornTimestamp in the requestHeader
        is set to the bornTimestamp of the first message, which may not be accurate. When a bornTimestamp is
        required, the bornTimestamp of the message property should be used.
        * */
        try {
            requestHeader.setBornTimestamp(Long.parseLong(message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP)));
        } catch (Exception e) {
            log.warn("parse born time error, with value:{}", message.getProperty(MessageConst.PROPERTY_BORN_TIMESTAMP));
            requestHeader.setBornTimestamp(System.currentTimeMillis());
        }
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));
        requestHeader.setReconsumeTimes(0);
        if (messageList.size() > 1) {
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
