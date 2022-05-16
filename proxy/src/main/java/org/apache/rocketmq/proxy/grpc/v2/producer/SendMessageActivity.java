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
package org.apache.rocketmq.proxy.grpc.v2.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Encoding;
import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendReceipt;
import apache.rocketmq.v2.SystemProperties;
import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.grpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;

public class SendMessageActivity extends AbstractMessingActivity {

    public SendMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager) {
        super(messagingProcessor, grpcClientSettingsManager);
    }

    public CompletableFuture<SendMessageResponse> sendMessage(Context ctx, SendMessageRequest request) {
        ProxyContext context = createContext(ctx);
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();

        try {
            if (request.getMessagesCount() <= 0) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
            }

            List<Message> messageList = request.getMessagesList();
            Resource topic = messageList.get(0).getTopic();
            future = this.messagingProcessor.sendMessage(
                context,
                new SendMessageQueueSelector(request),
                GrpcConverter.wrapResourceWithNamespace(topic),
                buildMessage(context, request.getMessagesList(), topic)
            ).thenApply(result -> convertToSendMessageResponse(context, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected List<MessageExt> buildMessage(ProxyContext context, List<Message> protoMessageList, Resource topic) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        List<MessageExt> messageExtList = new ArrayList<>();
        for (apache.rocketmq.v2.Message protoMessage : protoMessageList) {
            if (!protoMessage.getTopic().equals(topic)) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "topic in message is not same");
            }
            // here use topicName as producerGroup for transactional checker.
            messageExtList.add(buildMessage(protoMessage, topicName));
        }
        return messageExtList;
    }

    protected MessageExt buildMessage(Message protoMessage, String producerGroup) {
        String topicName = GrpcConverter.wrapResourceWithNamespace(protoMessage.getTopic());

        MessageExt messageExt = new MessageExt();
        messageExt.setTopic(topicName);
        messageExt.setBody(protoMessage.getBody().toByteArray());
        Map<String, String> messageProperty = this.buildMessageProperty(protoMessage, producerGroup);

        // sysFlag (body encoding & message type)
        SystemProperties systemProperties = protoMessage.getSystemProperties();
        int sysFlag = 0;
        Encoding bodyEncoding = systemProperties.getBodyEncoding();
        if (bodyEncoding.equals(Encoding.GZIP)) {
            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        }
        // transaction
        MessageType messageType = systemProperties.getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }
        messageExt.setSysFlag(sysFlag);

        MessageAccessor.setProperties(messageExt, messageProperty);
        return messageExt;
    }

    protected Map<String, String> buildMessageProperty(Message message, String producerGroup) {
        org.apache.rocketmq.common.message.Message messageWithHeader = new org.apache.rocketmq.common.message.Message();
        // set user properties
        Map<String, String> userProperties = message.getUserPropertiesMap();
        for (String key : userProperties.keySet()) {
            if (MessageConst.STRING_HASH_SET.contains(key)) {
                throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_PROPERTY_KEY, "property is used by system: " + key);
            }
        }
        MessageAccessor.setProperties(messageWithHeader, Maps.newHashMap(userProperties));

        // set tag
        String tag = message.getSystemProperties().getTag();
        if (!"".equals(tag)) {
            messageWithHeader.setTags(tag);
        }

        // set keys
        List<String> keysList = message.getSystemProperties().getKeysList();
        if (keysList.size() > 0) {
            messageWithHeader.setKeys(keysList);
        }

        // set message id
        String messageId = message.getSystemProperties().getMessageId();
        if ("".equals(messageId)) {
            throw new GrpcProxyException(Code.ILLEGAL_MESSAGE_ID, "message id is empty");
        }
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, messageId);

        // set transaction property
        MessageType messageType = message.getSystemProperties().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");

            Duration transactionResolveDelay = message.getSystemProperties().getOrphanedTransactionRecoveryDuration();

            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS,
                String.valueOf(Durations.toSeconds(transactionResolveDelay)));
        }

        // set delay level or deliver timestamp
        if (message.getSystemProperties().hasDeliveryTimestamp()) {
            Timestamp deliveryTimestamp = message.getSystemProperties().getDeliveryTimestamp();
            String timestampString = String.valueOf(Timestamps.toMillis(deliveryTimestamp));
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TIMER_DELIVER_MS, timestampString);
        }

        // set reconsume times
        int reconsumeTimes = message.getSystemProperties().getDeliveryAttempt();
        MessageAccessor.setReconsumeTime(messageWithHeader, String.valueOf(reconsumeTimes));
        // set producer group
        MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
        // set message group
        String messageGroup = message.getSystemProperties().getMessageGroup();
        if (!messageGroup.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_SHARDING_KEY, messageGroup);
        }
        // set trace context
        String traceContext = message.getSystemProperties().getTraceContext();
        if (!traceContext.isEmpty()) {
            MessageAccessor.putProperty(messageWithHeader, MessageConst.PROPERTY_TRACE_CONTEXT, traceContext);
        }
        return messageWithHeader.getProperties();
    }

    protected SendMessageResponse convertToSendMessageResponse(ProxyContext ctx, SendMessageRequest request,
        SendResult result) {
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            return SendMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, "send message failed, sendStatus=" + result.getSendStatus()))
                .build();
        }

        List<SendReceipt> sendReceiptList = Lists.newArrayList();
        sendReceiptList.add(SendReceipt.newBuilder()
            .setMessageId(StringUtils.defaultString(result.getMsgId()))
            .setTransactionId(StringUtils.defaultString(result.getTransactionId()))
            .build());
        return SendMessageResponse.newBuilder()
            .setStatus(ResponseBuilder.buildStatus(Code.OK, Code.OK.name()))
            .addAllReceipts(sendReceiptList)
            .build();
    }

    protected static class SendMessageQueueSelector implements QueueSelector {

        private final SendMessageRequest request;

        public SendMessageQueueSelector(SendMessageRequest request) {
            this.request = request;
        }

        @Override
        public SelectableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                Message message = request.getMessages(0);
                String shardingKey = null;
                if (request.getMessagesCount() == 1) {
                    shardingKey = message.getSystemProperties().getMessageGroup();
                }
                SelectableMessageQueue targetMessageQueue;
                if (StringUtils.isNotEmpty(shardingKey)) {
                    // With shardingKey
                    List<SelectableMessageQueue> writeQueues = messageQueueView.getWriteSelector().getQueues();
                    int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
                    targetMessageQueue = writeQueues.get(bucket);
                } else {
                    targetMessageQueue = messageQueueView.getWriteSelector().selectOne(false);
                }
                return targetMessageQueue;
            } catch (Exception e) {
                return null;
            }
        }
    }

}
