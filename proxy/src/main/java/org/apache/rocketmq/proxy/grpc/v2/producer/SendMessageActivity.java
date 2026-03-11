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
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessagingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcProxyException;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.QueueSelector;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;

public class SendMessageActivity extends AbstractMessagingActivity {

    public SendMessageActivity(MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    public CompletableFuture<SendMessageResponse> sendMessage(ProxyContext ctx, SendMessageRequest request) {
        CompletableFuture<SendMessageResponse> future = new CompletableFuture<>();

        try {
            if (request.getMessagesCount() <= 0) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "no message to send");
            }

            List<apache.rocketmq.v2.Message> messageList = request.getMessagesList();
            apache.rocketmq.v2.Message message = messageList.get(0);
            Resource topic = message.getTopic();
            validateTopic(topic);

            future = this.messagingProcessor.sendMessage(
                ctx,
                new SendMessageQueueSelector(request),
                topic.getName(),
                buildSysFlag(message),
                buildMessage(ctx, request.getMessagesList(), topic)
            ).thenApply(result -> convertToSendMessageResponse(ctx, request, result));
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected List<Message> buildMessage(ProxyContext context, List<apache.rocketmq.v2.Message> protoMessageList,
        Resource topic) {
        String topicName = topic.getName();
        List<Message> messageExtList = new ArrayList<>();
        for (apache.rocketmq.v2.Message protoMessage : protoMessageList) {
            if (!protoMessage.getTopic().equals(topic)) {
                throw new GrpcProxyException(Code.MESSAGE_CORRUPTED, "topic in message is not same");
            }
            // here use topicName as producerGroup for transactional checker.
            messageExtList.add(buildMessage(context, protoMessage, topicName));
        }
        return messageExtList;
    }

    protected Message buildMessage(ProxyContext context, apache.rocketmq.v2.Message protoMessage, String producerGroup) {
        String topicName = protoMessage.getTopic().getName();

        BuildMessagePropertyUtil.validateMessageBodySize(protoMessage.getBody());
        Message messageExt = new Message();
        messageExt.setTopic(topicName);
        messageExt.setBody(protoMessage.getBody().toByteArray());
        Map<String, String> messageProperty = BuildMessagePropertyUtil.buildMessageProperty(context, protoMessage, producerGroup);

        MessageAccessor.setProperties(messageExt, messageProperty);
        return messageExt;
    }

    protected int buildSysFlag(apache.rocketmq.v2.Message protoMessage) {
        // sysFlag (body encoding & message type)
        int sysFlag = 0;
        Encoding bodyEncoding = protoMessage.getSystemProperties().getBodyEncoding();
        if (bodyEncoding.equals(Encoding.GZIP)) {
            sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        }
        // transaction
        MessageType messageType = protoMessage.getSystemProperties().getMessageType();
        if (messageType.equals(MessageType.TRANSACTION)) {
            sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        }
        return sysFlag;
    }

    protected SendMessageResponse convertToSendMessageResponse(ProxyContext ctx, SendMessageRequest request,
        List<SendResult> resultList) {
        SendMessageResponse.Builder builder = SendMessageResponse.newBuilder();

        Set<Code> responseCodes = new HashSet<>();
        for (SendResult result : resultList) {
            SendResultEntry resultEntry;
            switch (result.getSendStatus()) {
                case FLUSH_DISK_TIMEOUT:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.MASTER_PERSISTENCE_TIMEOUT, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case FLUSH_SLAVE_TIMEOUT:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.SLAVE_PERSISTENCE_TIMEOUT, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case SLAVE_NOT_AVAILABLE:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.HA_NOT_AVAILABLE, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
                case SEND_OK:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                        .setOffset(result.getQueueOffset())
                        .setMessageId(StringUtils.defaultString(result.getMsgId()))
                        .setTransactionId(StringUtils.defaultString(result.getTransactionId()))
                        .setRecallHandle(StringUtils.defaultString(result.getRecallHandle()))
                        .build();
                    break;
                default:
                    resultEntry = SendResultEntry.newBuilder()
                        .setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "send message failed, sendStatus=" + result.getSendStatus()))
                        .build();
                    break;
            }
            builder.addEntries(resultEntry);
            responseCodes.add(resultEntry.getStatus().getCode());
        }
        if (responseCodes.size() > 1) {
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.MULTIPLE_RESULTS, Code.MULTIPLE_RESULTS.name()));
        } else if (responseCodes.size() == 1) {
            Code code = responseCodes.stream().findAny().get();
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(code, code.name()));
        } else {
            builder.setStatus(ResponseBuilder.getInstance().buildStatus(Code.INTERNAL_SERVER_ERROR, "send status is empty"));
        }
        return builder.build();
    }

    protected static class SendMessageQueueSelector implements QueueSelector {

        private final SendMessageRequest request;

        public SendMessageQueueSelector(SendMessageRequest request) {
            this.request = request;
        }

        @Override
        public AddressableMessageQueue select(ProxyContext ctx, MessageQueueView messageQueueView) {
            try {
                apache.rocketmq.v2.Message message = request.getMessages(0);
                String shardingKey = null;
                if (request.getMessagesCount() == 1) {
                    shardingKey = message.getSystemProperties().getMessageGroup();
                    // lite topic
                    if (StringUtils.isBlank(shardingKey)) {
                        shardingKey = message.getSystemProperties().getLiteTopic();
                    }
                }
                AddressableMessageQueue targetMessageQueue;
                if (StringUtils.isNotEmpty(shardingKey)) {
                    // With shardingKey
                    List<AddressableMessageQueue> writeQueues = messageQueueView.getWriteSelector().getQueues();
                    int bucket = Hashing.consistentHash(shardingKey.hashCode(), writeQueues.size());
                    targetMessageQueue = writeQueues.get(bucket);
                } else {
                    targetMessageQueue = messageQueueView.getWriteSelector().selectOneByPipeline(false);
                }
                return targetMessageQueue;
            } catch (Exception e) {
                return null;
            }
        }
    }

}
