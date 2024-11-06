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
package org.apache.rocketmq.proxy.service.message;

import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.channel.InvocationContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.route.AddressableMessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BatchAck;
import org.apache.rocketmq.remoting.protocol.body.BatchAckMessageRequestBody;
import org.apache.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class LocalMessageService implements MessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final BrokerController brokerController;
    private final ChannelManager channelManager;

    public LocalMessageService(BrokerController brokerController, ChannelManager channelManager, RPCHook rpcHook) {
        this.brokerController = brokerController;
        this.channelManager = channelManager;
    }

    @Override
    public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        List<Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
        byte[] body;
        String messageId;
        if (msgList.size() > 1) {
            requestHeader.setBatch(true);
            MessageBatch msgBatch = MessageBatch.generateFromList(msgList);
            MessageClientIDSetter.setUniqID(msgBatch);
            body = msgBatch.encode();
            msgBatch.setBody(body);
            messageId = MessageClientIDSetter.getUniqID(msgBatch);
        } else {
            Message message = msgList.get(0);
            body = message.getBody();
            messageId = MessageClientIDSetter.getUniqID(message);
        }
        RemotingCommand request = LocalRemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader, ctx.getLanguage());
        request.setBody(body);
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        SimpleChannel channel = channelManager.createInvocationChannel(ctx);
        InvocationContext invocationContext = new InvocationContext(future);
        channel.registerInvocationContext(request.getOpaque(), invocationContext);
        ChannelHandlerContext simpleChannelHandlerContext = channel.getChannelHandlerContext();
        try {
            RemotingCommand response = brokerController.getSendMessageProcessor().processRequest(simpleChannelHandlerContext, request);
            if (response != null) {
                invocationContext.handle(response);
                channel.eraseInvocationContext(request.getOpaque());
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            channel.eraseInvocationContext(request.getOpaque());
            log.error("Failed to process sendMessage command", e);
        }
        return future.thenApply(r -> {
            SendResult sendResult = new SendResult();
            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) r.readCustomHeader();
            SendStatus sendStatus;
            switch (r.getCode()) {
                case ResponseCode.FLUSH_DISK_TIMEOUT: {
                    sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                    break;
                }
                case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                    sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                    break;
                }
                case ResponseCode.SLAVE_NOT_AVAILABLE: {
                    sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                    break;
                }
                case ResponseCode.SUCCESS: {
                    sendStatus = SendStatus.SEND_OK;
                    break;
                }
                default: {
                    throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, r.getRemark());
                }
            }
            sendResult.setSendStatus(sendStatus);
            sendResult.setMsgId(messageId);
            sendResult.setMessageQueue(new MessageQueue(requestHeader.getTopic(), brokerController.getBrokerConfig().getBrokerName(), requestHeader.getQueueId()));
            sendResult.setQueueOffset(responseHeader.getQueueOffset());
            sendResult.setTransactionId(responseHeader.getTransactionId());
            sendResult.setOffsetMsgId(responseHeader.getMsgId());
            return Collections.singletonList(sendResult);
        });
    }

    @Override
    public CompletableFuture<RemotingCommand> sendMessageBack(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = brokerController.getSendMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process sendMessageBack command", e);
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> endTransactionOneway(ProxyContext ctx, String brokerName,
        EndTransactionRequestHeader requestHeader,
        long timeoutMillis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader, ctx.getLanguage());
        try {
            brokerController.getEndTransactionProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public CompletableFuture<PopResult> popMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {
        requestHeader.setBornTime(System.currentTimeMillis());
        RemotingCommand request = LocalRemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        SimpleChannel channel = channelManager.createInvocationChannel(ctx);
        InvocationContext invocationContext = new InvocationContext(future);
        channel.registerInvocationContext(request.getOpaque(), invocationContext);
        ChannelHandlerContext simpleChannelHandlerContext = channel.getChannelHandlerContext();
        try {
            RemotingCommand response = brokerController.getPopMessageProcessor().processRequest(simpleChannelHandlerContext, request);
            if (response != null) {
                invocationContext.handle(response);
                channel.eraseInvocationContext(request.getOpaque());
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            channel.eraseInvocationContext(request.getOpaque());
            log.error("Failed to process popMessage command", e);
        }
        return future.thenApply(r -> {
            PopStatus popStatus;
            List<MessageExt> messageExtList = new ArrayList<>();
            switch (r.getCode()) {
                case ResponseCode.SUCCESS:
                    popStatus = PopStatus.FOUND;
                    ByteBuffer byteBuffer = ByteBuffer.wrap(r.getBody());
                    messageExtList = MessageDecoder.decodesBatch(
                        byteBuffer,
                        true,
                        false,
                        true
                    );
                    break;
                case ResponseCode.POLLING_FULL:
                    popStatus = PopStatus.POLLING_FULL;
                    break;
                case ResponseCode.POLLING_TIMEOUT:
                case ResponseCode.PULL_NOT_FOUND:
                    popStatus = PopStatus.POLLING_NOT_FOUND;
                    break;
                default:
                    throw new ProxyException(ProxyExceptionCode.INTERNAL_SERVER_ERROR, r.getRemark());
            }
            PopResult popResult = new PopResult(popStatus, messageExtList);
            PopMessageResponseHeader responseHeader = (PopMessageResponseHeader) r.readCustomHeader();

            if (popStatus == PopStatus.FOUND) {
                Map<String, Long> startOffsetInfo;
                Map<String, List<Long>> msgOffsetInfo;
                Map<String, Integer> orderCountInfo;
                popResult.setInvisibleTime(responseHeader.getInvisibleTime());
                popResult.setPopTime(responseHeader.getPopTime());
                startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
                msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
                orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
                // <topicMark@queueId, msg queueOffset>
                Map<String, List<Long>> sortMap = new HashMap<>(16);
                for (MessageExt messageExt : messageExtList) {
                    // Value of POP_CK is used to determine whether it is a pop retry,
                    // cause topic could be rewritten by broker.
                    String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(),
                        messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getQueueId());
                    if (!sortMap.containsKey(key)) {
                        sortMap.put(key, new ArrayList<>(4));
                    }
                    sortMap.get(key).add(messageExt.getQueueOffset());
                }
                Map<String, String> map = new HashMap<>(5);
                for (MessageExt messageExt : messageExtList) {
                    if (startOffsetInfo == null) {
                        // we should set the check point info to extraInfo field , if the command is popMsg
                        // find pop ck offset
                        String key = messageExt.getTopic() + messageExt.getQueueId();
                        if (!map.containsKey(messageExt.getTopic() + messageExt.getQueueId())) {
                            map.put(key, ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(),
                                messageExt.getTopic(), messageQueue.getBrokerName(), messageExt.getQueueId()));
                        }
                        messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
                    } else {
                        if (messageExt.getProperty(MessageConst.PROPERTY_POP_CK) == null) {
                            String key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
                            int index = sortMap.get(key).indexOf(messageExt.getQueueOffset());
                            Long msgQueueOffset = msgOffsetInfo.get(key).get(index);
                            if (msgQueueOffset != messageExt.getQueueOffset()) {
                                log.warn("Queue offset [{}] of msg is strange, not equal to the stored in msg, {}", msgQueueOffset, messageExt);
                            }

                            messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK,
                                ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(key), responseHeader.getPopTime(), responseHeader.getInvisibleTime(),
                                    responseHeader.getReviveQid(), messageExt.getTopic(), messageQueue.getBrokerName(), messageExt.getQueueId(), msgQueueOffset)
                            );
                            if (requestHeader.isOrder() && orderCountInfo != null) {
                                Integer count = orderCountInfo.get(key);
                                if (count != null && count > 0) {
                                    messageExt.setReconsumeTimes(count);
                                }
                            }
                        }
                    }
                    messageExt.getProperties().computeIfAbsent(MessageConst.PROPERTY_FIRST_POP_TIME, k -> String.valueOf(responseHeader.getPopTime()));
                    messageExt.setBrokerName(messageExt.getBrokerName());
                    messageExt.setTopic(messageQueue.getTopic());
                }
            }
            return popResult;
        });
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            future = brokerController.getChangeInvisibleTimeProcessor()
                .processRequestAsync(channelHandlerContext.channel(), command, true);
        } catch (Exception e) {
            log.error("Fail to process changeInvisibleTime command", e);
            future.completeExceptionally(e);
        }
        return future.thenApply(r -> {
            ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) r.readCustomHeader();
            AckResult ackResult = new AckResult();
            if (ResponseCode.SUCCESS == r.getCode()) {
                ackResult.setStatus(AckStatus.OK);
            } else {
                ackResult.setStatus(AckStatus.NO_EXIST);
            }
            ackResult.setPopTime(responseHeader.getPopTime());
            ackResult.setExtraInfo(ReceiptHandle.builder()
                .startOffset(handle.getStartOffset())
                .retrieveTime(responseHeader.getPopTime())
                .invisibleTime(responseHeader.getInvisibleTime())
                .reviveQueueId(responseHeader.getReviveQid())
                .topicType(handle.getTopicType())
                .brokerName(handle.getBrokerName())
                .queueId(handle.getQueueId())
                .offset(handle.getOffset())
                .build()
                .encode());
            return ackResult;
        });
    }

    @Override
    public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader, ctx.getLanguage());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = brokerController.getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process ackMessage command", e);
            future.completeExceptionally(e);
        }
        return future.thenApply(r -> {
            AckResult ackResult = new AckResult();
            if (ResponseCode.SUCCESS == r.getCode()) {
                ackResult.setStatus(AckStatus.OK);
            } else {
                ackResult.setStatus(AckStatus.NO_EXIST);
            }
            return ackResult;
        });
    }

    @Override
    public CompletableFuture<AckResult> batchAckMessage(ProxyContext ctx, List<ReceiptHandleMessage> handleList,
        String consumerGroup, String topic, long timeoutMillis) {
        SimpleChannel channel = channelManager.createChannel(ctx);
        ChannelHandlerContext channelHandlerContext = channel.getChannelHandlerContext();
        RemotingCommand command = LocalRemotingCommand.createRequestCommand(RequestCode.BATCH_ACK_MESSAGE, null);

        Map<String, BatchAck> batchAckMap = new HashMap<>();
        for (ReceiptHandleMessage receiptHandleMessage : handleList) {
            String extraInfo = receiptHandleMessage.getReceiptHandle().getReceiptHandle();
            String[] extraInfoData = ExtraInfoUtil.split(extraInfo);
            String mergeKey = ExtraInfoUtil.getRetry(extraInfoData) + "@" +
                ExtraInfoUtil.getQueueId(extraInfoData) + "@" +
                ExtraInfoUtil.getCkQueueOffset(extraInfoData) + "@" +
                ExtraInfoUtil.getPopTime(extraInfoData);
            BatchAck bAck = batchAckMap.computeIfAbsent(mergeKey, k -> {
                BatchAck newBatchAck = new BatchAck();
                newBatchAck.setConsumerGroup(consumerGroup);
                newBatchAck.setTopic(topic);
                newBatchAck.setRetry(ExtraInfoUtil.getRetry(extraInfoData));
                newBatchAck.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfoData));
                newBatchAck.setQueueId(ExtraInfoUtil.getQueueId(extraInfoData));
                newBatchAck.setReviveQueueId(ExtraInfoUtil.getReviveQid(extraInfoData));
                newBatchAck.setPopTime(ExtraInfoUtil.getPopTime(extraInfoData));
                newBatchAck.setInvisibleTime(ExtraInfoUtil.getInvisibleTime(extraInfoData));
                newBatchAck.setBitSet(new BitSet());
                return newBatchAck;
            });
            bAck.getBitSet().set((int) (ExtraInfoUtil.getQueueOffset(extraInfoData) - ExtraInfoUtil.getCkQueueOffset(extraInfoData)));
        }
        BatchAckMessageRequestBody requestBody = new BatchAckMessageRequestBody();
        requestBody.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
        requestBody.setAcks(new ArrayList<>(batchAckMap.values()));

        command.setBody(requestBody.encode());
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            RemotingCommand response = brokerController.getAckMessageProcessor()
                .processRequest(channelHandlerContext, command);
            future.complete(response);
        } catch (Exception e) {
            log.error("Fail to process batchAckMessage command", e);
            future.completeExceptionally(e);
        }
        return future.thenApply(r -> {
            AckResult ackResult = new AckResult();
            if (ResponseCode.SUCCESS == r.getCode()) {
                ackResult.setStatus(AckStatus.OK);
            } else {
                ackResult.setStatus(AckStatus.NO_EXIST);
            }
            return ackResult;
        });
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(ProxyContext ctx, AddressableMessageQueue messageQueue,
        PullMessageRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("pullMessage is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> queryConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("queryConsumerOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("updateConsumerOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> updateConsumerOffsetAsync(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("updateConsumerOffsetAsync is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Set<MessageQueue>> lockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        LockBatchRequestBody requestBody, long timeoutMillis) {
        throw new NotImplementedException("lockBatchMQ is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> unlockBatchMQ(ProxyContext ctx, AddressableMessageQueue messageQueue,
        UnlockBatchRequestBody requestBody, long timeoutMillis) {
        throw new NotImplementedException("unlockBatchMQ is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> getMaxOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("getMaxOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Long> getMinOffset(ProxyContext ctx, AddressableMessageQueue messageQueue,
        GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
        throw new NotImplementedException("getMinOffset is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<RemotingCommand> request(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new NotImplementedException("request is not implemented in LocalMessageService");
    }

    @Override
    public CompletableFuture<Void> requestOneway(ProxyContext ctx, String brokerName, RemotingCommand request,
        long timeoutMillis) {
        throw new NotImplementedException("requestOneway is not implemented in LocalMessageService");
    }
}
