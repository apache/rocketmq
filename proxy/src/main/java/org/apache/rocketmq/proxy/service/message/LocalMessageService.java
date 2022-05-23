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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.PopMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.channel.ChannelManager;
import org.apache.rocketmq.proxy.service.channel.InvocationContext;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.route.SelectableMessageQueue;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalMessageService implements MessageService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final BrokerController brokerController;
    private final ChannelManager channelManager;

    public LocalMessageService(BrokerController brokerController, ChannelManager channelManager, RPCHook rpcHook) {
        this.brokerController = brokerController;
        this.channelManager = channelManager;
    }

    @Override public CompletableFuture<List<SendResult>> sendMessage(ProxyContext ctx, SelectableMessageQueue messageQueue,
        List<? extends Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
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
        RemotingCommand request = LocalRemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
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
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
            log.error("Failed to process send message command", e);
        } finally {
            channel.eraseInvocationContext(request.getOpaque());
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
                    throw new ProxyException(ProxyExceptionCode.ILLEGAL_MESSAGE, r.getRemark());
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
        return null;
    }

    @Override public void endTransactionOneway(ProxyContext ctx, TransactionId transactionId,
        EndTransactionRequestHeader requestHeader, long timeoutMillis) {

    }

    @Override public CompletableFuture<PopResult> popMessage(ProxyContext ctx, SelectableMessageQueue messageQueue,
        PopMessageRequestHeader requestHeader, long timeoutMillis) {
        return null;
    }

    @Override
    public CompletableFuture<AckResult> changeInvisibleTime(ProxyContext ctx, ReceiptHandle handle, String messageId,
        ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
        return null;
    }

    @Override public CompletableFuture<AckResult> ackMessage(ProxyContext ctx, ReceiptHandle handle, String messageId,
        AckMessageRequestHeader requestHeader, long timeoutMillis) {
        return null;
    }
}
