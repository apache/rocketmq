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

package org.apache.rocketmq.proxy.grpc.v2.adapter.handler;

import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendReceipt;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.channel.InvocationContext;
import org.apache.rocketmq.proxy.service.transaction.TransactionId;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseBuilder;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class SendMessageResponseHandler implements ResponseHandler<SendMessageRequest, SendMessageResponse> {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final String messageId;
    private final int sysFlag;
    private final String localAddress;

    public SendMessageResponseHandler(String messageId, int sysFlag, String localAddress) {
        this.messageId = messageId;
        this.sysFlag = sysFlag;
        this.localAddress = localAddress;
    }

    @Override
    public void handle(RemotingCommand responseCommand,
        InvocationContext<SendMessageRequest, SendMessageResponse> context) {
        // If responseCommand equals to null, then the response has been written to channel.
        // org.apache.rocketmq.broker.processor.SendMessageProcessor#handlePutMessageResult
        // org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#doResponse
        if (null != responseCommand) {
            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) responseCommand.readCustomHeader();
            int tranType = MessageSysFlag.getTransactionValue(sysFlag);
            String transactionIdString = "";
            if (responseCommand.getCode() == ResponseCode.SUCCESS && tranType == MessageSysFlag.TRANSACTION_PREPARED_TYPE) {
                long commitLogOffset = 0L;
                try {
                    commitLogOffset = TransactionId.generateCommitLogOffset(responseHeader.getMsgId());
                } catch (IllegalArgumentException e) {
                    log.warn("illegal messageId:{}", responseHeader.getMsgId());
                    e.printStackTrace();
                }
                TransactionId transactionId = TransactionId.genByBrokerTransactionId(
                    RemotingUtil.string2SocketAddress(localAddress),
                    responseHeader.getTransactionId(),
                    commitLogOffset,
                    responseHeader.getQueueOffset()
                );
                transactionIdString = transactionId.getProxyTransactionId();
            }
            SendMessageResponse response = SendMessageResponse.newBuilder()
                .setStatus(ResponseBuilder.buildStatus(responseCommand.getCode(), responseCommand.getRemark()))
                .addReceipts(SendReceipt.newBuilder()
                    .setMessageId(StringUtils.defaultString(messageId))
                    .setTransactionId(StringUtils.defaultString(transactionIdString))
                    .build())
                .build();
            context.getResponse().complete(response);
        }
    }
}