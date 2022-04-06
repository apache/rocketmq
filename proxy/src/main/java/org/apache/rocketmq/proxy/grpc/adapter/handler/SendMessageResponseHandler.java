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

package org.apache.rocketmq.proxy.grpc.adapter.handler;

import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendReceipt;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.proxy.grpc.adapter.InvocationContext;
import org.apache.rocketmq.proxy.grpc.adapter.ResponseBuilderV2;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class SendMessageResponseHandler implements ResponseHandler<SendMessageRequest, SendMessageResponse> {
    public SendMessageResponseHandler() {
    }

    @Override public void handle(RemotingCommand responseCommand,
        InvocationContext<SendMessageRequest, SendMessageResponse> context) {
        // If responseCommand equals to null, then the response has been written to channel.
        // org.apache.rocketmq.broker.processor.SendMessageProcessor#handlePutMessageResult
        // org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#doResponse
        if (null != responseCommand) {
            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) responseCommand.readCustomHeader();
            String messageId = "";
            String transactionId = "";
            if (responseHeader != null) {
                messageId = responseHeader.getMsgId();
                transactionId = responseHeader.getTransactionId();
            }
            SendMessageResponse response = SendMessageResponse.newBuilder()
                .setStatus(ResponseBuilderV2.buildStatus(responseCommand.getCode(), responseCommand.getRemark()))
                .addReceipts(SendReceipt.newBuilder()
                    .setMessageId(StringUtils.defaultString(messageId))
                    .setTransactionId(StringUtils.defaultString(transactionId))
                    .build())
                .build();
            context.getResponse().complete(response);
        }
    }
}