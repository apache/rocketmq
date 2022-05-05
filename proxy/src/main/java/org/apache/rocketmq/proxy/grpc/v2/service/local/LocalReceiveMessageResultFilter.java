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

package org.apache.rocketmq.proxy.grpc.v2.service.local;

import apache.rocketmq.v2.ReceiveMessageRequest;
import io.grpc.Context;
import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.service.BaseReceiveMessageResultFilter;
import org.apache.rocketmq.proxy.grpc.v2.service.GrpcClientManager;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalReceiveMessageResultFilter extends BaseReceiveMessageResultFilter {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    private final ChannelManager channelManager;
    private final BrokerController brokerController;

    public LocalReceiveMessageResultFilter(ChannelManager channelManager, BrokerController brokerController, GrpcClientManager grpcClientManager) {
        super(grpcClientManager);
        this.channelManager = channelManager;
        this.brokerController = brokerController;
    }

    @Override
    protected void processNoMatchMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt) {
        ReceiptHandle handle = ReceiptHandle.create(messageExt);
        if (handle == null) {
            return;
        }
        Channel channel = channelManager.createChannel(ctx);
        AckMessageRequestHeader requestHeader = GrpcConverter.buildAckMessageRequestHeader(request, handle);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, requestHeader);
        command.makeCustomHeaderToNet();
        try {
            brokerController.getAckMessageProcessor().processRequest(new SimpleChannelHandlerContext(channel), command);
        } catch (RemotingCommandException e) {
            log.error("AckMessage failed in filterMessage", e);
        }
    }

    @Override
    protected void processExceedMaxAttemptsMessage(Context ctx, ReceiveMessageRequest request, MessageExt messageExt,
        int maxAttempts) {
        try {
            ReceiptHandle handle = ReceiptHandle.create(messageExt);
            if (handle == null) {
                return;
            }
            Channel channel = channelManager.createChannel(ctx);
            SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
            ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = GrpcConverter.buildConsumerSendMsgBackRequestHeader(request, handle, messageExt.getMsgId(), maxAttempts);
            RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackRequestHeader);
            command.makeCustomHeaderToNet();
            RemotingCommand response = brokerController.getSendMessageProcessor().processRequest(simpleChannelHandlerContext, command);
            if (response.getCode() == ResponseCode.SUCCESS) {
                AckMessageRequestHeader ackMessageRequestHeader = GrpcConverter.buildAckMessageRequestHeader(request, handle);
                command = RemotingCommand.createRequestCommand(RequestCode.ACK_MESSAGE, ackMessageRequestHeader);
                command.makeCustomHeaderToNet();
                brokerController.getAckMessageProcessor().processRequest(simpleChannelHandlerContext, command);
            }
        } catch (Exception e) {
            log.error("ForwardMessageToDLQ failed in filterMessage", e);
        }
    }
}
