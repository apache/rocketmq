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

import apache.rocketmq.v2.Message;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.proxy.channel.ChannelManager;
import org.apache.rocketmq.proxy.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ResponseHook;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResponseStreamWriter;
import org.apache.rocketmq.proxy.grpc.v2.service.ReceiveMessageResultFilter;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalReceiveMessageResponseStreamWriter extends ReceiveMessageResponseStreamWriter {
    private final static Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ChannelManager channelManager;
    private final BrokerController brokerController;

    public LocalReceiveMessageResponseStreamWriter(
        StreamObserver<ReceiveMessageResponse> observer,
        ResponseHook<ReceiveMessageRequest, ReceiveMessageResponse> hook,
        ChannelManager channelManager,
        BrokerController brokerController,
        ReceiveMessageResultFilter receiveMessageResultFilter) {
        super(observer, hook, receiveMessageResultFilter);
        this.channelManager = channelManager;
        this.brokerController = brokerController;
    }

    @Override
    protected void processThrowableWhenWriteMessage(Throwable throwable, Context context, ReceiveMessageRequest request,
        Message message) {
        this.changeInvisibleTime(context, request, ReceiptHandle.decode(message.getSystemProperties().getReceiptHandle()));
    }

    private void changeInvisibleTime(Context ctx, ReceiveMessageRequest request, ReceiptHandle handle) {
        Channel channel = channelManager.createChannel(ctx);
        SimpleChannelHandlerContext simpleChannelHandlerContext = new SimpleChannelHandlerContext(channel);
        ChangeInvisibleTimeRequestHeader requestHeader = GrpcConverter.buildChangeInvisibleTimeRequestHeader(request, handle);
        RemotingCommand command = RemotingCommand.createRequestCommand(RequestCode.CHANGE_MESSAGE_INVISIBLETIME, requestHeader);
        command.makeCustomHeaderToNet();
        try {
            brokerController.getChangeInvisibleTimeProcessor().processRequest(simpleChannelHandlerContext, command);
        } catch (RemotingCommandException e) {
            log.error("ChangeInvisibleTime error when write response", e);
        }
    }
}
