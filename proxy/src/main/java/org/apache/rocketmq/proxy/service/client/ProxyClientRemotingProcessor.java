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
package org.apache.rocketmq.proxy.service.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.proxy.common.utils.ProxyUtils;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

public class ProxyClientRemotingProcessor extends ClientRemotingProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    private final ProducerManager producerManager;

    public ProxyClientRemotingProcessor(ProducerManager producerManager) {
        super(null);
        this.producerManager = producerManager;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        if (request.getCode() == RequestCode.CHECK_TRANSACTION_STATE) {
            return this.checkTransactionState(ctx, request);
        }
        return null;
    }

    @Override
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer, true, false, false);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                CheckTransactionStateRequestHeader requestHeader =
                    (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
                request.writeCustomHeader(requestHeader);
                request.addExtField(ProxyUtils.BROKER_ADDR, NetworkUtil.socketAddress2String(ctx.channel().remoteAddress()));
                Channel channel = this.producerManager.getAvailableChannel(group);
                if (channel != null) {
                    channel.writeAndFlush(request);
                } else {
                    log.warn("check transaction failed, channel is empty. groupId={}, requestHeader:{}", group, requestHeader);
                }
            }
        }
        return null;
    }
}
