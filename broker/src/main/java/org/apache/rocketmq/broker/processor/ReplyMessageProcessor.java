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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;

public class ReplyMessageProcessor extends AbstractSendMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public ReplyMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {

        final ReplyMessageRequestHeader requestHeader =
                (ReplyMessageRequestHeader) request.decodeCommandCustomHeader(ReplyMessageRequestHeader.class);
        if (requestHeader == null) {
            return null;
        }
        boolean result = this.pushReplyMessageToProducer(request, requestHeader);
        log.debug("send ReplyMessage result, {}", result);
        return null;
    }

    public boolean pushReplyMessageToProducer(RemotingCommand remotingCommand,
                                               final ReplyMessageRequestHeader requestHeader) {

        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());

        String correlationId = properties.get(MessageConst.PROPERTY_CORRELATION_ID);
        String producer = properties.get(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT);
        String ttl = properties.get(MessageConst.PROPERTY_MESSAGE_REPLY_TTL);
        String sendTime = properties.get(MessageConst.PROPERTY_MESSAGE_REPLY_SEND_TIME);
        if (StringUtils.isBlank(correlationId)) {
            log.warn("'" + MessageConst.PROPERTY_CORRELATION_ID + "' property is null, can not reply message");
            return false;
        }

        if (StringUtils.isNotBlank(ttl) && StringUtils.isNotBlank(sendTime)) {
            long replyTimeout = Long.parseLong(sendTime) + Long.parseLong(ttl);
            if (System.currentTimeMillis() > replyTimeout) {
                log.warn("reply message timeout: " + replyTimeout + ", can not reply message");
                return false;
            }
        }

        if (StringUtils.isBlank(producer)) {
            log.warn("'" + MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT + "' property is null, can not reply message");
            return false;
        }

        Channel channel = this.brokerController.getProducerManager().findChannel(producer);
        if (channel == null) {
            log.warn("push reply message fail, channel of <" + producer + "> not found.");
            return false;
        }

        properties.put(MessageConst.PROPERTY_PUSH_REPLY_TIME, String.valueOf(System.currentTimeMillis()));
        requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, requestHeader);
        request.setBody(remotingCommand.getBody()); // msg body

        boolean res = false;
        try {
            RemotingCommand pushResponse = this.brokerController.getBroker2Client().callClient(channel, request);
            assert pushResponse != null;

            res = pushResponse.getCode() == ResponseCode.SUCCESS;
            if (!res) {
                log.warn("push reply message to <{}> return fail, response remark: {}", producer, pushResponse.getRemark());
            }
        } catch (RemotingException | InterruptedException e) {
            log.warn("push reply message to <{}> fail. {}", producer, channel, e);
        }
        return res;
    }
}
