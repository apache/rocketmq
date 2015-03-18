/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl;

import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.body.GetConsumerStatusBody;
import com.alibaba.rocketmq.common.protocol.body.ResetOffsetBody;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;


    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE:
                return this.checkTransactionState(ctx, request);
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return this.notifyConsumerIdsChanged(ctx, request);
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return this.getConsumeStatus(ctx, request);

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            default:
                break;
        }
        return null;
    }


    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumeMessageDirectlyResultRequestHeader requestHeader =
                (ConsumeMessageDirectlyResultRequestHeader) request
                        .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        final MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(request.getBody()));

        ConsumeMessageDirectlyResult result =
                this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.getConsumerGroup(),
                        requestHeader.getBrokerName());

        if (null != result) {
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(result.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer",
                    requestHeader.getConsumerGroup()));
        }

        return response;
    }


    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerRunningInfoRequestHeader requestHeader =
                (GetConsumerRunningInfoRequestHeader) request
                        .decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        ConsumerRunningInfo consumerRunningInfo =
                this.mqClientFactory.consumerRunningInfo(requestHeader.getConsumerGroup());
        if (null != consumerRunningInfo) {
            if (requestHeader.isJstackEnable()) {
                String jstack = UtilAll.jstack();
                consumerRunningInfo.setJstack(jstack);
            }

            response.setCode(ResponseCode.SUCCESS);
            response.setBody(consumerRunningInfo.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer",
                    requestHeader.getConsumerGroup()));
        }

        return response;
    }

    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader =
                (CheckTransactionStateRequestHeader) request
                        .decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    public RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        try {
            final NotifyConsumerIdsChangedRequestHeader requestHeader =
                    (NotifyConsumerIdsChangedRequestHeader) request
                            .decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
            log.info(
                    "receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",//
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),//
                    requestHeader.getConsumerGroup());
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
        }
        return null;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
                (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info(
                "invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                        requestHeader.getGroup(), requestHeader.getTimestamp()});
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
        if (request.getBody() != null) {
            ResetOffsetBody body = ResetOffsetBody.decode(request.getBody(), ResetOffsetBody.class);
            offsetTable = body.getOffsetTable();
        }
        this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
        return null;
    }

    @Deprecated
    public RemotingCommand getConsumeStatus(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerStatusRequestHeader requestHeader =
                (GetConsumerStatusRequestHeader) request
                        .decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        Map<MessageQueue, Long> offsetTable =
                this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
        GetConsumerStatusBody body = new GetConsumerStatusBody();
        body.setMessageQueueTable(offsetTable);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }
}
