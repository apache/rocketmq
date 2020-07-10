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

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.AllocateMessageQueueRequestBody;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueResponseBody;
import org.apache.rocketmq.common.protocol.header.AllocateMessageQueueResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueAveragelyByCircle;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueByConfig;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueByMachineRoom;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueConsistentHash;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueSticky;
import org.apache.rocketmq.common.rebalance.AllocateMessageQueueStrategyConstants;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public ConsumerManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(ctx, request);
            case RequestCode.ALLOCATE_MESSAGE_QUEUE:
                return this.allocateMessageQueue(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupRequestHeader requestHeader =
            (GetConsumerListByGroupRequestHeader) request
                .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(
                requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader =
            (UpdateConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
            (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader =
            (QueryConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        long offset =
            this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            long minOffset =
                this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(),
                    requestHeader.getQueueId());
            if (minOffset <= 0
                && !this.brokerController.getMessageStore().checkInDiskByConsumeOffset(
                requestHeader.getTopic(), requestHeader.getQueueId(), 0)) {
                responseHeader.setOffset(0L);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            } else {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return response;
    }

    private synchronized RemotingCommand allocateMessageQueue(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(AllocateMessageQueueResponseHeader.class);
        final AllocateMessageQueueRequestHeader requestHeader =
            (AllocateMessageQueueRequestHeader) request.decodeCommandCustomHeader(AllocateMessageQueueRequestHeader.class);
        final AllocateMessageQueueRequestBody requestBody = AllocateMessageQueueRequestBody.decode(request.getBody(),
            AllocateMessageQueueRequestBody.class);

        AllocateMessageQueueStrategy strategy = null;
        String consumerGroup = requestHeader.getConsumerGroup();
        String strategyName = requestHeader.getStrategyName();
        Map<String, AllocateMessageQueueStrategy> strategyTable = this.brokerController.getAllocateMessageQueueStrategyTable();

        if (strategyTable.containsKey(consumerGroup) && strategyName.equals(strategyTable.get(consumerGroup).getName())) {
            strategy = strategyTable.get(consumerGroup);
        } else {
            switch (strategyName) {
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_AVERAGELY:
                    strategy = new AllocateMessageQueueAveragely();
                    break;
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_AVERAGELY_BY_CIRCLE:
                    strategy = new AllocateMessageQueueAveragelyByCircle();
                    break;
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_BY_CONFIG:
                    strategy = new AllocateMessageQueueByConfig();
                    break;
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_BY_MACHINE_ROOM:
                    strategy = new AllocateMessageQueueByMachineRoom();
                    break;
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_CONSISTENT_HASH:
                    strategy = new AllocateMessageQueueConsistentHash();
                    break;
                case AllocateMessageQueueStrategyConstants.ALLOCATE_MESSAGE_QUEUE_STICKY:
                    strategy = new AllocateMessageQueueSticky();
                    break;
                default:
                    response.setCode(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
                    response.setRemark("AllocateMessageQueueStrategy[" + strategyName + "] is not supported by broker");
                    return response;
            }
            strategyTable.put(consumerGroup, strategy);
        }

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(consumerGroup);
        List<MessageQueue> allocateResult = null;
        try {
            allocateResult = strategy.allocate(
                requestHeader.getConsumerGroup(),
                requestHeader.getClientID(),
                requestBody.getMqAll(),
                consumerGroupInfo != null ? consumerGroupInfo.getAllClientId() : null);
        } catch (Throwable e) {
            log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}",
                strategy.getName(), e);
            response.setCode(ResponseCode.ALLOCATE_MESSAGE_QUEUE_FAILED);
            response.setRemark(e.getMessage());
            return response;
        }

        AllocateMessageQueueResponseBody body = new AllocateMessageQueueResponseBody();
        body.setAllocateResult(allocateResult);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }
}
