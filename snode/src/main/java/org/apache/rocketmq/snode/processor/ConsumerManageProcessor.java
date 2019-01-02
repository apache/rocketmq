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
package org.apache.rocketmq.snode.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.ConsumerGroupInfo;

public class ConsumerManageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final SnodeController snodeController;

    public ConsumerManageProcessor(final SnodeController brokerController) {
        this.snodeController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException  {
        NettyChannelHandlerContextImpl nettyChannelHandlerContext = (NettyChannelHandlerContextImpl)remotingChannel;
        ChannelHandlerContext ctx = nettyChannelHandlerContext.getChannelHandlerContext();

        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(ctx, request);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return searchOffsetByTimestamp(ctx, request);
            case RequestCode.GET_MAX_OFFSET:
                return getMaxOffset(ctx, request);
            case RequestCode.GET_MIN_OFFSET:
                return getMinOffset(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final SearchOffsetRequestHeader requestHeader =
            (SearchOffsetRequestHeader) request
                .decodeCommandCustomHeader(SearchOffsetRequestHeader.class);
        try {
            return this.snodeController.getEnodeService().getOffsetByTimestamp(requestHeader.getEnodeName(), request);
        } catch (Exception ex) {
            log.error("Search offset by timestamp error:{}", ex);
        }
        return null;
    }

    public RemotingCommand getMinOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetMinOffsetRequestHeader requestHeader =
            (GetMinOffsetRequestHeader) request
                .decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);
        try {
            return this.snodeController.getEnodeService().getMinOffsetInQueue(requestHeader.getEnodeName(), requestHeader.getTopic(), requestHeader.getQueueId());
        } catch (Exception ex) {
            log.error("Get min offset error:{}", ex);
        }
        return null;
    }

    public RemotingCommand getMaxOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetMaxOffsetRequestHeader requestHeader =
            (GetMaxOffsetRequestHeader) request
                .decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);
        try {
            return this.snodeController.getEnodeService().getMaxOffsetInQueue(requestHeader.getEnodeName(), request);
        } catch (Exception ex) {
            log.error("Get min offset error:{}", ex);
        }
        return null;
    }

    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupRequestHeader requestHeader =
            (GetConsumerListByGroupRequestHeader) request
                .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
            this.snodeController.getConsumerManager().getConsumerGroupInfo(
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
                log.warn("GetAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("GetConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("No consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader =
            (UpdateConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        this.snodeController.getConsumerOffsetManager().commitOffset(requestHeader.getEnodeName(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
            (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader =
            (QueryConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        long offset =
            this.snodeController.getConsumerOffsetManager().queryOffset(requestHeader.getEnodeName(),
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        if (offset < 0) {
            log.info("Load offset from enode server, enodeName: {}, consumer group: {}, topic: {}, queueId: {}",
                requestHeader.getEnodeName(),
                requestHeader.getConsumerGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId());
            return this.snodeController.getEnodeService().loadOffset(requestHeader.getEnodeName(), requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                requestHeader.getQueueId());
        } else {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
    }
}

