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

import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CreateRetryTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.snode.SnodeController;

public class ConsumerManageProcessor implements RequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeController snodeController;

    public ConsumerManageProcessor(final SnodeController brokerController) {
        this.snodeController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(remotingChannel, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(remotingChannel, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(remotingChannel, request);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return searchOffsetByTimestamp(remotingChannel, request);
            case RequestCode.GET_MAX_OFFSET:
                return getMaxOffset(remotingChannel, request);
            case RequestCode.GET_MIN_OFFSET:
                return getMinOffset(remotingChannel, request);
            case RequestCode.CREATE_RETRY_TOPIC:
                return createRetryTopic(remotingChannel, request);
            case RequestCode.LOCK_BATCH_MQ:
                return lockBatchMQ(remotingChannel, request);
            case RequestCode.UNLOCK_BATCH_MQ:
                return unlockBatchMQ(remotingChannel, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand searchOffsetByTimestamp(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        final SearchOffsetRequestHeader requestHeader =
            (SearchOffsetRequestHeader) request
                .decodeCommandCustomHeader(SearchOffsetRequestHeader.class);
        try {
            final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
            final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();

            long offset = this.snodeController.getEnodeService().getOffsetByTimestamp(requestHeader.getEnodeName(),
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getTimestamp(), request);
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } catch (Exception ex) {
            log.error("Search offset by timestamp error:{}", ex);
        }
        return null;
    }

    public RemotingCommand getMinOffset(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        final GetMinOffsetRequestHeader requestHeader =
            (GetMinOffsetRequestHeader) request
                .decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);
        try {
            final RemotingCommand response = RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
            final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();

            long offset = this.snodeController.getEnodeService().getMinOffsetInQueue(requestHeader.getEnodeName(),
                requestHeader.getTopic(), requestHeader.getQueueId(), request);
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } catch (Exception ex) {
            log.error("Get min offset error:{}", ex);
        }
        return null;
    }

    public RemotingCommand getMaxOffset(RemotingChannel remotingChannel,
        RemotingCommand request) throws RemotingCommandException {
        final GetMaxOffsetRequestHeader requestHeader =
            (GetMaxOffsetRequestHeader) request
                .decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);
        try {
            long offset = this.snodeController.getEnodeService().getMaxOffsetInQueue(requestHeader.getEnodeName(),
                requestHeader.getTopic(), requestHeader.getQueueId(), request);
            final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
            final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        } catch (Exception ex) {
            log.error("Get min offset error, remoting: {} error: {} ", remotingChannel.remoteAddress(), ex);
        }
        return null;
    }

    public RemotingCommand getConsumerListByGroup(RemotingChannel remotingChannel, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupRequestHeader requestHeader =
            (GetConsumerListByGroupRequestHeader) request
                .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        List<String> clientIds = this.snodeController.getConsumerManager().getAllClientId(requestHeader.getConsumerGroup());
        if (!clientIds.isEmpty()) {
            GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
            body.setConsumerIdList(clientIds);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            response.setBody(body.encode());
            return response;
        } else {
            log.warn("GetAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("No consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    private RemotingCommand updateConsumerOffset(RemotingChannel remotingChannel, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader =
            (UpdateConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        this.snodeController.getConsumerOffsetManager().commitOffset(remotingChannel, requestHeader.getEnodeName(), RemotingHelper.parseChannelRemoteAddr(remotingChannel.remoteAddress()), requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand queryConsumerOffset(RemotingChannel remotingChannel, RemotingCommand request)
        throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        final QueryConsumerOffsetRequestHeader requestHeader =
            (QueryConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);
        log.info("Load offset from enode server, enodeName: {}, consumer group: {}, topic: {}, queueId: {}",
            requestHeader.getEnodeName(),
            requestHeader.getConsumerGroup(),
            requestHeader.getTopic(),
            requestHeader.getQueueId());
        long offset = this.snodeController.getEnodeService().queryOffset(requestHeader.getEnodeName(), requestHeader.getConsumerGroup(), requestHeader.getTopic(),
            requestHeader.getQueueId());

        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
        responseHeader.setOffset(offset);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand createRetryTopic(RemotingChannel remotingChannel,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException, RemotingCommandException {
        final CreateRetryTopicRequestHeader requestHeader = (CreateRetryTopicRequestHeader) request.decodeCommandCustomHeader(CreateRetryTopicRequestHeader.class);
        requestHeader.getEnodeName();
        return this.snodeController.getEnodeService().creatRetryTopic(remotingChannel, requestHeader.getEnodeName(), request);
    }

    public RemotingCommand lockBatchMQ(RemotingChannel remotingChannel,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        return this.snodeController.getEnodeService().lockBatchMQ(remotingChannel, request);
    }

    public RemotingCommand unlockBatchMQ(RemotingChannel remotingChannel,
        RemotingCommand request) throws InterruptedException, RemotingTimeoutException,
        RemotingSendRequestException, RemotingConnectException {
        return this.snodeController.getEnodeService().unlockBatchMQ(remotingChannel, request);
    }
}

