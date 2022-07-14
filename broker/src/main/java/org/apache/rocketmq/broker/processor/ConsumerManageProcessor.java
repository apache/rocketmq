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
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.rpc.RpcClientUtils;
import org.apache.rocketmq.common.rpc.RpcRequest;
import org.apache.rocketmq.common.rpc.RpcResponse;
import org.apache.rocketmq.common.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingContext;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

import static org.apache.rocketmq.remoting.protocol.RemotingCommand.buildErrorResponse;

public class ConsumerManageProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
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
                LOGGER.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            LOGGER.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    public  RemotingCommand rewriteRequestForStaticTopic(final UpdateConsumerOffsetRequestHeader requestHeader, final TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", requestHeader.getTopic(), requestHeader.getQueueId(), mappingDetail.getBname()));
            }
            Long globalOffset = requestHeader.getCommitOffset();
            LogicQueueMappingItem mappingItem = TopicQueueMappingUtils.findLogicQueueMappingItem(mappingContext.getMappingItemList(), globalOffset, true);
            requestHeader.setQueueId(mappingItem.getQueueId());
            requestHeader.setLo(false);
            requestHeader.setBname(mappingItem.getBname());
            requestHeader.setCommitOffset(mappingItem.computePhysicalQueueOffset(globalOffset));
            //leader, let it go, do not need to rewrite the response
            if (mappingDetail.getBname().equals(mappingItem.getBname())) {
                return null;
            }
            RpcRequest rpcRequest = new RpcRequest(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader, null);
            RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
            if (rpcResponse.getException() != null) {
                throw rpcResponse.getException();
            }
            return RpcClientUtils.createCommandForRpcResponse(rpcResponse);
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }


    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetRequestHeader requestHeader =
            (UpdateConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader);

        RemotingCommand rewriteResult  =  rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }
        Set<String> topicSets = this.brokerController.getTopicConfigManager().getTopicConfigTable().keySet();
        if (topicSets.contains(requestHeader.getTopic())) {
            this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("Topic " + requestHeader.getTopic() + " not exist!");
        }

        return response;
    }


    public  RemotingCommand rewriteRequestForStaticTopic(QueryConsumerOffsetRequestHeader requestHeader, TopicQueueMappingContext mappingContext) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            TopicQueueMappingDetail mappingDetail = mappingContext.getMappingDetail();
            if (!mappingContext.isLeader()) {
                return buildErrorResponse(ResponseCode.NOT_LEADER_FOR_QUEUE, String.format("%s-%d does not exit in request process of current broker %s", requestHeader.getTopic(), requestHeader.getQueueId(), mappingDetail.getBname()));
            }
            List<LogicQueueMappingItem> mappingItemList = mappingContext.getMappingItemList();
            if (mappingItemList.size() == 1
                    &&  mappingItemList.get(0).getLogicOffset() == 0) {
                //as physical, just let it go
                mappingContext.setCurrentItem(mappingItemList.get(0));
                requestHeader.setQueueId(mappingContext.getLeaderItem().getQueueId());
                return null;
            }
            //double read check
            List<LogicQueueMappingItem> itemList = mappingContext.getMappingItemList();
            //by default, it is -1
            long offset = -1;
            //double read, first from leader, then from second leader
            for (int i = itemList.size() - 1; i >= 0; i--) {
                LogicQueueMappingItem mappingItem = itemList.get(i);
                mappingContext.setCurrentItem(mappingItem);
                if (mappingItem.getBname().equals(mappingDetail.getBname())) {
                    offset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(), requestHeader.getTopic(), mappingItem.getQueueId());
                    if (offset >= 0) {
                        break;
                    } else {
                        //not found
                        continue;
                    }
                } else {
                    //maybe we need to reconstruct an object
                    requestHeader.setBname(mappingItem.getBname());
                    requestHeader.setQueueId(mappingItem.getQueueId());
                    requestHeader.setLo(false);
                    requestHeader.setSetZeroIfNotFound(false);
                    RpcRequest rpcRequest = new RpcRequest(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader, null);
                    RpcResponse rpcResponse = this.brokerController.getBrokerOuterAPI().getRpcClient().invoke(rpcRequest, this.brokerController.getBrokerConfig().getForwardTimeout()).get();
                    if (rpcResponse.getException() != null) {
                        throw rpcResponse.getException();
                    }
                    if (rpcResponse.getCode() == ResponseCode.SUCCESS) {
                        offset = ((QueryConsumerOffsetResponseHeader) rpcResponse.getHeader()).getOffset();
                        break;
                    } else if (rpcResponse.getCode() == ResponseCode.QUERY_NOT_FOUND) {
                        continue;
                    } else {
                        //this should not happen
                        throw new RuntimeException("Unknown response code " + rpcResponse.getCode());
                    }
                }
            }
            final RemotingCommand response = RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
            final QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.readCustomHeader();
            if (offset >= 0) {
                responseHeader.setOffset(offset);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            } else {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, maybe this group consumer boot first");
            }
            RemotingCommand rewriteResponseResult = rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, response.getCode());
            if (rewriteResponseResult != null) {
                return rewriteResponseResult;
            }
            return response;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
    }


    public  RemotingCommand rewriteResponseForStaticTopic(final QueryConsumerOffsetRequestHeader requestHeader, final QueryConsumerOffsetResponseHeader responseHeader,
        final TopicQueueMappingContext mappingContext, final int code) {
        try {
            if (mappingContext.getMappingDetail() == null) {
                return null;
            }
            if (code != ResponseCode.SUCCESS) {
                return null;
            }
            LogicQueueMappingItem item = mappingContext.getCurrentItem();
            responseHeader.setOffset(item.computeStaticQueueOffsetStrictly(responseHeader.getOffset()));
            //no need to construct new object
            return null;
        } catch (Throwable t) {
            return buildErrorResponse(ResponseCode.SYSTEM_ERROR, t.getMessage());
        }
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


        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader);
        RemotingCommand rewriteResult  = rewriteRequestForStaticTopic(requestHeader, mappingContext);
        if (rewriteResult != null) {
            return rewriteResult;
        }

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
            if (requestHeader.getSetZeroIfNotFound() != null && Boolean.FALSE.equals(requestHeader.getSetZeroIfNotFound())) {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, do not set to zero, maybe this group boot first");
            } else if (minOffset <= 0
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

        RemotingCommand rewriteResponseResult = rewriteResponseForStaticTopic(requestHeader, responseHeader, mappingContext, response.getCode());
        if (rewriteResponseResult != null) {
            return rewriteResponseResult;
        }

        return response;
    }
}
