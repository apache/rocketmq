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
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.body.LockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.body.LockBatchResponseBody;
import com.alibaba.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetAllTopicConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * 管理类请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        // 更新创建Topic
        case UPDATE_AND_CREATE_TOPIC:
            return this.updateAndCreateTopic(ctx, request);
            // 删除Topic
        case DELETE_TOPIC:
            return this.deleteTopic(ctx, request);
            // 获取Topic配置
        case GET_ALL_TOPIC_CONFIG:
            return this.getAllTopicConfig(ctx, request);

            // 更新Broker配置 TODO 可能存在并发问题
        case UPDATE_BROKER_CONFIG:
            return this.updateBrokerConfig(ctx, request);
            // 获取Broker配置
        case GET_BROKER_CONFIG:
            return this.getBrokerConfig(ctx, request);

            // 根据时间查询Offset
        case SEARCH_OFFSET_BY_TIMESTAMP:
            return this.searchOffsetByTimestamp(ctx, request);
        case GET_MAX_OFFSET:
            return this.getMaxOffset(ctx, request);
        case GET_MIN_OFFSET:
            return this.getMinOffset(ctx, request);
        case GET_EARLIEST_MSG_STORETIME:
            return this.getEarliestMsgStoretime(ctx, request);

            // 更新Consumer Offset
        case UPDATE_CONSUMER_OFFSET:
            return this.updateConsumerOffset(ctx, request);
        case QUERY_CONSUMER_OFFSET:
            return this.queryConsumerOffset(ctx, request);

            // 获取Broker运行时信息
        case GET_BROKER_RUNTIME_INFO:
            break;

        // 锁队列与解锁队列
        case LOCK_BATCH_MQ:
            return this.lockBatchMQ(ctx, request);
        case UNLOCK_BATCH_MQ:
            return this.unlockBatchMQ(ctx, request);
            // 订阅组配置
        case UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
            return this.updateAndCreateSubscriptionGroup(ctx, request);
        case GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
            return this.getAllSubscriptionGroup(ctx, request);
        default:
            break;
        }

        return null;
    }


    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx,
            RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateAndCreateSubscriptionGroup called by {}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config =
                RemotingSerializable.decode(request.getBody(), SubscriptionGroupConfig.class);
        if (config != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getAllSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        String content = this.brokerController.getSubscriptionGroupManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            }
            catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No subscription group in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);

        return response;
    }


    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody =
                LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        Set<MessageQueue> lockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(//
            requestBody.getConsumerGroup(),//
            requestBody.getMqSet(),//
            requestBody.getClientId());

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody =
                UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);

        this.brokerController.getRebalanceLockManager().unlockBatch(//
            requestBody.getConsumerGroup(),//
            requestBody.getMqSet(),//
            requestBody.getClientId());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
                (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);

        log.info("updateAndCreateTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        topicConfig.setPerm(requestHeader.getPerm());

        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand deleteTopic(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteTopicRequestHeader requestHeader =
                (DeleteTopicRequestHeader) request.decodeCommandCustomHeader(DeleteTopicRequestHeader.class);

        log.info("deleteTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getTopicConfigManager().deleteTopicConfig(requestHeader.getTopic());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getAllTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetAllTopicConfigResponseHeader.class);
        final GetAllTopicConfigResponseHeader responseHeader =
                (GetAllTopicConfigResponseHeader) response.getCustomHeader();

        String content = this.brokerController.getTopicConfigManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            }
            catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No topic in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);

        return response;
    }


    private RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateBrokerConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    log.info("updateBrokerConfig, new config: " + properties + " client: "
                            + ctx.channel().remoteAddress());
                    this.brokerController.updateAllConfig(properties);
                }
                else {
                    log.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                    response.setRemark("string2Properties error");
                    return response;
                }
            }
            catch (UnsupportedEncodingException e) {
                log.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader =
                (GetBrokerConfigResponseHeader) response.getCustomHeader();

        String content = this.brokerController.encodeAllConfig();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            }
            catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfigDataVersion());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
        final SearchOffsetResponseHeader responseHeader =
                (SearchOffsetResponseHeader) response.getCustomHeader();
        final SearchOffsetRequestHeader requestHeader =
                (SearchOffsetRequestHeader) request
                    .decodeCommandCustomHeader(SearchOffsetRequestHeader.class);

        long offset =
                this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(),
                    requestHeader.getQueueId(), requestHeader.getTimestamp());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader =
                (GetMaxOffsetResponseHeader) response.getCustomHeader();
        final GetMaxOffsetRequestHeader requestHeader =
                (GetMaxOffsetRequestHeader) request
                    .decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        long offset =
                this.brokerController.getMessageStore().getMaxOffsetInQuque(requestHeader.getTopic(),
                    requestHeader.getQueueId());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getMinOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
        final GetMinOffsetResponseHeader responseHeader =
                (GetMinOffsetResponseHeader) response.getCustomHeader();
        final GetMinOffsetRequestHeader requestHeader =
                (GetMinOffsetRequestHeader) request
                    .decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);

        long offset =
                this.brokerController.getMessageStore().getMinOffsetInQuque(requestHeader.getTopic(),
                    requestHeader.getQueueId());

        responseHeader.setOffset(offset);
        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getEarliestMsgStoretime(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
        final GetEarliestMsgStoretimeResponseHeader responseHeader =
                (GetEarliestMsgStoretimeResponseHeader) response.getCustomHeader();
        final GetEarliestMsgStoretimeRequestHeader requestHeader =
                (GetEarliestMsgStoretimeRequestHeader) request
                    .decodeCommandCustomHeader(GetEarliestMsgStoretimeRequestHeader.class);

        long timestamp =
                this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(),
                    requestHeader.getQueueId());

        responseHeader.setTimestamp(timestamp);
        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        final UpdateConsumerOffsetResponseHeader responseHeader =
                (UpdateConsumerOffsetResponseHeader) response.getCustomHeader();
        final UpdateConsumerOffsetRequestHeader requestHeader =
                (UpdateConsumerOffsetRequestHeader) request
                    .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);

        this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
                (QueryConsumerOffsetResponseHeader) response.getCustomHeader();
        final QueryConsumerOffsetRequestHeader requestHeader =
                (QueryConsumerOffsetRequestHeader) request
                    .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        long offset =
                this.brokerController.getConsumerOffsetManager().queryOffset(
                    requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
        }
        else {
            response.setCode(MQResponseCode.QUERY_NOT_FOUND_VALUE);
            response.setRemark("Not found, maybe this group consumer boot first");
        }

        return response;
    }
}
