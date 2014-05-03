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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.broker.client.ConsumerGroupInfo;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.admin.TopicOffset;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.GroupList;
import com.alibaba.rocketmq.common.protocol.body.KVTable;
import com.alibaba.rocketmq.common.protocol.body.LockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.body.LockBatchResponseBody;
import com.alibaba.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import com.alibaba.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetAllTopicConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;
import com.alibaba.rocketmq.store.DefaultMessageStore;


/**
 * 管理类请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<manhong.yqd@taobao.com>
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
        switch (request.getCode()) {
        // 更新创建Topic
        case RequestCode.UPDATE_AND_CREATE_TOPIC:
            return this.updateAndCreateTopic(ctx, request);
            // 删除Topic
        case RequestCode.DELETE_TOPIC_IN_BROKER:
            return this.deleteTopic(ctx, request);
            // 获取Topic配置
        case RequestCode.GET_ALL_TOPIC_CONFIG:
            return this.getAllTopicConfig(ctx, request);

            // 更新Broker配置 TODO 可能存在并发问题
        case RequestCode.UPDATE_BROKER_CONFIG:
            return this.updateBrokerConfig(ctx, request);
            // 获取Broker配置
        case RequestCode.GET_BROKER_CONFIG:
            return this.getBrokerConfig(ctx, request);

            // 根据时间查询Offset
        case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
            return this.searchOffsetByTimestamp(ctx, request);
        case RequestCode.GET_MAX_OFFSET:
            return this.getMaxOffset(ctx, request);
        case RequestCode.GET_MIN_OFFSET:
            return this.getMinOffset(ctx, request);
        case RequestCode.GET_EARLIEST_MSG_STORETIME:
            return this.getEarliestMsgStoretime(ctx, request);

            // 更新Consumer Offset
        case RequestCode.UPDATE_CONSUMER_OFFSET:
            return this.updateConsumerOffset(ctx, request);
        case RequestCode.QUERY_CONSUMER_OFFSET:
            return this.queryConsumerOffset(ctx, request);

            // 获取Broker运行时信息
        case RequestCode.GET_BROKER_RUNTIME_INFO:
            return this.getBrokerRuntimeInfo(ctx, request);

            // 锁队列与解锁队列
        case RequestCode.LOCK_BATCH_MQ:
            return this.lockBatchMQ(ctx, request);
        case RequestCode.UNLOCK_BATCH_MQ:
            return this.unlockBatchMQ(ctx, request);

            // 订阅组配置
        case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
            return this.updateAndCreateSubscriptionGroup(ctx, request);
        case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
            return this.getAllSubscriptionGroup(ctx, request);
        case RequestCode.DELETE_SUBSCRIPTIONGROUP:
            return this.deleteSubscriptionGroup(ctx, request);

            // 统计信息，获取Topic统计信息
        case RequestCode.GET_TOPIC_STATS_INFO:
            return this.getTopicStatsInfo(ctx, request);

            // Consumer连接管理
        case RequestCode.GET_CONSUMER_CONNECTION_LIST:
            return this.getConsumerConnectionList(ctx, request);
            // Producer连接管理
        case RequestCode.GET_PRODUCER_CONNECTION_LIST:
            return this.getProducerConnectionList(ctx, request);

            // 查询消费进度，订阅组下的所有Topic
        case RequestCode.GET_CONSUME_STATS:
            return this.getConsumeStats(ctx, request);
        case RequestCode.GET_ALL_CONSUMER_OFFSET:
            return this.getAllConsumerOffset(ctx, request);

            // 定时进度
        case RequestCode.GET_ALL_DELAY_OFFSET:
            return this.getAllDelayOffset(ctx, request);

            // 调用客户端重置 offset
        case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
            return this.resetOffset(ctx, request);

            // 调用客户端订阅消息处理
        case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
            return this.getConsumerStatus(ctx, request);

            // 查询Topic被哪些消费者消费
        case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
            return this.queryTopicConsumeByWho(ctx, request);

        case RequestCode.REGISTER_FILTER_SERVER:
            return this.registerFilterServer(ctx, request);
        default:
            break;

        }

        return null;
    }


    private RemotingCommand registerFilterServer(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(RegisterFilterServerResponseHeader.class);
        final RegisterFilterServerResponseHeader responseHeader =
                (RegisterFilterServerResponseHeader) response.getCustomHeader();
        final RegisterFilterServerRequestHeader requestHeader =
                (RegisterFilterServerRequestHeader) request
                    .decodeCommandCustomHeader(RegisterFilterServerRequestHeader.class);

        this.brokerController.getFilterServerManager().registerFilterServer(ctx.channel(),
            requestHeader.getFilterServerAddr());

        responseHeader.setBrokerId(this.brokerController.getBrokerConfig().getBrokerId());
        responseHeader.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getConsumeStats(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumeStatsRequestHeader requestHeader =
                (GetConsumeStatsRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumeStatsRequestHeader.class);

        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics =
                this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(
                    requestHeader.getConsumerGroup());

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("consumeStats, topic config not exist, {}", topic);
                continue;
            }

            SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(
                        requestHeader.getConsumerGroup(), topic);
            if (null == findSubscriptionData) {
                log.warn("consumeStats, the consumer group[{}], topic[{}] not exist",
                    requestHeader.getConsumerGroup(), topic);
                continue;
            }

            for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
                if (brokerOffset < 0)
                    brokerOffset = 0;

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                    requestHeader.getConsumerGroup(),//
                    topic,//
                    i);
                if (consumerOffset < 0)
                    consumerOffset = 0;

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);

                // 查询消费者最后一条消息对应的时间戳
                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp =
                            this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i,
                                timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            long consumeTps =
                    this.brokerController.getConsumerOffsetManager().computePullTPS(topic,
                        requestHeader.getConsumerGroup());

            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
                (GetProducerConnectionListRequestHeader) request
                    .decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        ConsumerConnection bodydata = new ConsumerConnection();
        HashMap<Channel, ClientChannelInfo> channelInfoHashMap =
                this.brokerController.getProducerManager().getGroupChannelTable()
                    .get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }


    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
                (GetConsumerConnectionListRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(
                    requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            Iterator<Map.Entry<Channel, ClientChannelInfo>> it =
                    consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }


    private RemotingCommand getTopicStatsInfo(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicStatsInfoRequestHeader requestHeader =
                (GetTopicStatsInfoRequestHeader) request
                    .decodeCommandCustomHeader(GetTopicStatsInfoRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQuque(topic, i);
            if (min < 0)
                min = 0;

            long max = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
            if (max < 0)
                max = 0;

            long timestamp = 0;
            if (max > 0) {
                timestamp =
                        this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, (max - 1));
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
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

        response.setCode(ResponseCode.SUCCESS);
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

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No subscription group in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
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
        response.setCode(ResponseCode.SUCCESS);
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

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
                (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
        log.info("updateAndCreateTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        // Topic名字是否与保留字段冲突
        if (requestHeader.getTopic().equals(this.brokerController.getBrokerConfig().getBrokerClusterName())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        topicConfig.setPerm(requestHeader.getPerm());

        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        response.setCode(ResponseCode.SUCCESS);
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

        response.setCode(ResponseCode.SUCCESS);
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

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No topic in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
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
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            }
            catch (UnsupportedEncodingException e) {
                log.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
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

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        responseHeader.setVersion(this.brokerController.getConfigDataVersion());

        response.setCode(ResponseCode.SUCCESS);
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

        response.setCode(ResponseCode.SUCCESS);
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

        response.setCode(ResponseCode.SUCCESS);
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
        response.setCode(ResponseCode.SUCCESS);
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
        response.setCode(ResponseCode.SUCCESS);
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
        response.setCode(ResponseCode.SUCCESS);
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

        // 订阅组存在
        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        }
        // 订阅组不存在
        else {
            long minOffset =
                    this.brokerController.getMessageStore().getMinOffsetInQuque(requestHeader.getTopic(),
                        requestHeader.getQueueId());
            // 订阅组不存在情况下，如果这个队列的消息最小Offset是0，则表示这个Topic上线时间不长，服务器堆积的数据也不多，那么这个订阅组就从0开始消费。
            // 尤其对于Topic队列数动态扩容时，必须要从0开始消费。
            if (minOffset <= 0) {
                responseHeader.setOffset(0L);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
            }
            // 新版本服务器不做消费进度纠正
            else {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return response;
    }


    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CurrentVersion));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CurrentVersion));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("sendThreadPoolQueueSize",
            String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));

        runtimeInfo.put("sendThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        return runtimeInfo;
    }


    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        byte[] body = kvTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getAllConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = this.brokerController.getConsumerOffsetManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            }
            catch (UnsupportedEncodingException e) {
                log.error("get all consumer offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No consumer offset in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No consumer offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }


    private RemotingCommand getAllDelayOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content =
                ((DefaultMessageStore) this.brokerController.getMessageStore()).getScheduleMessageService()
                    .encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            }
            catch (UnsupportedEncodingException e) {
                log.error("get all delay offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        else {
            log.error("No delay offset in this broker, client: " + ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No delay offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }


    private RemotingCommand deleteSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteSubscriptionGroupRequestHeader requestHeader =
                (DeleteSubscriptionGroupRequestHeader) request
                    .decodeCommandCustomHeader(DeleteSubscriptionGroupRequestHeader.class);

        log.info("deleteSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(
            requestHeader.getGroupName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand resetOffset(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
                (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
            new Object[] { RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                          requestHeader.getGroup(), requestHeader.getTimestamp(), requestHeader.isForce() });
        return this.brokerController.getBroker2Client().resetOffset(requestHeader.getTopic(),
            requestHeader.getGroup(), requestHeader.getTimestamp(), requestHeader.isForce());
    }


    public RemotingCommand getConsumerStatus(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final GetConsumerStatusRequestHeader requestHeader =
                (GetConsumerStatusRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        log.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
            new Object[] { RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                          requestHeader.getGroup() });

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(),
            requestHeader.getGroup(), requestHeader.getClientAddr());
    }


    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryTopicConsumeByWhoRequestHeader requestHeader =
                (QueryTopicConsumeByWhoRequestHeader) request
                    .decodeCommandCustomHeader(QueryTopicConsumeByWhoRequestHeader.class);

        HashSet<String> groups =
                this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
