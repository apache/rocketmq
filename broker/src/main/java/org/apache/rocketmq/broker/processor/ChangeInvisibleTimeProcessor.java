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

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.metrics.PopMetricsManager;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRemotingAbstract;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.exception.ConsumeQueueException;
import org.apache.rocketmq.store.pop.AckMsg;
import org.apache.rocketmq.store.pop.PopCheckPoint;

public class ChangeInvisibleTimeProcessor implements NettyRequestProcessor {
    private static final Logger POP_LOGGER = LoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private final String reviveTopic;

    public ChangeInvisibleTimeProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {

        CompletableFuture<RemotingCommand> responseFuture = processRequestAsync(channel, request, brokerAllowSuspend);

        if (brokerController.getBrokerConfig().isAppendCkAsync() && brokerController.getBrokerConfig().isAppendAckAsync()) {
            responseFuture.thenAccept(response -> doResponse(channel, request, response)).exceptionally(throwable -> {
                RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                doResponse(channel, request, response);
                POP_LOGGER.error("append checkpoint or ack origin failed", throwable);
                return null;
            });
        } else {
            RemotingCommand response;
            try {
                response = responseFuture.get(3000, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setOpaque(request.getOpaque());
                POP_LOGGER.error("append checkpoint or ack origin failed", e);
            }
            return response;
        }
        return null;
    }

    public CompletableFuture<RemotingCommand> processRequestAsync(final Channel channel, RemotingCommand request,
        boolean brokerAllowSuspend) throws RemotingCommandException {
        final ChangeInvisibleTimeRequestHeader requestHeader = (ChangeInvisibleTimeRequestHeader) request.decodeCommandCustomHeader(ChangeInvisibleTimeRequestHeader.class);
        RemotingCommand response = RemotingCommand.createResponseCommand(ChangeInvisibleTimeResponseHeader.class);
        response.setCode(ResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());
        final ChangeInvisibleTimeResponseHeader responseHeader = (ChangeInvisibleTimeResponseHeader) response.readCustomHeader();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return CompletableFuture.completedFuture(response);
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return CompletableFuture.completedFuture(response);
        }
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset;
        try {
            maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        } catch (ConsumeQueueException e) {
            throw new RemotingCommandException("Failed to get max consume offset", e);
        }
        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            response.setCode(ResponseCode.NO_MESSAGE);
            return CompletableFuture.completedFuture(response);
        }

        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

        if (ExtraInfoUtil.isOrder(extraInfo)) {
            return CompletableFuture.completedFuture(processChangeInvisibleTimeForOrder(requestHeader, extraInfo, response, responseHeader));
        }

        // add new ck
        long now = System.currentTimeMillis();

        CompletableFuture<Boolean> futureResult = appendCheckPointThenAckOrigin(requestHeader, ExtraInfoUtil.getReviveQid(extraInfo), requestHeader.getQueueId(), requestHeader.getOffset(), now, extraInfo);
        return futureResult.thenCompose(result -> {
            if (result) {
                responseHeader.setInvisibleTime(requestHeader.getInvisibleTime());
                responseHeader.setPopTime(now);
                responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
            }
            return CompletableFuture.completedFuture(response);
        });
    }

    protected RemotingCommand processChangeInvisibleTimeForOrder(ChangeInvisibleTimeRequestHeader requestHeader,
        String[] extraInfo, RemotingCommand response, ChangeInvisibleTimeResponseHeader responseHeader) {
        long popTime = ExtraInfoUtil.getPopTime(extraInfo);
        long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
            requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < oldOffset) {
            return response;
        }
        while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId())) {
        }
        try {
            oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < oldOffset) {
                return response;
            }

            long nextVisibleTime = System.currentTimeMillis() + requestHeader.getInvisibleTime();
            this.brokerController.getConsumerOrderInfoManager().updateNextVisibleTime(
                requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId(), requestHeader.getOffset(), popTime, nextVisibleTime);

            responseHeader.setInvisibleTime(nextVisibleTime - popTime);
            responseHeader.setPopTime(popTime);
            responseHeader.setReviveQid(ExtraInfoUtil.getReviveQid(extraInfo));
        } finally {
            this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueId());
        }
        return response;
    }

    private CompletableFuture<Boolean> ackOrigin(final ChangeInvisibleTimeRequestHeader requestHeader,
        String[] extraInfo) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        AckMsg ackMsg = new AckMsg();

        ackMsg.setAckOffset(requestHeader.getOffset());
        ackMsg.setStartOffset(ExtraInfoUtil.getCkQueueOffset(extraInfo));
        ackMsg.setConsumerGroup(requestHeader.getConsumerGroup());
        ackMsg.setTopic(requestHeader.getTopic());
        ackMsg.setQueueId(requestHeader.getQueueId());
        ackMsg.setPopTime(ExtraInfoUtil.getPopTime(extraInfo));
        ackMsg.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));

        int rqId = ExtraInfoUtil.getReviveQid(extraInfo);

        this.brokerController.getBrokerStatsManager().incBrokerAckNums(1);
        this.brokerController.getBrokerStatsManager().incGroupAckNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);

        if (brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            return CompletableFuture.completedFuture(true);
        }

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(ExtraInfoUtil.getPopTime(extraInfo) + ExtraInfoUtil.getInvisibleTime(extraInfo));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenCompose(putMessageResult -> {
            if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
                POP_LOGGER.error("change Invisible, put ack msg fail: {}, {}", ackMsg, putMessageResult);
            }
            PopMetricsManager.incPopReviveAckPutCount(ackMsg, putMessageResult.getPutMessageStatus());
            return CompletableFuture.completedFuture(true);
        }).exceptionally(e -> {
            POP_LOGGER.error("change Invisible, put ack msg error: {}, {}", requestHeader.getExtraInfo(), e.getMessage());
            return false;
        });
    }

    private CompletableFuture<Boolean> appendCheckPointThenAckOrigin(
        final ChangeInvisibleTimeRequestHeader requestHeader,
        int reviveQid,
        int queueId, long offset, long popTime, String[] extraInfo) {
        // add check point msg to revive log
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(reviveTopic);
        PopCheckPoint ck = new PopCheckPoint();
        ck.setBitMap(0);
        ck.setNum((byte) 1);
        ck.setPopTime(popTime);
        ck.setInvisibleTime(requestHeader.getInvisibleTime());
        ck.setStartOffset(offset);
        ck.setCId(requestHeader.getConsumerGroup());
        ck.setTopic(requestHeader.getTopic());
        ck.setQueueId(queueId);
        ck.addDiff(0);
        ck.setBrokerName(ExtraInfoUtil.getBrokerName(extraInfo));

        msgInner.setBody(JSON.toJSONString(ck).getBytes(StandardCharsets.UTF_8));
        msgInner.setQueueId(reviveQid);
        msgInner.setTags(PopAckConstants.CK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(ck.getReviveTime() - PopAckConstants.ackTimeInterval);
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genCkUniqueId(ck));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return this.brokerController.getEscapeBridge().asyncPutMessageToSpecificQueue(msgInner).thenCompose(putMessageResult -> {
            if (brokerController.getBrokerConfig().isEnablePopLog()) {
                POP_LOGGER.info("change Invisible, appendCheckPoint, topic {}, queueId {},reviveId {}, cid {}, startOffset {}, rt {}, result {}", requestHeader.getTopic(), queueId, reviveQid, requestHeader.getConsumerGroup(), offset,
                    ck.getReviveTime(), putMessageResult);
            }

            if (putMessageResult != null) {
                PopMetricsManager.incPopReviveCkPutCount(ck, putMessageResult.getPutMessageStatus());
                if (putMessageResult.isOk()) {
                    this.brokerController.getBrokerStatsManager().incBrokerCkNums(1);
                    this.brokerController.getBrokerStatsManager().incGroupCkNums(requestHeader.getConsumerGroup(), requestHeader.getTopic(), 1);
                }
            }
            if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
                && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
                POP_LOGGER.error("change invisible, put new ck error: {}", putMessageResult);
                return CompletableFuture.completedFuture(false);
            } else {
                return ackOrigin(requestHeader, extraInfo);
            }
        }).exceptionally(throwable -> {
            POP_LOGGER.error("change invisible, put new ck error", throwable);
            return null;
        });
    }

    protected void doResponse(Channel channel, RemotingCommand request,
        final RemotingCommand response) {
        NettyRemotingAbstract.writeResponse(channel, request, response);
    }
}
