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
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.PopAckConstants;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.utils.DataConverter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.pop.AckMsg;

public class AckMessageProcessor implements NettyRequestProcessor {
    private static final InternalLogger POP_LOGGER = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_POP_LOGGER_NAME);
    private final BrokerController brokerController;
    private String reviveTopic;
    private PopReviveService[] popReviveServices;

    public AckMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.reviveTopic = PopAckConstants.buildClusterReviveTopic(this.brokerController.getBrokerConfig().getBrokerClusterName());
        this.popReviveServices = new PopReviveService[this.brokerController.getBrokerConfig().getReviveQueueNum()];
        for (int i = 0; i < this.brokerController.getBrokerConfig().getReviveQueueNum(); i++) {
            this.popReviveServices[i] = new PopReviveService(brokerController, reviveTopic, i);
            this.popReviveServices[i].setShouldRunPopRevive(brokerController.getBrokerConfig().getBrokerId() == 0);
        }
    }

    public void startPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.start();
        }
    }

    public void shutdownPopReviveService() {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.stop();
        }
    }

    public void setPopReviveServiceStatus(boolean shouldStart) {
        for (PopReviveService popReviveService : popReviveServices) {
            popReviveService.setShouldRunPopRevive(shouldStart);
        }
    }

    public boolean isPopReviveServiceRunning() {
        for (PopReviveService popReviveService : popReviveServices) {
            if (popReviveService.isShouldRunPopRevive()) {
                return true;
            }
        }

        return false;
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
        final AckMessageRequestHeader requestHeader = (AckMessageRequestHeader) request.decodeCommandCustomHeader(AckMessageRequestHeader.class);
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        AckMsg ackMsg = new AckMsg();
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            POP_LOGGER.error("The topic {} not exist, consumer: {} ", requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(channel));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic[%s] not exist, apply first please! %s", requestHeader.getTopic(), FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL)));
            return response;
        }

        if (requestHeader.getQueueId() >= topicConfig.getReadQueueNums() || requestHeader.getQueueId() < 0) {
            String errorInfo = String.format("queueId[%d] is illegal, topic:[%s] topicConfig.readQueueNums:[%d] consumer:[%s]",
                requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNums(), channel.remoteAddress());
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark(errorInfo);
            return response;
        }
        long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());
        if (requestHeader.getOffset() < minOffset || requestHeader.getOffset() > maxOffset) {
            String errorInfo = String.format("offset is illegal, key:%s@%d, commit:%d, store:%d~%d",
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getOffset(), minOffset, maxOffset);
            POP_LOGGER.warn(errorInfo);
            response.setCode(ResponseCode.NO_MESSAGE);
            response.setRemark(errorInfo);
            return response;
        }
        String[] extraInfo = ExtraInfoUtil.split(requestHeader.getExtraInfo());

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

        if (rqId == KeyBuilder.POP_ORDER_REVIVE_QUEUE) {
            // order
            String lockKey = requestHeader.getTopic() + PopAckConstants.SPLIT
                + requestHeader.getConsumerGroup() + PopAckConstants.SPLIT + requestHeader.getQueueId();
            long oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId());
            if (requestHeader.getOffset() < oldOffset) {
                return response;
            }
            while (!this.brokerController.getPopMessageProcessor().getQueueLockManager().tryLock(lockKey)) {
            }
            try {
                oldOffset = this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId());
                if (requestHeader.getOffset() < oldOffset) {
                    return response;
                }
                long nextOffset = brokerController.getConsumerOrderInfoManager().commitAndNext(
                    requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                    requestHeader.getQueueId(), requestHeader.getOffset());
                if (nextOffset > -1) {
                    this.brokerController.getConsumerOffsetManager().commitOffset(channel.remoteAddress().toString(),
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                        requestHeader.getQueueId(),
                        nextOffset);
                    this.brokerController.getPopMessageProcessor().notifyMessageArriving(requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                        requestHeader.getQueueId());
                } else if (nextOffset == -1) {
                    String errorInfo = String.format("offset is illegal, key:%s, old:%d, commit:%d, next:%d, %s",
                        lockKey, oldOffset, requestHeader.getOffset(), nextOffset, channel.remoteAddress());
                    POP_LOGGER.warn(errorInfo);
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark(errorInfo);
                    return response;
                }
            } finally {
                this.brokerController.getPopMessageProcessor().getQueueLockManager().unLock(lockKey);
            }
            return response;
        }

        if (this.brokerController.getPopMessageProcessor().getPopBufferMergeService().addAk(rqId, ackMsg)) {
            return response;
        }

        msgInner.setTopic(reviveTopic);
        msgInner.setBody(JSON.toJSONString(ackMsg).getBytes(DataConverter.charset));
        //msgInner.setQueueId(Integer.valueOf(extraInfo[3]));
        msgInner.setQueueId(rqId);
        msgInner.setTags(PopAckConstants.ACK_TAG);
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.brokerController.getStoreHost());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        msgInner.setDeliverTimeMs(ExtraInfoUtil.getPopTime(extraInfo) + ExtraInfoUtil.getInvisibleTime(extraInfo));
        msgInner.getProperties().put(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PopMessageProcessor.genAckUniqueId(ackMsg));
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        PutMessageResult putMessageResult = this.brokerController.getEscapeBridge().putMessageToSpecificQueue(msgInner);
        if (putMessageResult.getPutMessageStatus() != PutMessageStatus.PUT_OK
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_DISK_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.FLUSH_SLAVE_TIMEOUT
            && putMessageResult.getPutMessageStatus() != PutMessageStatus.SLAVE_NOT_AVAILABLE) {
            POP_LOGGER.error("put ack msg error:" + putMessageResult);
        }
        return response;
    }

}
