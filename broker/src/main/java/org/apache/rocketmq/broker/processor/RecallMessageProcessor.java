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
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.timer.TimerMessageStore;

import java.nio.charset.StandardCharsets;

public class RecallMessageProcessor implements NettyRequestProcessor {
    private static final String RECALL_MESSAGE_TAG = "_RECALL_TAG_";
    private final BrokerController brokerController;

    public RecallMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
            RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RecallMessageResponseHeader.class);
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        final RecallMessageRequestHeader requestHeader =
            request.decodeCommandCustomHeader(RecallMessageRequestHeader.class);

        if (!brokerController.getBrokerConfig().isRecallMessageEnable()) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("recall failed, operation is forbidden");
            return response;
        }

        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            response.setRemark("recall failed, broker service not available");
            return response;
        }

        final long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
            response.setRemark("recall failed, broker service not available");
            return response;
        }

        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && !this.brokerController.getBrokerConfig().isAllowRecallWhenBrokerNotWriteable()) {
            response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
            response.setRemark("recall failed, broker service not available");
            return response;
        }

        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("recall failed, the topic[" + requestHeader.getTopic() + "] not exist");
            return response;
        }

        RecallMessageHandle.HandleV1 handle;
        try {
            handle = (RecallMessageHandle.HandleV1) RecallMessageHandle.decodeHandle(requestHeader.getRecallHandle());
        } catch (DecoderException e) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark(e.getMessage());
            return response;
        }

        if (!requestHeader.getTopic().equals(handle.getTopic())) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("recall failed, topic not match");
            return response;
        }
        if (!brokerController.getBrokerConfig().getBrokerName().equals(handle.getBrokerName())) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("recall failed, broker service not available");
            return response;
        }

        long timestamp = NumberUtils.toLong(handle.getTimestampStr(), -1);
        long timeLeft = timestamp - System.currentTimeMillis();
        if (timeLeft <= 0
            || timeLeft >= brokerController.getMessageStoreConfig().getTimerMaxDelaySec() * 1000L) {
            response.setCode(ResponseCode.ILLEGAL_OPERATION);
            response.setRemark("recall failed, timestamp invalid");
            return response;
        }

        MessageExtBrokerInner msgInner = buildMessage(ctx, requestHeader, handle);
        long beginTimeMillis = this.brokerController.getMessageStore().now();
        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        handlePutMessageResult(putMessageResult, request, response, msgInner, ctx, beginTimeMillis);
        return response;
    }

    public MessageExtBrokerInner buildMessage(ChannelHandlerContext ctx, RecallMessageRequestHeader requestHeader,
        RecallMessageHandle.HandleV1 handle) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(handle.getTopic());
        msgInner.setBody("0".getBytes(StandardCharsets.UTF_8));
        msgInner.setTags(RECALL_MESSAGE_TAG);
        msgInner.setTagsCode(RECALL_MESSAGE_TAG.hashCode());
        msgInner.setQueueId(0);
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TIMER_DEL_UNIQKEY,
            TimerMessageStore.buildDeleteKey(handle.getTopic(), handle.getMessageId()));
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TIMER_DEL_MS, String.valueOf(handle.getDelaytime()));
        MessageAccessor.putProperty(msgInner,
            MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, handle.getMessageId());
        MessageAccessor.putProperty(msgInner,
            MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(handle.getTimestampStr()));
        MessageAccessor.putProperty(msgInner,
            MessageConst.PROPERTY_BORN_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRACE_CONTEXT, "");
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_PRODUCER_GROUP, requestHeader.getProducerGroup());
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.brokerController.getStoreHost());
        return msgInner;
    }

    public void handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand request,
        RemotingCommand response, MessageExt message, ChannelHandlerContext ctx, long beginTimeMillis) {
        if (null == putMessageResult) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("recall failed, execute error");
            return;
        }
        RecallMessageResponseHeader responseHeader = (RecallMessageResponseHeader) response.readCustomHeader();
        switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                this.brokerController.getBrokerStatsManager().incTopicPutNums(
                    message.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1); // system timer topic
                this.brokerController.getBrokerStatsManager().incTopicPutSize(
                    message.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
                this.brokerController.getBrokerStatsManager().incBrokerPutNums(
                    message.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum());
                this.brokerController.getBrokerStatsManager().incTopicPutLatency(
                    message.getTopic(), 0, (int) (this.brokerController.getMessageStore().now() - beginTimeMillis));
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SUCCESS);
                responseHeader.setMsgId(MessageClientIDSetter.getUniqID(message));
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("recall failed, execute error");
                break;
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
