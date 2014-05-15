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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;


/**
 * 处理客户端发送消息的请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class SendMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final static int DLQ_NUMS_PER_GROUP = 1;
    private final BrokerController brokerController;
    private final Random random = new Random(System.currentTimeMillis());
    private final SocketAddress storeHost;


    public SendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                    .getNettyServerConfig().getListenPort());
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
        case RequestCode.SEND_MESSAGE:
            return this.sendMessage(ctx, request);
        case RequestCode.CONSUMER_SEND_MSG_BACK:
            return this.consumerSendMsgBack(ctx, request);
        default:
            break;
        }
        return null;
    }


    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumerSendMsgBackRequestHeader requestHeader =
                (ConsumerSendMsgBackRequestHeader) request
                    .decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        // 确保订阅组存在
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                    requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        // 如果重试队列数目为0，则直接丢弃消息
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        int queueIdInt =
                Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
                    newTopic,//
                    subscriptionGroupConfig.getRetryQueueNums(), //
                    PermName.PERM_WRITE | PermName.PERM_READ);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        // 检查topic权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the topic[" + newTopic + "] sending message is forbidden");
            return response;
        }

        // 查询消息，这里如果堆积消息过多，会访问磁盘
        // 另外如果频繁调用，是否会引起gc问题，需要关注 TODO
        MessageExt msgExt =
                this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        // 构造消息
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        // 客户端自动决定定时级别
        int delayLevel = requestHeader.getDelayLevel();

        // 死信消息处理
        if (msgExt.getReconsumeTimes() >= subscriptionGroupConfig.getRetryMaxTimes()//
                || delayLevel < 0) {
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig =
                    this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic, //
                        DLQ_NUMS_PER_GROUP,//
                        PermName.PERM_WRITE);
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
        }
        // 继续重试
        else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                // 统计
                this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(),
                    msgExt.getTopic(), 1);

                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);

                return response;
            default:
                break;
            }

            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(putMessageResult.getPutMessageStatus().name());
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("putMessageResult is null");
        return response;
    }


    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis = this.brokerController.getMessageStoreConfig().getStorePathConsumeQueue();
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = this.brokerController.getMessageStoreConfig().getStorePathIndex();
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }


    private RemotingCommand sendMessage(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader =
                (SendMessageResponseHeader) response.getCustomHeader();
        final SendMessageRequestHeader requestHeader =
                (SendMessageRequestHeader) request.decodeCommandCustomHeader(SendMessageRequestHeader.class);

        // 由于有直接返回的逻辑，所以必须要设置
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive SendMessage request command, " + request);
        }

        // 检查Broker权限
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return response;
        }

        final byte[] body = request.getBody();

        // Topic名字是否与保留字段冲突
        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: "
                    + ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(//
                requestHeader.getTopic(), //
                requestHeader.getDefaultTopic(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                requestHeader.getDefaultTopicQueueNums());

            // 尝试看下是否是失败消息发回
            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                                requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ);
                }
            }

            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        /**
         * Broker本身不做Topic的权限验证，由Name Server负责通知Client处理
         */
        // // 检查topic权限
        // if (!PermName.isWriteable(topicConfig.getPerm())) {
        // response.setCode(ResponseCode.NO_PERMISSION);
        // response.setRemark("the topic[" + requestHeader.getTopic() +
        // "] sending message is forbidden");
        // return response;
        // }

        // 检查队列有效性
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt >= topicConfig.getWriteQueueNums()) {
            String errorInfo =
                    "queueId[" + queueIdInt + "] is illagal, topicConfig.writeQueueNums: "
                            + topicConfig.getWriteQueueNums() + " producer: " + ctx.channel().remoteAddress();
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);
            return response;
        }

        // 随机指定一个队列
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        int sysFlag = requestHeader.getSysFlag();
        // 多标签过滤需要置位
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MultiTagsFlag;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
            MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());

        msgInner.setReconsumeTimes(0);

        // 检查事务消息
        if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
            String traFlag = msgInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            if (traFlag != null) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");
                return response;
            }
        }

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            boolean sendOK = false;

            switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create maped file failed, please make sure OS and JDK both 64bit.");
                break;
            case MESSAGE_ILLEGAL:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark("the message is illegal, maybe length not matched.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark("service not available now, maybe disk full, " + diskUtil()
                        + ", maybe your broker machine memory too small.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
            }

            if (sendOK) {
                // 统计
                this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
                this.brokerController.getBrokerStatsManager().incBrokerPutNums();

                response.setRemark(null);

                responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
                responseHeader.setQueueId(queueIdInt);
                responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

                // 直接返回
                if (!request.isOnewayRPC()) {
                    try {
                        ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    log.error("SendMessageProcessor response to "
                                            + future.channel().remoteAddress() + " failed", future.cause());
                                    log.error(request.toString());
                                    log.error(response.toString());
                                }
                            }
                        });
                    }
                    catch (Throwable e) {
                        log.error("SendMessageProcessor process request over, but response failed", e);
                        log.error(request.toString());
                        log.error(response.toString());
                    }
                }

                this.brokerController.getPullRequestHoldService().notifyMessageArriving(
                    requestHeader.getTopic(), queueIdInt,
                    putMessageResult.getAppendMessageResult().getLogicsOffset());
                return null;
            }
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }

        return response;
    }


    public SocketAddress getStoreHost() {
        return storeHost;
    }
}
