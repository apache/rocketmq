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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.DBMsgConstants;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.common.utils.ChannelUtil;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;


/**
 * 澶勭悊瀹㈡埛绔彂閫佹秷鎭殑璇锋眰
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    protected final SocketAddress storeHost;


    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                    .getNettyServerConfig().getListenPort());
    }


   

	protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader) {
		if (!this.hasSendMessageHook()) {
			return null;
		}
		SendMessageContext mqtraceContext;
		mqtraceContext = new SendMessageContext();
		mqtraceContext.setProducerGroup(requestHeader.getProducerGroup());
		mqtraceContext.setTopic(requestHeader.getTopic());
		mqtraceContext.setMsgProps(requestHeader.getProperties());
		mqtraceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
		mqtraceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
		return mqtraceContext;
	}
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {

        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
        case RequestCode.SEND_MESSAGE_V2:
            requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
        case RequestCode.SEND_MESSAGE:
            if (null == requestHeaderV2) {
                requestHeader =
                        (SendMessageRequestHeader) request
                            .decodeCommandCustomHeader(SendMessageRequestHeader.class);
            }
            else {
                requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
            }
        default:
            break;
        }
        return requestHeader;
	}

    protected MessageExtBrokerInner buildInnerMsg(
			final ChannelHandlerContext ctx,
			final SendMessageRequestHeader requestHeader, final byte[] body,
			TopicConfig topicConfig) {
        int queueIdInt=requestHeader.getQueueId();
		// 闅忔満鎸囧畾涓�涓槦鍒�
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }
		int sysFlag = requestHeader.getSysFlag();
		
        // 澶氭爣绛捐繃婊ら渶瑕佺疆浣�
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
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
            .getReconsumeTimes());
		return msgInner;
	}
    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,final SendMessageRequestHeader requestHeader,RemotingCommand request,
			final RemotingCommand response) {
        // message topic长度校验
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + requestHeader.getTopic().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        // message properties长度校验
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + requestHeader.getProperties().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        if (request.getBody().length > DBMsgConstants.maxBodySize) {
            log.warn(" topic {}  msg body size {}  from {}", requestHeader.getTopic(), request.getBody().length, ChannelUtil.getRemoteIp(ctx.channel()));
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

	protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,final SendMessageRequestHeader requestHeader,
			final RemotingCommand response) {
		// 妫�鏌roker鏉冮檺, 椤哄簭娑堟伅绂佸啓锛涢潪椤哄簭娑堟伅閫氳繃 nameserver 閫氱煡瀹㈡埛绔墧闄ょ鍐欏垎鍖�
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
                && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return response;
        }
        // Topic鍚嶅瓧鏄惁涓庝繚鐣欏瓧娈靛啿绐�
        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        // 妫�鏌opic鏄惁瀛樺湪
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            // 濡傛灉鏄崟鍏冨寲妯″紡锛屽垯瀵� topic 杩涜璁剧疆
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: "
                    + ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(//
                requestHeader.getTopic(), //
                requestHeader.getDefaultTopic(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            // 灏濊瘯鐪嬩笅鏄惁鏄け璐ユ秷鎭彂鍥�
            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                                requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                                topicSysFlag);
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
         * Broker鏈韩涓嶅仛Topic鐨勬潈闄愰獙璇侊紝鐢盢ame Server璐熻矗閫氱煡Client澶勭悊
         */
        // // 妫�鏌opic鏉冮檺
        // if (!PermName.isWriteable(topicConfig.getPerm())) {
        // response.setCode(ResponseCode.NO_PERMISSION);
        // response.setRemark("the topic[" + requestHeader.getOriginTopic() +
        // "] sending message is forbidden");
        // return response;
        // }

        // 妫�鏌ラ槦鍒楁湁鏁堟��
        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illagal, %s Producer: %s",//
                queueIdInt,//
                topicConfig.toString(),//
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

            return response;
        }
        return response;
	}


    public SocketAddress getStoreHost() {
        return storeHost;
    }

    /**
     * 鍙戦�佹瘡鏉℃秷鎭細鍥炶皟
     */
    private List<SendMessageHook> sendMessageHookList;


    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }


    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }
	protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request, final RemotingCommand response) {
		if (!request.isOnewayRPC()) {
			try {
				ctx.writeAndFlush(response);
			} catch (Throwable e) {
				log.error("SendMessageProcessor process request over, but response failed", e);
				log.error(request.toString());
				log.error(response.toString());
			}
		}
	}

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand request,
            SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    final SendMessageRequestHeader requestHeader =
                            (SendMessageRequestHeader) request
                                .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                    context.setProducerGroup(requestHeader.getProducerGroup());
                    context.setTopic(requestHeader.getTopic());
                    context.setBodyLength(request.getBody().length);
                    context.setMsgProps(requestHeader.getProperties());
                    context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                    context.setBrokerAddr(this.brokerController.getBrokerAddr());
                    context.setQueueId(requestHeader.getQueueId());
                    hook.sendMessageBefore(context);
                    requestHeader.setProperties(context.getMsgProps());
                }
                catch (Throwable e) {
                }
            }
        }
    }


    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                                (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                }
                catch (Throwable e) {
                	
                }
            }
        }
    }

   
}
