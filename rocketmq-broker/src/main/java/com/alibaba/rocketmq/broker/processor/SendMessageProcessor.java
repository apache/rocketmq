/**
 * $Id: SendMessageProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
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
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;


/**
 * 处理客户端发送消息的请求
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class SendMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

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
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.getCustomHeader();
        final SendMessageRequestHeader requestHeader =
                (SendMessageRequestHeader) request.decodeCommandCustomHeader(SendMessageRequestHeader.class);

        // 由于有直接返回的逻辑，所以必须要设置
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive SendMessage request command, " + request);
        }

        // 检查Broker权限
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
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
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(errorMsg);
            return response;
        }

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: "
                    + ctx.channel().remoteAddress());
            topicConfig =
                    this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                        requestHeader.getTopic(), requestHeader.getDefaultTopic(), ctx,
                        requestHeader.getDefaultTopicQueueNums());
            if (null == topicConfig) {
                response.setCode(MQResponseCode.TOPIC_NOT_EXIST_VALUE);
                response.setRemark("topic not exist, apply first please!"
                        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        // 检查topic权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] sending message is forbidden");
            return response;
        }

        // 检查队列有效性
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt >= topicConfig.getWriteQueueNums()) {
            String errorInfo =
                    "queueId[" + queueIdInt + "] is illagal, topicConfig.writeQueueNums: "
                            + topicConfig.getWriteQueueNums() + " producer: " + ctx.channel().remoteAddress();
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(errorInfo);
            return response;
        }

        // 随机指定一个队列
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt()) % topicConfig.getWriteQueueNums();
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
        msgInner.setProperties(MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());

        msgInner.setReconsumeTimes(0);

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            boolean sendOK = false;

            switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS_VALUE);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(MQResponseCode.FLUSH_DISK_TIMEOUT_VALUE);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(MQResponseCode.FLUSH_SLAVE_TIMEOUT_VALUE);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(MQResponseCode.SLAVE_NOT_AVAILABLE_VALUE);
                sendOK = true;
                break;

            // Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("create maped file failed.");
                break;
            case MESSAGE_ILLEGAL:
                response.setCode(MQResponseCode.MESSAGE_ILLEGAL_VALUE);
                response.setRemark("the message is illegal, maybe length not matched.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(MQResponseCode.SERVICE_NOT_AVAILABLE_VALUE);
                response.setRemark("service not available now.");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
            }

            if (sendOK) {
                response.setRemark(null);

                responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
                responseHeader.setQueueId(queueIdInt);
                responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

                // 直接返回
                if (!request.isOnewayRPC()) {
                    try {
                        ctx.write(response).addListener(new ChannelFutureListener() {
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

                this.brokerController.getPullRequestHoldService().notifyMessageArriving(requestHeader.getTopic(),
                    queueIdInt, putMessageResult.getAppendMessageResult().getLogicsOffset());

                return null;
            }
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("store putMessage return null");
        }

        return response;
    }


    public SocketAddress getStoreHost() {
        return storeHost;
    }

}
