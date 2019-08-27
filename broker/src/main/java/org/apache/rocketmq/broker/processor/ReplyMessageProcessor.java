package org.apache.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.*;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ReplyMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    public ReplyMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        SendMessageContext mqtraceContext = null;
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return null;
        }

        mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

        RemotingCommand response = this.processReplyMessageRequest(ctx, request, mqtraceContext, requestHeader);

        this.executeSendMessageHookAfter(response, mqtraceContext);
        return response;
    }

    @Override
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_REPLY_MESSAGE_V2:
                requestHeaderV2 =
                        (SendMessageRequestHeaderV2) request
                                .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_REPLY_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader =
                            (SendMessageRequestHeader) request
                                    .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    private RemotingCommand processReplyMessageRequest(final ChannelHandlerContext ctx,
                                                       final RemotingCommand request,
                                                       final SendMessageContext sendMessageContext,
                                                       final SendMessageRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendReplyMessage request command, {}", request);
        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        boolean pushOk = this.pushReplyMessage(ctx, requestHeader, msgInner, response);

        if (pushOk && this.brokerController.getBrokerConfig().isStoreReplyMessageEnable()) {
            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
            this.handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);
        } else {
            responseHeader.setMsgId("0");
            responseHeader.setQueueId(0);
            responseHeader.setQueueOffset(0L);
        }

        return response;
    }

    private boolean pushReplyMessage(final ChannelHandlerContext ctx, final SendMessageRequestHeader requestHeader, final Message msg, final RemotingCommand response) {
        ReplyMessageRequestHeader replyMessageRequestHeader = new ReplyMessageRequestHeader();
        replyMessageRequestHeader.setBornHost(ctx.channel().remoteAddress().toString());
        replyMessageRequestHeader.setStoreHost(this.getStoreHost().toString());
        replyMessageRequestHeader.setStoreTimestamp(System.currentTimeMillis());
        replyMessageRequestHeader.setProducerGroup(requestHeader.getProducerGroup());
        replyMessageRequestHeader.setTopic(requestHeader.getTopic());
        replyMessageRequestHeader.setDefaultTopic(requestHeader.getDefaultTopic());
        replyMessageRequestHeader.setDefaultTopicQueueNums(requestHeader.getDefaultTopicQueueNums());
        replyMessageRequestHeader.setQueueId(requestHeader.getQueueId());
        replyMessageRequestHeader.setSysFlag(requestHeader.getSysFlag());
        replyMessageRequestHeader.setBornTimestamp(requestHeader.getBornTimestamp());
        replyMessageRequestHeader.setFlag(requestHeader.getFlag());
        replyMessageRequestHeader.setProperties(requestHeader.getProperties());
        replyMessageRequestHeader.setReconsumeTimes(requestHeader.getReconsumeTimes());
        replyMessageRequestHeader.setUnitMode(requestHeader.isUnitMode());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, replyMessageRequestHeader);
        request.setBody(msg.getBody());

        String senderId = msg.getProperties().get(MessageConst.PROPERTY_MESSAGE_REPLY_TO);
        boolean pushOk = false;

        if (senderId != null) {
            Channel channel = this.brokerController.getProducerManager().findChannel(senderId);
            if (channel != null) {
                msg.getProperties().put(MessageConst.PROPERTY_PUSH_REPLY_TIME, String.valueOf(System.currentTimeMillis()));
                replyMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                try {
                    RemotingCommand pushResponse = this.brokerController.getBroker2Client().callClient(channel, request);
                    assert pushResponse != null;
                    switch (pushResponse.getCode()) {
                        case ResponseCode.SUCCESS: {
                            response.setCode(ResponseCode.SUCCESS);
                            response.setRemark(null);
                            pushOk = true;
                            break;
                        }
                        default: {
                            response.setCode(ResponseCode.SYSTEM_ERROR);
                            response.setRemark("push reply message to requester fail");
                            log.warn("push reply message to <{}> return fail, remark: {}", senderId, response.getRemark());
                        }
                    }
                } catch (InterruptedException e) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("push reply message to requester fail");
                    log.warn("push reply message to <{}> fail. {}", senderId, channel, e);
                } catch (RemotingException e) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("push reply message to requester fail");
                    log.warn("push reply message to <{}> fail. {}", senderId, channel, e);
                }
            } else {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("channel of <" + senderId + "> not found");
                log.warn("push reply message fial, channel of <{}> not found.", senderId);
            }
            return pushOk;
        }
        log.warn("REPLY_TO is null, can not reply message");
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("REPLY_TO is null");
        return pushOk;
    }

    private void handlePutMessageResult(PutMessageResult putMessageResult, final RemotingCommand response,
                                                   final RemotingCommand request, final MessageExt msg,
                                                   final SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        if (putMessageResult == null) {
            response.setRemark("push reply to requester success, but store putMessage return null");
            return ;
        }
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
//                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
//                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                        "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
//                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now, maybe disk full, maybe your broker machine memory too small.");
                break;
            case OS_PAGECACHE_BUSY:
//                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
//                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
//                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                    putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());
            response.setRemark(null);
            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        } else {
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
    }
}
