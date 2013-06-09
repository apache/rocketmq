/**
 * $Id: PullMessageProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.longpolling.PullRequest;
import com.alibaba.rocketmq.broker.pagecache.ManyMessageTransfer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.logger.LoggerName;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.alibaba.rocketmq.store.GetMessageResult;


/**
 * 拉消息请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        return this.processRequest(ctx.channel(), request, true);
    }


    public void excuteRequestWhenWakeup(final Channel channel, final RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = this.processRequest(channel, request, false);
        if (response != null) {
            response.setOpaque(request.getOpaque());
            response.markResponseType();
            try {
                channel.write(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            log.error("processRequestWrapper response to " + future.channel().remoteAddress()
                                    + " failed", future.cause());
                            log.error(request.toString());
                            log.error(response.toString());
                        }
                    }
                });
            }
            catch (Throwable e) {
                log.error("processRequestWrapper process request over, but response failed", e);
                log.error(request.toString());
                log.error(response.toString());
            }
        }
    }


    private RemotingCommand processRequest(final Channel channel, RemotingCommand request,
            boolean brokerAllowSuspend) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.getCustomHeader();
        final PullMessageRequestHeader requestHeader =
                (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);

        // 由于使用sendfile，所以必须要设置
        response.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive PullMessage request command, " + request);
        }

        // 检查Broker权限
        if (!MixAll.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] pulling message is forbidden");
            return response;
        }

        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic " + requestHeader.getTopic() + " not exist, producer: " + channel.remoteAddress());
            response.setCode(MQResponseCode.TOPIC_NOT_EXIST_VALUE);
            response.setRemark("topic not exist, apply first please!");
            return response;
        }

        // 检查topic权限
        if (!MixAll.isReadable(topicConfig.getPerm())) {
            response.setCode(MQResponseCode.NO_PERMISSION_VALUE);
            response.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return response;
        }

        // 检查队列有效性
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo =
                    "queueId[" + requestHeader.getQueueId() + "] is illagal, topicConfig.readQueueNums: "
                            + topicConfig.getReadQueueNums() + " consumer: " + channel.remoteAddress();
            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark(errorInfo);
            return response;
        }

        final GetMessageResult getMessageResult =
                this.brokerController.getMessageStore().getMessage(requestHeader.getTopic(),
                    requestHeader.getQueueId(), requestHeader.getQueueOffset(), requestHeader.getMaxMsgNums(),
                    null);
        if (getMessageResult != null) {
            response.setRemark(getMessageResult.getStatus().name());

            responseHeader.setNextBeginOffset(getMessageResult.getNextBeginOffset());
            responseHeader.setSuggestPullingFromSlave(getMessageResult.isSuggestPullingFromSlave());
            responseHeader.setMinOffset(getMessageResult.getMinOffset());
            responseHeader.setMaxOffset(getMessageResult.getMaxOffset());

            switch (getMessageResult.getStatus()) {
            case FOUND:
                response.setCode(ResponseCode.SUCCESS_VALUE);
                break;
            case MESSAGE_WAS_REMOVING:
                response.setCode(MQResponseCode.PULL_RETRY_IMMEDIATELY_VALUE);
                break;
            // 这两个返回值都表示服务器暂时没有这个队列，应该立刻将客户端Offset重置为0
            case NO_MATCHED_LOGIC_QUEUE:
            case NO_MESSAGE_IN_QUEUE:
                response.setCode(MQResponseCode.PULL_NOT_FOUND_VALUE);
                break;
            case NO_MATCHED_MESSAGE:
                response.setCode(MQResponseCode.PULL_RETRY_IMMEDIATELY_VALUE);
                break;
            case OFFSET_FOUND_NULL:
                response.setCode(MQResponseCode.PULL_NOT_FOUND_VALUE);
                break;
            case OFFSET_OVERFLOW_BADLY:
                response.setCode(MQResponseCode.PULL_OFFSET_MOVED_VALUE);
                log.info("the request offset: " + requestHeader.getQueueOffset()
                        + " over flow badly, broker max offset: " + getMessageResult.getMaxOffset()
                        + ", consumer: " + channel.remoteAddress());
                break;
            case OFFSET_OVERFLOW_ONE:
                response.setCode(MQResponseCode.PULL_NOT_FOUND_VALUE);
                break;
            case OFFSET_TOO_SMALL:
                response.setCode(MQResponseCode.PULL_OFFSET_MOVED_VALUE);
                log.info("the request offset: " + requestHeader.getQueueOffset()
                        + " too small, broker min offset: " + getMessageResult.getMinOffset() + ", consumer: "
                        + channel.remoteAddress());
                break;
            default:
                assert false;
                break;
            }

            switch (response.getCode()) {
            case ResponseCode.SUCCESS_VALUE:
                try {
                    FileRegion fileRegion =
                            new ManyMessageTransfer(response.encodeHeader(getMessageResult.getBufferTotalSize()),
                                getMessageResult);
                    channel.sendFile(fileRegion).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            getMessageResult.release();
                            if (!future.isSuccess()) {
                                log.error("transfer many message by pagecache failed, " + channel.remoteAddress(),
                                    future.cause());
                            }
                        }
                    });
                }
                catch (Throwable e) {
                    log.error("", e);
                    getMessageResult.release();
                }

                response = null;
                break;
            case MQResponseCode.PULL_NOT_FOUND_VALUE:
                // 长轮询
                if (brokerAllowSuspend && hasSuspendFlag) {
                    PullRequest pullRequest =
                            new PullRequest(request, channel, suspendTimeoutMillisLong, this.brokerController
                                .getMessageStore().now(), requestHeader.getQueueOffset());
                    this.brokerController.getPullRequestHoldService().suspendPullRequest(requestHeader.getTopic(),
                        requestHeader.getQueueId(), pullRequest);
                    response = null;
                    break;
                }

                // 向Consumer返回应答
            case MQResponseCode.PULL_RETRY_IMMEDIATELY_VALUE:
            case MQResponseCode.PULL_OFFSET_MOVED_VALUE:
                break;
            default:
                assert false;
            }
        }
        else {
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("store getMessage return null");
        }

        // 存储Consumer消费进度
        if (brokerAllowSuspend) { // 说明是首次调用，相对于长轮询通知
            if (hasCommitOffsetFlag) {
                this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
            }
        }

        return response;
    }
}
