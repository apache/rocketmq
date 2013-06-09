package com.alibaba.rocketmq.client.impl;

import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * Client接收Broker的回调操作，例如事务回调，或者其他管理类命令回调
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log;
    private final MQClientFactory mqClientFactory;


    public ClientRemotingProcessor(final MQClientFactory mqClientFactory, final Logger log) {
        this.log = log;
        this.mqClientFactory = mqClientFactory;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case CHECK_TRANSACTION_STATE:
            return this.checkTransactionState(ctx, request);
        default:
            break;
        }
        return null;
    }


    /**
     * Oneway调用，无返回值
     */
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader =
                (CheckTransactionStateRequestHeader) request
                    .decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            final String group = messageExt.getProperty(Message.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                }
                else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            }
            else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        }
        else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }
}
