package com.alibaba.rocketmq.client.impl;

import io.netty.channel.ChannelHandlerContext;

import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageDecoder;
import com.alibaba.rocketmq.common.MessageExt;
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
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
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


    private void processTransactionState(//
            final ChannelHandlerContext ctx,//
            final CheckTransactionStateRequestHeader requestHeader,//
            final LocalTransactionState localTransactionState,//
            final String producerGroup,//
            final Throwable exception) {
        final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
        final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
        thisHeader.setCommitLogOffset(requestHeader.getCommitLogOffset());
        thisHeader.setProducerGroup(producerGroup);
        thisHeader.setTranStateTableOffset(requestHeader.getTranStateTableOffset());
        thisHeader.setFromTransactionCheck(true);
        switch (localTransactionState) {
        case COMMIT_MESSAGE:
            thisHeader.setCommitOrRollback(MessageSysFlag.TransactionCommitType);
            break;
        case ROLLBACK_MESSAGE:
            thisHeader.setCommitOrRollback(MessageSysFlag.TransactionRollbackType);
            log.warn("when broker check, client rollback this transaction, {}", thisHeader);
            break;
        case UNKNOW:
            thisHeader.setCommitOrRollback(MessageSysFlag.TransactionNotType);
            log.warn("when broker check, client donot know this transaction state, {}", thisHeader);
            break;
        default:
            break;
        }

        RemotingCommand request =
                RemotingCommand.createRequestCommand(MQRequestCode.END_TRANSACTION_VALUE, thisHeader);
        if (exception != null) {
            request.setRemark("checkLocalTransactionState Exception: " + exception.toString());
        }

        try {
            mqClientFactory.getMQClientAPIImpl().getRemotingClient().invokeOneway(addr, request, 3000);
        }
        catch (Exception e) {
            log.error("endTransactionOneway exception", e);
        }
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
                    TransactionCheckListener transactionCheckListener = producer.checkListener();
                    if (transactionCheckListener != null) {
                        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                        Throwable exception = null;
                        try {
                            localTransactionState =
                                    transactionCheckListener.checkLocalTransactionState(messageExt);
                        }
                        catch (Throwable e) {
                            log.error(
                                "Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                            exception = e;
                        }

                        this.processTransactionState(ctx, requestHeader, localTransactionState, group, exception);
                    }
                    else {
                        log.warn("checkTransactionState, pick transactionCheckListener by group[{}] failed", group);
                    }
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
