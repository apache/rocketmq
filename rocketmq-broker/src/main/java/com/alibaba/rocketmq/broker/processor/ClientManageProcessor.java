/**
 * $Id: ClientManageProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * Client注册与注销管理
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(MixAll.BrokerLoggerName);

    private final BrokerController brokerController;


    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case HEART_BEAT:
            return this.heartBeat(ctx, request);
        case UNREGISTER_CLIENT:
            break;
        default:
            break;
        }
        return null;
    }


    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {

        return null;
    }
}
