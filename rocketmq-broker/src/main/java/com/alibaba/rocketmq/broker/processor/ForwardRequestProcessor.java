/**
 * $Id: ForwardRequestProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.logger.LoggerName;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * 向Client转发请求，通常用于管理、监控、统计目的
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ForwardRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public ForwardRequestProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        // TODO Auto-generated method stub
        return null;
    }
}
