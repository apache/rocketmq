/**
 * $Id: ClientManageProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * Client注册与注销管理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case HEART_BEAT:
            return this.heartBeat(ctx, request);
        case UNREGISTER_CLIENT:
            return this.unregisterClient(ctx, request);
        default:
            break;
        }
        return null;
    }


    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientResponseHeader responseHeader =
                (UnregisterClientResponseHeader) response.getCustomHeader();
        final UnregisterClientRequestHeader requestHeader =
                (UnregisterClientRequestHeader) request
                    .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
            ctx.channel(),//
            requestHeader.getClientID(),//
            request.getLanguage(),//
            request.getVersion()//
                );

        // 注销Producer
        final String producerGroup = requestHeader.getProducerGroup();
        if (producerGroup != null) {
            this.brokerController.getProducerManager().unregisterProducer(producerGroup, clientChannelInfo);
        }

        // 注销Consumer
        final String consumerGroup = requestHeader.getProducerGroup();
        if (consumerGroup != null) {
            this.brokerController.getConsumerManager().unregisterConsumer(consumerGroup, clientChannelInfo);
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
            ctx.channel(),//
            heartbeatData.getClientID(),//
            request.getLanguage(),//
            request.getVersion()//
                );

        // 注册Consumer
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            this.brokerController.getConsumerManager().registerConsumer(//
                data.getGroupName(),//
                clientChannelInfo,//
                data.getConsumeType(),//
                data.getMessageModel(),//
                data.getSubscriptionDataSet()//
                );
        }

        // 注册Producer
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), clientChannelInfo);
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }
}
