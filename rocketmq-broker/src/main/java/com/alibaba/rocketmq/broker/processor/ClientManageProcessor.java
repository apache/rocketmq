/**
 * $Id: ClientManageProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * Client×¢²áÓë×¢Ïú¹ÜÀí
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
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = null;
        try {
            heartbeatData = HeartbeatData.decode(request.getBody());
        }
        catch (InvalidProtocolBufferException e) {
            log.error("decode heartbeat body from channel[{}] error", ctx.channel().remoteAddress().toString());
            response.setCode(ResponseCode.SYSTEM_ERROR_VALUE);
            response.setRemark("decode heartbeat body error");
            return response;
        }

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
            ctx.channel(),//
            heartbeatData.getClientID(),//
            request.getLanguage(),//
            request.getVersion()//
                );

        // ×¢²áConsumer
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            this.brokerController.getConsumerManager().registerConsumer(//
                data.getGroupName(),//
                clientChannelInfo,//
                data.getConsumeType(),//
                data.getMessageModel(),//
                data.getSubscriptionDataSet()//
                );
        }

        // ×¢²áProducer
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), clientChannelInfo);
        }

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }
}
