package com.alibaba.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.namesrv.NamesrvController2;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvController2 namesrvController;


    public DefaultRequestProcessor(NamesrvController2 namesrvController) {
        this.namesrvController = namesrvController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case PUT_KV_CONFIG:
            return this.putKVConfig(ctx, request);
        case GET_KV_CONFIG:
            return this.getKVConfig(ctx, request);
        case DELETE_KV_CONFIG:
            return this.deleteKVConfig(ctx, request);
        case REGISTER_BROKER:
            return this.registerBroker(ctx, request);
        case UNREGISTER_BROKER:
            return this.unregisterBroker(ctx, request);
        case GET_ROUTEINTO_BY_TOPIC:
            return this.getRouteInfoByTopic(ctx, request);
        default:
            break;
        }
        return null;
    }


    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
                (GetRouteInfoRequestHeader) request
                    .decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData =
                this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData != null) {
            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
            return response;
        }

        response.setCode(MQResponseCode.TOPIC_NOT_EXIST_VALUE);
        response.setRemark("No topic route info for this topic: " + requestHeader.getTopic());
        return response;
    }


    public RemotingCommand putKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader =
                (PutKVConfigRequestHeader) request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().putKVConfig(//
            requestHeader.getNamespace(),//
            requestHeader.getKey(),//
            requestHeader.getValue()//
            );

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand getKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader =
                (GetKVConfigResponseHeader) response.getCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
                (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfig(//
            requestHeader.getNamespace(),//
            requestHeader.getKey()//
            );

        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
            return response;
        }

        response.setCode(MQResponseCode.QUERY_NOT_FOUND_VALUE);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: "
                + requestHeader.getKey());
        return response;
    }


    public RemotingCommand deleteKVConfig(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request
                    .decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfig(//
            requestHeader.getNamespace(),//
            requestHeader.getKey()//
            );

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader =
                (RegisterBrokerResponseHeader) response.getCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request
                    .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        TopicConfigSerializeWrapper topicConfigWrapper =
                TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);

        String masterHAServer = this.namesrvController.getRouteInfoManager().registerBroker(//
            requestHeader.getClusterName(), // 1
            requestHeader.getBrokerAddr(), // 2
            requestHeader.getBrokerName(), // 3
            requestHeader.getBrokerId(), // 4
            requestHeader.getHaServerAddr(),// 5
            topicConfigWrapper, // 6
            ctx.channel()// 7
            );

        responseHeader.setHaServerAddr(masterHAServer);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        return null;
    }
}
