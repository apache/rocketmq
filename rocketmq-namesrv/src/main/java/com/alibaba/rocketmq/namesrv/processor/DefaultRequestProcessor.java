/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.DeleteTopicInNamesrvRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerResponseHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * Name Server网络请求处理
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvController namesrvController;


    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        if (log.isDebugEnabled()) {
            log.debug("receive request, {} {} {}",//
                code, //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                request);
        }

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
        case GET_BROKER_CLUSTER_INFO:
            return this.getBrokerClusterInfo(ctx, request);
        case WIPE_WRITE_PERM_OF_BROKER:
            return this.wipeWritePermOfBroker(ctx, request);
        case GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
            return getAllTopicListFromNameserver(ctx, request);
        case DELETE_TOPIC_IN_NAMESRV:
            return deleteTopicInNamesrv(ctx, request);
        case GET_KV_CONFIG_BY_VALUE:
            return getKVConfigByValue(ctx, request);
        case DELETE_KV_CONFIG_BY_VALUE:
            return deleteKVConfigByValue(ctx, request);
        case GET_KVLIST_BY_NAMESPACE:
            return this.getKVListByNamespace(ctx, request);
        default:
            break;
        }
        return null;
    }


    /**
     * 获取一个Namespace下的所有kv
     */
    private RemotingCommand getKVListByNamespace(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader =
                (GetKVListByNamespaceRequestHeader) request
                    .decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(//
            requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
            return response;
        }

        response.setCode(MQResponseCode.QUERY_NOT_FOUND_VALUE);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }


    private RemotingCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicInNamesrvRequestHeader requestHeader =
                (DeleteTopicInNamesrvRequestHeader) request
                    .decodeCommandCustomHeader(DeleteTopicInNamesrvRequestHeader.class);

        this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    /**
     * 获取全部Topic列表
     * 
     * @param ctx
     * @param request
     * @return
     */
    private RemotingCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader =
                (WipeWritePermOfBrokerResponseHeader) response.getCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader =
                (WipeWritePermOfBrokerRequestHeader) request
                    .decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);

        int wipeTopicCnt =
                this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(
                    requestHeader.getBrokerName());

        log.info("wipe write perm of broker[{}], client: {}, {}", //
            requestHeader.getBrokerName(), //
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
            wipeTopicCnt);

        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    private RemotingCommand getBrokerClusterInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo();
        response.setBody(content);

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
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
            String orderTopicConf =
                    this.namesrvController.getKvConfigManager().getKVConfig(
                        NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
            topicRouteData.setOrderTopicConf(orderTopicConf);

            byte[] content = topicRouteData.encode();
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS_VALUE);
            response.setRemark(null);
            return response;
        }

        response.setCode(MQResponseCode.TOPIC_NOT_EXIST_VALUE);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic()
                + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
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
                RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader =
                (RegisterBrokerResponseHeader) response.getCustomHeader();
        final RegisterBrokerRequestHeader requestHeader =
                (RegisterBrokerRequestHeader) request
                    .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);

        TopicConfigSerializeWrapper topicConfigWrapper = null;
        if (request.getBody() != null) {
            topicConfigWrapper =
                    TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        }
        else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(//
            requestHeader.getClusterName(), // 1
            requestHeader.getBrokerAddr(), // 2
            requestHeader.getBrokerName(), // 3
            requestHeader.getBrokerId(), // 4
            requestHeader.getHaServerAddr(),// 5
            topicConfigWrapper, // 6
            ctx.channel()// 7
            );

        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand unregisterBroker(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader =
                (UnRegisterBrokerRequestHeader) request
                    .decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

        this.namesrvController.getRouteInfoManager().unregisterBroker(//
            requestHeader.getClusterName(), // 1
            requestHeader.getBrokerAddr(), // 2
            requestHeader.getBrokerName(), // 3
            requestHeader.getBrokerId());

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }


    public RemotingCommand getKVConfigByValue(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response =
                RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader =
                (GetKVConfigResponseHeader) response.getCustomHeader();
        final GetKVConfigRequestHeader requestHeader =
                (GetKVConfigRequestHeader) request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);

        String value = this.namesrvController.getKvConfigManager().getKVConfigByValue(//
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


    public RemotingCommand deleteKVConfigByValue(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader =
                (DeleteKVConfigRequestHeader) request
                    .decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);

        this.namesrvController.getKvConfigManager().deleteKVConfigByValue(//
            requestHeader.getNamespace(),//
            requestHeader.getKey()//
            );

        response.setCode(ResponseCode.SUCCESS_VALUE);
        response.setRemark(null);
        return response;
    }

}
