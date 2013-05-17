/**
 * $Id: AllRequestProcessor.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQRequestCode;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.RegisterOrderTopicRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import com.alibaba.rocketmq.namesrv.topic.TopicRuntimeDataManager;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * @author lansheng.zj@taobao.com
 */
public class AllRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(MixAll.NamesrvLoggerName);

    private final NamesrvController namesrvController;
    private TopicRuntimeDataManager topicInfoManager;


    public AllRequestProcessor(final NamesrvController namesrvController, TopicRuntimeDataManager topicInfoManager) {
        this.namesrvController = namesrvController;
        this.topicInfoManager = topicInfoManager;
    }


    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        MQRequestCode code = MQRequestCode.valueOf(request.getCode());
        switch (code) {
        case REGISTER_BROKER:
            RegisterBrokerRequestHeader requestBrokerHeader =
                    (RegisterBrokerRequestHeader) request
                        .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
            return topicInfoManager.registerBroker(requestBrokerHeader.getBrokerAddr());

        case REGISTER_BROKER_SINGLE:
            RegisterBrokerRequestHeader requestBrokerSingleHeader =
                    (RegisterBrokerRequestHeader) request
                        .decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
            return topicInfoManager.registerBrokerSingle(requestBrokerSingleHeader.getBrokerAddr());

        case UNREGISTER_BROKER:
            UnRegisterBrokerRequestHeader unRequestBrokerHeader =
                    (UnRegisterBrokerRequestHeader) request
                        .decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);
            return topicInfoManager.unRegisterBroker(unRequestBrokerHeader.getBrokerName());
        case UNREGISTER_BROKER_SINGLE:
            UnRegisterBrokerRequestHeader unRequestBrokerSingleHeader =
                    (UnRegisterBrokerRequestHeader) request
                        .decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);

            return topicInfoManager.unRegisterBrokerSingle(unRequestBrokerSingleHeader.getBrokerName());
        case GET_BROKER_LIST:
            break;
        case REGISTER_ORDER_TOPIC:
            RegisterOrderTopicRequestHeader registerOrderTopicHeader =
                    (RegisterOrderTopicRequestHeader) request
                        .decodeCommandCustomHeader(RegisterOrderTopicRequestHeader.class);
            return topicInfoManager.registerOrderTopic(registerOrderTopicHeader.getTopic(),
                registerOrderTopicHeader.getOrderTopicString());

        case REGISTER_ORDER_TOPIC_SINGLE:
            RegisterOrderTopicRequestHeader registerOrderTopicSingleHeader =
                    (RegisterOrderTopicRequestHeader) request
                        .decodeCommandCustomHeader(RegisterOrderTopicRequestHeader.class);
            return topicInfoManager.registerOrderTopicSingle(registerOrderTopicSingleHeader.getTopic(),
                registerOrderTopicSingleHeader.getOrderTopicString());

        case UNREGISTER_ORDER_TOPIC:
            break;
        case GET_ORDER_TOPIC_LIST:
            break;
        case UPDATE_NAMESRV_CONFIG:
            break;
        case GET_NAMESRV_CONFIG:
            break;
        case GET_NAMESRV_RUNTIME_INFO:
            break;
        case GET_ROUTEINTO_BY_TOPIC:
            GetRouteInfoRequestHeader getRouteInfoHeader =
                    (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
            return topicInfoManager.getRouteInfoByTopic(getRouteInfoHeader.getTopic());

        case SYNC_NAMESRV_RUNTIME_CONF:
            return topicInfoManager.getTopicRuntimeData();

        default:
            break;
        }

        return null;
    }
}
