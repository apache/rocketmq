/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.remoting.activity;

import com.alibaba.fastjson2.JSONWriter;
import com.google.common.net.HostAndPort;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.config.ProxyConfig;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.List;

public class GetTopicRouteActivity extends AbstractRemotingActivity {
    public GetTopicRouteActivity(RequestPipeline requestPipeline,
        MessagingProcessor messagingProcessor) {
        super(requestPipeline, messagingProcessor);
    }

    @Override
    protected RemotingCommand processRequest0(ChannelHandlerContext ctx, RemotingCommand request,
        ProxyContext context) throws Exception {
        ProxyConfig proxyConfig = ConfigurationManager.getProxyConfig();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader =
            (GetRouteInfoRequestHeader) request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        List<Address> addressList = new ArrayList<>();
        // AddressScheme is just a placeholder and will not affect topic route result in this case.
        addressList.add(new Address(HostAndPort.fromParts(proxyConfig.getRemotingAccessAddr(), proxyConfig.getRemotingListenPort())));
        ProxyTopicRouteData proxyTopicRouteData = messagingProcessor.getTopicRouteDataForProxy(context, addressList, requestHeader.getTopic());
        TopicRouteData topicRouteData = proxyTopicRouteData.buildTopicRouteData();

        byte[] content;
        Boolean standardJsonOnly = requestHeader.getAcceptStandardJsonOnly();
        if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || null != standardJsonOnly && standardJsonOnly) {
            content = topicRouteData.encode(JSONWriter.Feature.BrowserCompatible, JSONWriter.Feature.MapSortField);
        } else {
            content = topicRouteData.encode();
        }

        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
