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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.remoting.pipeline.RequestPipeline;
import org.apache.rocketmq.proxy.service.channel.SimpleChannel;
import org.apache.rocketmq.proxy.service.channel.SimpleChannelHandlerContext;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GetTopicRouteActivityTest {

    @Mock
    private RequestPipeline requestPipeline;

    @Mock
    private MessagingProcessor messagingProcessor;

    private GetTopicRouteActivity getTopicRouteActivity;

    private ChannelHandlerContext ctx;

    private ProxyContext context;

    @Before
    public void setup() throws Exception {
        getTopicRouteActivity = new GetTopicRouteActivity(requestPipeline, messagingProcessor);

        ConfigurationManager.initEnv();
        ConfigurationManager.intConfig();

        Channel channel = new SimpleChannel(null, "0.0.0.0:0", "1.1.1.1:1");
        ctx = new SimpleChannelHandlerContext(channel);

        context = ProxyContext.create();
    }

    @Test
    public void testProcessRequest0_HighVersion_SerializeWithFeatures() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setVersion(MQVersion.Version.V4_9_4.ordinal());

        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic("TestTopic");
        header.setAcceptStandardJsonOnly(false);
        request.writeCustomHeader(header);

        TopicRouteData topicRouteData = prepareTopicRouteData();

        TopicRouteData spyTopicRouteData = Mockito.spy(topicRouteData);

        ProxyTopicRouteData proxyTopicRouteData = mock(ProxyTopicRouteData.class);
        when(proxyTopicRouteData.buildTopicRouteData()).thenReturn(spyTopicRouteData);
        when(messagingProcessor.getTopicRouteDataForProxy(any(ProxyContext.class), anyList(), any()))
                .thenReturn(proxyTopicRouteData);

        RemotingCommand response = getTopicRouteActivity.processRequest0(ctx, request, context);

        assertNotNull(response);
        assertEquals(ResponseCode.SUCCESS, response.getCode());

        verify(spyTopicRouteData).encode(
                JSONWriter.Feature.BrowserCompatible,
                JSONWriter.Feature.MapSortField
        );

        TopicRouteData deserializedData = JSON.parseObject(response.getBody(), TopicRouteData.class);
        assertEquals(topicRouteData.getOrderTopicConf(), deserializedData.getOrderTopicConf());
        assertEquals(topicRouteData.getQueueDatas().size(), deserializedData.getQueueDatas().size());
    }

    @Test
    public void testProcessRequest0_LowVersion_StandardJsonOnly_SerializeWithFeatures() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setVersion(MQVersion.Version.V4_9_3.ordinal());

        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic("TestTopic");
        header.setAcceptStandardJsonOnly(true);
        request.writeCustomHeader(header);

        TopicRouteData topicRouteData = prepareTopicRouteData();

        TopicRouteData spyTopicRouteData = Mockito.spy(topicRouteData);

        ProxyTopicRouteData proxyTopicRouteData = mock(ProxyTopicRouteData.class);
        when(proxyTopicRouteData.buildTopicRouteData()).thenReturn(spyTopicRouteData);
        when(messagingProcessor.getTopicRouteDataForProxy(any(ProxyContext.class), anyList(), any()))
                .thenReturn(proxyTopicRouteData);

        RemotingCommand response = getTopicRouteActivity.processRequest0(ctx, request, context);

        assertNotNull(response);
        assertEquals(ResponseCode.SUCCESS, response.getCode());

        verify(spyTopicRouteData).encode();
    }

    private TopicRouteData prepareTopicRouteData() {
        TopicRouteData result = new TopicRouteData();
        result.setOrderTopicConf("orderTopicConf");

        List<QueueData> queueDatas = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setPerm(6);
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDatas.add(queueData);
        result.setQueueDatas(queueDatas);

        List<BrokerData> brokerDatas = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker-a");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        result.setBrokerDatas(brokerDatas);
        return result;
    }
}