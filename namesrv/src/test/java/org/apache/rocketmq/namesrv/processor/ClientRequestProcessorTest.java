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

package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
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
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientRequestProcessorTest {

    @Mock
    private NamesrvController namesrvController;

    @Mock
    private RouteInfoManager routeInfoManager;

    @Mock
    private NamesrvConfig namesrvConfig;

    @Mock
    private ChannelHandlerContext ctx;

    private ClientRequestProcessor clientRequestProcessor;

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        when(namesrvController.getRouteInfoManager()).thenReturn(routeInfoManager);
        when(namesrvController.getNamesrvConfig()).thenReturn(namesrvConfig);

        when(namesrvConfig.getWaitSecondsForService()).thenReturn(0);
        when(namesrvConfig.isNeedWaitForService()).thenReturn(true);

        clientRequestProcessor = new ClientRequestProcessor(namesrvController);

        Field startupTimeMillisField = ClientRequestProcessor.class.getDeclaredField("startupTimeMillis");
        startupTimeMillisField.setAccessible(true);
        startupTimeMillisField.set(clientRequestProcessor, System.currentTimeMillis() - 60000);
    }

    @Test
    public void testGetRouteInfoByTopicWithHighVersionClient() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setVersion(MQVersion.Version.V4_9_4.ordinal());

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic("TestTopic");

        RemotingCommand spyRequest = spy(request);
        doReturn(requestHeader).when(spyRequest).decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = createMockTopicRouteData();

        when(routeInfoManager.pickupTopicRouteData("TestTopic")).thenReturn(topicRouteData);

        RemotingCommand response = clientRequestProcessor.getRouteInfoByTopic(ctx, spyRequest);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
    }

    @Test
    public void testGetRouteInfoByTopicWithLowVersionClientAndNoStandardJsonFlag() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setVersion(MQVersion.Version.V4_9_3.ordinal());

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic("TestTopic");
        requestHeader.setAcceptStandardJsonOnly(false);

        RemotingCommand spyRequest = spy(request);
        doReturn(requestHeader).when(spyRequest).decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        TopicRouteData topicRouteData = createMockTopicRouteData();

        when(routeInfoManager.pickupTopicRouteData("TestTopic")).thenReturn(topicRouteData);

        RemotingCommand response = clientRequestProcessor.getRouteInfoByTopic(ctx, spyRequest);

        assertEquals(ResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
    }

    @Test
    public void testGetRouteInfoByTopicWithNameServerNotReady() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic("TestTopic");

        RemotingCommand spyRequest = spy(request);
        doReturn(requestHeader).when(spyRequest).decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        when(namesrvConfig.getWaitSecondsForService()).thenReturn(60);
        when(namesrvConfig.isNeedWaitForService()).thenReturn(true);

        try {
            Field startupTimeMillisField = ClientRequestProcessor.class.getDeclaredField("startupTimeMillis");
            startupTimeMillisField.setAccessible(true);
            startupTimeMillisField.set(clientRequestProcessor, System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }

        RemotingCommand response = clientRequestProcessor.getRouteInfoByTopic(ctx, spyRequest);

        assertEquals(ResponseCode.SYSTEM_ERROR, response.getCode());
        assertEquals("name server not ready", response.getRemark());
    }

    @Test
    public void testGetRouteInfoByTopicWithTopicNotExist() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);

        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic("NonExistentTopic");

        RemotingCommand spyRequest = spy(request);
        doReturn(requestHeader).when(spyRequest).decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);

        when(routeInfoManager.pickupTopicRouteData("NonExistentTopic")).thenReturn(null);

        RemotingCommand response = clientRequestProcessor.getRouteInfoByTopic(ctx, spyRequest);

        assertEquals(ResponseCode.TOPIC_NOT_EXIST, response.getCode());
        assertNotNull(response.getRemark());
    }

    private TopicRouteData createMockTopicRouteData() {
        TopicRouteData result = new TopicRouteData();

        List<QueueData> queueDataList = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueData.setPerm(6);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        result.setQueueDatas(queueDataList);

        List<BrokerData> brokerDataList = new ArrayList<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker-a");
        brokerData.setCluster("default-cluster");
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        result.setBrokerDatas(brokerDataList);

        return result;
    }
}