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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterTestRequestProcessorTest {
    private ClusterTestRequestProcessor clusterTestProcessor;
    private DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private MQClientInstance mqClientInstance = MQClientManager.getInstance()
            .getOrCreateMQClientInstance(new ClientConfig());
    private MQClientAPIImpl mQClientAPIImpl;
    private ChannelHandlerContext ctx;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException, RemotingException, MQClientException,
            InterruptedException {
        NamesrvController namesrvController = new NamesrvController(
                new NamesrvConfig(),
                new NettyServerConfig());

        clusterTestProcessor = new ClusterTestRequestProcessor(namesrvController, "default-producer");
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);
        ctx = mock(ChannelHandlerContext.class);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = ClusterTestRequestProcessor.class.getDeclaredField("adminExt");
        field.setAccessible(true);
        field.set(clusterTestProcessor, defaultMQAdminExt);

        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1234L, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerName("default-broker");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
    }

    @After
    public void terminate() {
    }

    @Test
    public void testGetRouteInfoByTopic() throws RemotingCommandException {
        RemotingCommand request = RemotingCommand.createRequestCommand(12, new CommandCustomHeader() {
            @Override
            public void checkFields() throws RemotingCommandException {

            }
        });
        RemotingCommand remoting = clusterTestProcessor.getRouteInfoByTopic(ctx, request);
        assertThat(remoting.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
        assertThat(remoting.getBody()).isNull();
        assertThat(remoting.getRemark()).isNotNull();
    }

    @Test
    public void testNamesrvReady() throws Exception {
        String topicName = "rocketmq-topic-test-ready";
        RouteInfoManager routeInfoManager = mockRouteInfoManager();
        NamesrvController namesrvController = mockNamesrvController(routeInfoManager, true, -1,true);
        ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(namesrvController);
        GetRouteInfoRequestHeader routeInfoRequestHeader = mockRouteInfoRequestHeader(topicName);
        RemotingCommand remotingCommand = mockTopicRouteCommand(routeInfoRequestHeader);
        RemotingCommand response = clientRequestProcessor.processRequest(mock(ChannelHandlerContext.class),
                remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testNamesrvNoNeedWaitForService() throws Exception {
        String topicName = "rocketmq-topic-test-ready";
        RouteInfoManager routeInfoManager = mockRouteInfoManager();
        NamesrvController namesrvController = mockNamesrvController(routeInfoManager, true, 45,false);
        ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(namesrvController);
        GetRouteInfoRequestHeader routeInfoRequestHeader = mockRouteInfoRequestHeader(topicName);
        RemotingCommand remotingCommand = mockTopicRouteCommand(routeInfoRequestHeader);
        RemotingCommand response = clientRequestProcessor.processRequest(mock(ChannelHandlerContext.class),
            remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testNamesrvNotReady() throws Exception {
        String topicName = "rocketmq-topic-test";
        RouteInfoManager routeInfoManager = mockRouteInfoManager();
        NamesrvController namesrvController = mockNamesrvController(routeInfoManager, false, 45,true);
        GetRouteInfoRequestHeader routeInfoRequestHeader = mockRouteInfoRequestHeader(topicName);
        RemotingCommand remotingCommand = mockTopicRouteCommand(routeInfoRequestHeader);
        ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(namesrvController);
        RemotingCommand response = clientRequestProcessor.processRequest(mock(ChannelHandlerContext.class),
                remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testNamesrv() throws Exception {
        int waitSecondsForService = 3;
        String topicName = "rocketmq-topic-test";
        RouteInfoManager routeInfoManager = mockRouteInfoManager();
        NamesrvController namesrvController = mockNamesrvController(routeInfoManager, false, waitSecondsForService,true);
        GetRouteInfoRequestHeader routeInfoRequestHeader = mockRouteInfoRequestHeader(topicName);
        RemotingCommand remotingCommand = mockTopicRouteCommand(routeInfoRequestHeader);
        ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(namesrvController);
        RemotingCommand response = clientRequestProcessor.processRequest(mock(ChannelHandlerContext.class),
                remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        TimeUnit.SECONDS.sleep(waitSecondsForService + 1);
        response = clientRequestProcessor.processRequest(mock(ChannelHandlerContext.class), remotingCommand);
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private RemotingCommand mockTopicRouteCommand(
            GetRouteInfoRequestHeader routeInfoRequestHeader) throws RemotingCommandException {
        RemotingCommand remotingCommand = mock(RemotingCommand.class);
        when(remotingCommand.decodeCommandCustomHeader(any())).thenReturn(routeInfoRequestHeader);
        when(remotingCommand.getCode()).thenReturn(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        return remotingCommand;
    }

    public NamesrvController mockNamesrvController(RouteInfoManager routeInfoManager, boolean ready,
            int waitSecondsForService,boolean needWaitForService) {
        NamesrvConfig namesrvConfig = mock(NamesrvConfig.class);
        when(namesrvConfig.isNeedWaitForService()).thenReturn(needWaitForService);
        when(namesrvConfig.getUnRegisterBrokerQueueCapacity()).thenReturn(10);
        when(namesrvConfig.getWaitSecondsForService()).thenReturn(ready ? 0 : waitSecondsForService);
        NamesrvController namesrvController = mock(NamesrvController.class);
        when(namesrvController.getNamesrvConfig()).thenReturn(namesrvConfig);
        when(namesrvController.getRouteInfoManager()).thenReturn(routeInfoManager);

        return namesrvController;
    }

    public RouteInfoManager mockRouteInfoManager() {
        RouteInfoManager routeInfoManager = mock(RouteInfoManager.class);
        TopicRouteData topicRouteData = mock(TopicRouteData.class);
        when(routeInfoManager.pickupTopicRouteData(any())).thenReturn(topicRouteData);
        return routeInfoManager;
    }

    public GetRouteInfoRequestHeader mockRouteInfoRequestHeader(String topicName) {
        GetRouteInfoRequestHeader routeInfoRequestHeader = mock(GetRouteInfoRequestHeader.class);
        when(routeInfoRequestHeader.getTopic()).thenReturn(topicName);
        return routeInfoRequestHeader;
    }

}
