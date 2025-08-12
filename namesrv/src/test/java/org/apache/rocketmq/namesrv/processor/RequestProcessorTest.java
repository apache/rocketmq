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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.RegisterBrokerBody;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RequestProcessorTest {
    private DefaultRequestProcessor defaultRequestProcessor;

    private ClientRequestProcessor clientRequestProcessor;

    private NamesrvController namesrvController;

    private NamesrvConfig namesrvConfig;

    private NettyServerConfig nettyServerConfig;

    private RouteInfoManager routeInfoManager;

    private Logger logger;

    @Before
    public void init() throws Exception {
        namesrvConfig = new NamesrvConfig();
        namesrvConfig.setEnableAllTopicList(true);
        nettyServerConfig = new NettyServerConfig();
        routeInfoManager = new RouteInfoManager(new NamesrvConfig(), null);

        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);

        Field field = NamesrvController.class.getDeclaredField("routeInfoManager");
        field.setAccessible(true);
        field.set(namesrvController, routeInfoManager);
        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);

        clientRequestProcessor = new ClientRequestProcessor(namesrvController);

        registerRouteInfoManager();

        logger = mock(Logger.class);
        FieldUtils.writeStaticField(DefaultRequestProcessor.class.getDeclaredField("log"), logger, true);
    }

    @Test
    public void testProcessRequest_PutKVConfig() throws RemotingCommandException {
        PutKVConfigRequestHeader header = new PutKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");
        request.addExtField("value", "value");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key"))
            .isEqualTo("value");
    }

    @Test
    public void testProcessRequest_GetKVConfigReturnNotNull() throws RemotingCommandException {
        namesrvController.getKvConfigManager().putKVConfig("namespace", "key", "value");

        GetKVConfigRequestHeader header = new GetKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
            .readCustomHeader();

        assertThat(responseHeader.getValue()).isEqualTo("value");
    }

    @Test
    public void testProcessRequest_GetKVConfigReturnNull() throws RemotingCommandException {
        GetKVConfigRequestHeader header = new GetKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);
        assertThat(response.getRemark()).isEqualTo("No config item, Namespace: namespace Key: key");

        GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response
            .readCustomHeader();

        assertThat(responseHeader.getValue()).isNull();
    }

    @Test
    public void testProcessRequest_DeleteKVConfig() throws RemotingCommandException {
        namesrvController.getKvConfigManager().putKVConfig("namespace", "key", "value");

        DeleteKVConfigRequestHeader header = new DeleteKVConfigRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG,
            header);
        request.addExtField("namespace", "namespace");
        request.addExtField("key", "key");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        assertThat(namesrvController.getKvConfigManager().getKVConfig("namespace", "key"))
            .isNull();
    }

    @Test
    public void testProcessRequest_UnSupportedRequest() throws RemotingCommandException {
        final RemotingCommand unSupportedRequest = RemotingCommand.createRequestCommand(99999, null);
        final RemotingCommand response = defaultRequestProcessor.processRequest(null, unSupportedRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.REQUEST_CODE_NOT_SUPPORTED);
    }

    @Test
    public void testProcessRequest_UpdateConfigPath() throws RemotingCommandException {
        final RemotingCommand updateConfigRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        Properties properties = new Properties();

        // Update allowed value
        properties.setProperty("enableTopicList", "true");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = defaultRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

        //update disallowed value
        properties.clear();
        properties.setProperty("configStorePath", "test/path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = defaultRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list.");

        //update disallowed values
        properties.clear();
        properties.setProperty("kvConfigPath", "test/path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = defaultRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list");

        //update disallowed values
        properties.clear();
        properties.setProperty("configBlackList", "test;path");
        updateConfigRequest.setBody(MixAll.properties2String(properties).getBytes(StandardCharsets.UTF_8));

        response = defaultRequestProcessor.processRequest(null, updateConfigRequest);

        assertThat(response).isNotNull();
        assertThat(response.getCode()).isEqualTo(ResponseCode.NO_PERMISSION);
        assertThat(response.getRemark()).contains("Can not update config in black list");
    }

    @Test
    public void testProcessRequest_RegisterBroker() throws RemotingCommandException,
        NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterCmd(true);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker");
        broker.setBrokerAddrs((HashMap) Maps.newHashMap(new Long(2333), "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
            .contains(new HashMap.SimpleEntry("broker", broker));
    }

    /*@Test
    public void testProcessRequest_RegisterBrokerLogicalQueue() throws Exception {
        String cluster = "cluster";
        String broker1Name = "broker1";
        String broker1Addr = "10.10.1.1";
        String broker2Name = "broker2";
        String broker2Addr = "10.10.1.2";
        String topic = "foobar";

        LogicalQueueRouteData queueRouteData1 = new LogicalQueueRouteData(0, 0, new MessageQueue(topic, broker1Name, 0), MessageQueueRouteState.ReadOnly, 0, 10, 100, 100, broker1Addr);
        {
            RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
            header.setBrokerName(broker1Name);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                RequestCode.REGISTER_BROKER, header);
            request.addExtField("brokerName", broker1Name);
            request.addExtField("brokerAddr", broker1Addr);
            request.addExtField("clusterName", cluster);
            request.addExtField("haServerAddr", "10.10.2.1");
            request.addExtField("brokerId", String.valueOf(MixAll.MASTER_ID));
            request.setVersion(MQVersion.CURRENT_VERSION);
            TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
            topicConfigSerializeWrapper.setTopicConfigTable(new ConcurrentHashMap<>(Collections.singletonMap(topic, new TopicConfig(topic))));
            topicConfigSerializeWrapper.setLogicalQueuesInfoMap(Maps.newHashMap(topic, new LogicalQueuesInfo(Collections.singletonMap(0, Lists.newArrayList(
                queueRouteData1
            )))));
            topicConfigSerializeWrapper.setDataVersion(new DataVersion());
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);
            requestBody.setFilterServerList(Lists.<String>newArrayList());
            request.setBody(requestBody.encode());

            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            when(ctx.channel()).thenReturn(null);

            RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getRemark()).isNull();
        }
        LogicalQueueRouteData queueRouteData2 = new LogicalQueueRouteData(0, 100, new MessageQueue(topic, broker2Name, 0), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker2Addr);
        LogicalQueueRouteData queueRouteData3 = new LogicalQueueRouteData(1, 100, new MessageQueue(topic, broker2Name, 0), MessageQueueRouteState.Normal, 0, -1, -1, -1, broker2Addr);
        {
            RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
            header.setBrokerName(broker2Name);
            RemotingCommand request = RemotingCommand.createRequestCommand(
                RequestCode.REGISTER_BROKER, header);
            request.addExtField("brokerName", broker2Name);
            request.addExtField("brokerAddr", broker2Addr);
            request.addExtField("clusterName", cluster);
            request.addExtField("haServerAddr", "10.10.2.1");
            request.addExtField("brokerId", String.valueOf(MixAll.MASTER_ID));
            request.setVersion(MQVersion.CURRENT_VERSION);
            TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
            topicConfigSerializeWrapper.setTopicConfigTable(new ConcurrentHashMap<>(Collections.singletonMap(topic, new TopicConfig(topic))));
            topicConfigSerializeWrapper.setLogicalQueuesInfoMap(Maps.newHashMap(topic, new LogicalQueuesInfo(ImmutableMap.of(
                0, Collections.singletonList(queueRouteData2),
                1, Collections.singletonList(queueRouteData3)
            ))));
            topicConfigSerializeWrapper.setDataVersion(new DataVersion());
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigSerializeWrapper);
            requestBody.setFilterServerList(Lists.<String>newArrayList());
            request.setBody(requestBody.encode());

            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            when(ctx.channel()).thenReturn(null);

            RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
            assertThat(response.getRemark()).isNull();
        }

        {
            GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
            header.setTopic(topic);
            header.setSysFlag(MessageSysFlag.LOGICAL_QUEUE_FLAG);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, header);
            request.makeCustomHeaderToNet();

            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            when(ctx.channel()).thenReturn(null);

            RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

            assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);

            TopicRouteDataNameSrv topicRouteDataNameSrv = JSON.parseObject(response.getBody(), TopicRouteDataNameSrv.class);
            assertThat(topicRouteDataNameSrv).isNotNull();
            LogicalQueuesInfoUnordered logicalQueuesInfoUnordered = new LogicalQueuesInfoUnordered();
            logicalQueuesInfoUnordered.put(0, ImmutableMap.of(
                new LogicalQueuesInfoUnordered.Key(queueRouteData1.getBrokerName(), queueRouteData1.getQueueId(), queueRouteData1.getOffsetDelta()), queueRouteData1,
                new LogicalQueuesInfoUnordered.Key(queueRouteData2.getBrokerName(), queueRouteData2.getQueueId(), queueRouteData2.getOffsetDelta()), queueRouteData2
            ));
            logicalQueuesInfoUnordered.put(1, ImmutableMap.of(new LogicalQueuesInfoUnordered.Key(queueRouteData3.getBrokerName(), queueRouteData3.getQueueId(), queueRouteData3.getOffsetDelta()), queueRouteData3));
            assertThat(topicRouteDataNameSrv.getLogicalQueuesInfoUnordered()).isEqualTo(logicalQueuesInfoUnordered);
        }
    }
*/
    @Test
    public void testProcessRequest_RegisterBrokerWithFilterServer() throws RemotingCommandException,
        NoSuchFieldException, IllegalAccessException {
        RemotingCommand request = genSampleRegisterCmd(true);

        // version >= MQVersion.Version.V3_0_11.ordinal() to register with filter server
        request.setVersion(100);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        BrokerData broker = new BrokerData();
        broker.setBrokerName("broker");
        broker.setBrokerAddrs((HashMap) Maps.newHashMap(new Long(2333), "10.10.1.1"));

        assertThat((Map) brokerAddrTable.get(routes))
            .contains(new HashMap.SimpleEntry("broker", broker));
    }

    @Test
    public void testProcessRequest_UnregisterBroker() throws RemotingCommandException, NoSuchFieldException, IllegalAccessException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);

        //Register broker
        RemotingCommand regRequest = genSampleRegisterCmd(true);
        defaultRequestProcessor.processRequest(ctx, regRequest);

        //Unregister broker
        RemotingCommand unregRequest = genSampleRegisterCmd(false);
        RemotingCommand unregResponse = defaultRequestProcessor.processRequest(ctx, unregRequest);

        assertThat(unregResponse.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(unregResponse.getRemark()).isNull();

        RouteInfoManager routes = namesrvController.getRouteInfoManager();
        Field brokerAddrTable = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTable.setAccessible(true);

        assertThat((Map) brokerAddrTable.get(routes)).isNotEmpty();
    }

    @Test
    public void testGetAllTopicList() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(null);
        when(ctx.channel()).thenReturn(channel);

        namesrvController.getNamesrvConfig().setEnableAllTopicList(true);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);

        RemotingCommand response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(response.getRemark()).isNull();

        namesrvController.getNamesrvConfig().setEnableAllTopicList(false);

        response = defaultRequestProcessor.processRequest(ctx, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
    }

    @Test
    public void testGetRouteInfoByTopic() throws Exception {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        RemotingCommand remotingCommandSuccess = clientRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommandSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
        request.getExtFields().put("topic", "test");
        RemotingCommand remotingCommandNoTopicRouteInfo = clientRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommandNoTopicRouteInfo.getCode()).isEqualTo(ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testGetBrokerClusterInfo() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_BROKER_CLUSTER_INFO);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testQueryDataVersion()throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.QUERY_DATA_VERSION);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetBrokerMemberBroker() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_BROKER_MEMBER_GROUP);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testBrokerHeartBeat() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.BROKER_HEARTBEAT);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testAddWritePermOfBroker() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testWipeWritePermOfBroker() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetAllTopicListFromNameserver() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(mock(Channel.class));
        when(ctx.channel().remoteAddress()).thenReturn(new InetSocketAddress(123));
        RemotingCommand request = getRemotingCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testDeleteTopicInNamesrv() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV);
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetKVListByNamespace() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_KVLIST_BY_NAMESPACE);
        request.addExtField("namespace", "default-namespace-1");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.QUERY_NOT_FOUND);
        namesrvController.getKvConfigManager().putKVConfig("default-namespace-1", "key", "value");
        RemotingCommand remotingCommandSuccess = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommandSuccess.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetTopicsByCluster() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_TOPICS_BY_CLUSTER);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetSystemTopicListFromNs() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetUnitTopicList() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_UNIT_TOPIC_LIST);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetHasUnitSubTopicList() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetHasUnitSubUnUnitTopicList() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testUpdateConfig() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.UPDATE_NAMESRV_CONFIG);
        request.addExtField("cluster", "default-cluster");
        Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put("key", "value");
        request.setBody(propertiesMap.toString().getBytes());
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    @Test
    public void testGetConfig() throws RemotingCommandException {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(null);
        RemotingCommand request = getRemotingCommand(RequestCode.GET_NAMESRV_CONFIG);
        request.addExtField("cluster", "default-cluster");
        RemotingCommand remotingCommand = defaultRequestProcessor.processRequest(ctx, request);
        assertThat(remotingCommand.getCode()).isEqualTo(ResponseCode.SUCCESS);
    }

    private RemotingCommand getRemotingCommand(int code) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName("broker");
        RemotingCommand request = RemotingCommand.createRequestCommand(code, header);
        request.addExtField("brokerName", "broker");
        request.addExtField("brokerAddr", "10.10.1.1");
        request.addExtField("clusterName", "cluster");
        request.addExtField("haServerAddr", "10.10.2.1");
        request.addExtField("brokerId", "2333");
        request.addExtField("topic", "unit-test0");
        return request;
    }

    private static RemotingCommand genSampleRegisterCmd(boolean reg) {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        byte[] body = null;
        if (reg) {
            TopicConfigAndMappingSerializeWrapper topicConfigWrapper = new TopicConfigAndMappingSerializeWrapper();
            topicConfigWrapper.getTopicConfigTable().put("unit-test1", new TopicConfig());
            topicConfigWrapper.getTopicConfigTable().put("unit-test2", new TopicConfig());
            RegisterBrokerBody requestBody = new RegisterBrokerBody();
            requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
            body = requestBody.encode(false);
            final int bodyCrc32 = UtilAll.crc32(body);
            header.setBodyCrc32(bodyCrc32);
        }
        header.setBrokerName("broker");
        RemotingCommand request = RemotingCommand.createRequestCommand(
            reg ? RequestCode.REGISTER_BROKER : RequestCode.UNREGISTER_BROKER, header);
        request.addExtField("brokerName", "broker");
        request.addExtField("brokerAddr", "10.10.1.1");
        request.addExtField("clusterName", "cluster");
        request.addExtField("haServerAddr", "10.10.2.1");
        request.addExtField("brokerId", "2333");
        request.setBody(body);
        return request;
    }

    private void registerRouteInfoManager() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        for (int i = 0; i < 2; i++) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName("unit-test" + i);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(8);
            topicConfig.setOrder(false);
            topicConfigConcurrentHashMap.put(topicConfig.getTopicName(), topicConfig);
        }
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
        Channel channel = mock(Channel.class);
        RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234, "127.0.0.1:1001", "",
            null, topicConfigSerializeWrapper, new ArrayList<>(), channel);

    }

}
