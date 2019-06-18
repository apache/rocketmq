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
package org.apache.rocketmq.mqtt;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.common.service.NnodeService;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPublishMessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.transport.mqtt.MqttRemotingServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class MqttPublishMessageHandlerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    @Mock
    private MqttRemotingServer mqttRemotingServer;
    @Mock
    private EnodeService enodeService;
    @Mock
    private NnodeService nnodeService;

    private DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    private MqttPublishMessageHandler mqttPublishMessageHandler;

    @Mock
    private RemotingChannel remotingChannel;

    private MqttMessage mqttPublishMessage;

    @Before
    public void before() {
        defaultMqttMessageProcessor = Mockito.spy(new DefaultMqttMessageProcessor(new MqttConfig(), new SnodeConfig(), mqttRemotingServer, enodeService, nnodeService));
        mqttPublishMessageHandler = new MqttPublishMessageHandler(defaultMqttMessageProcessor);
    }

    @Test
    public void test_handleMessage_wrongMessageType() throws Exception {
        MqttMessage mqttMessage = new MqttConnectMessage(new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 10), new MqttConnectVariableHeader("name", 0, false, false, false, 1, false, false, 10), new MqttConnectPayload("client1", null, (byte[]) null, null, null));

        exception.expect(WrongMessageTypeException.class);
        mqttPublishMessageHandler.handleMessage(mqttMessage, remotingChannel);
    }

    @Test
    public void test_handleMessage_wrongQos() throws Exception {
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.FAILURE, false, 10), new MqttPublishVariableHeader("topicTest", 1), Unpooled.copiedBuffer("Hello".getBytes()));

        Mockito.when(remotingChannel.toString()).thenReturn("testRemotingChannel");
        RemotingCommand remotingCommand = mqttPublishMessageHandler.handleMessage(mqttPublishMessage, remotingChannel);
        assert remotingCommand == null;
        Mockito.verify(remotingChannel).close();
    }

    @Test
    public void test_handleMessage_qos0() throws Exception {
        prepareSubscribeData();
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 10), new MqttPublishVariableHeader("topic/c", 1), Unpooled.copiedBuffer("Hello".getBytes()));
        Mockito.when(remotingChannel.toString()).thenReturn("testRemotingChannel");
        RemotingCommand remotingCommand = mqttPublishMessageHandler.handleMessage(mqttPublishMessage, remotingChannel);
        assert remotingCommand == null;
    }

    @Test
    public void test_handleMessage_qos1() throws Exception {
        prepareSubscribeData();
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 10), new MqttPublishVariableHeader("topic/c", 1), Unpooled.copiedBuffer("Hello".getBytes()));
        Mockito.when(remotingChannel.toString()).thenReturn("testRemotingChannel");
        TopicRouteData topicRouteData = buildTopicRouteData();
        Mockito.when(nnodeService.getTopicRouteDataByTopic(anyString(), anyBoolean())).thenReturn(topicRouteData);
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        Mockito.when(this.defaultMqttMessageProcessor.getEnodeService().sendMessage(any(), anyString(), any(RemotingCommand.class))).thenReturn(future);
        RemotingCommand remotingCommand = mqttPublishMessageHandler.handleMessage(mqttPublishMessage, remotingChannel);
        assert remotingCommand != null;
        MqttHeader mqttHeader = (MqttHeader) remotingCommand.readCustomHeader();
        assertEquals(1, mqttHeader.getPacketId().intValue());
        assertEquals(MqttQoS.AT_MOST_ONCE, mqttHeader.getQosLevel());
        assertEquals(false, mqttHeader.isDup());
        assertEquals(false, mqttHeader.isRetain());
        assertEquals(2, mqttHeader.getRemainingLength());
    }

    private TopicRouteData buildTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> brokerDataList = new ArrayList<>();
        brokerDataList.add(new BrokerData("DefaultCluster", "broker1", null));
        topicRouteData.setBrokerDatas(brokerDataList);
        return topicRouteData;
    }

    private void prepareSubscribeData() {
        IOTClientManagerImpl manager = (IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager();
        ConcurrentHashMap<String, Subscription> subscriptions = manager.getClientId2Subscription();
        ConcurrentHashMap<String, Set<Client>> topic2Clients = manager.getTopic2Clients();

        Subscription subscription1 = new Subscription();
        subscription1.setCleanSession(true);
        subscription1.getSubscriptionTable().put("topic/a", new MqttSubscriptionData(1, "client1", "topic/a"));
        subscription1.getSubscriptionTable().put("topic/+", new MqttSubscriptionData(1, "client1", "topic/+"));
        subscriptions.put("client1", subscription1);

        Subscription subscription2 = new Subscription();
        subscription2.setCleanSession(true);
        subscription2.getSubscriptionTable().put("topic/b", new MqttSubscriptionData(1, "client2", "topic/b"));
        subscription2.getSubscriptionTable().put("topic/+", new MqttSubscriptionData(1, "client2", "topic/+"));
        subscriptions.put("client2", subscription2);

        Subscription subscription3 = new Subscription();
        subscription3.setCleanSession(true);
        subscription3.getSubscriptionTable().put("test/c", new MqttSubscriptionData(1, "client3", "topic/c"));
        subscription3.getSubscriptionTable().put("test/d", new MqttSubscriptionData(1, "client3", "topic/d"));
        subscriptions.put("client3", subscription3);

        Set<Client> clients_1 = new HashSet<>();
        Set<String> groups = new HashSet<String>();
        groups.add("IOT_GROUP");
        clients_1.add(new MQTTSession("client1", ClientRole.IOTCLIENT, groups, true, true, remotingChannel, System.currentTimeMillis(), defaultMqttMessageProcessor));
        clients_1.add(new MQTTSession("client2", ClientRole.IOTCLIENT, groups, true, true, remotingChannel, System.currentTimeMillis(), defaultMqttMessageProcessor));

        Set<Client> clients_2 = new HashSet<>();
        groups.add("IOT_GROUP");
        clients_1.add(new MQTTSession("client3", ClientRole.IOTCLIENT, groups, true, true, remotingChannel, System.currentTimeMillis(), defaultMqttMessageProcessor));

        topic2Clients.put("topic", clients_1);
        topic2Clients.put("test", clients_2);

    }
}
