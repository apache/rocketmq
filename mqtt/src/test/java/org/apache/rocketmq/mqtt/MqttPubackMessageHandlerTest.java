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

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.InFlightMessage;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPubackMessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class MqttPubackMessageHandlerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private MqttPubackMessageHandler mqttPubackMessageHandler;

    @Mock
    private RemotingChannel remotingChannel;

    @Before
    public void before() {
        defaultMqttMessageProcessor = Mockito.spy(new DefaultMqttMessageProcessor(new MqttConfig(), new SnodeConfig(), null, null, null));
        mqttPubackMessageHandler = new MqttPubackMessageHandler(defaultMqttMessageProcessor);
    }

    @Test
    public void test_handleMessage_wrongMessageType() {
        MqttMessage mqttMessage = new MqttConnectMessage(new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 10), new MqttConnectVariableHeader("name", 0, false, false, false, 1, false, false, 10), new MqttConnectPayload("client1", null, (byte[]) null, null, null));

        exception.expect(WrongMessageTypeException.class);
        mqttPubackMessageHandler.handleMessage(mqttMessage, remotingChannel);
    }

    @Test
    public void test_handleMessage() {
        MqttMessage mqttMessage = new MqttPubAckMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 10), MqttMessageIdVariableHeader.from(1));
        IOTClientManagerImpl iotClientManager = Mockito.mock(IOTClientManagerImpl.class);
        Mockito.when((IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager()).thenReturn(iotClientManager);

        MQTTSession mqttSession = Mockito.spy(new MQTTSession("client1", ClientRole.IOTCLIENT, null, true, true, remotingChannel, System.currentTimeMillis(), defaultMqttMessageProcessor));
        Mockito.when(iotClientManager.getClient(anyString(), any(RemotingChannel.class))).thenReturn(mqttSession);
        InFlightMessage inFlightMessage = Mockito.spy(new InFlightMessage("topicTest", 0, "Hello".getBytes(), null, 0));
        doReturn(inFlightMessage).when(mqttSession).pubAckReceived(anyInt());
        RemotingCommand remotingCommand = mqttPubackMessageHandler.handleMessage(mqttMessage, remotingChannel);
        assert remotingCommand == null;
    }
}
