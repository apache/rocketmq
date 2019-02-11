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
package org.apache.rocketmq.snode.processor;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.message.mqtt.WillMessage;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.IOTClientManagerImpl;
import org.apache.rocketmq.snode.processor.mqtthandler.MqttDisconnectMessageHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MqttDisconnectMessageHandlerTest {

    @Mock
    private RemotingChannel remotingChannel;

    @Test
    public void testHandlerMessage() throws Exception {

        SnodeController snodeController = new SnodeController(new ServerConfig(),
                new ClientConfig(), new SnodeConfig());
        MqttDisconnectMessageHandler mqttDisconnectMessageHandler = new MqttDisconnectMessageHandler(
                snodeController);
        Client client = new Client();
        client.setRemotingChannel(remotingChannel);
        client.setClientId("123456");
        snodeController.getIotClientManager().register(IOTClientManagerImpl.IOTGROUP, client);
        snodeController.getWillMessageService().saveWillMessage("123456", new WillMessage());
        MqttMessage mqttDisconnectMessage = new MqttMessage(new MqttFixedHeader(
                MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 200));

        mqttDisconnectMessageHandler.handleMessage(mqttDisconnectMessage, remotingChannel);
    }
}
