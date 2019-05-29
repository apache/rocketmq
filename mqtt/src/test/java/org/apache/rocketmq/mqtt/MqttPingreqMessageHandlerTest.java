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

import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.mqtthandler.impl.MqttPingreqMessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MqttPingreqMessageHandlerTest {
    @Mock
    private RemotingChannel remotingChannel;
    @Mock
    private IOTClientManagerImpl iotClientManager;
    @Mock
    private MqttMessage mqttMessage;
    @Mock
    private MQTTSession client;
    @Mock
    private DefaultMqttMessageProcessor processor;

    private MqttPingreqMessageHandler mqttPingreqMessageHandler;

    @Before
    public void init() {
        mqttPingreqMessageHandler = new MqttPingreqMessageHandler(processor);
        when(processor.getIotClientManager()).thenReturn(iotClientManager);
        when(iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel)).thenReturn(client);
        when(client.getClientId()).thenReturn("Mock Client");
    }

    @Test
    public void testHandlerMessageReturnResp() {
        when(client.isConnected()).thenReturn(true);
        RemotingCommand response = mqttPingreqMessageHandler.handleMessage(mqttMessage, remotingChannel);
        verify(client).setLastUpdateTimestamp(anyLong());
        assertEquals(ResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testHandlerMessageReturnNull() {
        when(client.isConnected()).thenReturn(false);
        RemotingCommand response = mqttPingreqMessageHandler.handleMessage(mqttMessage, remotingChannel);
        assertEquals(ResponseCode.SYSTEM_ERROR,response.getCode());

    }
}
