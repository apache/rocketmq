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

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.processor.mqtthandler.MqttConnectMessageHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MqttConnectMessageHandlerTest {

    @Mock
    private RemotingChannel remotingChannel;

    @Test
    public void testHandlerMessage() throws Exception {

        MqttConnectMessageHandler mqttConnectMessageHandler = new MqttConnectMessageHandler(new SnodeController(new SnodeConfig(), new MqttConfig()));
        MqttConnectPayload payload = new MqttConnectPayload("1234567", "testTopic", "willMessage".getBytes(), null, "1234567".getBytes());
        MqttConnectMessage mqttConnectMessage = new MqttConnectMessage(new MqttFixedHeader(
            MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 200), new MqttConnectVariableHeader(null, 4, false, false, false, 0, false, false, 50), new MqttConnectPayload("abcd", "ttest", "message".getBytes(), "user", "password".getBytes()));

        mqttConnectMessageHandler.handleMessage(mqttConnectMessage, remotingChannel);
    }
}
