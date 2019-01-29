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

import com.alibaba.fastjson.JSON;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.CodecHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.transport.mqtt.RocketMQMqttConnectPayload;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.apache.rocketmq.snode.processor.mqtthandler.MqttConnectMessageHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMqttMessageProcessorTest {
    private DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    @Spy
    private SnodeController snodeController = new SnodeController(new ServerConfig(), new ClientConfig(), new SnodeConfig());

    @Mock
    private RemotingChannel remotingChannel;

    private String topic = "SnodeTopic";

    private String group = "SnodeGroup";

    private String enodeName = "enodeName";

    @Before
    public void init() {
        defaultMqttMessageProcessor = new DefaultMqttMessageProcessor();
    }

    @Test
    public void testProcessRequest() throws RemotingCommandException, UnsupportedEncodingException {
        RemotingCommand request = createMqttConnectMesssageCommand();
        defaultMqttMessageProcessor.registerMessageHanlder(MqttMessageType.CONNECT, new MqttConnectMessageHandler(snodeController));
        defaultMqttMessageProcessor.processRequest(remotingChannel, request);
    }

    private MqttHeader createMqttConnectMesssageHeader() {
        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setMessageType(MqttMessageType.CONNECT.value());
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
        mqttHeader.setRetain(false);
        mqttHeader.setRemainingLength(200);

        mqttHeader.setName("MQTT");
        mqttHeader.setVersion(4);
        mqttHeader.setHasUserName(false);
        mqttHeader.setHasPassword(false);
        mqttHeader.setWillRetain(false);
        mqttHeader.setWillQos(0);
        mqttHeader.setWillFlag(false);
        mqttHeader.setCleanSession(false);
        mqttHeader.setKeepAliveTimeSeconds(60);

        return mqttHeader;
    }

    private RemotingCommand createMqttConnectMesssageCommand() {
        MqttHeader mqttHeader = createMqttConnectMesssageHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_MESSAGE, mqttHeader);
        MqttConnectPayload payload = new MqttConnectPayload("1234567", "testTopic", "willMessage".getBytes(), null, "1234567".getBytes());
        request.setBody(RocketMQMqttConnectPayload.fromMqttConnectPayload(payload).encode());
        CodecHelper.makeCustomHeaderToNet(request);
        return request;
    }
    private byte[] encode(Object obj) {
        String json = JSON.toJSONString(obj, false);
        if (json != null) {
            return json.getBytes(Charset.forName(RemotingUtil.REMOTING_CHARSET));
        }
        return null;
    }
}
