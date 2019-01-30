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

package org.apache.rocketmq.remoting.transport.mqtt.dispatcher;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.remoting.netty.CodecHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.transport.mqtt.RocketMQMqttConnectPayload;

public class MqttConnectEncodeDecode implements Message2MessageEncodeDecode {

    @Override
    public RemotingCommand decode(MqttMessage mqttMessage) {
        RocketMQMqttConnectPayload payload = RocketMQMqttConnectPayload
                .fromMqttConnectPayload(((MqttConnectMessage) mqttMessage).payload());
        RemotingCommand requestCommand = null;
        MqttFixedHeader mqttFixedHeader = mqttMessage.fixedHeader();
        MqttConnectVariableHeader variableHeader = (MqttConnectVariableHeader) mqttMessage
                .variableHeader();

        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setMessageType(mqttFixedHeader.messageType().value());
        mqttHeader.setDup(mqttFixedHeader.isDup());
        mqttHeader.setQosLevel(mqttFixedHeader.qosLevel().value());
        mqttHeader.setRetain(mqttFixedHeader.isRetain());
        mqttHeader.setRemainingLength(mqttFixedHeader.remainingLength());

        mqttHeader.setName(variableHeader.name());
        mqttHeader.setVersion(variableHeader.version());
        mqttHeader.setHasUserName(variableHeader.hasUserName());
        mqttHeader.setHasPassword(variableHeader.hasPassword());
        mqttHeader.setWillRetain(variableHeader.isWillRetain());
        mqttHeader.setWillQos(variableHeader.willQos());
        mqttHeader.setWillFlag(variableHeader.isWillFlag());
        mqttHeader.setCleanSession(variableHeader.isCleanSession());
        mqttHeader
                .setKeepAliveTimeSeconds(variableHeader.keepAliveTimeSeconds());

        requestCommand = RemotingCommand
                .createRequestCommand(1000, mqttHeader);
        CodecHelper.makeCustomHeaderToNet(requestCommand);

        requestCommand.setBody(payload.encode());
        return requestCommand;
    }

    @Override
    public MqttMessage encode(RemotingCommand remotingCommand) {
        return null;
    }
}
