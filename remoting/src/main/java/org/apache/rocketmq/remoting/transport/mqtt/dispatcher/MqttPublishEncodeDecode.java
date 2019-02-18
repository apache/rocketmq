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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.rocketmq.remoting.netty.CodecHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPublishEncodeDecode implements Message2MessageEncodeDecode {

    @Override
    public RemotingCommand decode(MqttMessage mqttMessage) {
        ByteBuf byteBuf = ((MqttPublishMessage) mqttMessage).payload();
        byte[] payload = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(payload);

        RemotingCommand requestCommand = null;
        MqttFixedHeader mqttFixedHeader = mqttMessage.fixedHeader();
        MqttConnAckVariableHeader variableHeader = (MqttConnAckVariableHeader) mqttMessage
            .variableHeader();

        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setMessageType(mqttFixedHeader.messageType().value());
        mqttHeader.setDup(mqttFixedHeader.isDup());
        mqttHeader.setQosLevel(mqttFixedHeader.qosLevel().value());
        mqttHeader.setRetain(mqttFixedHeader.isRetain());
        mqttHeader.setRemainingLength(mqttFixedHeader.remainingLength());

        mqttHeader.setConnectReturnCode(variableHeader.connectReturnCode().name());
        mqttHeader.setSessionPresent(variableHeader.isSessionPresent());

        requestCommand = RemotingCommand
            .createRequestCommand(1000, mqttHeader);
        CodecHelper.makeCustomHeaderToNet(requestCommand);

        requestCommand.setBody(payload);
        return requestCommand;
    }

    @Override
    public MqttMessage encode(RemotingCommand remotingCommand) {
        return null;
    }
}
