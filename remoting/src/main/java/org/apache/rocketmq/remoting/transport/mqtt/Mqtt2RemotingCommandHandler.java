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

package org.apache.rocketmq.remoting.transport.mqtt;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class Mqtt2RemotingCommandHandler extends MessageToMessageDecoder<MqttMessage> {

    /**
     * Decode from one message to an other. This method will be called for each written message that
     * can be handled by this encoder.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder}
     * belongs to
     * @param msg the message to decode to an other one
     * @param out the {@link List} to which decoded messages should be added
     * @throws Exception is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, MqttMessage msg, List<Object> out)
            throws Exception {
        if (!(msg instanceof MqttMessage)) {
            return;
        }
        RemotingCommand requestCommand = null;
        MqttFixedHeader mqttFixedHeader = msg.fixedHeader();
        Object variableHeader = msg.variableHeader();

        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                RocketMQMqttConnectPayload payload = RocketMQMqttConnectPayload
                        .fromMqttConnectPayload(((MqttConnectMessage) msg).payload());
                MqttHeader mqttHeader = new MqttHeader();
                mqttHeader.setMessageType(mqttFixedHeader.messageType().value());
                mqttHeader.setDup(mqttFixedHeader.isDup());
                mqttHeader.setQosLevel(mqttFixedHeader.qosLevel().value());
                mqttHeader.setRetain(mqttFixedHeader.isRetain());
                mqttHeader.setRemainingLength(mqttFixedHeader.remainingLength());

                MqttConnectVariableHeader mqttConnectVariableHeader = (MqttConnectVariableHeader) variableHeader;
                mqttHeader.setName(mqttConnectVariableHeader.name());
                mqttHeader.setVersion(mqttConnectVariableHeader.version());
                mqttHeader.setHasUserName(mqttConnectVariableHeader.hasUserName());
                mqttHeader.setHasPassword(mqttConnectVariableHeader.hasPassword());
                mqttHeader.setWillRetain(mqttConnectVariableHeader.isWillRetain());
                mqttHeader.setWillQos(mqttConnectVariableHeader.willQos());
                mqttHeader.setWillFlag(mqttConnectVariableHeader.isWillFlag());
                mqttHeader.setCleanSession(mqttConnectVariableHeader.isCleanSession());
                mqttHeader
                        .setKeepAliveTimeSeconds(mqttConnectVariableHeader.keepAliveTimeSeconds());

                requestCommand = RemotingCommand
                        .createRequestCommand(1000, mqttHeader);
                requestCommand.makeCustomHeaderToNet();

                requestCommand.setBody(payload.encode());
                out.add(requestCommand);
            case CONNACK:
            case DISCONNECT:
            case PUBLISH:
            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
            case SUBSCRIBE:
            case SUBACK:
            case UNSUBSCRIBE:
            case UNSUBACK:
            case PINGREQ:
            case PINGRESP:
        }
    }

    private byte[] encode(Object obj) {
        String json = JSON.toJSONString(obj, false);
        if (json != null) {
            return json.getBytes(Charset.forName(RemotingUtil.REMOTING_CHARSET));
        }
        return null;
    }
}
