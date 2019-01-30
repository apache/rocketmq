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

package org.apache.rocketmq.snode.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import org.apache.rocketmq.snode.constant.MqttConstant;

public class MessageUtil {

    public static final String MQTT_QOS_LEVEL = "MQTT_QOS_LEVEL";
    public static final String MQTT_IS_RETAIN = "MQTT_IS_RETAIN";
    public static final String MQTT_PACKET_ID = "MQTT_PACKET_ID";
    public static final String MQTT_TOPIC_NAME = "MQTT_TOPIC_NAME";
    public static final String MQTT_REMAINING_LENGTH = "MQTT_REMAINING_LENGTH";
    public static final String MQTT_IS_DUP = "MQTT_IS_DUP";
    public static final String MQTT_CLIENT_NAME = "MQTT_CLIENT_NAME";
    public static final String MQTT_IS_CLEAN_SESSION = "MQTT_IS_CLEAN_SESSION";
    public static final String MQTT_KEEP_ALIVE_TIME = "MQTT_KEEP_ALIVE_TIME";
    public static final String MQTT_PROTOCOL_VERSION = "MQTT_PROTOCOL_VERSION";

    public static MqttSubAckMessage getMqttSubackMessage(MqttSubscribeMessage message,
            MqttSubAckPayload payload) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.SUBACK,
                false,
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                0
        );
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader
                .from(message.variableHeader().messageId());

        return new MqttSubAckMessage(fixedHeader, variableHeader, payload);
    }

    public static MqttPublishMessage getMqttPublishMessage(MqttMessage message, boolean isDup) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH,
                isDup,
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                message.fixedHeader().remainingLength()
        );
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(
                ((MqttPublishVariableHeader)message.variableHeader()).topicName(),
                ((MqttPublishVariableHeader)message.variableHeader()).packetId()
        );
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes((byte[]) message.payload());
        return new MqttPublishMessage(fixedHeader, variableHeader, buf);
    }

    public static MqttConnAckMessage getMqttConnackMessage(MqttConnectMessage message,
            MqttConnectReturnCode returnCode) {
        assert message.fixedHeader().messageType() == MqttMessageType.CONNECT;
        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(
                returnCode,
                message.variableHeader().isCleanSession()
        );
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNACK,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                0);
        return new MqttConnAckMessage(fixedHeader, variableHeader);
    }

    public static MqttPubAckMessage getMqttPubackMessage(MqttPublishMessage message) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBACK,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                message.fixedHeader().remainingLength()
        );

        return new MqttPubAckMessage(fixedHeader,
                MqttMessageIdVariableHeader.from(message.variableHeader().packetId()));

    }

    public static MqttMessage getMqttPubrecMessage(MqttPublishMessage message) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBREC,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                message.fixedHeader().remainingLength()
        );
        return new MqttMessage(fixedHeader);
    }

    public static MqttMessage getMqttPubrelMessage(MqttMessage message) {
        assert message.fixedHeader().messageType() == MqttMessageType.PUBREC;
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBREL,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                message.fixedHeader().remainingLength()
        );
        return new MqttMessage(fixedHeader);
    }

    public static MqttMessage getMqttPubcompMessage(MqttMessage message) {
        assert message.fixedHeader().messageType() == MqttMessageType.PUBREL;
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBCOMP,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                message.fixedHeader().remainingLength()
        );
        return new MqttMessage(fixedHeader);
    }

    public static MqttMessage getMqttPingrespMessage(MqttMessage message) {
        assert message.fixedHeader().messageType() == MqttMessageType.PINGREQ;
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PINGRESP,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                0
        );
        return new MqttMessage(fixedHeader);
    }

    public static MqttUnsubAckMessage getMqttUnsubackMessage(MqttUnsubscribeMessage message) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.UNSUBACK,
                message.fixedHeader().isDup(),
                message.fixedHeader().qosLevel(),
                message.fixedHeader().isRetain(),
                0
        );
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader
                .from(message.variableHeader().messageId());
        return new MqttUnsubAckMessage(fixedHeader, variableHeader);
    }

    public static int actualQos(int qos) {
        return Math.min(MqttConstant.MAX_SUPPORTED_QOS, qos);
    }
}
