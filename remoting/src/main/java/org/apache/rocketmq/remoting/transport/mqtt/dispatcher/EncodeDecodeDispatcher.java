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

import io.netty.handler.codec.mqtt.MqttMessageType;
import java.util.HashMap;
import java.util.Map;

public class EncodeDecodeDispatcher {

    private static Map<MqttMessageType, Message2MessageEncodeDecode> encodeDecodeDispatcher = new HashMap<>();

    static {
        encodeDecodeDispatcher.put(MqttMessageType.CONNECT, new MqttConnectEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.CONNACK, new MqttConnectackEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.DISCONNECT, null);
        encodeDecodeDispatcher.put(MqttMessageType.PUBLISH, new MqttPublishEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.PUBACK, new MqttPubackEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.PUBREC, null);
        encodeDecodeDispatcher.put(MqttMessageType.PUBREL, null);
        encodeDecodeDispatcher.put(MqttMessageType.PUBCOMP, null);
        encodeDecodeDispatcher.put(MqttMessageType.SUBSCRIBE, new MqttSubscribeEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.SUBACK, new MqttSubackEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.UNSUBSCRIBE, new MqttUnSubscribeEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.UNSUBACK, new MqttUnSubackEncodeDecode());
        encodeDecodeDispatcher.put(MqttMessageType.PINGREQ, null);
        encodeDecodeDispatcher.put(MqttMessageType.PINGRESP, null);
    }

    public static Map<MqttMessageType, Message2MessageEncodeDecode> getEncodeDecodeDispatcher() {
        return encodeDecodeDispatcher;
    }

}
