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
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.transport.mqtt.RocketMQMqttConnectPayload;
import org.apache.rocketmq.snode.processor.mqtthandler.MessageHandler;

public class DefaultMqttMessageProcessor implements RequestProcessor {

    private Map<MqttMessageType, MessageHandler> type2handler = new HashMap<>();
    private static final int MIN_AVAILABLE_VERSION = 3;
    private static final int MAX_AVAILABLE_VERSION = 4;


    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand message)
            throws RemotingCommandException, UnsupportedEncodingException {
        MqttHeader mqttHeader = (MqttHeader) message.decodeCommandCustomHeader(MqttHeader.class);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.valueOf(mqttHeader.getMessageType()),
                mqttHeader.isDup(), MqttQoS.valueOf(mqttHeader.getQosLevel()), mqttHeader.isRetain(),
                mqttHeader.getRemainingLength());
        MqttMessage mqttMessage = null;
        switch (fixedHeader.messageType()) {
            case CONNECT:
                MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                        mqttHeader.getName(), mqttHeader.getVersion(), mqttHeader.isHasUserName(),
                        mqttHeader.isHasPassword(), mqttHeader.isWillRetain(),
                        mqttHeader.getWillQos(), mqttHeader.isWillFlag(),
                        mqttHeader.isCleanSession(), mqttHeader.getKeepAliveTimeSeconds());
                RocketMQMqttConnectPayload payload = decode(message.getBody(), RocketMQMqttConnectPayload.class);
                mqttMessage = new MqttConnectMessage(fixedHeader, variableHeader, payload.toMqttConnectPayload());
            case DISCONNECT:
        }
        return type2handler.get(MqttMessageType.valueOf(mqttHeader.getMessageType())).handleMessage(mqttMessage, remotingChannel);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, Charset.forName(RemotingUtil.REMOTING_CHARSET));
        return JSON.parseObject(json, classOfT);
    }

    public void registerMessageHanlder(MqttMessageType type, MessageHandler handler) {
        type2handler.put(type, handler);
    }
}
