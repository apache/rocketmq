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

package org.apache.rocketmq.mqtt.mqtthandler.impl;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPingreqMessageHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttPingreqMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
    }

    /**
     * handle the PINGREQ message from client <ol> <li>check client exists</li> <li>check client is connected</li>
     * <li>generate the PINGRESP message</li> <li>send the PINGRESP message to the client</li> </ol>
     *
     * @param message
     * @return
     */
    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
        Client client = iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
        log.debug("Handle MQTT client: {} Pingreq.", client.getClientId());
        RemotingCommand response = RemotingCommand.createResponseCommand(MqttHeader.class);
        if (client != null && client.isConnected()) {
            client.setLastUpdateTimestamp(System.currentTimeMillis());
            MqttHeader mqttHeader = (MqttHeader) response.readCustomHeader();
            mqttHeader.setMessageType(MqttMessageType.PINGRESP.value());
            mqttHeader.setDup(false);
            mqttHeader.setQosLevel(0);
            mqttHeader.setRetain(false);
            mqttHeader.setRemainingLength(0);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("MQTT Client is null or not connected");
        return response;
    }
}
