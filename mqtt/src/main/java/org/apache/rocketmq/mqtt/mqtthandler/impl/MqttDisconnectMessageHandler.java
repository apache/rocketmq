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

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class MqttDisconnectMessageHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttDisconnectMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
    }

    /**
     * handle the DISCONNECT message from the client <ol> <li>discard the Will Message and Will Topic</li> <li>remove
     * the client from the IOTClientManager</li> <li>disconnect the connection</li> </ol>
     */
    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        // TODO discard the Will Message and Will Topic
        MqttFixedHeader fixedHeader = message.fixedHeader();
        if (fixedHeader.qosLevel() != MqttQoS.AT_MOST_ONCE || !fixedHeader.isDup() || !fixedHeader
            .isRetain()) {
            log.error(
                "The reserved bits(qos/isDup/isRetain) are not zero. Qos={}, isDup={}, isRetain={}",
                fixedHeader.qosLevel(), fixedHeader.isDup(), fixedHeader.isRetain());
            remotingChannel.close();
            return null;
        }

        //discard will message associated with the current connection(client)
        Client client = defaultMqttMessageProcessor.getIotClientManager()
            .getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
        if (client != null) {
            defaultMqttMessageProcessor.getWillMessageService().deleteWillMessage(client.getClientId());
        }
        client.setConnected(false);
        if (remotingChannel.isActive()) {
            remotingChannel.close();
        }
        return null;
    }
}
