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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.Set;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.InnerMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class MqttMessageForwarder implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final InnerMqttMessageProcessor innerMqttMessageProcessor;

    public MqttMessageForwarder(InnerMqttMessageProcessor processor) {
        this.innerMqttMessageProcessor = processor;
    }

    /**
     * handle PUBLISH message from client
     *
     * @param message
     * @return whether the message is handled successfully
     */
    @Override public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) message;
        MqttFixedHeader fixedHeader = mqttPublishMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = mqttPublishMessage.variableHeader();
        if (fixedHeader.qosLevel().equals(MqttQoS.AT_MOST_ONCE)) {
            ByteBuf payload = mqttPublishMessage.payload();
            //Publish message to clients
            Set<Client> clientsTobePublish = findCurrentNodeClientsTobePublish(variableHeader.topicName(), (IOTClientManagerImpl) this.innerMqttMessageProcessor.getIotClientManager());
            innerMqttMessageProcessor.getDefaultMqttMessageProcessor().getMqttPushService().pushMessageQos0(variableHeader.topicName(), payload, clientsTobePublish);
        }else if(fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE)){
            //TODO
        }
        return doResponse(fixedHeader);
    }
}
