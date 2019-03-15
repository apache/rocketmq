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
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.HashSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientManager;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.mqtt.WillMessage;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.exception.MqttConnectException;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttConnectMessageHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private static final int MIN_AVAILABLE_VERSION = 3;
    private static final int MAX_AVAILABLE_VERSION = 4;
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttConnectMessageHandler(DefaultMqttMessageProcessor defaultMqttMessageProcessor) {
        this.defaultMqttMessageProcessor = defaultMqttMessageProcessor;
    }

    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttConnectMessage)) {
            log.error("Wrong message type! Expected type: CONNECT but {} was received.", message.fixedHeader().messageType());
            throw new WrongMessageTypeException("Wrong message type exception.");
        }
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) message;
        MqttConnectPayload payload = mqttConnectMessage.payload();

        RemotingCommand command = RemotingCommand.createResponseCommand(MqttHeader.class);
        MqttHeader mqttHeader = (MqttHeader) command.readCustomHeader();
        mqttHeader.setMessageType(MqttMessageType.CONNACK.value());
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
        mqttHeader.setRetain(false);
        mqttHeader.setRemainingLength(0x02);
        /* TODO when clientId.length=0 and cleanSession=0, the server should assign a unique clientId to the client.*/
        //validate clientId
        if (StringUtils.isBlank(payload.clientIdentifier()) && !mqttConnectMessage.variableHeader()
            .isCleanSession()) {
            mqttHeader.setConnectReturnCode(
                MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED.name());
            mqttHeader.setSessionPresent(false);
            command.setCode(ResponseCode.SYSTEM_ERROR);
            command.setRemark("CONNECTION_REFUSED_IDENTIFIER_REJECTED");
            return command;
        }
        //authentication
        if (mqttConnectMessage.variableHeader().hasPassword() && mqttConnectMessage.variableHeader()
            .hasUserName()
            && !authorized(payload.userName(), payload.password())) {
            mqttHeader.setConnectReturnCode(
                MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.name());
            mqttHeader.setSessionPresent(false);
            command.setCode(ResponseCode.SYSTEM_ERROR);
            command.setRemark("CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD");
            return command;
        }
        //treat a second CONNECT packet as a protocol violation and disconnect
        if (isConnected(remotingChannel, payload.clientIdentifier())) {
            log.error("This client has been connected. The second CONNECT packet is treated as a protocol vialation and the connection will be closed.");
            remotingChannel.close();
            return null;
        }
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
        //set Session Present according to whether the server has already stored Session State for the clientId
        if (mqttConnectMessage.variableHeader().isCleanSession()) {
            mqttHeader.setSessionPresent(false);
            //do the logic of clean Session State
            iotClientManager.cleanSessionState(payload.clientIdentifier());
            Subscription subscription = new Subscription();
            subscription.setCleanSession(true);
            iotClientManager.initSubscription(payload.clientIdentifier(), subscription);
        } else {
            if (alreadyStoredSession(payload.clientIdentifier())) {
                mqttHeader.setSessionPresent(true);
            } else {
                mqttHeader.setSessionPresent(false);
                Subscription subscription = new Subscription();
                subscription.setCleanSession(false);
                iotClientManager.initSubscription(payload.clientIdentifier(), subscription);
            }
        }

        Client client = new Client();
        client.setClientId(payload.clientIdentifier());
        client.setClientRole(ClientRole.IOTCLIENT);
        client.setGroups(new HashSet<String>() {
            {
                add("IOT_GROUP");
            }
        });
        client.setConnected(true);
        client.setRemotingChannel(remotingChannel);
        client.setLastUpdateTimestamp(System.currentTimeMillis());
        //register remotingChannel<--->client
        iotClientManager.register(IOTClientManagerImpl.IOT_GROUP, client);

        //save will message if have
        if (mqttConnectMessage.variableHeader().isWillFlag()) {
            if (payload.willTopic() == null || payload.willMessageInBytes() == null) {
                log.error("Will message and will topic can not be null.");
                throw new MqttConnectException("Will message and will topic can not be null.");
            }
            WillMessage willMessage = new WillMessage();
            willMessage.setQos(mqttConnectMessage.variableHeader().willQos());
            willMessage.setWillTopic(payload.willTopic());
            willMessage.setRetain(mqttConnectMessage.variableHeader().isWillRetain());
            willMessage.setBody(payload.willMessageInBytes());
            defaultMqttMessageProcessor.getWillMessageService().saveWillMessage(client.getClientId(), willMessage);
        }

        mqttHeader.setConnectReturnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED.name());
        command.setCode(ResponseCode.SUCCESS);
        command.setRemark(null);
        return command;
    }

    private boolean alreadyStoredSession(String clientId) {
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
        Subscription subscription = iotClientManager.getSubscriptionByClientId(clientId);
        if (subscription == null) {
            return false;
        }
        if (subscription.isCleanSession()) {
            return false;
        }
        return true;
    }

    private boolean authorized(String username, String password) {
        return true;
    }

    private boolean isConnected(RemotingChannel remotingChannel, String clientId) {
        ClientManager iotClientManager = defaultMqttMessageProcessor.getIotClientManager();
        Client client = iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
        if (client != null && client.getClientId().equals(clientId) && client.isConnected()) {
            return true;
        }
        return false;
    }

    private boolean isServiceAvailable(MqttConnectMessage connectMessage) {
        int version = connectMessage.variableHeader().version();
        return version >= MIN_AVAILABLE_VERSION && version <= MAX_AVAILABLE_VERSION;
    }

    private boolean checkPassword(byte[] bytes) {
        return true;
    }

    private boolean checkUsername(String s) {
        return true;
    }

    private boolean isAuthorized(MqttConnectMessage message) {
        return true;
    }

    private boolean isClientIdValid(String s) {
        return true;
    }
}
