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

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.InFlightMessage;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.task.MqttPushTask;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.mqtt.util.orderedexecutor.SafeRunnable;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPubackMessageHandler implements MessageHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttPubackMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
    }

    /**
     * handle the PUBACK message from the client <ol> <li>remove the message from the published in-flight messages</li>
     * <li>ack the message in the MessageStore</li> </ol>
     *
     * @param
     * @return
     */
    @Override public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttPubAckMessage)) {
            log.error("Wrong message type! Expected type: PUBACK but {} was received. MqttMessage={}", message.fixedHeader().messageType(), message.toString());
            throw new WrongMessageTypeException("Wrong message type exception.");
        }
        MqttPubAckMessage mqttPubAckMessage = (MqttPubAckMessage) message;
        MqttMessageIdVariableHeader variableHeader = mqttPubAckMessage.variableHeader();
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
        MQTTSession client = (MQTTSession) iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);

        InFlightMessage removedMessage = client.pubAckReceived(variableHeader.messageId());
        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setMessageType(MqttMessageType.PUBLISH.value());
        mqttHeader.setDup(false);
        mqttHeader.setRetain(false); //TODO set to false temporarily, need to be implemented.
        MqttPushTask task = new MqttPushTask(defaultMqttMessageProcessor, mqttHeader, MqttUtil.getRootTopic(removedMessage.getTopic()), client, removedMessage.getBrokerData());
        //add task to orderedExecutor
        this.defaultMqttMessageProcessor.getOrderedExecutor().executeOrdered(client.getClientId(), SafeRunnable.safeRun(task));
        return null;
    }
}
