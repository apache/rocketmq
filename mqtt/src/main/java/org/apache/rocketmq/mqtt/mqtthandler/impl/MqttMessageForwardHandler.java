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
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.processor.InnerMqttMessageProcessor;
import org.apache.rocketmq.mqtt.task.MqttPushTask;
import org.apache.rocketmq.mqtt.transfer.TransferDataQos1;
import org.apache.rocketmq.mqtt.util.orderedexecutor.SafeRunnable;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttMessageForwardHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final InnerMqttMessageProcessor innerMqttMessageProcessor;
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttMessageForwardHandler(InnerMqttMessageProcessor processor) {
        this.innerMqttMessageProcessor = processor;
        this.defaultMqttMessageProcessor = innerMqttMessageProcessor.getDefaultMqttMessageProcessor();
    }

    /**
     * handle messages transferred from other nodes
     *
     * @param message the message that transferred from other node
     */
    @Override public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) message;
        MqttFixedHeader fixedHeader = mqttPublishMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = mqttPublishMessage.variableHeader();
        ByteBuf payload = mqttPublishMessage.payload();
        byte[] body = new byte[payload.readableBytes()];
        payload.readBytes(body);

        if (fixedHeader.qosLevel().equals(MqttQoS.AT_MOST_ONCE)) {
            MqttHeader mqttHeaderQos0 = new MqttHeader();
            mqttHeaderQos0.setTopicName(variableHeader.topicName());
            mqttHeaderQos0.setMessageType(MqttMessageType.PUBLISH.value());
            mqttHeaderQos0.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
            mqttHeaderQos0.setRetain(false); //TODO set to false temporarily, need to be implemented later.
            Set<Client> clientsTobePublish = findCurrentNodeClientsTobePublish(variableHeader.topicName(), (IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager());
            for (Client client : clientsTobePublish) {
                ((MQTTSession) client).pushMessageQos0(mqttHeaderQos0, body, this.defaultMqttMessageProcessor);
            }
        } else if (fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE)) {
            TransferDataQos1 transferDataQos1 = TransferDataQos1.decode(body, TransferDataQos1.class);
            //TODO : find clients that subscribed this topic from current node
            List<Client> clientsTobePublished = new ArrayList<>();
            for (Client client : clientsTobePublished) {
                //For each client, wrap a task:
                //Pull message one by one, and push them if current client match.
                MqttHeader mqttHeaderQos1 = new MqttHeader();
                mqttHeaderQos1.setTopicName(variableHeader.topicName());
                mqttHeaderQos1.setMessageType(MqttMessageType.PUBLISH.value());
                mqttHeaderQos1.setRetain(false); //TODO set to false temporarily, need to be implemented later.
                MqttPushTask mqttPushTask = new MqttPushTask(this.defaultMqttMessageProcessor, mqttHeaderQos1, client, transferDataQos1.getBrokerData());
                //add task to orderedExecutor
                this.defaultMqttMessageProcessor.getOrderedExecutor().executeOrdered(client.getClientId(), SafeRunnable.safeRun(mqttPushTask));
            }
            return doResponse(fixedHeader);
        }
        return null;
    }
}
