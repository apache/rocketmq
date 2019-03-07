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

package org.apache.rocketmq.snode.processor.mqtthandler;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.exception.WrongMessageTypeException;
import org.apache.rocketmq.snode.util.MqttUtil;

public class MqttPublishMessageHandler implements MessageHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private final SnodeController snodeController;

    public MqttPublishMessageHandler(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttPublishMessage)) {
            log.error("Wrong message type! Expected type: PUBLISH but {} was received.", message.fixedHeader().messageType());
            throw new WrongMessageTypeException("Wrong message type exception.");
        }
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) message;
        MqttFixedHeader fixedHeader = mqttPublishMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = mqttPublishMessage.variableHeader();
        if (MqttUtil.isQosLegal(fixedHeader.qosLevel())) {
            log.error("The QoS level should be 0 or 1 or 2. The connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }

        ByteBuf payload = mqttPublishMessage.payload();
        if (fixedHeader.qosLevel().equals(MqttQoS.AT_MOST_ONCE)) {
            snodeController.getMqttPushService().pushMessageQos0(variableHeader.topicName(), payload);
        } else if (fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE)) {
            //Push messages to subscribers and add it to IN-FLIGHT messages
        }
        if (fixedHeader.qosLevel().value() > 0) {
            RemotingCommand command = RemotingCommand.createResponseCommand(MqttHeader.class);
            MqttHeader mqttHeader = (MqttHeader) command.readCustomHeader();
            if (fixedHeader.qosLevel().equals(MqttQoS.AT_MOST_ONCE)) {
                mqttHeader.setMessageType(MqttMessageType.PUBACK.value());
                mqttHeader.setDup(false);
                mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
                mqttHeader.setRetain(false);
                mqttHeader.setRemainingLength(2);
                mqttHeader.setPacketId(0);
            } else if (fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE)) {
                //PUBREC/PUBREL/PUBCOMP
            }
            return command;
        }
        return null;
    }
}
