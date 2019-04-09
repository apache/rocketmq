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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPublishMessageHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttPublishMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
    }

    @Override
    public RemotingCommand handleMessage(MqttMessage message,
        RemotingChannel remotingChannel) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
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
            defaultMqttMessageProcessor.getMqttPushService().pushMessageQos0(variableHeader.topicName(), payload);
        } else if (fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE)) {
            // Store msg and invoke callback to publish msg to subscribers
            // 1. Check if the root topic has been created
            String rootTopic = MqttUtil.getRootTopic(variableHeader.topicName());
            TopicRouteData topicRouteData = null;

            try {
                topicRouteData = this.defaultMqttMessageProcessor.getNnodeService().getTopicRouteDataByTopic(rootTopic, false);
            } catch (MQClientException e) {
                log.error("The rootTopic {} does not exist. Please create it first.", rootTopic);
                throw new MQClientException(e.getResponseCode(), e.getErrorMessage());
            }

            //2. Store msg
            List<BrokerData> datas = topicRouteData.getBrokerDatas();
            BrokerData brokerData = datas.get(new Random().nextInt(datas.size()));
            RemotingCommand request = createSendMessageRequest(rootTopic, variableHeader, payload, brokerData.getBrokerName());
            CompletableFuture<RemotingCommand> responseFuture = this.defaultMqttMessageProcessor.getEnodeService().sendMessage(null, brokerData.getBrokerName(), request);
            responseFuture.whenComplete((data, ex) -> {
                if (ex == null) {
                    //publish msg to subscribers
                    try {
                        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) data.decodeCommandCustomHeader(SendMessageResponseHeader.class);
                        //find clients that subscribed this topic from all clients and put it to map.
                        Map<String, List<String>> snodeAddr2ClientIds = new HashMap<>();

                        //for clientIds connected to current snode, publish msg directly
                        List<String> clientIds = snodeAddr2ClientIds.get(this.defaultMqttMessageProcessor.getSnodeConfig().getSnodeIP1() + this.defaultMqttMessageProcessor.getSnodeConfig().getListenPort());
                        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager();
                        for (String clientId : clientIds) {
                            Subscription subscription = iotClientManager.getSubscriptionByClientId(clientId);
                            Enumeration<String> topicFilters = subscription.getSubscriptionTable().keys();
                            while (topicFilters.hasMoreElements()) {
                                String topicFilter = topicFilters.nextElement();
                                if(MqttUtil.isMatch(topicFilter, variableHeader.topicName())) {
                                    long offset = this.defaultMqttMessageProcessor.getEnodeService().queryOffset(brokerData.getBrokerName(), clientId, topicFilter, 0);
                                    if (offset == -1) {
//                                        this.defaultMqttMessageProcessor.getEnodeService().persistOffset(null, brokerData.getBrokerName(), clientId, 0, );
                                    }
                                }
                            }
                        }
                        //for clientIds connected to other snodes, forward msg

                    } catch (RemotingCommandException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (RemotingConnectException e) {
                        e.printStackTrace();
                    } catch (RemotingTimeoutException e) {
                        e.printStackTrace();
                    } catch (RemotingSendRequestException e) {
                        e.printStackTrace();
                    }
                } else {
                    log.error("Store Qos=1 Message error: {}", ex);
                }
            });

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

    private RemotingCommand createSendMessageRequest(String rootTopic, MqttPublishVariableHeader variableHeader,
        ByteBuf payload, String enodeName) {
        byte[] body = new byte[payload.readableBytes()];
        payload.readBytes(body);
        Message msg = new Message(rootTopic, "", body);
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setProducerGroup(IOTClientManagerImpl.IOT_GROUP);
        requestHeader.setTopic(rootTopic);
        requestHeader.setQueueId(0);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
        requestHeader.setBatch(false);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, variableHeader.topicName());
        requestHeader.setEnodeName(enodeName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(msg.getBody());
        return request;
    }
}
