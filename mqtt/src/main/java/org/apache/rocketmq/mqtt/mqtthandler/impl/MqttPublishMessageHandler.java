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
import io.netty.util.ReferenceCountUtil;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.mqtt.exception.MqttRuntimeException;
import org.apache.rocketmq.mqtt.exception.WrongMessageTypeException;
import org.apache.rocketmq.mqtt.mqtthandler.MessageHandler;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.task.MqttPushTask;
import org.apache.rocketmq.mqtt.transfer.TransferDataQos1;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.mqtt.util.orderedexecutor.SafeRunnable;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttPublishMessageHandler implements MessageHandler {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private IOTClientManagerImpl iotClientManager;
    private EnodeService enodeService;

    public MqttPublishMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
        this.iotClientManager = (IOTClientManagerImpl) processor.getIotClientManager();
        this.enodeService = this.defaultMqttMessageProcessor.getEnodeService();
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

        switch (fixedHeader.qosLevel()) {
            case AT_MOST_ONCE:
                //For clients connected to the current snode and isConnected is true
                Set<Client> clientsTobePublish = findCurrentNodeClientsTobePublish(variableHeader.topicName(), this.iotClientManager);
                byte[] body = new byte[payload.readableBytes()];
                payload.readBytes(body);
                MqttHeader mqttHeaderQos0 = new MqttHeader();
                mqttHeaderQos0.setTopicName(variableHeader.topicName());
                mqttHeaderQos0.setMessageType(MqttMessageType.PUBLISH.value());
                mqttHeaderQos0.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
                mqttHeaderQos0.setRetain(false); //TODO set to false temporarily, need to be implemented later.
                for (Client client : clientsTobePublish) {
                    ((MQTTSession) client).pushMessageQos0(mqttHeaderQos0, body);
                }

                //For clients that connected to other snodes, transfer the message to them
                //pseudo code:
                //step1. find clients that match the publish topic
                //step2. remove the clients that connected to the current node(clientsTobePublish)
                //step2. get snode ips by clients in step2.
                Set<String> snodeIpsTobeTransfer = new HashSet<>();
                try {
                    transferMessage(snodeIpsTobeTransfer, variableHeader.topicName(), body);
                } catch (MqttException e) {
                    log.error("Transfer message failed: {}", e.getMessage());
                } finally {
                    ReferenceCountUtil.release(message);
                }

            case AT_LEAST_ONCE:
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
                //select a broker randomly, need to be optimized here
                BrokerData brokerData = datas.get(new Random().nextInt(datas.size()));
                RemotingCommand request = createSendMessageRequest(rootTopic, fixedHeader, variableHeader, payload, brokerData.getBrokerName());
                CompletableFuture<RemotingCommand> responseFuture = this.defaultMqttMessageProcessor.getEnodeService().sendMessage(null, brokerData.getBrokerName(), request);
                responseFuture.whenComplete((data, ex) -> {
                    if (ex == null) {
                        //publish msg to subscribers
                        //TODO find clients that subscribed this topic from all snodes and put it to map.
                        Map<String, List<Client>> snodeAddr2Clients = new HashMap<>();

                        //for clientIds connected to current snode, trigger the logic of push message
                        List<Client> clients = snodeAddr2Clients.get(this.defaultMqttMessageProcessor.getSnodeConfig().getSnodeIP1());
                        for (Client client : clients) {
                            //For each client, wrap a task:
                            //Pull message one by one, and push them if current client match.
                            MqttHeader mqttHeaderQos1 = new MqttHeader();
                            mqttHeaderQos1.setMessageType(MqttMessageType.PUBLISH.value());
                            mqttHeaderQos1.setRetain(false); //TODO set to false temporarily, need to be implemented later.
                            MqttPushTask mqttPushTask = new MqttPushTask(defaultMqttMessageProcessor, mqttHeaderQos1, MqttUtil.getRootTopic(variableHeader.topicName()), client, brokerData);
                            //add task to orderedExecutor
                            this.defaultMqttMessageProcessor.getOrderedExecutor().executeOrdered(client.getClientId(), SafeRunnable.safeRun(mqttPushTask));
                        }
                        //for clients connected to other snodes, forward msg
                        Set<String> snodesTobeTransfered = new HashSet<>();
                        TransferDataQos1 transferDataQos1 = new TransferDataQos1();
                        transferDataQos1.setBrokerData(brokerData);
                        transferDataQos1.setTopic(variableHeader.topicName());
                        byte[] encode = TransferDataQos1.encode(transferDataQos1);
                        try {
                            transferMessage(snodesTobeTransfered, variableHeader.topicName(), encode);
                        } catch (MqttException e) {
                            log.error("Transfer message failed: {}", e.getMessage());
                        } finally {
                            ReferenceCountUtil.release(message);
                        }
                    } else {
                        log.error("Store Qos=1 Message error: {}", ex);
                    }
                });

            case EXACTLY_ONCE:
                throw new MqttRuntimeException("Qos = 2 messages are not supported yet.");
        }
        return doResponse(fixedHeader);
    }

    private void transferMessage(Set<String> snodeAddresses, String topic, byte[] body) throws MqttException {
        SnodeConfig snodeConfig = defaultMqttMessageProcessor.getSnodeConfig();
        MqttConfig mqttConfig = defaultMqttMessageProcessor.getMqttConfig();
        String url = "tcp://" + snodeConfig.getSnodeIP1() + ":" + (mqttConfig.getListenPort() - 1);
        String clientId = defaultMqttMessageProcessor.getSnodeConfig().getSnodeIP1();

        for (String snodeAddress : snodeAddresses) {
            final Map<String, MqttClient> snode2MqttClient = iotClientManager.getSnode2MqttClient();
            MqttClient client;
            if (snode2MqttClient.containsKey(snodeAddresses)) {
                client = snode2MqttClient.get(snodeAddress);
            } else {
                MemoryPersistence persistence = new MemoryPersistence();
                client = new MqttClient(url, clientId, persistence);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                client.connect(connOpts);
                snode2MqttClient.put(snodeAddress, client);
            }
            org.eclipse.paho.client.mqttv3.MqttMessage message = new org.eclipse.paho.client.mqttv3.MqttMessage(body);
            message.setQos(0);
            client.publish(topic, message);
        }
    }

    private RemotingCommand createSendMessageRequest(String rootTopic, MqttFixedHeader fixedHeader,
        MqttPublishVariableHeader variableHeader,
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
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TAGS, MqttUtil.getRootTopic(variableHeader.topicName()));
        MessageAccessor.putProperty(msg, MqttConstant.PROPERTY_MQTT_QOS, fixedHeader.qosLevel().name());
        requestHeader.setEnodeName(enodeName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(msg.getBody());
        return request;
    }

}
