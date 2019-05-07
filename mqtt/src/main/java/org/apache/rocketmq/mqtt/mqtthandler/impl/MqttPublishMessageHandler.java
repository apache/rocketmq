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
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
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
        MqttHeader mqttHeader = new MqttHeader();
        mqttHeader.setTopicName(variableHeader.topicName());
        mqttHeader.setMessageType(MqttMessageType.PUBLISH.value());
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(fixedHeader.qosLevel().value());
        mqttHeader.setRetain(false); //set to false tempararily, need to be implemented.
        switch (fixedHeader.qosLevel()) {
            case AT_MOST_ONCE:
                //For clients connected to the current snode and isConnected is true
                Set<Client> clientsTobePublish = findCurrentNodeClientsTobePublish(variableHeader.topicName(), this.iotClientManager);

                for (Client client : clientsTobePublish) {
                    ((MQTTSession) client).pushMessageAtQos(mqttHeader, payload, this.defaultMqttMessageProcessor);
                }

                //For clients that connected to other snodes, transfer the message to them
                //pseudo code:
                //step1. find clients that match the publish topic
                //step2. remove the clients that connected to the current node(clientsTobePublish)
                //step2. get snode ips by clients in step2.
                Set<String> snodeIpsTobeTransfer = new HashSet<>();
                try {
                    transferMessage(snodeIpsTobeTransfer, variableHeader.topicName(), payload);
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
                        try {
                            SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) data.decodeCommandCustomHeader(SendMessageResponseHeader.class);
                            //find clients that subscribed this topic from all snodes and put it to map.
                            Map<String, List<Client>> snodeAddr2Clients = new HashMap<>();

                            //for clientIds connected to current snode, trigger the logic of push message
                            List<Client> clients = snodeAddr2Clients.get(this.defaultMqttMessageProcessor.getSnodeConfig().getSnodeIP1());
                            for (Client client : clients) {
                                Subscription subscription = this.iotClientManager.getSubscriptionByClientId(client.getClientId());
                                ConcurrentHashMap<String, SubscriptionData> subscriptionTable = subscription.getSubscriptionTable();

                                //for each client, wrap a task: pull messages from commitlog one by one, and push them if current client subscribe it.
                                Runnable task = new Runnable() {

                                    @Override
                                    public void run() {
                                        //compare current consumeOffset of rootTopic@clientId with maxOffset, pull message if consumeOffset < maxOffset
                                        long maxOffsetInQueue;
                                        try {
                                            maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
                                            long consumeOffset = enodeService.queryOffset(brokerData.getBrokerName(), client.getClientId(), rootTopic, 0);
                                            long i = consumeOffset;
                                            while (i < maxOffsetInQueue) {
                                                //TODO query messages from enode
                                                RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
                                                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                                                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                                                final String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);

                                                boolean needSkip = needSkip(realTopic);
                                                if (needSkip) {
                                                    log.info("Current client doesn't subscribe topic:{}, skip this message", realTopic);
                                                    maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
                                                    i += 1;
                                                    continue;
                                                }
                                                Integer pushQos = lowerQosToTheSubscriptionDesired(realTopic, Integer.valueOf(messageExt.getProperty(MqttConstant.PROPERTY_MQTT_QOS)));
                                                mqttHeader.setQosLevel(pushQos);
                                                //push message
                                                MQTTSession mqttSession = (MQTTSession) client;
                                                mqttSession.pushMessageAtQos(mqttHeader, payload, defaultMqttMessageProcessor);

                                                maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
                                                i += 1;
                                            }
                                        } catch (Exception ex) {
                                            log.error("Get max offset error, remoting: {} error: {} ", remotingChannel.remoteAddress(), ex);
                                        }
                                    }

                                    private boolean needSkip(final String realTopic) {
                                        Enumeration<String> topicFilters = subscriptionTable.keys();
                                        while (topicFilters.hasMoreElements()) {
                                            if (MqttUtil.isMatch(topicFilters.nextElement(), realTopic)) {
                                                return false;
                                            }
                                        }
                                        return true;
                                    }

                                    private Integer lowerQosToTheSubscriptionDesired(String publishTopic,
                                        Integer publishingQos) {
                                        Integer pushQos = Integer.valueOf(publishingQos);
                                        Iterator<Map.Entry<String, SubscriptionData>> iterator = subscriptionTable.entrySet().iterator();
                                        Integer maxRequestedQos = 0;
                                        while (iterator.hasNext()) {
                                            final String topicFilter = iterator.next().getKey();
                                            if (MqttUtil.isMatch(topicFilter, publishTopic)) {
                                                MqttSubscriptionData mqttSubscriptionData = (MqttSubscriptionData) iterator.next().getValue();
                                                maxRequestedQos = mqttSubscriptionData.getQos() > maxRequestedQos ? mqttSubscriptionData.getQos() : maxRequestedQos;
                                            }
                                        }
                                        if (publishingQos > maxRequestedQos) {
                                            pushQos = maxRequestedQos;
                                        }
                                        return pushQos;
                                    }
                                };

                            }
                            //for clientIds connected to other snodes, forward msg
                        } catch (RemotingCommandException e) {
                            e.printStackTrace();
                        }
                    } else {
                        log.error("Store Qos=1 Message error: {}", ex);
                    }
                });

        }
        return doResponse(fixedHeader);
    }

    private void transferMessage(Set<String> snodeAddresses, String topic, ByteBuf payload) throws MqttException {
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

            byte[] body = new byte[payload.readableBytes()];
            payload.readBytes(body);
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
        MessageAccessor.putProperty(msg, MqttConstant.PROPERTY_MQTT_QOS, fixedHeader.qosLevel().name());
        requestHeader.setEnodeName(enodeName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
        request.setBody(msg.getBody());
        return request;
    }

    private long getMaxOffset(String enodeName,
        String topic) throws InterruptedException, RemotingTimeoutException, RemotingCommandException, RemotingSendRequestException, RemotingConnectException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        return this.defaultMqttMessageProcessor.getEnodeService().getMaxOffsetInQueue(enodeName, topic, 0, request);
    }
}
