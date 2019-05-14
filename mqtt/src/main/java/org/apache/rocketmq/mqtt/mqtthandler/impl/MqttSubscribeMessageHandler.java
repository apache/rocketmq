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
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
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
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.remoting.util.MqttEncodeDecodeUtil;

public class MqttSubscribeMessageHandler implements MessageHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;

    public MqttSubscribeMessageHandler(DefaultMqttMessageProcessor processor) {
        this.defaultMqttMessageProcessor = processor;
    }

    /**
     * handle the SUBSCRIBE message from the client <ol> <li>validate the topic filters in each subscription</li>
     * <li>set actual qos of each filter</li> <li>get the topics matching given filters</li> <li>check the client
     * authorization of each topic</li> <li>generate SUBACK message which includes the subscription result for each
     * TopicFilter</li> <li>send SUBACK message to the client</li> </ol>
     *
     * @param message the message wrapping MqttSubscriptionMessage
     * @return
     */
    @Override public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttSubscribeMessage)) {
            log.error("Wrong message type! Expected type: SUBSCRIBE but {} was received. MqttMessage={}", message.fixedHeader().messageType(), message.toString());
            throw new WrongMessageTypeException("Wrong message type exception.");
        }
        MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) message;
        MqttSubscribePayload payload = mqttSubscribeMessage.payload();
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
        MQTTSession client = (MQTTSession)iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
        if (client == null) {
            log.error("Can't find associated client, the connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }
        if (payload.topicSubscriptions() == null || payload.topicSubscriptions().size() == 0) {
            log.error("The payload of a SUBSCRIBE packet MUST contain at least one Topic Filter / QoS pair. This will be treated as protocol violation and the connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }
        if (isQosLegal(payload.topicSubscriptions())) {
            log.error("The QoS level of Topic Filter / QoS pairs should be 0 or 1 or 2. The connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }
        if (isTopicWithWildcard(payload.topicSubscriptions())) {
            log.error("Client can not subscribe topic starts with wildcards! clientId={}, topicSubscriptions={}", client.getClientId(), payload.topicSubscriptions().toString());

        }

        RemotingCommand command = RemotingCommand.createResponseCommand(MqttHeader.class);
        MqttHeader mqttHeader = (MqttHeader) command.readCustomHeader();
        mqttHeader.setMessageType(MqttMessageType.SUBACK.value());
        // dup/qos/retain value are always as below of SUBACK
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
        mqttHeader.setRetain(false);
        mqttHeader.setMessageId(mqttSubscribeMessage.variableHeader().messageId());

        List<Integer> grantQoss = doSubscribe(client, payload.topicSubscriptions(), iotClientManager);
        //Publish retained messages to subscribers.
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(grantQoss);
        command.setBody(MqttEncodeDecodeUtil.encode(mqttSubAckPayload));
        mqttHeader.setRemainingLength(0x02 + mqttSubAckPayload.grantedQoSLevels().size());
        command.setRemark(null);
        command.setCode(ResponseCode.SUCCESS);
        return command;
    }

    private List<Integer> doSubscribe(Client client, List<MqttTopicSubscription> mqttTopicSubscriptions,
        IOTClientManagerImpl iotClientManager) {
        //do the logic when client sends subscribe packet.
        //1.update clientId2Subscription
        ConcurrentHashMap<String, Subscription> clientId2Subscription = iotClientManager.getClientId2Subscription();
        ConcurrentHashMap<String, Set<Client>> topic2Clients = iotClientManager.getTopic2Clients();
        Subscription subscription;
        if (clientId2Subscription.containsKey(client.getClientId())) {
            subscription = clientId2Subscription.get(client.getClientId());
        } else {
            subscription = new Subscription();
            subscription.setCleanSession(((MQTTSession)client).isCleanSession());
        }
        ConcurrentHashMap<String, SubscriptionData> subscriptionDatas = subscription.getSubscriptionTable();
        List<Integer> grantQoss = new ArrayList<>();
        for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
            int actualQos = MqttUtil.actualQos(mqttTopicSubscription.qualityOfService().value());
            grantQoss.add(actualQos);
            SubscriptionData subscriptionData = new MqttSubscriptionData(mqttTopicSubscription.qualityOfService().value(), client.getClientId(), mqttTopicSubscription.topicName());
            subscriptionDatas.put(mqttTopicSubscription.topicName(), subscriptionData);
            //2.update topic2ClientIds
            String rootTopic = MqttUtil.getRootTopic(mqttTopicSubscription.topicName());
            if (topic2Clients.contains(rootTopic)) {
                final Set<Client> clientIds = topic2Clients.get(rootTopic);
                clientIds.add(client);
            } else {
                Set<Client> clients = new HashSet<>();
                clients.add(client);
                Set<Client> prev = topic2Clients.putIfAbsent(rootTopic, clients);
                if (prev != null) {
                    prev.add(client);
                }
            }
        }
        //TODO update persistent store of topic2Clients and clientId2Subscription
        return grantQoss;
    }

    private boolean isQosLegal(List<MqttTopicSubscription> mqttTopicSubscriptions) {
        for (MqttTopicSubscription subscription : mqttTopicSubscriptions) {
            if (!(subscription.qualityOfService().equals(MqttQoS.AT_LEAST_ONCE) || subscription.qualityOfService().equals(MqttQoS.EXACTLY_ONCE) || subscription.qualityOfService().equals(MqttQoS.AT_MOST_ONCE))) {
                return true;
            }
        }
        return false;
    }

    private boolean isTopicWithWildcard(List<MqttTopicSubscription> mqttTopicSubscriptions) {
        for (MqttTopicSubscription subscription : mqttTopicSubscriptions) {
            String rootTopic = MqttUtil.getRootTopic(subscription.topicName());
            if (rootTopic.contains(MqttConstant.SUBSCRIPTION_FLAG_PLUS) || rootTopic.contains(MqttConstant.SUBSCRIPTION_FLAG_SHARP)) {
                return true;
            }
        }
        return false;
    }
}
