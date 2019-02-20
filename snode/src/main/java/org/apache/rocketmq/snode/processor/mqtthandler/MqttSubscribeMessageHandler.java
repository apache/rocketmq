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
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;
import org.apache.rocketmq.snode.client.impl.IOTClientManagerImpl;
import org.apache.rocketmq.snode.client.impl.Subscription;
import org.apache.rocketmq.snode.constant.MqttConstant;
import org.apache.rocketmq.snode.exception.WrongMessageTypeException;
import org.apache.rocketmq.snode.util.MqttUtil;

public class MqttSubscribeMessageHandler implements MessageHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private final SnodeController snodeController;

    public MqttSubscribeMessageHandler(SnodeController snodeController) {
        this.snodeController = snodeController;
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
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) snodeController.getIotClientManager();
        Client client = iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
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
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
        mqttHeader.setRetain(false);
        mqttHeader.setMessageId(mqttSubscribeMessage.variableHeader().messageId());

        List<Integer> grantQoss = doSubscribe(client, payload.topicSubscriptions(), iotClientManager);
        //Publish retained messages to subscribers.
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(grantQoss);
        command.setPayload(mqttSubAckPayload);
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
        ConcurrentHashMap<String, ConcurrentHashMap<Client, Set<SubscriptionData>>> topic2SubscriptionTable = iotClientManager.getTopic2SubscriptionTable();
        Subscription subscription = null;
        if (clientId2Subscription.containsKey(client.getClientId())) {
            subscription = clientId2Subscription.get(client.getClientId());
        } else {
            subscription = new Subscription();
            subscription.setCleanSession(client.isCleanSession());
        }
        ConcurrentHashMap<String, SubscriptionData> subscriptionDatas = subscription.getSubscriptionTable();
        List<Integer> grantQoss = new ArrayList<>();
        for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
            int actualQos = MqttUtil.actualQos(mqttTopicSubscription.qualityOfService().value());
            grantQoss.add(actualQos);
            SubscriptionData subscriptionData = new MqttSubscriptionData(mqttTopicSubscription.qualityOfService().value(), client.getClientId(), mqttTopicSubscription.topicName());
            subscriptionDatas.put(mqttTopicSubscription.topicName(), subscriptionData);
            //2.update topic2SubscriptionTable
            String rootTopic = MqttUtil.getRootTopic(mqttTopicSubscription.topicName());
            ConcurrentHashMap<Client, Set<SubscriptionData>> client2SubscriptionData = topic2SubscriptionTable.get(rootTopic);
            if (client2SubscriptionData == null) {
                client2SubscriptionData = new ConcurrentHashMap<>();
                ConcurrentHashMap<Client, Set<SubscriptionData>> prev = topic2SubscriptionTable.putIfAbsent(rootTopic, client2SubscriptionData);
                if (prev != null) {
                    client2SubscriptionData = prev;
                }
                Set<SubscriptionData> subscriptionDataSet = client2SubscriptionData.get(client);
                if (subscriptionDataSet == null) {
                    subscriptionDataSet = new HashSet<>();
                    Set<SubscriptionData> prevSubscriptionDataSet = client2SubscriptionData.putIfAbsent(client, subscriptionDataSet);
                    if (prevSubscriptionDataSet != null) {
                        subscriptionDataSet = prevSubscriptionDataSet;
                    }
                    subscriptionDataSet.add(subscriptionData);
                }
            }
        }
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
