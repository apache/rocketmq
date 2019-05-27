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

package org.apache.rocketmq.mqtt.mqtthandler;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.exception.MQClientException;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public interface MessageHandler {

    /**
     * Handle message from client
     *
     * @param message
     */
    RemotingCommand handleMessage(MqttMessage message,
        RemotingChannel remotingChannel) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException;

    default Set<Client> findCurrentNodeClientsTobePublish(String topic, IOTClientManagerImpl iotClientManager) {
        //find those clients publishing the message to
        ConcurrentHashMap<String, Set<Client>> topic2Clients = iotClientManager.getTopic2Clients();
        ConcurrentHashMap<String, Subscription> clientId2Subscription = iotClientManager.getClientId2Subscription();
        Set<Client> clientsTobePush = new HashSet<>();
        if (topic2Clients.containsKey(MqttUtil.getRootTopic(topic))) {
            Set<Client> clients = topic2Clients.get(MqttUtil.getRootTopic(topic));
            for (Client client : clients) {
                if(((MQTTSession)client).isConnected()) {
                    Subscription subscription = clientId2Subscription.get(client.getClientId());
                    Enumeration<String> keys = subscription.getSubscriptionTable().keys();
                    while (keys.hasMoreElements()) {
                        String topicFilter = keys.nextElement();
                        if (MqttUtil.isMatch(topicFilter, topic)) {
                            clientsTobePush.add(client);
                        }
                    }
                }
            }
        }
        return clientsTobePush;
    }

    default RemotingCommand doResponse(MqttFixedHeader fixedHeader) {
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
