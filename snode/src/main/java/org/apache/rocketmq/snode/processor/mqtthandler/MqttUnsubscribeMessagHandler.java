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

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
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
import org.apache.rocketmq.snode.exception.WrongMessageTypeException;
import org.apache.rocketmq.snode.util.MqttUtil;

/**
 * handle the UNSUBSCRIBE message from the client
 */
public class MqttUnsubscribeMessagHandler implements MessageHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);
    private final SnodeController snodeController;

    public MqttUnsubscribeMessagHandler(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    @Override
    public RemotingCommand handleMessage(MqttMessage message, RemotingChannel remotingChannel) {
        if (!(message instanceof MqttUnsubscribeMessage)) {
            log.error("Wrong message type! Expected type: UNSUBSCRIBE but {} was received. MqttMessage={}", message.fixedHeader().messageType(), message.toString());
            throw new WrongMessageTypeException("Wrong message type exception.");
        }
        MqttUnsubscribeMessage unsubscribeMessage = (MqttUnsubscribeMessage) message;
        MqttFixedHeader fixedHeader = unsubscribeMessage.fixedHeader();
        if (fixedHeader.isDup() || !fixedHeader.qosLevel().equals(MqttQoS.AT_LEAST_ONCE) || fixedHeader.isRetain()) {
            log.error("Malformed value of reserved bits(bits 3,2,1,0) of fixed header. Expected=0010, received={}{}{}{}", fixedHeader.isDup() ? 1 : 0, Integer.toBinaryString(fixedHeader.qosLevel().value()), fixedHeader.isRetain() ? 1 : 0);
            remotingChannel.close();
            return null;
        }
        MqttUnsubscribePayload payload = unsubscribeMessage.payload();
        if (payload.topics() == null || payload.topics().size() == 0) {
            log.error("The payload of a UNSUBSCRIBE packet MUST contain at least one Topic Filter. This will be treated as protocol violation and the connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) snodeController.getIotClientManager();
        Client client = iotClientManager.getClient(IOTClientManagerImpl.IOT_GROUP, remotingChannel);
        if (client == null) {
            log.error("Can't find associated client, the connection will be closed. remotingChannel={}, MqttMessage={}", remotingChannel.toString(), message.toString());
            remotingChannel.close();
            return null;
        }

        RemotingCommand command = RemotingCommand.createResponseCommand(MqttHeader.class);
        MqttHeader mqttHeader = (MqttHeader) command.readCustomHeader();
        mqttHeader.setMessageType(MqttMessageType.SUBACK.value());
        mqttHeader.setDup(false);
        mqttHeader.setQosLevel(MqttQoS.AT_MOST_ONCE.value());
        mqttHeader.setRetain(false);
        mqttHeader.setMessageId(unsubscribeMessage.variableHeader().messageId());

        doUnsubscribe(client, payload.topics(), iotClientManager);

        mqttHeader.setRemainingLength(0x02);
        command.setRemark(null);
        command.setCode(ResponseCode.SUCCESS);
        return command;
    }

    private void doUnsubscribe(Client client, List<String> topics, IOTClientManagerImpl iotClientManager) {
            ConcurrentHashMap<String, Subscription> clientId2Subscription = iotClientManager.getClientId2Subscription();
        ConcurrentHashMap<String, ConcurrentHashMap<Client, Set<SubscriptionData>>> topic2SubscriptionTable = iotClientManager.getTopic2SubscriptionTable();

        for (String topicFilter : topics) {
            //1.update clientId2Subscription
            if (clientId2Subscription.containsKey(client.getClientId())) {
                Subscription subscription = clientId2Subscription.get(client.getClientId());
                subscription.getSubscriptionTable().remove(topicFilter);
            }
            //2.update topic2SubscriptionTable
            String rootTopic = MqttUtil.getRootTopic(topicFilter);
            ConcurrentHashMap<Client, Set<SubscriptionData>> client2SubscriptionData = topic2SubscriptionTable.get(rootTopic);
            if (client2SubscriptionData != null) {
                Set<SubscriptionData> subscriptionDataSet = client2SubscriptionData.get(client);
                if (subscriptionDataSet != null) {
                    Iterator<SubscriptionData> iterator = subscriptionDataSet.iterator();
                    while (iterator.hasNext()) {
                        if (iterator.next().getTopic().equals(topicFilter))
                            iterator.remove();
                    }
                }
            }
        }
    }
}
