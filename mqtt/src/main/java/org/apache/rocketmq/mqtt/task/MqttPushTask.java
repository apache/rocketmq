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
package org.apache.rocketmq.mqtt.task;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.client.IOTClientManagerImpl;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MqttPushTask implements Runnable {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private MqttHeader mqttHeader;
    private MQTTSession client;
    private BrokerData brokerData;

    public MqttPushTask(DefaultMqttMessageProcessor processor, final MqttHeader mqttHeader, Client client,
        BrokerData brokerData) {
        this.defaultMqttMessageProcessor = processor;
        this.mqttHeader = mqttHeader;
        this.client = (MQTTSession) client;
        this.brokerData = brokerData;
    }

    @Override
    public void run() {

        String rootTopic = MqttUtil.getRootTopic(mqttHeader.getTopicName());
        EnodeService enodeService = this.defaultMqttMessageProcessor.getEnodeService();
        IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager();
        Subscription subscription = iotClientManager.getSubscriptionByClientId(client.getClientId());
        ConcurrentHashMap<String, SubscriptionData> subscriptionTable = subscription.getSubscriptionTable();
        //compare current consumeOffset of rootTopic@clientId with maxOffset, pull message if consumeOffset < maxOffset
        long maxOffsetInQueue;
        try {
            maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
            final long consumeOffset = enodeService.queryOffset(brokerData.getBrokerName(), client.getClientId(), rootTopic, 0);
            long i = consumeOffset + 1;
            while (i <= maxOffsetInQueue) {
                //TODO query messages(queueOffset=i) from enode above(brokerData.getBrokerName)
                RemotingCommand response = null;
                ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);

                final String realTopic = messageExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC);

                boolean needSkip = needSkip(realTopic, subscriptionTable);
                boolean alreadyInFlight = alreadyInFight(brokerData.getBrokerName(), realTopic, client.getClientId(), messageExt.getQueueOffset());
                if (needSkip) {
                    log.info("Current client doesn't subscribe topic:{}, skip this message", realTopic);
                    maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
                    i += 1;
                    continue;
                }
                if (alreadyInFlight) {
                    log.info("The message is already inflight. MessageId={}", messageExt.getMsgId());
                    break;
                }
                Integer pushQos = lowerQosToTheSubscriptionDesired(realTopic, Integer.valueOf(messageExt.getProperty(MqttConstant.PROPERTY_MQTT_QOS)), subscriptionTable);
                mqttHeader.setQosLevel(pushQos);
                mqttHeader.setTopicName(realTopic);
                if (client.getInflightSlots().get() == 0) {
                    log.info("The in-flight window is full, stop pushing message to consumers and update consumeOffset. ClientId={}, rootTopic={}", client.getClientId(), rootTopic);
                    break;
                }
                //push message if in-flight window has slot(not full)
                client.pushMessageQos1(mqttHeader, messageExt, brokerData);

                maxOffsetInQueue = getMaxOffset(brokerData.getBrokerName(), rootTopic);
                i += 1;
            }

            //TODO update consumeOffset of rootTopic@clientId in brokerData.getBrokerName()
            enodeService.persistOffset(null, brokerData.getBrokerName(), client.getClientId(), rootTopic, 0, i - 1);
        } catch (Exception ex) {
            log.error("Exception was thrown when pushing messages to consumer.{}", ex);
        }
    }

    private boolean needSkip(final String realTopic, ConcurrentHashMap<String, SubscriptionData> subscriptionTable) {
        Enumeration<String> topicFilters = subscriptionTable.keys();
        while (topicFilters.hasMoreElements()) {
            if (MqttUtil.isMatch(topicFilters.nextElement(), realTopic)) {
                return false;
            }
        }
        return true;
    }

    private boolean alreadyInFight(String brokerName, String topic, String clientId, Long queueOffset) {
        ConcurrentHashMap<String, ConcurrentHashMap<String, TreeMap<Long, MessageExt>>> processTable = ((IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager()).getProcessTable();
        ConcurrentHashMap<String, TreeMap<Long, MessageExt>> map = processTable.get(brokerName);
        if (map != null) {
            TreeMap<Long, MessageExt> treeMap = map.get(MqttUtil.getRootTopic(topic) + "@" + clientId);
            if (treeMap != null && treeMap.get(queueOffset) != null) {
                return true;
            }
        }
        return false;
    }

    private Integer lowerQosToTheSubscriptionDesired(String publishTopic, Integer publishingQos,
        ConcurrentHashMap<String, SubscriptionData> subscriptionTable) {
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

    private long getMaxOffset(String enodeName,
        String topic) throws InterruptedException, RemotingTimeoutException, RemotingCommandException, RemotingSendRequestException, RemotingConnectException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        return this.defaultMqttMessageProcessor.getEnodeService().getMaxOffsetInQueue(enodeName, topic, 0, request);
    }
}
