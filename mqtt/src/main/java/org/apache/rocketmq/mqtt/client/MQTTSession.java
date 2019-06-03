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
package org.apache.rocketmq.mqtt.client;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientRole;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.mqtt.exception.MqttRuntimeException;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.mqtt.util.MqttUtil;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.netty.NettyChannelHandlerContextImpl;
import org.apache.rocketmq.remoting.netty.NettyChannelImpl;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

import static org.apache.rocketmq.mqtt.constant.MqttConstant.FLIGHT_BEFORE_RESEND_MS;
import static org.apache.rocketmq.mqtt.constant.MqttConstant.TOPIC_CLIENTID_SEPARATOR;

public class MQTTSession extends Client {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    private boolean cleanSession;
    private boolean isConnected;
    private boolean willFlag;

    private final DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private final AtomicInteger inflightSlots = new AtomicInteger(10);
    private final Map<Integer, InFlightMessage> inflightWindow = new HashMap<>();
    private Hashtable inUsePacketIds = new Hashtable();
    private int nextPacketId = 0;

    public MQTTSession(String clientId, ClientRole clientRole, Set<String> groups, boolean isConnected,
        boolean cleanSession, RemotingChannel remotingChannel, long lastUpdateTimestamp,
        DefaultMqttMessageProcessor defaultMqttMessageProcessor) {
        super(clientId, clientRole, groups, remotingChannel, lastUpdateTimestamp);
        this.isConnected = isConnected;
        this.cleanSession = cleanSession;
        this.defaultMqttMessageProcessor = defaultMqttMessageProcessor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Client)) {
            return false;
        }
        Client client = (Client) o;
        return Objects.equals(this.getClientId(), client.getClientId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClientId());
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public boolean isWillFlag() {
        return willFlag;
    }

    public void setWillFlag(boolean willFlag) {
        this.willFlag = willFlag;
    }

    public void pushMessageQos0(MqttHeader mqttHeader, byte[] body) {
        pushMessage2Client(mqttHeader, body);
    }

    public void pushMessageQos1(MqttHeader mqttHeader, MessageExt messageExt, BrokerData brokerData) {
        if (inflightSlots.get() > 0) {
            inflightSlots.decrementAndGet();
            mqttHeader.setPacketId(getNextPacketId());
            inflightWindow.put(mqttHeader.getPacketId(), new InFlightMessage(mqttHeader.getTopicName(), mqttHeader.getQosLevel(), messageExt.getBody(), brokerData, messageExt.getQueueOffset()));
            IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager();
            iotClientManager.getInflightTimeouts().add(new InFlightPacket(this, mqttHeader.getPacketId(), FLIGHT_BEFORE_RESEND_MS));
            put2processTable(iotClientManager.getProcessTable(), brokerData.getBrokerName(), MqttUtil.getRootTopic(mqttHeader.getTopicName()), messageExt);
            pushMessage2Client(mqttHeader, messageExt.getBody());
        }
    }

    public InFlightMessage pubAckReceived(int ackPacketId) {
        InFlightMessage remove = inflightWindow.remove(ackPacketId);
        String rootTopic = MqttUtil.getRootTopic(remove.getTopic());
        ConcurrentHashMap<String, ConcurrentHashMap<String, TreeMap<Long, MessageExt>>> processTable = ((IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager()).getProcessTable();
        ConcurrentHashMap<String, TreeMap<Long, MessageExt>> map = processTable.get(remove.getBrokerData().getBrokerName());
        if (map != null) {
            TreeMap<Long, MessageExt> treeMap = map.get(rootTopic + TOPIC_CLIENTID_SEPARATOR + this.getClientId());
            if (treeMap != null) {
                treeMap.remove(remove.getQueueOffset());
            }
        }
        inflightSlots.incrementAndGet();
        ((IOTClientManagerImpl) this.defaultMqttMessageProcessor.getIotClientManager()).getInflightTimeouts().remove(new InFlightPacket(this, ackPacketId, 0));
        releasePacketId(ackPacketId);
        return remove;
    }

    public void pushMessage2Client(MqttHeader mqttHeader, byte[] body) {
        try {
            //set remaining length
            int remainingLength = mqttHeader.getTopicName().getBytes().length + body.length;
            if (mqttHeader.getQosLevel() > 0) {
                remainingLength += 2; //add packetId length
            }
            mqttHeader.setRemainingLength(remainingLength);
            RemotingCommand requestCommand = RemotingCommand.createRequestCommand(RequestCode.MQTT_MESSAGE, mqttHeader);

            RemotingChannel remotingChannel = this.getRemotingChannel();
            if (this.getRemotingChannel() instanceof NettyChannelHandlerContextImpl) {
                remotingChannel = new NettyChannelImpl(((NettyChannelHandlerContextImpl) this.getRemotingChannel()).getChannelHandlerContext().channel());
            }
            requestCommand.setBody(body);
            this.defaultMqttMessageProcessor.getMqttRemotingServer().push(remotingChannel, requestCommand, MqttConstant.DEFAULT_TIMEOUT_MILLS);
        } catch (Exception ex) {
            log.warn("Exception was thrown when pushing MQTT message. Topic: {}, clientId:{}, exception={}", mqttHeader.getTopicName(), this.getClientId(), ex.getMessage());
        }
    }

    private void put2processTable(
        ConcurrentHashMap<String, ConcurrentHashMap<String, TreeMap<Long, MessageExt>>> processTable,
        String brokerName,
        String rootTopic,
        MessageExt messageExt) {
        ConcurrentHashMap<String, TreeMap<Long, MessageExt>> map;
        TreeMap<Long, MessageExt> treeMap;
        String offsetKey = rootTopic + TOPIC_CLIENTID_SEPARATOR + this.getClientId();
        if (processTable.containsKey(brokerName)) {
            map = processTable.get(brokerName);
            if (map.containsKey(offsetKey)) {
                treeMap = map.get(offsetKey);
                treeMap.putIfAbsent(messageExt.getQueueOffset(), messageExt);
            } else {
                treeMap = new TreeMap<>();
                treeMap.put(messageExt.getQueueOffset(), messageExt);
                map.putIfAbsent(offsetKey, treeMap);
            }
        } else {
            map = new ConcurrentHashMap<>();
            treeMap = new TreeMap<>();
            treeMap.put(messageExt.getQueueOffset(), messageExt);
            map.put(offsetKey, treeMap);
            ConcurrentHashMap<String, TreeMap<Long, MessageExt>> old = processTable.putIfAbsent(brokerName, map);
            if (old != null) {
                old.putIfAbsent(offsetKey, treeMap);
            }
        }
    }

    private synchronized void releasePacketId(int msgId) {
        this.inUsePacketIds.remove(new Integer(msgId));
    }

    private synchronized int getNextPacketId() {
        int startingMessageId = this.nextPacketId;
        int loopCount = 0;

        do {
            ++this.nextPacketId;
            if (this.nextPacketId > 65535) {
                this.nextPacketId = 1;
            }

            if (this.nextPacketId == startingMessageId) {
                ++loopCount;
                if (loopCount == 2) {
                    throw new MqttRuntimeException("Could not get available packetId.");
                }
            }
        }
        while (this.inUsePacketIds.containsKey(new Integer(this.nextPacketId)));

        Integer id = new Integer(this.nextPacketId);
        this.inUsePacketIds.put(id, id);
        return this.nextPacketId;
    }

    public AtomicInteger getInflightSlots() {
        return inflightSlots;
    }

    public Map<Integer, InFlightMessage> getInflightWindow() {
        return inflightWindow;
    }

    public Hashtable getInUsePacketIds() {
        return inUsePacketIds;
    }
}
