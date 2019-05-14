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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.mqtt.exception.MqttRuntimeException;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.transport.mqtt.MqttHeader;

public class MQTTSession extends Client {
    private boolean cleanSession;
    private boolean isConnected;
    private boolean willFlag;
    private final AtomicInteger inflightSlots = new AtomicInteger(10);
    private final Map<Integer, InFlightMessage> inflightWindow = new HashMap<>();
    private final DelayQueue<InFlightPacket> inflightTimeouts = new DelayQueue<>();
    private final AtomicInteger lastPacketId = new AtomicInteger(0);
    private Hashtable inUsePacketIds = new Hashtable();
    private int nextPacketId = 0;

    static class InFlightPacket implements Delayed {

        final int packetId;
        private long startTime;

        InFlightPacket(int packetId, long delayInMilliseconds) {
            this.packetId = packetId;
            this.startTime = System.currentTimeMillis() + delayInMilliseconds;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long diff = startTime - System.currentTimeMillis();
            return unit.convert(diff, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if ((this.startTime - ((InFlightPacket) o).startTime) == 0) {
                return 0;
            }
            if ((this.startTime - ((InFlightPacket) o).startTime) > 0) {
                return 1;
            } else {
                return -1;
            }
        }
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

    public void pushMessageAtQos(MqttHeader mqttHeader, ByteBuf payload,
        DefaultMqttMessageProcessor defaultMqttMessageProcessor) {

        if (mqttHeader.getQosLevel() > 0) {
//            IOTClientManagerImpl iotClientManager = (IOTClientManagerImpl) defaultMqttMessageProcessor.getIotClientManager();
//            ConcurrentHashMap<String, Subscription> clientId2Subscription = iotClientManager.getClientId2Subscription();
//            Subscription subscription = clientId2Subscription.get(this.getClientId());
//            Enumeration<String> topicFilters = subscription.getSubscriptionTable().keys();
//            while (topicFilters.hasMoreElements()) {
//                String topicFilter = topicFilters.nextElement();
//            }

            inflightSlots.decrementAndGet();
            mqttHeader.setPacketId(getNextPacketId());
            inflightWindow.put(mqttHeader.getPacketId(), new InFlightMessage(mqttHeader.getTopicName(), ));
        }
        defaultMqttMessageProcessor.getMqttPushService().pushMessageQos(mqttHeader, payload, this);
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
}
