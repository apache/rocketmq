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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientManagerImpl;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.eclipse.paho.client.mqttv3.MqttClient;

public class IOTClientManagerImpl extends ClientManagerImpl {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    public static final String IOT_GROUP = "IOT_GROUP";

    private final ConcurrentHashMap<String/*root topic*/, Set<Client>> topic2Clients = new ConcurrentHashMap<>(
        1024);
    private final ConcurrentHashMap<String/*clientId*/, Subscription> clientId2Subscription = new ConcurrentHashMap<>(1024);
    private final Map<String/*snode ip*/, MqttClient> snode2MqttClient = new HashMap<>();
    private final ConcurrentHashMap<String /*broker*/, ConcurrentHashMap<String /*rootTopic@clientId*/, TreeMap<Long/*queueOffset*/, MessageExt>>> processTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String /*rootTopic@clientId*/, Integer> consumeOffsetTable = new ConcurrentHashMap<>();
    private final DelayQueue<InFlightPacket> inflightTimeouts = new DelayQueue<>();

    public IOTClientManagerImpl() {
    }

    @Override public void onClose(Set<String> groups, RemotingChannel remotingChannel) {
        for (String groupId : groups) {
            //remove client after invoking onClosed method(client may be used in onClosed)
            onClosed(groupId, remotingChannel);
            removeClient(groupId, remotingChannel);
        }
    }

    @Override
    public void onClosed(String group, RemotingChannel remotingChannel) {
        //do the logic when connection is closed by any reason.
        //step1. Clean subscription data if cleanSession=1
        MQTTSession client = (MQTTSession) this.getClient(IOT_GROUP, remotingChannel);
        if (client.isCleanSession()) {
            cleanSessionState(client.getClientId());
        } else {
            client.setConnected(false);
            //TODO update persistence store
        }
        //step2. Publish will message associated with current connection(Question: Does will message need to be deleted after publishing.)

        //step3. If will retain is true, add the will message to retain message.
    }

    @Override
    public void onUnregister(String group, RemotingChannel remotingChannel) {

    }

    @Override public void onRegister(String group, RemotingChannel remotingChannel) {

    }

    public void cleanSessionState(String clientId) {
        if (clientId2Subscription.remove(clientId) == null) {
            return;
        }
        Map<String, Set<Client>> toBeRemoveFromPersistentStore = new HashMap<>();
        for (Iterator<Map.Entry<String, Set<Client>>> iterator = topic2Clients.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, Set<Client>> next = iterator.next();
            Iterator<Client> iterator1 = next.getValue().iterator();
            while (iterator1.hasNext()) {
                Client client = iterator1.next();
                if (client.getClientId().equals(clientId)) {
                    iterator1.remove();
                    Set<Client> clients = toBeRemoveFromPersistentStore.getOrDefault((next.getKey()), new HashSet<>());
                    clients.add(client);
                    toBeRemoveFromPersistentStore.put(next.getKey(), clients);
                }
            }
        }
        //TODO update persistence store base on toBeRemoveFromPersistentStore

        //TODO update persistence store
        //TODO remove offline messages
    }

    public Subscription getSubscriptionByClientId(String clientId) {
        return clientId2Subscription.get(clientId);
    }

    public ConcurrentHashMap<String/*root topic*/, Set<Client>> getTopic2Clients() {
        return topic2Clients;
    }

    public ConcurrentHashMap<String, Subscription> getClientId2Subscription() {
        return clientId2Subscription;
    }

    public void initSubscription(String clientId, Subscription subscription) {
        clientId2Subscription.put(clientId, subscription);
    }

    public Map<String, MqttClient> getSnode2MqttClient() {
        return snode2MqttClient;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, TreeMap<Long, MessageExt>>> getProcessTable() {
        return processTable;
    }

    public DelayQueue<InFlightPacket> getInflightTimeouts() {
        return inflightTimeouts;
    }
}
