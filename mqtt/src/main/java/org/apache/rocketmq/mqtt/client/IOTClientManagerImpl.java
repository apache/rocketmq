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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.ClientManagerImpl;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;

public class IOTClientManagerImpl extends ClientManagerImpl {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);

    public static final String IOT_GROUP = "IOT_GROUP";

    private final ConcurrentHashMap<String/*root topic*/, ConcurrentHashMap<Client, Set<SubscriptionData>>> topic2SubscriptionTable = new ConcurrentHashMap<>(
        1024);
    private final ConcurrentHashMap<String/*clientId*/, Subscription> clientId2Subscription = new ConcurrentHashMap<>(1024);

    public IOTClientManagerImpl() {
    }

    public void onUnsubscribe(Client client, List<String> topics) {
        //do the logic when client sends unsubscribe packet.
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
        Client client = this.getClient(IOT_GROUP, remotingChannel);
        if (client.isCleanSession()) {
            cleanSessionState(client.getClientId());
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
        clientId2Subscription.remove(clientId);
        for (Iterator<Map.Entry<String, ConcurrentHashMap<Client, Set<SubscriptionData>>>> iterator = topic2SubscriptionTable.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, ConcurrentHashMap<Client, Set<SubscriptionData>>> next = iterator.next();
            for (Iterator<Map.Entry<Client, Set<SubscriptionData>>> iterator1 = next.getValue().entrySet().iterator(); iterator1.hasNext(); ) {
                Map.Entry<Client, Set<SubscriptionData>> next1 = iterator1.next();
                if (!next1.getKey().getClientId().equals(clientId)) {
                    continue;
                }
                iterator1.remove();
            }
            if (next.getValue() == null || next.getValue().size() == 0) {
                iterator.remove();
            }
        }
        //remove offline messages
    }

    public Subscription getSubscriptionByClientId(String clientId) {
        return clientId2Subscription.get(clientId);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Client, Set<SubscriptionData>>> getTopic2SubscriptionTable() {
        return topic2SubscriptionTable;
    }

    public ConcurrentHashMap<String, Subscription> getClientId2Subscription() {
        return clientId2Subscription;
    }

    public void initSubscription(String clientId, Subscription subscription) {
        clientId2Subscription.put(clientId, subscription);
    }
}
