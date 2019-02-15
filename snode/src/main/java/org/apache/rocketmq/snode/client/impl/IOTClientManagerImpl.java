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
package org.apache.rocketmq.snode.client.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.Client;

public class IOTClientManagerImpl extends ClientManagerImpl {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    public static final String IOT_GROUP = "IOT_GROUP";
    private final SnodeController snodeController;

    private final ConcurrentHashMap<String/*root topic*/, ConcurrentHashMap<Client, List<MqttSubscriptionData>>> topic2SubscriptionTable = new ConcurrentHashMap<>(
        1024);
    private final ConcurrentHashMap<String/*clientId*/, Subscription> clientId2Subscription = new ConcurrentHashMap<>(1024);

    public IOTClientManagerImpl(SnodeController snodeController) {
        this.snodeController = snodeController;
    }

    public void onUnsubscribe(Client client, List<String> topics) {
        //do the logic when client sends unsubscribe packet.
    }

    @Override
    public void onClosed(String group, RemotingChannel remotingChannel) {
        //do the logic when connection is closed by any reason.
    }

    @Override
    public void onUnregister(String group, RemotingChannel remotingChannel) {

    }

    @Override public void onRegister(String group, RemotingChannel remotingChannel) {

    }

    public void cleanSessionState(String clientId) {
        clientId2Subscription.remove(clientId);
        for (Iterator<Map.Entry<String, ConcurrentHashMap<Client, List<MqttSubscriptionData>>>> iterator = topic2SubscriptionTable.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<String, ConcurrentHashMap<Client, List<MqttSubscriptionData>>> next = iterator.next();
            for (Iterator<Map.Entry<Client, List<MqttSubscriptionData>>> iterator1 = next.getValue().entrySet().iterator(); iterator1.hasNext(); ) {
                Map.Entry<Client, List<MqttSubscriptionData>> next1 = iterator1.next();
                if (!next1.getKey().getClientId().equals(clientId)) {
                    continue;
                }
                iterator1.remove();
            }
        }
        //remove offline messages
    }

    public Subscription getSubscriptionByClientId(String clientId) {
        return clientId2Subscription.get(clientId);
    }

    public SnodeController getSnodeController() {
        return snodeController;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Client, List<MqttSubscriptionData>>> getTopic2SubscriptionTable() {
        return topic2SubscriptionTable;
    }

    public ConcurrentHashMap<String, Subscription> getClientId2Subscription() {
        return clientId2Subscription;
    }

    public void initSubscription(String clientId, Subscription subscription) {
        clientId2Subscription.put(clientId, subscription);
    }

}
