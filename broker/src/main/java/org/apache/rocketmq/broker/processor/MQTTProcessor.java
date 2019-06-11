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

package org.apache.rocketmq.broker.processor;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.heartbeat.MqttSubscriptionData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MQTTInfoStore;

public class MQTTProcessor implements RequestProcessor {
    private final BrokerController brokerController;
    private static final Gson GSON = new Gson();
    private final MQTTInfoStore mqttInfoStore;
    public MQTTProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.mqttInfoStore = this.brokerController.getMqttInfoStore();
    }

    @Override public boolean rejectRequest() {
        return false;
    }

    @Override
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand request)  {


        switch (request.getCode()) {
            case RequestCode.MQTT_IS_CLIENT2SUBSCRIPTION_PERSISTED:
                return this.isClient2SubscriptionPersistedHandler(remotingChannel, request);
            case RequestCode.MQTT_ADD_OR_UPDATE_CLIENT2SUBSCRIPTION:
                return this.addOrUpdateClient2Subscription(remotingChannel,request);
            case RequestCode.MQTT_DELETE_CLIENT2SUBSCRIPTION:
                return this.deleteClient2Subscription(request);
            case RequestCode.MQTT_GET_SNODEADDRESS2CLIENT:
                return this.getSnodeAddress2Clients(request);
            case RequestCode.MQTT_CLIENT_UNSUBSRIBE:
                return this.clientUnsubscribe(request);
            case RequestCode.MQTT_ADD_OR_UPDATE_ROOTTOPIC2CLIENTS:
                return this.addorUpdateRootTopic2Clients(request);
            case RequestCode.MQTT_GET_ROOTTOPIC2CLIENTS:
                return this.getRootTopic2Clients(request);
            case RequestCode.MQTT_DELETE_ROOTTOPIC2CLIENT:
                return this.deleteRootTopic2Client(request);
            default:
                return null;
        }
    }

    private RemotingCommand isClient2SubscriptionPersistedHandler(final RemotingChannel remotingChannel,final RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(request.getCode(),null);
        String clientId = request.getExtFields().get("clientId");
        boolean cleanSession = Boolean.parseBoolean(request.getExtFields().get("cleanSession"));

        String subscriptionJson = mqttInfoStore.getValue(clientId + "-sub");
        if (subscriptionJson != null) {
            Subscription subscription = GSON.fromJson(subscriptionJson, Subscription.class);
            if (subscription.isCleanSession() != cleanSession) {
                subscription.setCleanSession(cleanSession);
                mqttInfoStore.putData(clientId + "-sub", GSON.toJson(subscription));
            }
            response.addExtField("isPersisted", "true");
        } else {
            response.addExtField("isPersisted", "false");
        }

        mqttInfoStore.putData(clientId + "-sno", remotingChannel.remoteAddress().toString());

        return response;
    }

    private RemotingCommand addOrUpdateClient2Subscription(final RemotingChannel remotingChannel,final RemotingCommand request) {
        String clientId = request.getExtFields().get("clientId");
        String subscription = request.getExtFields().get("subscription");
        boolean client2SubResult = this.mqttInfoStore.putData(clientId + "-sub",subscription);
        boolean client2SnoResult = this.mqttInfoStore.putData(clientId + "-sno",remotingChannel.remoteAddress().toString());
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.addExtField("result", String.valueOf(client2SnoResult && client2SubResult));
        return response;
    }

    private RemotingCommand deleteClient2Subscription(final RemotingCommand request) {
        String clientId = request.getExtFields().get("clientId");
        String subscriptionString = this.mqttInfoStore.getValue(clientId + "-sub");
        String result = String.valueOf(this.mqttInfoStore.deleteData(clientId + "-sub") && this.mqttInfoStore.deleteData(clientId + "-sno"));
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.addExtField("subscription",subscriptionString);
        response.addExtField("result", result);
        return response;
    }

    private RemotingCommand getSnodeAddress2Clients(final RemotingCommand request) {
        Map<String, Map<String,Integer>> snodeAddress2ClientsId = new ConcurrentHashMap<>();
        Map<String,Integer> clientsIdAndQos = new ConcurrentHashMap<>();
        String topic = request.getExtFields().get("topic");
        Set<String> clientsId = GSON.fromJson(request.getExtFields().get("clientsId"),new TypeToken<Set<String>>() {
        }.getType());
        for (String clientId:clientsId) {
            Integer qos = 0;
            ConcurrentHashMap<String/*Topic*/, SubscriptionData> subscriptionTable = GSON.fromJson(this.mqttInfoStore.getValue(clientId + "-sub"), Subscription.class).getSubscriptionTable();
            for (String topicFilter:subscriptionTable.keySet()) {
                if (isMatch(topicFilter,topic)) {
                    MqttSubscriptionData mqttSubscriptionData = (MqttSubscriptionData) subscriptionTable.get(topicFilter);
                    if (qos <= mqttSubscriptionData.getQos()) {
                        qos = mqttSubscriptionData.getQos();
                        clientsIdAndQos.putIfAbsent(clientId, qos);
                    }
                }
            }
        }
        for (String clientId:clientsIdAndQos.keySet()) {
            String snodeAddress = this.mqttInfoStore.getValue(clientId + "-sno");
            Map<String,Integer> map = snodeAddress2ClientsId.getOrDefault(snodeAddress, new ConcurrentHashMap<>());
            map.putIfAbsent(clientId,clientsIdAndQos.get(clientId));
        }
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (snodeAddress2ClientsId.size() == 0) {
            response.addExtField("result", "false");
        } else {
            response.addExtField("result","true");
            response.addExtField("snodeAddress2Clients", GSON.toJson(snodeAddress2ClientsId));
        }
        return response;
    }

    private RemotingCommand clientUnsubscribe(final RemotingCommand request) {
        String clientId = request.getExtFields().get("clientId");
        List<String> topics = GSON.fromJson(request.getExtFields().get("topics"),new TypeToken<List<String>>() {
        }.getType());
        Subscription  subscription = GSON.fromJson(this.mqttInfoStore.getValue(clientId + "-sub"),Subscription.class);
        ConcurrentHashMap<String,SubscriptionData> subscriptionTable = subscription.getSubscriptionTable();
        Set<String> rootTopicsBefore = subscriptionTable.keySet().stream().map(t -> t.split("/")[0]).collect(Collectors.toSet());
        for (String topic:topics) {
            subscriptionTable.remove(topic);
        }
        Set<String> rootTopicAfter = subscriptionTable.keySet().stream().map(t -> t.split("/")[0]).collect(Collectors.toSet());
        Set<String> rootTopicsDiff = new HashSet<>();
        rootTopicsDiff.addAll(rootTopicsBefore);
        rootTopicsDiff.removeAll(rootTopicAfter);

        subscription.setSubscriptionTable(subscriptionTable);
        boolean result = this.mqttInfoStore.putData(clientId + "-sub", GSON.toJson(subscription));
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.addExtField("result", String.valueOf(result));
        if (rootTopicsDiff.size() != 0) {
            response.addExtField("rootTopicsDiffExists", "true");
            response.addExtField("rootTopicsDiff", GSON.toJson(rootTopicsDiff));
        }
        return response;
    }
    private RemotingCommand addorUpdateRootTopic2Clients(final RemotingCommand request) {
        String rootTopic = request.getExtFields().get("rootTopic");
        Client client = GSON.fromJson(request.getExtFields().get("client"),Client.class);
        String value = this.mqttInfoStore.getValue(rootTopic);
        Set<Client> clients;
        if (value != null) {
            clients = this.clientsStringToClientsSet(value);
        } else {
            clients = new HashSet<>();
        }
        clients.add(client);
        RemotingCommand response = RemotingCommand.createResponseCommand(RequestCode.MQTT_ADD_OR_UPDATE_ROOTTOPIC2CLIENTS,null);
        response.addExtField("result",String.valueOf(this.mqttInfoStore.putData(rootTopic,GSON.toJson(clients))));
        return response;
    }

    private RemotingCommand getRootTopic2Clients(final RemotingCommand request) {
        String rootTopic = request.getExtFields().get("rootTopic");
        Set<String> clientsId = this.clientsStringToClientsSet(this.mqttInfoStore.getValue(rootTopic)).stream().map(c -> c.getClientId()).collect(Collectors.toSet());
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (clientsId.size() == 0) {
            response.addExtField("result", "false");
        } else {
            response.addExtField("result", "true");
            response.addExtField("clientsId", GSON.toJson(clientsId));
        }
        return response;
    }

    private RemotingCommand deleteRootTopic2Client(final RemotingCommand request) {
        String rootTopic = request.getExtFields().get("rootTopic");
        String clientId = request.getExtFields().get("clientId");
        Set<Client> clients = this.clientsStringToClientsSet(this.mqttInfoStore.getValue(rootTopic));
        Set<Client> clientsAfterDelete = clients.stream().filter(c -> c.getClientId() != clientId).collect(Collectors.toSet());
        boolean result;
        if (clientsAfterDelete.size() == 0) {
            result = this.mqttInfoStore.deleteData(rootTopic);
        } else {
            result = this.mqttInfoStore.putData(rootTopic,GSON.toJson(clientsAfterDelete));
        }
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.addExtField("result", String.valueOf(result));
        return response;
    }


    private Set<Client> clientsStringToClientsSet(String clientsString) {
        Set<Client> clients = new HashSet<>();
        Type type = new TypeToken<Set<Client>>() {
        }.getType();
        clients = GSON.fromJson(clientsString,type);
        return clients;
    }
    private  boolean isMatch(String topicFiter, String topic) {
        if (!topicFiter.contains("+") && !topicFiter.contains("#")) {
            return topicFiter.equals(topic);
        }
        String[] filterTopics = topicFiter.split("/");
        String[] actualTopics = topic.split("/");

        int i = 0;
        for (; i < filterTopics.length && i < actualTopics.length; i++) {
            if ("+".equals(filterTopics[i])) {
                continue;
            }
            if ("#".equals(filterTopics[i])) {
                return true;
            }
            if (!filterTopics[i].equals(actualTopics[i])) {
                return false;
            }
        }
        return i == actualTopics.length;
    }
}
