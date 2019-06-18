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
import org.apache.rocketmq.common.protocol.header.mqtt.AddOrUpdateClient2SubscriptionRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.AddOrUpdateClient2SubscriptionResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.AddOrUpdateRootTopic2ClientsRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.AddOrUpdateRootTopic2ClientsResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.ClientUnsubscribeRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.ClientUnsubscribeResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.DeleteClient2SubscriptionRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.DeleteClient2SubscriptionResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.DeleteRootTopic2ClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.DeleteRootTopic2ClientResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetRootTopic2ClientsRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetRootTopic2ClientsResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetSnodeAddress2ClientsRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetSnodeAddress2ClientsResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetSubscriptionByClientIdRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.GetSubscriptionByClientIdResponseHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.IsClient2SubscriptionPersistedRequestHeader;
import org.apache.rocketmq.common.protocol.header.mqtt.IsClient2SubscriptionPersistedResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.mqtt.client.MQTTSession;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
import org.apache.rocketmq.remoting.RemotingChannel;
import org.apache.rocketmq.remoting.RequestProcessor;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
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
    public RemotingCommand processRequest(RemotingChannel remotingChannel, RemotingCommand request) throws RemotingCommandException {


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
            case RequestCode.MQTT_GET_SUBSCRIPTION_BY_CLIENT:
                return this.getSubscriptionByClientId(request);
            default:
                return null;
        }
    }

    private RemotingCommand getSubscriptionByClientId(RemotingCommand request) throws RemotingCommandException {
        GetSubscriptionByClientIdRequestHeader requestHeader = (GetSubscriptionByClientIdRequestHeader) request.decodeCommandCustomHeader(GetSubscriptionByClientIdRequestHeader.class);
        String clientId = requestHeader.getClientId();
        Subscription subscription = GSON.fromJson(this.mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX),Subscription.class);
        RemotingCommand response = RemotingCommand.createResponseCommand(GetSubscriptionByClientIdResponseHeader.class);
        GetSubscriptionByClientIdResponseHeader responseHeader = (GetSubscriptionByClientIdResponseHeader) response.readCustomHeader();
        responseHeader.setSubscription(subscription);
        return response;
    }

    private RemotingCommand isClient2SubscriptionPersistedHandler(final RemotingChannel remotingChannel,final RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(IsClient2SubscriptionPersistedResponseHeader.class);
        IsClient2SubscriptionPersistedResponseHeader responseHeader = (IsClient2SubscriptionPersistedResponseHeader) response.readCustomHeader();
        IsClient2SubscriptionPersistedRequestHeader requestHeader = (IsClient2SubscriptionPersistedRequestHeader) request.decodeCommandCustomHeader(IsClient2SubscriptionPersistedRequestHeader.class);


        String clientId = requestHeader.getClientId();
        boolean cleanSession = requestHeader.isCleanSession();

        String subscriptionJson = mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX);
        if (subscriptionJson != null) {
            Subscription subscription = GSON.fromJson(subscriptionJson, Subscription.class);
            if (subscription.isCleanSession() != cleanSession) {
                subscription.setCleanSession(cleanSession);
                mqttInfoStore.putData(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX, GSON.toJson(subscription));
            }
            responseHeader.setPersisted(true);
        } else {
            responseHeader.setPersisted(false);
        }

        mqttInfoStore.putData(clientId + MqttConstant.PERSIST_SNODEADDRESS_SUFFIX, remotingChannel.remoteAddress().toString());

        return response;
    }

    private RemotingCommand addOrUpdateClient2Subscription(final RemotingChannel remotingChannel,final RemotingCommand request) throws RemotingCommandException {
        AddOrUpdateClient2SubscriptionRequestHeader requestHeader = (AddOrUpdateClient2SubscriptionRequestHeader) request.decodeCommandCustomHeader(AddOrUpdateClient2SubscriptionRequestHeader.class);
        Client client = requestHeader.getClient();
        Subscription subscription = requestHeader.getSubscription();

        boolean client2SubResult = this.mqttInfoStore.putData(client.getClientId() + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX,GSON.toJson(subscription));
        boolean client2SnoResult = this.mqttInfoStore.putData(client.getClientId() + MqttConstant.PERSIST_SNODEADDRESS_SUFFIX,remotingChannel.remoteAddress().toString());
        boolean client2EntityResult = this.mqttInfoStore.putData(client.getClientId() + MqttConstant.PERSIST_CLIENT_SUFFIX, GSON.toJson(client));

        RemotingCommand response = RemotingCommand.createResponseCommand(AddOrUpdateClient2SubscriptionResponseHeader.class);
        AddOrUpdateClient2SubscriptionResponseHeader responseHeader = (AddOrUpdateClient2SubscriptionResponseHeader) response.readCustomHeader();
        responseHeader.setResult(client2SubResult && client2SnoResult && client2EntityResult);
        return response;
    }

    private RemotingCommand deleteClient2Subscription(final RemotingCommand request) throws RemotingCommandException {
        DeleteClient2SubscriptionRequestHeader requestHeader = (DeleteClient2SubscriptionRequestHeader) request.decodeCommandCustomHeader(DeleteClient2SubscriptionRequestHeader.class);
        String clientId = requestHeader.getClientId();
        String subscriptionJson = this.mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX);
        Subscription subscription = GSON.fromJson(subscriptionJson,Subscription.class);
        boolean operationSuccess = this.mqttInfoStore.deleteData(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX) && this.mqttInfoStore.deleteData(clientId + MqttConstant.PERSIST_SNODEADDRESS_SUFFIX) && this.mqttInfoStore.deleteData(clientId + MqttConstant.PERSIST_CLIENT_SUFFIX);
        RemotingCommand response = RemotingCommand.createResponseCommand(DeleteClient2SubscriptionResponseHeader.class);
        DeleteClient2SubscriptionResponseHeader responseHeader = (DeleteClient2SubscriptionResponseHeader) response.readCustomHeader();
        responseHeader.setOperationSuccess(operationSuccess);
        responseHeader.setSubscription(subscription);
        return response;
    }

    private RemotingCommand getSnodeAddress2Clients(final RemotingCommand request) throws RemotingCommandException {
        Map<String, Set<Client>> snodeAddress2Clients = new ConcurrentHashMap<>();
        Set<Client> clients = new HashSet<>();
        GetSnodeAddress2ClientsRequestHeader requestHeader = (GetSnodeAddress2ClientsRequestHeader) request.decodeCommandCustomHeader(GetSnodeAddress2ClientsRequestHeader.class);
        String topic = requestHeader.getTopic();
        Set<String> clientsId = requestHeader.getClientsId();
        for (String clientId:clientsId) {
            ConcurrentHashMap<String/*Topic*/, SubscriptionData> subscriptionTable = GSON.fromJson(this.mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX), Subscription.class).getSubscriptionTable();
            for (String topicFilter:subscriptionTable.keySet()) {
                if (isMatch(topicFilter,topic)) {
                    clients.add(GSON.fromJson(this.mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_CLIENT_SUFFIX), MQTTSession.class));
                }
            }
        }
        for (Client client:clients) {
            String snodeAddress = this.mqttInfoStore.getValue(client.getClientId() + MqttConstant.PERSIST_SNODEADDRESS_SUFFIX);
            Set<Client> clientsTmp = snodeAddress2Clients.getOrDefault(snodeAddress,new HashSet<>());
            clientsTmp.add(client);
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(GetSnodeAddress2ClientsResponseHeader.class);
        GetSnodeAddress2ClientsResponseHeader responseHeader = (GetSnodeAddress2ClientsResponseHeader) response.readCustomHeader();
        responseHeader.setSnodeAddress2Clients(snodeAddress2Clients);

        return response;
    }

    private RemotingCommand clientUnsubscribe(final RemotingCommand request) throws RemotingCommandException {
        ClientUnsubscribeRequestHeader requestHeader = (ClientUnsubscribeRequestHeader) request.decodeCommandCustomHeader(ClientUnsubscribeRequestHeader.class);
        String clientId = requestHeader.getClientId();
        List<String> topics = requestHeader.getTopics();
        Subscription  subscription = GSON.fromJson(this.mqttInfoStore.getValue(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX),Subscription.class);
        ConcurrentHashMap<String,SubscriptionData> subscriptionTable = subscription.getSubscriptionTable();
        Set<String> rootTopicsBefore = subscriptionTable.keySet().stream().map(t -> t.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0]).collect(Collectors.toSet());
        for (String topic:topics) {
            subscriptionTable.remove(topic);
        }
        Set<String> rootTopicAfter = subscriptionTable.keySet().stream().map(t -> t.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0]).collect(Collectors.toSet());
        Set<String> rootTopicsDiff = new HashSet<>();
        rootTopicsDiff.addAll(rootTopicsBefore);
        rootTopicsDiff.removeAll(rootTopicAfter);

        subscription.setSubscriptionTable(subscriptionTable);
        boolean result = this.mqttInfoStore.putData(clientId + MqttConstant.PERSIST_SUBSCRIPTION_SUFFIX, GSON.toJson(subscription));
        RemotingCommand response = RemotingCommand.createResponseCommand(ClientUnsubscribeResponseHeader.class);
        ClientUnsubscribeResponseHeader responseHeader = (ClientUnsubscribeResponseHeader) response.readCustomHeader();
        responseHeader.setOperationSuccess(result);
        if (rootTopicsDiff.size() != 0) {
            responseHeader.setRootTopicDiffExists(true);
            responseHeader.setRootTopicsDiff(rootTopicsDiff);
        }
        return response;
    }
    private RemotingCommand addorUpdateRootTopic2Clients(final RemotingCommand request) throws RemotingCommandException {
        AddOrUpdateRootTopic2ClientsRequestHeader requestHeader = (AddOrUpdateRootTopic2ClientsRequestHeader) request.decodeCommandCustomHeader(AddOrUpdateRootTopic2ClientsRequestHeader.class);

        String rootTopic = requestHeader.getRootTopic();
        String clientId = requestHeader.getClientId();
        String value = this.mqttInfoStore.getValue(rootTopic);
        Set<String> clientsId;

        if (value != null) {
            clientsId = GSON.fromJson(value,new TypeToken<Set<String>>() {
            }.getType());
        } else {
            clientsId = new HashSet<>();
        }
        clientsId.add(clientId);

        RemotingCommand response = RemotingCommand.createResponseCommand(AddOrUpdateRootTopic2ClientsResponseHeader.class);
        AddOrUpdateRootTopic2ClientsResponseHeader responseHeader = (AddOrUpdateRootTopic2ClientsResponseHeader) response.readCustomHeader();
        responseHeader.setOperationSuccess(this.mqttInfoStore.putData(rootTopic,GSON.toJson(clientsId)));

        return response;
    }

    private RemotingCommand getRootTopic2Clients(final RemotingCommand request) throws RemotingCommandException {
        GetRootTopic2ClientsRequestHeader requestHeader = (GetRootTopic2ClientsRequestHeader) request.decodeCommandCustomHeader(GetRootTopic2ClientsRequestHeader.class);
        String rootTopic = requestHeader.getRootTopic();
        String json = this.mqttInfoStore.getValue(rootTopic);
        RemotingCommand response = RemotingCommand.createResponseCommand(GetRootTopic2ClientsResponseHeader.class);
        GetRootTopic2ClientsResponseHeader responseHeader = (GetRootTopic2ClientsResponseHeader) response.readCustomHeader();
        if (json != null) {
            Set<String> clientsId = GSON.fromJson(json, new TypeToken<Set<String>>() {
            }.getType());
            responseHeader.setOperationSuccess(true);
            responseHeader.setClientsId(clientsId);
        } else {
            responseHeader.setOperationSuccess(false);
        }

        return response;
    }

    private RemotingCommand deleteRootTopic2Client(final RemotingCommand request) throws RemotingCommandException {
        DeleteRootTopic2ClientRequestHeader requestHeader = (DeleteRootTopic2ClientRequestHeader) request.decodeCommandCustomHeader(DeleteRootTopic2ClientRequestHeader.class);
        String rootTopic = requestHeader.getRootTopic();
        String clientId = requestHeader.getClientId();
        Set<String> clientsId = GSON.fromJson(this.mqttInfoStore.getValue(rootTopic),new TypeToken<Set<String>>() {
        }.getType());
        Set<String> clientsIdAfterDelete = clientsId.stream().filter(c -> c != clientId).collect(Collectors.toSet());
        boolean result;
        if (clientsIdAfterDelete.size() == 0) {
            result = this.mqttInfoStore.deleteData(rootTopic);
        } else {
            result = this.mqttInfoStore.putData(rootTopic,GSON.toJson(clientsIdAfterDelete));
        }
        RemotingCommand response = RemotingCommand.createResponseCommand(DeleteRootTopic2ClientResponseHeader.class);
        DeleteRootTopic2ClientResponseHeader responseHeader = (DeleteRootTopic2ClientResponseHeader) response.readCustomHeader();
        responseHeader.setOperationSuccess(result);
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
