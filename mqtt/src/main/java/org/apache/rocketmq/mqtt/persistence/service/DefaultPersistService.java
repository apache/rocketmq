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

package org.apache.rocketmq.mqtt.persistence.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.persistence.rebalance.AllocatePersistentDataConsistentHash;
import org.apache.rocketmq.mqtt.persistence.rebalance.AllocatePersistentDataStrategy;
import org.apache.rocketmq.mqtt.processor.DefaultMqttMessageProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DefaultPersistService implements PersistService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.MQTT_LOGGER_NAME);
    private  DefaultMqttMessageProcessor defaultMqttMessageProcessor;
    private  EnodeService enodeService;
    private  AllocatePersistentDataStrategy allocatePersistentDataStrategy;

    private final Gson GSON = new Gson();

    public DefaultPersistService() {
    }

    @Override
    public void init(DefaultMqttMessageProcessor defaultMqttMessageProcessor) {
        this.defaultMqttMessageProcessor = defaultMqttMessageProcessor;
        this.enodeService = defaultMqttMessageProcessor.getEnodeService();
        this.allocatePersistentDataStrategy = new AllocatePersistentDataConsistentHash();
    }

    @Override public boolean isClient2SubsriptionPersisted(Client client,Subscription subscription) {

        String clientId = client.getClientId();
        String enodeName = this.getAllocateEnodeName(clientId);
        boolean cleanSession = subscription.isCleanSession();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_IS_CLIENT2SUBSCRIPTION_PERSISTED,null);
        request.addExtField("enodeName", enodeName);
        request.addExtField("clientId", clientId);
        request.addExtField("cleanSession", String.valueOf(cleanSession));

        try {
            RemotingCommand response = enodeService.transferMQTTInfo2Enode(request);
            return Boolean.parseBoolean(response.getExtFields().get("isPersisted"));
        } catch (Exception e) {
            log.error("Transfer MQTT info to Enode: {} failed, Err: {} ", enodeName, e);
        }
        return false;
    }

    @Override public boolean addOrUpdateClient2Susbscription(Client client, Subscription subscription) {

        // client2Subscription request
        boolean client2SubscriptionResult = true;
        String enodeName = this.getAllocateEnodeName(client.getClientId());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_ADD_OR_UPDATE_CLIENT2SUBSCRIPTION,null);
        request.addExtField("enodeName", enodeName);
        request.addExtField("clientId", client.getClientId());
        request.addExtField("subscription", GSON.toJson(subscription));
        try {
            RemotingCommand response = enodeService.transferMQTTInfo2Enode(request);
            client2SubscriptionResult = client2SubscriptionResult && Boolean.parseBoolean(response.getExtFields().get("result"));
        } catch (Exception e) {
            log.error("Transfer MQTT info to Enode: {} failed, Err: {} ", enodeName, e);
        }


        // rootTopic2Clients request
        boolean rootTopic2ClientsResult = true;
        for (String rootTopic:subscription.getSubscriptionTable().keySet().stream().map(t -> t.split("/")[0]).distinct().collect(Collectors.toList())) {
            String enodeNameForRootTopic = this.getAllocateEnodeName(rootTopic);
            RemotingCommand requestForRootTopic = RemotingCommand.createRequestCommand(RequestCode.MQTT_ADD_OR_UPDATE_ROOTTOPIC2CLIENTS, null);
            requestForRootTopic.addExtField("enodeName", enodeNameForRootTopic);
            requestForRootTopic.addExtField("rootTopic", rootTopic);
            requestForRootTopic.addExtField("client", GSON.toJson(client));
            try {
                rootTopic2ClientsResult = rootTopic2ClientsResult && Boolean.parseBoolean(enodeService.transferMQTTInfo2Enode(requestForRootTopic).getExtFields().get("result"));
            } catch (Exception ex) {
                log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, ex);
            }
        }


        return rootTopic2ClientsResult && client2SubscriptionResult;
    }

    @Override public boolean deleteClient2Subscription(Client client) {
        // delete client2subscription and client2snodeAddress
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_CLIENT2SUBSCRIPTION,null);
        String enodeName = this.getAllocateEnodeName(client.getClientId());
        request.addExtField("enodeName",enodeName);
        request.addExtField("clientId", client.getClientId());
        RemotingCommand response = null;
        try {
            response = this.enodeService.transferMQTTInfo2Enode(request);
        } catch (Exception e) {
            log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, e);
        }

        // delete rootTopic2Clients
        if (response != null) {
            boolean client2SubResult = Boolean.parseBoolean(response.getExtFields().get("result"));
            String subscriptionString = response.getExtFields().get("subscription");
            if (subscriptionString != null) {
                Subscription subscription = GSON.fromJson(subscriptionString,Subscription.class);
                boolean rootTopic2ClientsResult = true;
                for (String rootTopic:subscription.getSubscriptionTable().keySet().stream().map(t -> t.split("/")[0]).distinct().collect(Collectors.toList())) {
                    String enodeNameForRootTopic = this.getAllocateEnodeName(rootTopic);
                    request = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_ROOTTOPIC2CLIENT, null);
                    request.addExtField("enodeName", enodeNameForRootTopic);
                    request.addExtField("rootTopic", rootTopic);
                    request.addExtField("clientId", client.getClientId());
                    try {
                        rootTopic2ClientsResult = rootTopic2ClientsResult && Boolean.parseBoolean(enodeService.transferMQTTInfo2Enode(request).getExtFields().get("result"));
                    } catch (Exception ex) {
                        log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, ex);
                    }
                }
                return client2SubResult && rootTopic2ClientsResult;
            }
        }

        return false;
    }

    @Override public Map<String, Map<String, Integer>> getSnodeAddress2Clients(String topic) {
        Map<String,Map<String,Integer>> snodeAddress2Clients = new ConcurrentHashMap<>();
        // step1: get rootTopic2Clients
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_GET_ROOTTOPIC2CLIENTS,null);
        String rootTopic = topic.split("/")[0];
        String enodeName = this.getAllocateEnodeName(rootTopic);
        request.addExtField("enodeName",enodeName);
        request.addExtField("rootTopic", rootTopic);
        try {
            RemotingCommand response = this.enodeService.transferMQTTInfo2Enode(request);
            if (Boolean.parseBoolean(response.getExtFields().get("result"))) {

                Set<String> clientsId = this.clientsIdStringToClientsIdSet(response.getExtFields().get("clientIsId"));
                HashMap<String,Set<String>> enodeName2ClientsIdSet = new HashMap<>();
                for (String clientId:clientsId) {
                    String enodeNameTmp = this.getAllocateEnodeName(clientId);
                    if (enodeName2ClientsIdSet.get(enodeNameTmp) == null) {
                        Set<String> clientsIdTmp = new HashSet<>();
                        clientsIdTmp.add(clientId);
                        enodeName2ClientsIdSet.put(enodeNameTmp,clientsIdTmp);
                    } else {
                        enodeName2ClientsIdSet.get(enodeNameTmp).add(clientId);
                    }
                }
                // step2: get snodeAddress2ClientsId
                for (String enodeNameToSend:enodeName2ClientsIdSet.keySet()) {
                    RemotingCommand requestToSend = RemotingCommand.createRequestCommand(RequestCode.MQTT_GET_SNODEADDRESS2CLIENT,null);
                    requestToSend.addExtField("enodeName",enodeNameToSend);
                    requestToSend.addExtField("clientsId", GSON.toJson(enodeName2ClientsIdSet.get(enodeNameToSend)));
                    requestToSend.addExtField("topic", topic);
                    RemotingCommand responseReceived = this.enodeService.transferMQTTInfo2Enode(requestToSend);
                    if (Boolean.parseBoolean(responseReceived.getExtFields().get("result"))) {
                        snodeAddress2Clients.putAll(GSON.fromJson(responseReceived.getExtFields().get("snodeAddress2Clients"), new TypeToken<ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>>() {
                        }.getType()));
                    }
                }

            }
        } catch (Exception e) {
            log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, e);
        }
        return snodeAddress2Clients;
    }

    @Override public boolean clientUnsubscribe(Client client, List<String> topics) {
        boolean result = false;
        // step1: delete client2sub
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_CLIENT_UNSUBSRIBE,null);
        request.addExtField("enodeName", this.getAllocateEnodeName(client.getClientId()));
        request.addExtField("clientId",client.getClientId());
        request.addExtField("topics", GSON.toJson(topics));
        try {
            RemotingCommand response = this.enodeService.transferMQTTInfo2Enode(request);
            result = Boolean.parseBoolean(response.getExtFields().get("result"));
            // step2: delete rootTopic2Clients
            if (Boolean.parseBoolean(response.getExtFields().get("rootTopicsDiffExists"))) {
                Set<String> rootTopicsDiff = GSON.fromJson(response.getExtFields().get("rootTopicsDiff"), new TypeToken<Set<String>>() {
                }.getType());
                for (String rootTopic:rootTopicsDiff) {
                    RemotingCommand requestForDeleteRootTopic = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_ROOTTOPIC2CLIENT,null);
                    requestForDeleteRootTopic.addExtField("enodeName", this.getAllocateEnodeName(rootTopic));
                    requestForDeleteRootTopic.addExtField("rootTopic", rootTopic);
                    requestForDeleteRootTopic.addExtField("clientId", client.getClientId());
                    try {
                        this.enodeService.transferMQTTInfo2Enode(requestForDeleteRootTopic);
                    } catch (Exception ex) {
                        log.error("Transfer MQTT rootTopic2Clients info  failed, Err: {} ", ex);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Transfer MQTT rootTopic2Clients info  failed, Err: {} ", e);
        }

        return result;
    }

    private String getAllocateEnodeName(String key) {
        String clusterName = defaultMqttMessageProcessor.getSnodeConfig().getClusterName();
        Set<String> enodeNames = defaultMqttMessageProcessor.getNnodeService().getEnodeClusterInfo(clusterName);
        String enodeName = allocatePersistentDataStrategy.allocate(key,enodeNames);
        return enodeName;
    }
    private Set<String> clientsIdStringToClientsIdSet(String clientsString) {
        Set<String> clientsId = new HashSet<>();
        Type type = new TypeToken<Set<String>>() {
        }.getType();
        clientsId = GSON.fromJson(clientsString,type);
        return clientsId;
    }

}
