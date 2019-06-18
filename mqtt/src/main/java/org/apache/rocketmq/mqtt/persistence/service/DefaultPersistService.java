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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.client.Client;
import org.apache.rocketmq.common.client.Subscription;
import org.apache.rocketmq.common.constant.LoggerName;
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
import org.apache.rocketmq.common.service.EnodeService;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.mqtt.constant.MqttConstant;
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

        IsClient2SubscriptionPersistedRequestHeader requestHeader = new IsClient2SubscriptionPersistedRequestHeader();
        requestHeader.setClientId(clientId);
        requestHeader.setCleanSession(cleanSession);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_IS_CLIENT2SUBSCRIPTION_PERSISTED,requestHeader);
        request.addExtField(MqttConstant.ENODE_NAME,enodeName);

        try {
            RemotingCommand response = enodeService.requestMQTTInfoSync(request);
            IsClient2SubscriptionPersistedResponseHeader responseHeader = (IsClient2SubscriptionPersistedResponseHeader) response.decodeCommandCustomHeader(IsClient2SubscriptionPersistedResponseHeader.class);
            return responseHeader.isPersisted();
        } catch (Exception e) {
            log.error("Transfer MQTT info to Enode: {} failed, Err: {} ", enodeName, e);
        }
        return false;
    }

    @Override public boolean addOrUpdateClient2Susbscription(Client client, Subscription subscription) {
        // client2Subscription request
        boolean client2SubscriptionResult = false;
        String enodeName = this.getAllocateEnodeName(client.getClientId());
        AddOrUpdateClient2SubscriptionRequestHeader requestHeader = new AddOrUpdateClient2SubscriptionRequestHeader();
        requestHeader.setClient(client);
        requestHeader.setSubscription(subscription);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_ADD_OR_UPDATE_CLIENT2SUBSCRIPTION,requestHeader);
        request.addExtField(MqttConstant.ENODE_NAME, enodeName);

        try {
            RemotingCommand response = enodeService.requestMQTTInfoSync(request);
            AddOrUpdateClient2SubscriptionResponseHeader responseHeader = (AddOrUpdateClient2SubscriptionResponseHeader) response.decodeCommandCustomHeader(AddOrUpdateClient2SubscriptionResponseHeader.class);
            client2SubscriptionResult =  responseHeader.isResult();
        } catch (Exception e) {
            log.error("Transfer MQTT info to Enode: {} failed, Err: {} ", enodeName, e);
        }


        // rootTopic2Clients request
        boolean rootTopic2ClientsResult = true;
        for (String rootTopic:subscription.getSubscriptionTable().keySet().stream().map(t -> t.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0]).distinct().collect(Collectors.toList())) {
            String enodeNameForRootTopic = this.getAllocateEnodeName(rootTopic);
            AddOrUpdateRootTopic2ClientsRequestHeader addOrUpdateRootTopic2ClientsRequestHeader = new AddOrUpdateRootTopic2ClientsRequestHeader();
            addOrUpdateRootTopic2ClientsRequestHeader.setRootTopic(rootTopic);
            addOrUpdateRootTopic2ClientsRequestHeader.setClientId(client.getClientId());
            RemotingCommand requestForRootTopic = RemotingCommand.createRequestCommand(RequestCode.MQTT_ADD_OR_UPDATE_ROOTTOPIC2CLIENTS, addOrUpdateRootTopic2ClientsRequestHeader);
            requestForRootTopic.addExtField(MqttConstant.ENODE_NAME, enodeNameForRootTopic);
            try {
                AddOrUpdateRootTopic2ClientsResponseHeader responseHeader = (AddOrUpdateRootTopic2ClientsResponseHeader) enodeService.requestMQTTInfoSync(requestForRootTopic).decodeCommandCustomHeader(AddOrUpdateRootTopic2ClientsResponseHeader.class);
                rootTopic2ClientsResult = rootTopic2ClientsResult && responseHeader.isOperationSuccess();
            } catch (Exception ex) {
                log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, ex);
            }
        }


        return rootTopic2ClientsResult && client2SubscriptionResult;
    }

    @Override public boolean deleteClient2Subscription(Client client) {
        // delete client2subscription and client2snodeAddress
        DeleteClient2SubscriptionRequestHeader deleteClient2SubscriptionRequestHeader = new DeleteClient2SubscriptionRequestHeader();
        deleteClient2SubscriptionRequestHeader.setClientId(client.getClientId());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_CLIENT2SUBSCRIPTION, deleteClient2SubscriptionRequestHeader);
        String enodeName = this.getAllocateEnodeName(client.getClientId());
        request.addExtField(MqttConstant.ENODE_NAME,enodeName);
        RemotingCommand response = null;
        try {
            response = this.enodeService.requestMQTTInfoSync(request);
        } catch (Exception e) {
            log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, e);
        }

        // delete rootTopic2Clients
        if (response != null) {
            boolean client2SubResult;
            try {
                DeleteClient2SubscriptionResponseHeader deleteClient2SubscriptionResponseHeader = (DeleteClient2SubscriptionResponseHeader) response.decodeCommandCustomHeader(DeleteClient2SubscriptionResponseHeader.class);
                client2SubResult = deleteClient2SubscriptionResponseHeader.isOperationSuccess();
                Subscription subscription = deleteClient2SubscriptionResponseHeader.getSubscription();
                boolean rootTopic2ClientsResult = true;
                for (String rootTopic:subscription.getSubscriptionTable().keySet().stream().map(t -> t.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0]).distinct().collect(Collectors.toList())) {
                    String enodeNameForRootTopic = this.getAllocateEnodeName(rootTopic);
                    DeleteRootTopic2ClientRequestHeader deleteRootTopic2ClientRequestHeader = new DeleteRootTopic2ClientRequestHeader();
                    deleteRootTopic2ClientRequestHeader.setClientId(client.getClientId());
                    deleteRootTopic2ClientRequestHeader.setRootTopic(rootTopic);
                    request = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_ROOTTOPIC2CLIENT, deleteRootTopic2ClientRequestHeader);
                    request.addExtField(MqttConstant.ENODE_NAME, enodeNameForRootTopic);
                    try {
                        DeleteRootTopic2ClientResponseHeader deleteRootTopic2ClientResponseHeader = (DeleteRootTopic2ClientResponseHeader) enodeService.requestMQTTInfoSync(request).decodeCommandCustomHeader(DeleteClient2SubscriptionResponseHeader.class);
                        rootTopic2ClientsResult = rootTopic2ClientsResult && deleteRootTopic2ClientResponseHeader.isOperationSuccess();
                    } catch (Exception ex) {
                        log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, ex);
                    }
                }
                return client2SubResult && rootTopic2ClientsResult;
            } catch (Exception e) {
                log.error("Decode deleteClient2Subscription response header failed, error:{}",e);
            }
        }

        return false;
    }

    @Override
    public Map<String, Set<Client>> getSnodeAddress2Clients(String topic) {
        final Map<String,Set<Client>> snodeAddress2Clients = new ConcurrentHashMap<>();
        // step1: get rootTopic2Clients
        String rootTopic = topic.split(MqttConstant.SUBSCRIPTION_SEPARATOR)[0];
        GetRootTopic2ClientsRequestHeader requestHeader = new GetRootTopic2ClientsRequestHeader();
        requestHeader.setRootTopic(rootTopic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_GET_ROOTTOPIC2CLIENTS,requestHeader);
        String enodeName = this.getAllocateEnodeName(rootTopic);
        request.addExtField(MqttConstant.ENODE_NAME,enodeName);
        try {
            RemotingCommand response = this.enodeService.requestMQTTInfoSync(request);
            GetRootTopic2ClientsResponseHeader responseHeader = (GetRootTopic2ClientsResponseHeader) response.decodeCommandCustomHeader(GetRootTopic2ClientsResponseHeader.class);
            if (responseHeader.isOperationSuccess()) {

                Set<String> clientsId = responseHeader.getClientsId();
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
                final CountDownLatch countDownLatch = new CountDownLatch(enodeName2ClientsIdSet.size());
                for (String enodeNameToSend:enodeName2ClientsIdSet.keySet()) {
                    GetSnodeAddress2ClientsRequestHeader getSnodeAddress2ClientsRequestHeader = new GetSnodeAddress2ClientsRequestHeader();
                    getSnodeAddress2ClientsRequestHeader.setClientsId(enodeName2ClientsIdSet.get(enodeNameToSend));
                    getSnodeAddress2ClientsRequestHeader.setTopic(topic);
                    RemotingCommand requestToSend = RemotingCommand.createRequestCommand(RequestCode.MQTT_GET_SNODEADDRESS2CLIENT,getSnodeAddress2ClientsRequestHeader);
                    CompletableFuture<RemotingCommand> responseFuture = this.enodeService.sendMessage(null, enodeNameToSend, requestToSend);
                    responseFuture.whenComplete((data,ex) -> {
                        if (ex == null) {
                            try {
                                GetSnodeAddress2ClientsResponseHeader getSnodeAddress2ClientsResponseHeader = (GetSnodeAddress2ClientsResponseHeader) data.decodeCommandCustomHeader(GetSnodeAddress2ClientsResponseHeader.class);
                                Map<String,Set<Client>> snodeAddress2ClientsTmp = getSnodeAddress2ClientsResponseHeader.getSnodeAddress2Clients();
                                for (String snodeAddress:snodeAddress2ClientsTmp.keySet()) {
                                    snodeAddress2Clients.getOrDefault(snodeAddress, new HashSet<>()).addAll(snodeAddress2ClientsTmp.get(snodeAddress));
                                }
                            } catch (Exception e) {
                                log.error("Transfer MQTT snodeAddress2Clients info to Enode: {} failed, Err: {} ", enodeNameToSend, e);
                            }

                        }
                        countDownLatch.countDown();
                    });
                }
                countDownLatch.await();

            }
        } catch (Exception e) {
            log.error("Transfer MQTT rootTopic2Clients info to Enode: {} failed, Err: {} ", enodeName, e);
        }
        return snodeAddress2Clients;
    }



    @Override public boolean clientUnsubscribe(Client client, List<String> topics) {
        boolean result = false;
        // step1: delete client2sub
        ClientUnsubscribeRequestHeader requestHeader = new ClientUnsubscribeRequestHeader();
        requestHeader.setClientId(client.getClientId());
        requestHeader.setTopics(topics);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_CLIENT_UNSUBSRIBE,requestHeader);
        request.addExtField(MqttConstant.ENODE_NAME, this.getAllocateEnodeName(client.getClientId()));
        try {
            RemotingCommand response = this.enodeService.requestMQTTInfoSync(request);
            ClientUnsubscribeResponseHeader responseHeader = (ClientUnsubscribeResponseHeader) response.decodeCommandCustomHeader(ClientUnsubscribeResponseHeader.class);
            result = responseHeader.isOperationSuccess();
            // step2: delete rootTopic2Clients
            if (responseHeader.isRootTopicDiffExists()) {
                Set<String> rootTopicsDiff = responseHeader.getRootTopicsDiff();
                for (String rootTopic:rootTopicsDiff) {
                    DeleteRootTopic2ClientRequestHeader deleteRootTopic2ClientRequestHeader = new DeleteRootTopic2ClientRequestHeader();
                    deleteRootTopic2ClientRequestHeader.setRootTopic(rootTopic);
                    deleteRootTopic2ClientRequestHeader.setClientId(client.getClientId());
                    RemotingCommand requestForDeleteRootTopic = RemotingCommand.createRequestCommand(RequestCode.MQTT_DELETE_ROOTTOPIC2CLIENT,deleteRootTopic2ClientRequestHeader);
                    requestForDeleteRootTopic.addExtField(MqttConstant.ENODE_NAME, this.getAllocateEnodeName(rootTopic));
                    try {
                        this.enodeService.requestMQTTInfoSync(requestForDeleteRootTopic);
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

    @Override public Subscription getSubscriptionByClientId(String clientId) {
        GetSubscriptionByClientIdRequestHeader requestHeader = new GetSubscriptionByClientIdRequestHeader();
        requestHeader.setClientId(clientId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.MQTT_GET_SUBSCRIPTION_BY_CLIENT,requestHeader);
        request.addExtField(MqttConstant.ENODE_NAME, this.getAllocateEnodeName(clientId));
        try {
            RemotingCommand response = this.enodeService.requestMQTTInfoSync(request);
            GetSubscriptionByClientIdResponseHeader responseHeader = (GetSubscriptionByClientIdResponseHeader)response.decodeCommandCustomHeader(GetSubscriptionByClientIdResponseHeader.class);
            return responseHeader.getSubscription();
        } catch (Exception e) {
            log.error("Get Subscription failed, error: {}", e);
        }
        return null;
    }

    private String getAllocateEnodeName(String key) {
        String clusterName = defaultMqttMessageProcessor.getSnodeConfig().getClusterName();
        Set<String> enodeNames = defaultMqttMessageProcessor.getNnodeService().getEnodeClusterInfo(clusterName);
        String enodeName = allocatePersistentDataStrategy.allocate(key,enodeNames);
        return enodeName;
    }
}
