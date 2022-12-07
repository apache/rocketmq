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
package org.apache.rocketmq.broker.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.LogicQueueMappingItem;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.remoting.rpc.ClientMetadata;
import org.apache.rocketmq.remoting.rpc.RpcClient;
import org.apache.rocketmq.remoting.rpc.RpcRequest;
import org.apache.rocketmq.remoting.rpc.RpcResponse;
import org.apache.rocketmq.store.config.MessageStoreConfig;

public class TopicQueueMappingCleanService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private TopicQueueMappingManager topicQueueMappingManager;
    private BrokerOuterAPI brokerOuterAPI;
    private RpcClient rpcClient;
    private MessageStoreConfig messageStoreConfig;
    private BrokerConfig brokerConfig;
    private BrokerController brokerController;

    public TopicQueueMappingCleanService(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.topicQueueMappingManager = brokerController.getTopicQueueMappingManager();
        this.rpcClient = brokerController.getBrokerOuterAPI().getRpcClient();
        this.messageStoreConfig = brokerController.getMessageStoreConfig();
        this.brokerConfig = brokerController.getBrokerConfig();
        this.brokerOuterAPI = brokerController.getBrokerOuterAPI();
    }

    @Override
    public String getServiceName() {
        if (this.brokerConfig.isInBrokerContainer()) {
            return this.brokerController.getBrokerIdentity().getIdentifier() + TopicQueueMappingCleanService.class.getSimpleName();
        }
        return TopicQueueMappingCleanService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info("Start topic queue mapping clean service thread!");
        while (!this.isStopped()) {
            try {
                this.waitForRunning(5L * 60 * 1000);
            } catch (Throwable ignored) {
            }
            try {
                cleanItemExpired();
            } catch (Throwable t) {
                log.error("topic queue mapping cleanItemExpired failed", t);
            }
            try {
                cleanItemListMoreThanSecondGen();
            } catch (Throwable t) {
                log.error("topic queue mapping cleanItemListMoreThanSecondGen failed", t);
            }

        }
        log.info("End topic queue mapping clean service  thread!");
    }



    public void cleanItemExpired() {
        String when = messageStoreConfig.getDeleteWhen();
        if (!UtilAll.isItTimeToDo(when)) {
            return;
        }
        boolean changed = false;
        long start = System.currentTimeMillis();
        try {
            for (String topic : this.topicQueueMappingManager.getTopicQueueMappingTable().keySet()) {
                try {
                    if (isStopped()) {
                        break;
                    }
                    TopicQueueMappingDetail mappingDetail = this.topicQueueMappingManager.getTopicQueueMappingTable().get(topic);
                    if (mappingDetail == null
                            || mappingDetail.getHostedQueues().isEmpty()) {
                        continue;
                    }
                    if (!mappingDetail.getBname().equals(brokerConfig.getBrokerName())) {
                        log.warn("The TopicQueueMappingDetail [{}] should not exist in this broker", mappingDetail);
                        continue;
                    }
                    Set<String> brokers = new HashSet<>();
                    for (List<LogicQueueMappingItem> items: mappingDetail.getHostedQueues().values()) {
                        if (items.size() <= 1) {
                            continue;
                        }
                        if (!TopicQueueMappingUtils.checkIfLeader(items, mappingDetail)) {
                            continue;
                        }
                        LogicQueueMappingItem earlistItem = items.get(0);
                        brokers.add(earlistItem.getBname());
                    }
                    Map<String, TopicStatsTable> statsTable = new HashMap<>();
                    for (String broker: brokers) {
                        GetTopicStatsInfoRequestHeader header = new GetTopicStatsInfoRequestHeader();
                        header.setTopic(topic);
                        header.setBname(broker);
                        header.setLo(false);
                        try {
                            RpcRequest rpcRequest = new RpcRequest(RequestCode.GET_TOPIC_STATS_INFO, header, null);
                            RpcResponse rpcResponse = rpcClient.invoke(rpcRequest, brokerConfig.getForwardTimeout()).get();
                            if (rpcResponse.getException() != null) {
                                throw rpcResponse.getException();
                            }
                            statsTable.put(broker, (TopicStatsTable) rpcResponse.getBody());
                        } catch (Throwable rt) {
                            log.error("Get remote topic {} state info failed from broker {}", topic, broker, rt);
                        }
                    }
                    Map<Integer, List<LogicQueueMappingItem>> newHostedQueues = new HashMap<>();
                    boolean changedForTopic = false;
                    for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry : mappingDetail.getHostedQueues().entrySet()) {
                        Integer qid = entry.getKey();
                        List<LogicQueueMappingItem> items = entry.getValue();
                        if (items.size() <= 1) {
                            continue;
                        }
                        if (!TopicQueueMappingUtils.checkIfLeader(items, mappingDetail)) {
                            continue;
                        }
                        LogicQueueMappingItem earlistItem = items.get(0);
                        TopicStatsTable topicStats = statsTable.get(earlistItem.getBname());
                        if (topicStats == null) {
                            continue;
                        }
                        TopicOffset topicOffset = topicStats.getOffsetTable().get(new MessageQueue(topic, earlistItem.getBname(), earlistItem.getQueueId()));
                        if (topicOffset == null) {
                            //this may should not happen
                            log.error("Get null topicOffset for {} {}",topic,  earlistItem);
                            continue;
                        }
                        //ignore the maxOffset < 0, which may in case of some error
                        if (topicOffset.getMaxOffset() == topicOffset.getMinOffset()
                            || topicOffset.getMaxOffset() == 0) {
                            List<LogicQueueMappingItem> newItems = new ArrayList<>(items);
                            boolean result = newItems.remove(earlistItem);
                            if (result) {
                                changedForTopic = true;
                                newHostedQueues.put(qid, newItems);
                            }
                            log.info("The logic queue item {} {} is removed {} because of {}", topic, earlistItem, result, topicOffset);
                        }
                    }
                    if (changedForTopic) {
                        TopicQueueMappingDetail newMappingDetail = new TopicQueueMappingDetail(mappingDetail.getTopic(), mappingDetail.getTotalQueues(), mappingDetail.getBname(), mappingDetail.getEpoch());
                        newMappingDetail.getHostedQueues().putAll(mappingDetail.getHostedQueues());
                        newMappingDetail.getHostedQueues().putAll(newHostedQueues);
                        this.topicQueueMappingManager.updateTopicQueueMapping(newMappingDetail, false, true, false);
                        changed = true;
                    }
                } catch (Throwable tt) {
                    log.error("Try CleanItemExpired failed for {}", topic, tt);
                } finally {
                    UtilAll.sleep(10);
                }
            }
        } catch (Throwable t) {
            log.error("Try cleanItemExpired failed", t);
        } finally {
            if (changed) {
                this.topicQueueMappingManager.getDataVersion().nextVersion();
                this.topicQueueMappingManager.persist();
                log.info("CleanItemExpired changed");
            }
            log.info("cleanItemExpired cost {} ms", System.currentTimeMillis() - start);
        }
    }

    public void cleanItemListMoreThanSecondGen() {
        String when = messageStoreConfig.getDeleteWhen();
        if (!UtilAll.isItTimeToDo(when)) {
            return;
        }
        boolean changed = false;
        long start = System.currentTimeMillis();
        try {
            ClientMetadata clientMetadata = new ClientMetadata();
            for (String topic : this.topicQueueMappingManager.getTopicQueueMappingTable().keySet()) {
                try {
                    if (isStopped()) {
                        break;
                    }
                    TopicQueueMappingDetail mappingDetail = this.topicQueueMappingManager.getTopicQueueMappingTable().get(topic);
                    if (mappingDetail == null
                            || mappingDetail.getHostedQueues().isEmpty()) {
                        continue;
                    }
                    if (!mappingDetail.getBname().equals(brokerConfig.getBrokerName())) {
                        log.warn("The TopicQueueMappingDetail [{}] should not exist in this broker", mappingDetail);
                        continue;
                    }
                    Map<Integer, String> qid2CurrLeaderBroker = new HashMap<>();
                    for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry : mappingDetail.getHostedQueues().entrySet()) {
                        Integer qId = entry.getKey();
                        List<LogicQueueMappingItem> items = entry.getValue();
                        if (items.isEmpty()) {
                            continue;
                        }
                        LogicQueueMappingItem leaderItem = items.get(items.size() - 1);
                        if (!leaderItem.getBname().equals(mappingDetail.getBname())) {
                            qid2CurrLeaderBroker.put(qId, leaderItem.getBname());
                        }
                    }
                    if (qid2CurrLeaderBroker.isEmpty()) {
                        continue;
                    }
                    //find the topic route
                    TopicRouteData topicRouteData = brokerOuterAPI.getTopicRouteInfoFromNameServer(topic, brokerConfig.getForwardTimeout());
                    clientMetadata.freshTopicRoute(topic, topicRouteData);
                    Map<Integer, String> qid2RealLeaderBroker = new HashMap<>();
                    //fine the real leader
                    for (Map.Entry<Integer, String> entry : qid2CurrLeaderBroker.entrySet()) {
                        qid2RealLeaderBroker.put(entry.getKey(), clientMetadata.getBrokerNameFromMessageQueue(new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(mappingDetail.getScope()), entry.getKey())));
                    }

                    //find the mapping detail of real leader
                    Map<String, TopicQueueMappingDetail> mappingDetailMap = new HashMap<>();
                    for (Map.Entry<Integer, String> entry : qid2RealLeaderBroker.entrySet()) {
                        if (entry.getValue().startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                            continue;
                        }
                        String broker = entry.getValue();
                        GetTopicConfigRequestHeader header = new GetTopicConfigRequestHeader();
                        header.setTopic(topic);
                        header.setBname(broker);
                        header.setLo(true);
                        try {
                            RpcRequest rpcRequest = new RpcRequest(RequestCode.GET_TOPIC_CONFIG, header, null);
                            RpcResponse rpcResponse = rpcClient.invoke(rpcRequest, brokerConfig.getForwardTimeout()).get();
                            if (rpcResponse.getException() != null) {
                                throw rpcResponse.getException();
                            }
                            TopicQueueMappingDetail mappingDetailRemote = ((TopicConfigAndQueueMapping) rpcResponse.getBody()).getMappingDetail();
                            if (broker.equals(mappingDetailRemote.getBname())) {
                                mappingDetailMap.put(broker, mappingDetailRemote);
                            }
                        } catch (Throwable rt) {
                            log.error("Get remote topic {} state info failed from broker {}", topic, broker, rt);
                        }
                    }
                    //check all the info
                    Set<Integer> ids2delete = new HashSet<>();
                    for (Map.Entry<Integer, String> entry : qid2CurrLeaderBroker.entrySet()) {
                        Integer qId = entry.getKey();
                        String currLeaderBroker = entry.getValue();
                        String realLeaderBroker = qid2RealLeaderBroker.get(qId);
                        TopicQueueMappingDetail remoteMappingDetail = mappingDetailMap.get(realLeaderBroker);
                        if (remoteMappingDetail == null
                                || remoteMappingDetail.getTotalQueues() != mappingDetail.getTotalQueues()
                                || remoteMappingDetail.getEpoch() != mappingDetail.getEpoch()) {
                            continue;
                        }
                        List<LogicQueueMappingItem> items = remoteMappingDetail.getHostedQueues().get(qId);
                        if (items.isEmpty()) {
                            continue;
                        }
                        LogicQueueMappingItem leaderItem = items.get(items.size() - 1);
                        if (!realLeaderBroker.equals(leaderItem.getBname())) {
                            continue;
                        }
                        //all the check is ok
                        if (!realLeaderBroker.equals(currLeaderBroker)) {
                            ids2delete.add(qId);
                        }
                    }
                    for (Integer qid : ids2delete) {
                        List<LogicQueueMappingItem> items = mappingDetail.getHostedQueues().remove(qid);
                        changed =  true;
                        if (items != null) {
                            log.info("Remove the ItemListMoreThanSecondGen topic {} qid {} items {}", topic, qid, items);
                        }
                    }
                } catch (Throwable tt) {
                    log.error("Try cleanItemListMoreThanSecondGen failed for topic {}", topic, tt);
                } finally {
                    UtilAll.sleep(10);
                }
            }
        } catch (Throwable t) {
            log.error("Try cleanItemListMoreThanSecondGen failed", t);
        } finally {
            if (changed) {
                this.topicQueueMappingManager.getDataVersion().nextVersion();
                this.topicQueueMappingManager.persist();
            }
            log.info("Try cleanItemListMoreThanSecondGen cost {} ms", System.currentTimeMillis() - start);
        }
    }




}
