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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;

public class RouteInfoManagerTestBase {

    protected static class Cluster {
        ConcurrentMap<String, TopicConfig> topicConfig;
        Map<String, BrokerData> brokerDataMap;

        public Cluster(ConcurrentMap<String, TopicConfig> topicConfig, Map<String, BrokerData> brokerData) {
            this.topicConfig = topicConfig;
            this.brokerDataMap = brokerData;
        }

        public Set<String> getAllBrokerName() {
            return brokerDataMap.keySet();
        }

        public Set<String> getAllTopicName() {
            return topicConfig.keySet();
        }
    }

    protected Cluster registerCluster(RouteInfoManager routeInfoManager, String cluster,
                                      String brokerNamePrefix,
                                      int brokerNameNumber,
                                      int brokerPerName,
                                      String topicPrefix,
                                      int topicNumber) {

        Map<String, BrokerData> brokerDataMap = new HashMap<>();

        // no filterServer address
        List<String> filterServerAddr = new ArrayList<>();

        ConcurrentMap<String, TopicConfig> topicConfig = genTopicConfig(topicPrefix, topicNumber);

        for (int i = 0; i < brokerNameNumber; i++) {
            String brokerName = getBrokerName(brokerNamePrefix, i);

            BrokerData brokerData = genBrokerData(cluster, brokerName, brokerPerName, true);

            // avoid object reference copy
            ConcurrentMap<String, TopicConfig> topicConfigForBroker = genTopicConfig(topicPrefix, topicNumber);

            registerBrokerWithTopicConfig(routeInfoManager, brokerData, topicConfigForBroker, filterServerAddr);

            // avoid object reference copy
            brokerDataMap.put(brokerData.getBrokerName(), genBrokerData(cluster, brokerName, brokerPerName, true));
        }

        return new Cluster(topicConfig, brokerDataMap);
    }

    protected String getBrokerAddr(String cluster, String brokerName, long brokerNumber) {
        return cluster + "-" + brokerName + ":" + brokerNumber;
    }

    protected BrokerData genBrokerData(String clusterName, String brokerName, long totalBrokerNumber, boolean hasMaster) {
        HashMap<Long, String> brokerAddrMap = new HashMap<>();

        long startId = 0;
        if (hasMaster) {
            brokerAddrMap.put(MixAll.MASTER_ID, getBrokerAddr(clusterName, brokerName, MixAll.MASTER_ID));
            startId = 1;
        }

        for (long i = startId; i < totalBrokerNumber; i++) {
            brokerAddrMap.put(i, getBrokerAddr(clusterName, brokerName, i));
        }

        return new BrokerData(clusterName, brokerName, brokerAddrMap);
    }

    protected void registerBrokerWithTopicConfig(RouteInfoManager routeInfoManager, BrokerData brokerData,
                                                 ConcurrentMap<String, TopicConfig> topicConfigTable,
                                                 List<String> filterServerAddr) {

        brokerData.getBrokerAddrs().forEach((brokerId, brokerAddr) -> {
            registerBrokerWithTopicConfig(routeInfoManager, brokerData.getCluster(),
                    brokerAddr,
                    brokerData.getBrokerName(),
                    brokerId,
                    brokerAddr, // set ha server address the same as brokerAddr
                    new ConcurrentHashMap<>(topicConfigTable),
                    new ArrayList<>(filterServerAddr));
        });
    }

    protected void unregisterBrokerAll(RouteInfoManager routeInfoManager, BrokerData brokerData) {
        for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
            routeInfoManager.unregisterBroker(brokerData.getCluster(), entry.getValue(), brokerData.getBrokerName(), entry.getKey());
        }
    }

    protected void unregisterBroker(RouteInfoManager routeInfoManager, BrokerData brokerData, long brokerId) {
        HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
        if (brokerAddrs.containsKey(brokerId)) {
            String address = brokerAddrs.remove(brokerId);
            routeInfoManager.unregisterBroker(brokerData.getCluster(), address, brokerData.getBrokerName(), brokerId);
        }
    }

    protected RegisterBrokerResult registerBrokerWithTopicConfig(RouteInfoManager routeInfoManager, String clusterName,
                                                 String brokerAddr,
                                                 String brokerName,
                                                 long brokerId,
                                                 String haServerAddr,
                                                 ConcurrentMap<String, TopicConfig> topicConfigTable,
                                                 List<String> filterServerAddr) {

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

        Channel channel = new EmbeddedChannel();
        return routeInfoManager.registerBroker(clusterName,
                brokerAddr,
                brokerName,
                brokerId,
                "",
                haServerAddr,
                null,
                topicConfigSerializeWrapper,
                filterServerAddr,
                channel);
    }


    protected String getTopicName(String topicPrefix, int topicNumber) {
        return topicPrefix + "-" + topicNumber;
    }

    protected ConcurrentMap<String, TopicConfig> genTopicConfig(String topicPrefix, int topicNumber) {
        ConcurrentMap<String, TopicConfig> topicConfigMap = new ConcurrentHashMap<>();

        for (int i = 0; i < topicNumber; i++) {
            String topicName = getTopicName(topicPrefix, i);

            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(topicName);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(8);
            topicConfig.setOrder(false);
            topicConfigMap.put(topicName, topicConfig);
        }

        return topicConfigMap;
    }

    protected String getBrokerName(String brokerNamePrefix, long brokerNameNumber) {
        return brokerNamePrefix + "-" + brokerNameNumber;
    }

    protected BrokerData findBrokerDataByBrokerName(List<BrokerData> data, String brokerName) {
        return data.stream().filter(bd -> bd.getBrokerName().equals(brokerName)).findFirst().orElse(null);
    }

}
