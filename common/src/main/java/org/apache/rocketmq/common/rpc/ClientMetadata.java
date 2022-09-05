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
package org.apache.rocketmq.common.rpc;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientMetadata {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<String, ConcurrentMap<MessageQueue, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
            new ConcurrentHashMap<String, HashMap<String, Integer>>();

    public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
        if (topic == null
            || topicRouteData == null) {
            return;
        }
        TopicRouteData old = this.topicRouteTable.get(topic);
        if (!topicRouteData.topicRouteDataChanged(old)) {
            return ;
        }
        {
            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
            }
        }
        {
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
            if (mqEndPoints != null
                    && !mqEndPoints.isEmpty()) {
                topicEndPointsTable.put(topic, mqEndPoints);
            }
        }
    }

    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null
                && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    public void refreshClusterInfo(ClusterInfo clusterInfo) {
        if (clusterInfo == null
            || clusterInfo.getBrokerAddrTable() == null) {
            return;
        }
        for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
            brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
        }
    }

    public String findMasterBrokerAddr(String brokerName) {
        if (!brokerAddrTable.containsKey(brokerName)) {
            return null;
        }
        return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
    }

    public ConcurrentMap<String, HashMap<Long, String>> getBrokerAddrTable() {
        return brokerAddrTable;
    }

    public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
        if (route.getTopicQueueMappingByBroker() == null
                || route.getTopicQueueMappingByBroker().isEmpty()) {
            return new ConcurrentHashMap<MessageQueue, String>();
        }
        ConcurrentMap<MessageQueue, String> mqEndPointsOfBroker = new ConcurrentHashMap<MessageQueue, String>();

        Map<String, Map<String, TopicQueueMappingInfo>> mappingInfosByScope = new HashMap<String, Map<String, TopicQueueMappingInfo>>();
        for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
            TopicQueueMappingInfo info = entry.getValue();
            String scope = info.getScope();
            if (scope != null) {
                if (!mappingInfosByScope.containsKey(scope)) {
                    mappingInfosByScope.put(scope, new HashMap<String, TopicQueueMappingInfo>());
                }
                mappingInfosByScope.get(scope).put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfosByScope.entrySet()) {
            String scope = mapEntry.getKey();
            Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap =  mapEntry.getValue();
            ConcurrentMap<MessageQueue, TopicQueueMappingInfo> mqEndPoints = new ConcurrentHashMap<MessageQueue, TopicQueueMappingInfo>();
            List<Map.Entry<String, TopicQueueMappingInfo>> mappingInfos = new ArrayList<Map.Entry<String, TopicQueueMappingInfo>>(topicQueueMappingInfoMap.entrySet());
            Collections.sort(mappingInfos, new Comparator<Map.Entry<String, TopicQueueMappingInfo>>() {
                @Override
                public int compare(Map.Entry<String, TopicQueueMappingInfo> o1, Map.Entry<String, TopicQueueMappingInfo> o2) {
                    return  (int) (o2.getValue().getEpoch() - o1.getValue().getEpoch());
                }
            });
            int maxTotalNums = 0;
            long maxTotalNumOfEpoch = -1;
            for (Map.Entry<String, TopicQueueMappingInfo> entry : mappingInfos) {
                TopicQueueMappingInfo info = entry.getValue();
                if (info.getEpoch() >= maxTotalNumOfEpoch && info.getTotalQueues() > maxTotalNums) {
                    maxTotalNums = info.getTotalQueues();
                }
                for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
                    int globalId = idEntry.getKey();
                    MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(info.getScope()), globalId);
                    TopicQueueMappingInfo oldInfo = mqEndPoints.get(mq);
                    if (oldInfo == null ||  oldInfo.getEpoch() <= info.getEpoch()) {
                        mqEndPoints.put(mq, info);
                    }
                }
            }


            //accomplish the static logic queues
            for (int i = 0; i < maxTotalNums; i++) {
                MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(scope), i);
                if (!mqEndPoints.containsKey(mq)) {
                    mqEndPointsOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
                } else {
                    mqEndPointsOfBroker.put(mq, mqEndPoints.get(mq).getBname());
                }
            }
        }
        return mqEndPointsOfBroker;
    }

}
