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
package org.apache.rocketmq.common;

import com.google.common.collect.ImmutableList;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TopicQueueMappingUtils {

    public static class MappingAllocator {
        Map<String, Integer> brokerNumMap = new HashMap<String, Integer>();
        Map<Integer, String> idToBroker = new HashMap<Integer, String>();
        int currentIndex = 0;
        Random random = new Random();
        List<String> leastBrokers = new ArrayList<String>();
        private MappingAllocator(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap) {
            this.idToBroker.putAll(idToBroker);
            this.brokerNumMap.putAll(brokerNumMap);
        }

        private void freshState() {
            int minNum = -1;
            for (Map.Entry<String, Integer> entry : brokerNumMap.entrySet()) {
                if (entry.getValue() > minNum) {
                    leastBrokers.clear();
                    leastBrokers.add(entry.getKey());
                } else if (entry.getValue() == minNum) {
                    leastBrokers.add(entry.getKey());
                }
            }
            currentIndex = random.nextInt(leastBrokers.size());
        }
        private String nextBroker() {
            if (leastBrokers.isEmpty()) {
                freshState();
            }
            int tmpIndex = currentIndex % leastBrokers.size();
            return leastBrokers.remove(tmpIndex);
        }

        public Map<String, Integer> getBrokerNumMap() {
            return brokerNumMap;
        }

        public void upToNum(int maxQueueNum) {
            int currSize = idToBroker.size();
            if (maxQueueNum <= currSize) {
                return;
            }
            for (int i = currSize; i < maxQueueNum; i++) {
                String nextBroker = nextBroker();
                if (brokerNumMap.containsKey(nextBroker)) {
                    brokerNumMap.put(nextBroker, brokerNumMap.get(nextBroker) + 1);
                } else {
                    brokerNumMap.put(nextBroker, 1);
                }
                idToBroker.put(i, nextBroker);
            }
        }

        public Map<Integer, String> getIdToBroker() {
            return idToBroker;
        }
    }

    public static MappingAllocator buildMappingAllocator(Map<Integer, String> idToBroker, Map<String, Integer> brokerNumMap) {
        return new MappingAllocator(idToBroker, brokerNumMap);
    }

    public static Map.Entry<Integer, Integer> findMaxEpochAndQueueNum(List<TopicQueueMappingDetail> mappingDetailList) {
        int epoch = -1;
        int queueNum = 0;
        for (TopicQueueMappingDetail mappingDetail : mappingDetailList) {
            if (mappingDetail.getEpoch() > epoch) {
                epoch = mappingDetail.getEpoch();
            }
            if (mappingDetail.getTotalQueues() > queueNum) {
                queueNum = mappingDetail.getTotalQueues();
            }
        }
        return new AbstractMap.SimpleImmutableEntry<Integer, Integer>(epoch, queueNum);
    }

    public static Map<Integer, ImmutableList<LogicQueueMappingItem>> buildMappingItems(List<TopicQueueMappingDetail> mappingDetailList, boolean replace) {
        Collections.sort(mappingDetailList, new Comparator<TopicQueueMappingDetail>() {
            @Override
            public int compare(TopicQueueMappingDetail o1, TopicQueueMappingDetail o2) {
                return o2.getEpoch() - o1.getEpoch();
            }
        });

        Map<Integer, ImmutableList<LogicQueueMappingItem>> globalIdMap = new HashMap<Integer, ImmutableList<LogicQueueMappingItem>>();
        for (TopicQueueMappingDetail mappingDetail : mappingDetailList) {
            for (Map.Entry<Integer, ImmutableList<LogicQueueMappingItem>>  entry : mappingDetail.getHostedQueues().entrySet()) {
                Integer globalid = entry.getKey();
                String leaderBrokerName  = getLeaderBroker(entry.getValue());
                if (!leaderBrokerName.equals(mappingDetail.getBname())) {
                    //not the leader
                    continue;
                }
                if (globalIdMap.containsKey(globalid)) {
                    if (!replace) {
                        throw new RuntimeException(String.format("The queue id is duplicated in broker %s %s", leaderBrokerName, mappingDetail.getBname()));
                    }
                } else {
                    globalIdMap.put(globalid, entry.getValue());
                }
            }
        }
        return globalIdMap;
    }

    public static String getLeaderBroker(ImmutableList<LogicQueueMappingItem> items) {
        return getLeaderItem(items).getBname();
    }
    public static LogicQueueMappingItem getLeaderItem(ImmutableList<LogicQueueMappingItem> items) {
        assert items.size() > 0;
        return items.get(items.size() - 1);
    }
}
