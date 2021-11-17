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

    public static class MappingState {
        Map<String, Integer> brokerNumMap = new HashMap<String, Integer>();
        int currentIndex = 0;
        Random random = new Random();
        List<String> leastBrokers = new ArrayList<String>();
        private MappingState(Map<String, Integer> brokerNumMap) {
            this.brokerNumMap.putAll(brokerNumMap);
        }

        public void freshState() {
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

        public String nextBroker() {
            if (leastBrokers.isEmpty()) {
                freshState();
            }
            int tmpIndex = (++currentIndex) % leastBrokers.size();
            String broker = leastBrokers.remove(tmpIndex);
            currentIndex--;
            return broker;
        }
    }

    public static MappingState buildMappingState(Map<String, Integer> brokerNumMap) {
        return new MappingState(brokerNumMap);
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
                String leaerBrokerName  = entry.getValue().iterator().next().getBname();
                if (!leaerBrokerName.equals(mappingDetail.getBname())) {
                    //not the leader
                    continue;
                }
                if (globalIdMap.containsKey(globalid)) {
                    if (!replace) {
                        throw new RuntimeException(String.format("The queue id is duplicated in broker %s %s", leaerBrokerName, mappingDetail.getBname()));
                    }
                } else {
                    globalIdMap.put(globalid, entry.getValue());
                }
            }
        }
        return globalIdMap;
    }
}
