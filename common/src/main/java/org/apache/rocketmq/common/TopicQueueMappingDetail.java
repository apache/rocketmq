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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingDetail extends TopicQueueMappingInfo {

    // the mapping info in current broker, do not register to nameserver
    private ConcurrentMap<Integer/*global id*/, ImmutableList<LogicQueueMappingItem>> hostedQueues = new ConcurrentHashMap<Integer, ImmutableList<LogicQueueMappingItem>>();

    public TopicQueueMappingDetail(String topic, int totalQueues, String bname) {
        super(topic, totalQueues, bname);
        buildIdMap();
    }

    public boolean putMappingInfo(Integer globalId, ImmutableList<LogicQueueMappingItem> mappingInfo) {
        if (mappingInfo.isEmpty()) {
            return true;
        }
        hostedQueues.put(globalId, mappingInfo);
        buildIdMap();
        return true;
    }

    public void buildIdMap() {
        this.currIdMap = buildIdMap(LEVEL_0);
        this.prevIdMap = buildIdMap(LEVEL_1);
    }


    public ConcurrentMap<Integer, Integer> buildIdMap(int level) {
        //level 0 means current leader in this broker
        //level 1 means previous leader in this broker
        assert level == LEVEL_0 || level == LEVEL_1;

        if (hostedQueues == null || hostedQueues.isEmpty()) {
            return new ConcurrentHashMap<Integer, Integer>();
        }
        ConcurrentMap<Integer, Integer> tmpIdMap = new ConcurrentHashMap<Integer, Integer>();
        for (Map.Entry<Integer, ImmutableList<LogicQueueMappingItem>> entry: hostedQueues.entrySet()) {
            Integer globalId =  entry.getKey();
            ImmutableList<LogicQueueMappingItem> items = entry.getValue();
            if (level == LEVEL_0
                    && items.size() >= 1) {
                LogicQueueMappingItem curr = items.get(items.size() - 1);
                if (bname.equals(curr.getBname())) {
                    tmpIdMap.put(globalId, curr.getQueueId());
                }
            } else if (level == LEVEL_1
                    && items.size() >= 2) {
                LogicQueueMappingItem prev = items.get(items.size() - 1);
                if (bname.equals(prev.getBname())) {
                    tmpIdMap.put(globalId, prev.getQueueId());
                }
            }
        }
        return tmpIdMap;
    }

    public List<LogicQueueMappingItem> getMappingInfo(Integer globalId) {
        return hostedQueues.get(globalId);
    }


    public long computeStaticQueueOffset(Integer globalId, long physicalLogicOffset) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(globalId);
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return -1;
        }
        if (bname.equals(mappingItems.get(mappingItems.size() - 1).getBname())) {
            return mappingItems.get(mappingItems.size() - 1).computeStaticQueueOffset(physicalLogicOffset);
        }
        //Consider the "switch" process, reduce the error
        if (mappingItems.size() >= 2
            && bname.equals(mappingItems.get(mappingItems.size() - 2).getBname())) {
            return mappingItems.get(mappingItems.size() - 2).computeStaticQueueOffset(physicalLogicOffset);
        }
        return -1;
    }

    public LogicQueueMappingItem findLogicQueueMappingItem(Integer globalId, long logicOffset) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(globalId);
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return null;
        }
        //Could use bi-search to polish performance
        for (int i = mappingItems.size() - 1; i >= 0; i--) {
            LogicQueueMappingItem item =  mappingItems.get(i);
            if (logicOffset >= item.getLogicOffset()) {
                return item;
            }
        }
        //if not found, maybe out of range, return the first one
        for (int i = 0; i < mappingItems.size(); i++) {
            if (!mappingItems.get(i).checkIfShouldDeleted()) {
                return mappingItems.get(i);
            }
        }
        return null;
    }

    public long computeMaxOffsetFromMapping(Integer globalId) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(globalId);
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return -1;
        }
        LogicQueueMappingItem item =  mappingItems.get(mappingItems.size() - 1);
        return item.computeMaxStaticQueueOffset();
    }


    public TopicQueueMappingInfo cloneAsMappingInfo() {
        TopicQueueMappingInfo topicQueueMappingInfo = new TopicQueueMappingInfo(this.topic, this.totalQueues, this.bname);
        topicQueueMappingInfo.currIdMap = this.buildIdMap(LEVEL_0);
        topicQueueMappingInfo.prevIdMap = this.buildIdMap(LEVEL_1);

        return topicQueueMappingInfo;
    }

    public ConcurrentMap<Integer, ImmutableList<LogicQueueMappingItem>> getHostedQueues() {
        return hostedQueues;
    }
}
