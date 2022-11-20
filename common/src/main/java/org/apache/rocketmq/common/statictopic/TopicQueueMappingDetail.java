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
package org.apache.rocketmq.common.statictopic;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingDetail extends TopicQueueMappingInfo {

    // the mapping info in current broker, do not register to nameserver
    // make sure this value is not null
    private ConcurrentMap<Integer/*global id*/, List<LogicQueueMappingItem>> hostedQueues = new ConcurrentHashMap<Integer, List<LogicQueueMappingItem>>();

    //make sure there is a default constructor
    public TopicQueueMappingDetail() {

    }

    public TopicQueueMappingDetail(String topic, int totalQueues, String bname, long epoch) {
        super(topic, totalQueues, bname, epoch);
    }



    public static boolean putMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId, List<LogicQueueMappingItem> mappingInfo) {
        if (mappingInfo.isEmpty()) {
            return true;
        }
        mappingDetail.hostedQueues.put(globalId, mappingInfo);
        return true;
    }

    public static List<LogicQueueMappingItem> getMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        return mappingDetail.hostedQueues.get(globalId);
    }

    public static ConcurrentMap<Integer, Integer> buildIdMap(TopicQueueMappingDetail mappingDetail, int level) {
        //level 0 means current leader in this broker
        //level 1 means previous leader in this broker, reserved for
        assert level == LEVEL_0 ;

        if (mappingDetail.hostedQueues == null || mappingDetail.hostedQueues.isEmpty()) {
            return new ConcurrentHashMap<Integer, Integer>();
        }
        ConcurrentMap<Integer, Integer> tmpIdMap = new ConcurrentHashMap<Integer, Integer>();
        for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry: mappingDetail.hostedQueues.entrySet()) {
            Integer globalId =  entry.getKey();
            List<LogicQueueMappingItem> items = entry.getValue();
            if (level == LEVEL_0
                    && items.size() >= 1) {
                LogicQueueMappingItem curr = items.get(items.size() - 1);
                if (mappingDetail.bname.equals(curr.getBname())) {
                    tmpIdMap.put(globalId, curr.getQueueId());
                }
            }
        }
        return tmpIdMap;
    }


    public static long computeMaxOffsetFromMapping(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(mappingDetail, globalId);
        if (mappingItems == null
                || mappingItems.isEmpty()) {
            return -1;
        }
        LogicQueueMappingItem item =  mappingItems.get(mappingItems.size() - 1);
        return item.computeMaxStaticQueueOffset();
    }


    public static TopicQueueMappingInfo cloneAsMappingInfo(TopicQueueMappingDetail mappingDetail) {
        TopicQueueMappingInfo topicQueueMappingInfo = new TopicQueueMappingInfo(mappingDetail.topic, mappingDetail.totalQueues, mappingDetail.bname, mappingDetail.epoch);
        topicQueueMappingInfo.currIdMap = TopicQueueMappingDetail.buildIdMap(mappingDetail, LEVEL_0);
        return topicQueueMappingInfo;
    }

    public static boolean checkIfAsPhysical(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        List<LogicQueueMappingItem> mappingItems = getMappingInfo(mappingDetail, globalId);
        return mappingItems == null
                || mappingItems.size() == 1
                &&  mappingItems.get(0).getLogicOffset() == 0;
    }

    public ConcurrentMap<Integer, List<LogicQueueMappingItem>> getHostedQueues() {
        return hostedQueues;
    }

    public void setHostedQueues(ConcurrentMap<Integer, List<LogicQueueMappingItem>> hostedQueues) {
        this.hostedQueues = hostedQueues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof TopicQueueMappingDetail)) return false;

        TopicQueueMappingDetail that = (TopicQueueMappingDetail) o;

        return new EqualsBuilder()
                .append(hostedQueues, that.hostedQueues)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(hostedQueues)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TopicQueueMappingDetail{" +
                "hostedQueues=" + hostedQueues +
                ", topic='" + topic + '\'' +
                ", totalQueues=" + totalQueues +
                ", bname='" + bname + '\'' +
                ", epoch=" + epoch +
                ", dirty=" + dirty +
                ", currIdMap=" + currIdMap +
                '}';
    }
}
