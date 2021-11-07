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
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingInfo extends RemotingSerializable {
    public static final int LEVEL_0 = 0;
    public static final int LEVEL_1 = 1;

    private String topic; // redundant field
    private int totalQueues;
    private String bname;  //identify the hosted broker name
    // the mapping info in current broker, do not register to nameserver
    private ConcurrentMap<Integer/*global id*/, ImmutableList<LogicQueueMappingItem>> hostedQueues = new ConcurrentHashMap<Integer, ImmutableList<LogicQueueMappingItem>>();
    //register to broker to construct the route
    private ConcurrentMap<Integer, Integer> currIdMap = new ConcurrentHashMap<Integer, Integer>();
    //register to broker to help detect remapping failure
    private ConcurrentMap<Integer, Integer> prevIdMap = new ConcurrentHashMap<Integer, Integer>();

    public TopicQueueMappingInfo(String topic, int totalQueues, String bname) {
        this.topic = topic;
        this.totalQueues = totalQueues;
        this.bname = bname;
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
                    tmpIdMap.put(curr.getQueueId(), globalId);
                }
            } else if (level == LEVEL_1
                    && items.size() >= 2) {
                LogicQueueMappingItem prev = items.get(items.size() - 1);
                if (bname.equals(prev.getBname())) {
                    tmpIdMap.put(prev.getQueueId(), globalId);
                }
            }
        }
        return tmpIdMap;
    }

    public List<LogicQueueMappingItem> getMappingInfo(Integer globalId) {
        return hostedQueues.get(globalId);
    }


    public TopicQueueMappingInfo  clone4register() {
        TopicQueueMappingInfo topicQueueMappingInfo = new TopicQueueMappingInfo(this.topic, this.totalQueues, this.bname);
        topicQueueMappingInfo.currIdMap = this.buildIdMap(LEVEL_0);
        topicQueueMappingInfo.prevIdMap = this.buildIdMap(LEVEL_1);

        return topicQueueMappingInfo;
    }

    public int getTotalQueues() {
        return totalQueues;
    }

    public void setTotalQueues(int totalQueues) {
        this.totalQueues = totalQueues;
    }

    public String getBname() {
        return bname;
    }

    public String getTopic() {
        return topic;
    }
}
