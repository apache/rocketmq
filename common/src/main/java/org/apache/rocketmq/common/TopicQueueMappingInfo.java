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

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicQueueMappingInfo extends RemotingSerializable {

    private String topic; // redundant field
    private int totalQueues;
    private String bname;  //identify the host name
    //the newest mapping is in current broker
    private Map<Integer/*global id*/, List<LogicQueueMappingItem>> hostedQueues = new HashMap<Integer, List<LogicQueueMappingItem>>();


    public TopicQueueMappingInfo(String topic, int totalQueues, String bname) {
        this.topic = topic;
        this.totalQueues = totalQueues;
        this.bname = bname;
    }

    public boolean putMappingInfo(Integer globalId, List<LogicQueueMappingItem> mappingInfo) {
        if (mappingInfo.isEmpty()) {
            return true;
        }
        hostedQueues.put(globalId, mappingInfo);
        return true;
    }

    public List<LogicQueueMappingItem> getMappingInfo(Integer globalId) {
        return hostedQueues.get(globalId);
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
