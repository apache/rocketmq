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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingInfo extends RemotingSerializable {
    public static final int LEVEL_0 = 0;
    public static final int LEVEL_1 = 1;

    String topic; // redundant field
    int totalQueues;
    String bname;  //identify the hosted broker name
    //register to broker to construct the route
    ConcurrentMap<Integer, Integer> currIdMap = new ConcurrentHashMap<Integer, Integer>();
    //register to broker to help detect remapping failure
    protected ConcurrentMap<Integer, Integer> prevIdMap = new ConcurrentHashMap<Integer, Integer>();

    public TopicQueueMappingInfo(String topic, int totalQueues, String bname) {
        this.topic = topic;
        this.totalQueues = totalQueues;
        this.bname = bname;
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

    public ConcurrentMap<Integer, Integer> getCurrIdMap() {
        return currIdMap;
    }

    public ConcurrentMap<Integer, Integer> getPrevIdMap() {
        return prevIdMap;
    }
}
