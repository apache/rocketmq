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

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopicRemappingDetailWrapper extends RemotingSerializable {
    public static final String TYPE_CREATE_OR_UPDATE = "CREATE_OR_UPDATE";
    public static final String TYPE_REMAPPING = "REMAPPING";

    public static final String SUFFIX_BEFORE = ".before";
    public static final String SUFFIX_AFTER = ".after";


    private String topic;
    private String type;
    private long epoch;

    private Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<String, TopicConfigAndQueueMapping>();

    private Set<String> brokerToMapIn = new HashSet<String>();

    private Set<String> brokerToMapOut = new HashSet<String>();

    public TopicRemappingDetailWrapper() {

    }

    public TopicRemappingDetailWrapper(String topic, String type, long epoch, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, Set<String> brokerToMapIn, Set<String> brokerToMapOut) {
        this.topic = topic;
        this.type = type;
        this.epoch = epoch;
        this.brokerConfigMap = brokerConfigMap;
        this.brokerToMapIn = brokerToMapIn;
        this.brokerToMapOut = brokerToMapOut;
    }

    public String getTopic() {
        return topic;
    }

    public String getType() {
        return type;
    }

    public long getEpoch() {
        return epoch;
    }

    public Map<String, TopicConfigAndQueueMapping> getBrokerConfigMap() {
        return brokerConfigMap;
    }

    public Set<String> getBrokerToMapIn() {
        return brokerToMapIn;
    }

    public Set<String> getBrokerToMapOut() {
        return brokerToMapOut;
    }

    public void setBrokerConfigMap(Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
        this.brokerConfigMap = brokerConfigMap;
    }

    public void setBrokerToMapIn(Set<String> brokerToMapIn) {
        this.brokerToMapIn = brokerToMapIn;
    }

    public void setBrokerToMapOut(Set<String> brokerToMapOut) {
        this.brokerToMapOut = brokerToMapOut;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }
}
