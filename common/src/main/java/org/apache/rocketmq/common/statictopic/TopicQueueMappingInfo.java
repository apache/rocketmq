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
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicQueueMappingInfo extends RemotingSerializable {
    public static final int LEVEL_0 = 0;

    String topic; // redundant field
    int totalQueues;
    String bname;  //identify the hosted broker name
    long epoch; //important to fence the old dirty data
    boolean dirty; //indicate if the data is dirty
    //register to broker to construct the route
    protected ConcurrentMap<Integer/*logicId*/, Integer/*physicalId*/> currIdMap = new ConcurrentHashMap<Integer, Integer>();

    public TopicQueueMappingInfo() {

    }

    public TopicQueueMappingInfo(String topic, int totalQueues, String bname, long epoch) {
        this.topic = topic;
        this.totalQueues = totalQueues;
        this.bname = bname;
        this.epoch = epoch;
        this.dirty = false;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public int getTotalQueues() {
        return totalQueues;
    }


    public String getBname() {
        return bname;
    }

    public String getTopic() {
        return topic;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public void setTotalQueues(int totalQueues) {
        this.totalQueues = totalQueues;
    }

    public ConcurrentMap<Integer, Integer> getCurrIdMap() {
        return currIdMap;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }

    public void setCurrIdMap(ConcurrentMap<Integer, Integer> currIdMap) {
        this.currIdMap = currIdMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof TopicQueueMappingInfo)) return false;

        TopicQueueMappingInfo info = (TopicQueueMappingInfo) o;

        return new EqualsBuilder()
                .append(totalQueues, info.totalQueues)
                .append(epoch, info.epoch)
                .append(dirty, info.dirty)
                .append(topic, info.topic)
                .append(bname, info.bname)
                .append(currIdMap, info.currIdMap)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(topic)
                .append(totalQueues)
                .append(bname)
                .append(epoch)
                .append(dirty)
                .append(currIdMap)
                .toHashCode();
    }
}
