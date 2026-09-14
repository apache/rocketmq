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

package org.apache.rocketmq.remoting.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Set;
import java.util.Map;

public class GetBrokerLiteInfoResponseBody extends RemotingSerializable {

    private String storeType;
    private int maxLmqNum;
    private int currentLmqNum;
    private int liteSubscriptionCount;
    private int orderInfoCount;
    private int cqTableSize;
    private int offsetTableSize;
    private int eventMapSize;
    private Map<String, Integer> topicMeta;
    private Map<String, Set<String>> groupMeta;

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public int getMaxLmqNum() {
        return maxLmqNum;
    }

    public void setMaxLmqNum(int maxLmqNum) {
        this.maxLmqNum = maxLmqNum;
    }

    public int getCurrentLmqNum() {
        return currentLmqNum;
    }

    public void setCurrentLmqNum(int currentLmqNum) {
        this.currentLmqNum = currentLmqNum;
    }

    public int getLiteSubscriptionCount() {
        return liteSubscriptionCount;
    }

    public void setLiteSubscriptionCount(int liteSubscriptionCount) {
        this.liteSubscriptionCount = liteSubscriptionCount;
    }

    public int getOrderInfoCount() {
        return orderInfoCount;
    }

    public void setOrderInfoCount(int orderInfoCount) {
        this.orderInfoCount = orderInfoCount;
    }

    public int getCqTableSize() {
        return cqTableSize;
    }

    public void setCqTableSize(int cqTableSize) {
        this.cqTableSize = cqTableSize;
    }

    public int getOffsetTableSize() {
        return offsetTableSize;
    }

    public void setOffsetTableSize(int offsetTableSize) {
        this.offsetTableSize = offsetTableSize;
    }

    public int getEventMapSize() {
        return eventMapSize;
    }

    public void setEventMapSize(int eventMapSize) {
        this.eventMapSize = eventMapSize;
    }

    public Map<String, Integer> getTopicMeta() {
        return topicMeta;
    }

    public void setTopicMeta(Map<String, Integer> topicMeta) {
        this.topicMeta = topicMeta;
    }

    public Map<String, Set<String>> getGroupMeta() {
        return groupMeta;
    }

    public void setGroupMeta(Map<String, Set<String>> groupMeta) {
        this.groupMeta = groupMeta;
    }
}
