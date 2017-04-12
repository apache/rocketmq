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

/**
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TopicRouteData extends RemotingSerializable {
    private String orderTopicConf;
    private List<QueueData> queueDataList;
    private List<BrokerData> brokerDataList;
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public TopicRouteData cloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDataList(new ArrayList<QueueData>());
        topicRouteData.setBrokerDataList(new ArrayList<BrokerData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.queueDataList != null) {
            topicRouteData.getQueueDataList().addAll(this.queueDataList);
        }

        if (this.brokerDataList != null) {
            topicRouteData.getBrokerDataList().addAll(this.brokerDataList);
        }

        if (this.filterServerTable != null) {
            topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
        }

        return topicRouteData;
    }

    public List<QueueData> getQueueDataList() {
        return queueDataList;
    }

    public void setQueueDataList(List<QueueData> queueDataList) {
        this.queueDataList = queueDataList;
    }

    public List<BrokerData> getBrokerDataList() {
        return brokerDataList;
    }

    public void setBrokerDataList(List<BrokerData> brokerDataList) {
        this.brokerDataList = brokerDataList;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDataList == null) ? 0 : brokerDataList.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDataList == null) ? 0 : queueDataList.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDataList == null) {
            if (other.brokerDataList != null)
                return false;
        } else if (!brokerDataList.equals(other.brokerDataList))
            return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null)
                return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf))
            return false;
        if (queueDataList == null) {
            if (other.queueDataList != null)
                return false;
        } else if (!queueDataList.equals(other.queueDataList))
            return false;
        if (filterServerTable == null) {
            if (other.filterServerTable != null)
                return false;
        } else if (!filterServerTable.equals(other.filterServerTable))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDataList=" + queueDataList
            + ", brokerDataList=" + brokerDataList + ", filterServerTable=" + filterServerTable + "]";
    }
}
